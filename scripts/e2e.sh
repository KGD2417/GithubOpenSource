#!/usr/bin/env bash
# End-to-end run on the sample dataset (PLAN.md §13): both Airflow DAGs, a short streaming replay,
# then assertions on what came out at the far end of each path. Exit code 0 = everything passed.
#   scripts/e2e.sh          (LITE=1 on an 8 GB machine)
set -euo pipefail
cd "$(dirname "$0")/.."
LOG=data/logs; mkdir -p $LOG
fail() { echo "E2E FAIL: $*" >&2; exit 1; }
FROM=${E2E_FROM:-1}          # E2E_FROM=3 resumes at the checks without re-running both DAGs
af() { docker compose exec -T airflow "$@"; }

if [ "$FROM" -le 2 ]; then
echo "== 1. batch DAG (sample hours of 2025-09-01)"
scripts/stage.sh orchestrate >/dev/null
af airflow dags test gh_pulse_daily 2025-09-01 --conf '{"hours": "12-14"}' > $LOG/e2e_daily.log 2>&1 || true
grep -q "return code [1-9]" $LOG/e2e_daily.log && fail "daily DAG had a failing task ($LOG/e2e_daily.log)"

echo "== 2. models DAG (sample-sized parameters)"
af airflow dags test gh_pulse_models 2025-09-01 --conf \
  '{"top_n": 40, "feature_hours": 1, "label_hours": 1, "min_user_stars": 2, "min_repo_stars": 3}' \
  > $LOG/e2e_models.log 2>&1 || true
grep -q "return code [1-9]" $LOG/e2e_models.log && fail "models DAG had a failing task ($LOG/e2e_models.log)"
fi

echo "== 3. checks after batch"
scripts/stage.sh serving >/dev/null          # core (Postgres) + HBase, so the checks also work when resuming
n=$(docker compose exec -T postgres psql -U catalogue -d catalogue -Atc "select count(*) from trending_daily")
[ "$n" -gt 0 ] || fail "no trending rows exported to Postgres"
echo "   Postgres trending_daily rows: $n"
# pick a repo that this run actually produced, then look it up by NAME (repo_index -> salted key)
repo=$(echo "scan 'repo_similar', {LIMIT => 1, COLUMNS => ['m:name']}" | docker compose exec -T edge hbase shell -n 2>/dev/null \
       | awk -F'value=' '/column=m:name/ && !r {r = $2} END {print r}')   # no early exit: would SIGPIPE + pipefail
[ -n "$repo" ] || fail "repo_similar is empty"
# capture first: `cmd | grep -q` lets grep exit early, SIGPIPEs cmd, and pipefail then reports failure
out=$(scripts/hlookup.sh similar "$repo" 2>/dev/null)
grep -q "s:01" <<<"$out" || fail "HBase similar-repos lookup for $repo empty"
echo "   HBase similar-repos lookup ($repo): ok"

echo "== 4. streaming: 2 GitHub-minutes of replay through Kafka -> Storm -> HBase"
scripts/stage.sh streaming >/dev/null
docker compose exec -T edge pulse kafka-topics >/dev/null
scripts/topology.sh >/dev/null
# total PushEvents counted so far: Storm's Increments raise it on every run, even when the minute rows exist
pushes() { echo "scan 'live_minute', {COLUMNS => ['t:PushEvent:toLong']}" | docker compose exec -T edge hbase shell -n 2>/dev/null \
           | awk -F'value=' '/column=t:PushEvent/ {s += $2} END {print s + 0}'; }
before=$(pushes)
docker compose exec -T edge pulse replay --date 2025-09-01 --hours 13-13 --speed 600 --limit 30000 >/dev/null
sleep 45                                                   # windows close once the watermark passes them
after=$(pushes)
echo "   PushEvents counted in HBase live_minute: $before -> $after"
[ "$after" -gt "$before" ] || fail "Storm wrote no new live counters"
docker compose exec -T storm-nimbus storm kill pulse -w 1 >/dev/null 2>&1 || true

echo "E2E PASSED"
