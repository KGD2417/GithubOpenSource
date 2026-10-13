#!/usr/bin/env bash
# Usage: scripts/stage.sh <stage>
# Starts exactly the services a stage needs and stops every other running service,
# so the laptop never runs more than one stage's memory budget (see PLAN.md §3.2).
set -euo pipefail
cd "$(dirname "$0")/.."

CORE="postgres namenode datanode1 datanode2 resourcemanager nodemanager1 nodemanager2 historyserver"

# LITE=1 (8 GB machines): one NodeManager instead of two.
[ "${LITE:-0}" = 1 ] && CORE=${CORE/ nodemanager2/}

case "${1:-}" in
  core)  want="$CORE" ;;
  batch) want="$CORE edge" ;;
  warehouse) want="$CORE hive-metastore hiveserver2 edge" ;;
  ml) want="$CORE hive-metastore edge" ;;
  serving) want="$CORE zookeeper hbase-master hbase-regionserver edge" ;;
  # Streaming needs HDFS (for HBase) but not YARN or Postgres
  streaming) want="namenode datanode1 datanode2 zookeeper hbase-master hbase-regionserver kafka storm-nimbus storm-supervisor storm-ui edge" ;;
  # Impala needs HDFS + metastore only; YARN is stopped to free ~4 GB for the Impala daemons.
  impala) want="postgres namenode datanode1 datanode2 hive-metastore impala-hms impala-statestored impala-catalogd impalad edge" ;;
  lite)  want="postgres namenode datanode1 datanode2 resourcemanager nodemanager1 historyserver edge" ;;  # 8 GB machines
  orchestrate) want="$CORE airflow" ;;   # Airflow then switches stages per task
  down) want="" ;;
  *) echo "usage: $0 core|batch|warehouse|ml|serving|streaming|impala|orchestrate|lite|down" >&2; exit 1 ;;
esac

# The orchestrator switches stages itself, so it is never stopped by a stage change.
running=$(docker compose ps --services --status running | grep -vx airflow | sort)
stop=$(comm -23 <(echo "$running") <(printf '%s\n' $want | sort) | tr '\n' ' ')
if [ -n "${stop// }" ]; then docker compose stop $stop; fi
[ -z "$want" ] && exit 0

# Impala's CDP metastore shares Hive's metastore DB; make sure the few NOT NULL columns it needs exist.
if [ "$1" = impala ]; then
  docker compose up -d --wait postgres
  docker compose exec -T postgres psql -q -U hive -d metastore < docker/postgres/metastore_cdp_compat.sql
fi

# Naming services on the command line activates their profiles.
docker compose up -d --wait $want

# Idempotent HDFS layout (PLAN.md §4.1)
docker compose exec -T namenode bash -c '
  hdfs dfsadmin -safemode wait >/dev/null
  hdfs dfs -mkdir -p /tmp /app-logs /mr-history /gh/raw/events /gh/raw/catalogue /gh/clean /gh/curated /gh/ml /gh/serving /gh/dq
  hdfs dfs -chmod 1777 /tmp /app-logs /mr-history'
# HBase has no container health check: wait until the master reports a live RegionServer, so the
# next command (a load, a lookup, a topology) doesn't hit an HBase that is still starting.
if printf '%s\n' $want | grep -qx hbase-regionserver; then
  for _ in $(seq 1 60); do
    st=$(echo status | docker compose exec -T edge hbase shell -n 2>/dev/null || true)   # capture: `| grep -q` + pipefail misfires
    grep -qE '[1-9][0-9]* servers' <<<"$st" && break
    sleep 5
  done
fi
echo "stage '$1' is up"
