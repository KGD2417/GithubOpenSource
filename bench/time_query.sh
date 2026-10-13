#!/usr/bin/env bash
# time_query.sh <hive|impala> <file.sql> [reps] [csv]  -> one CSV row per repetition (runs inside edge)
# Same SQL file, same tables, same beeline client: only the engine behind the JDBC URL changes.
set -euo pipefail
engine=$1 sql=$2 reps=${3:-3} csv=${4:-/opt/pulse/bench/results/analytics_timings.csv}
case "$engine" in
  hive)   url="jdbc:hive2://hiveserver2:10000" ;;
  impala) url="jdbc:hive2://impalad:21050/;auth=noSasl" ;;
  *) echo "engine must be hive|impala" >&2; exit 1 ;;
esac
[ -f "$csv" ] || echo "ts,engine,query,rep,seconds,rows" > "$csv"
q=$(basename "$sql" .sql)
for rep in $(seq 1 "$reps"); do
  start=$EPOCHREALTIME
  rows=$(beeline -u "$url" -n root --silent=true --showHeader=false --outputformat=tsv2 -f "$sql" 2>/dev/null | grep -c . || true)
  secs=$(awk -v a="$start" -v b="$EPOCHREALTIME" 'BEGIN { print b - a }')
  printf '%s,%s,%s,%s,%.2f,%s\n' "$(date -u +%FT%TZ)" "$engine" "$q" "$rep" "$secs" "$rows" | tee -a "$csv"
done
