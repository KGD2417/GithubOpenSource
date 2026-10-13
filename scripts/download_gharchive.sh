#!/usr/bin/env bash
# Download GH Archive hours straight into the HDFS raw zone (runs inside the edge container).
#
#   download_gharchive.sh --mode sample|demo|large
#   download_gharchive.sh --start 2025-09-01 --end 2025-09-02 [--hours 0-23] [--parallel 4]
#
# Layout: /gh/raw/events/dt=YYYY-MM-DD/YYYY-MM-DD-H.json.gz  (hour NOT zero-padded, as on gharchive.org)
# Per file it logs a raw record count to /gh/dq/stage_counts/stage=raw/ (PLAN.md §13).
# Re-running skips files already in HDFS, so an interrupted download just resumes.
set -euo pipefail

mode="" start="" end="" hours="0-23" parallel=4
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) mode=$2; shift 2 ;;
    --start) start=$2; shift 2 ;;
    --end) end=$2; shift 2 ;;
    --hours) hours=$2; shift 2 ;;
    --parallel) parallel=$2; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 1 ;;
  esac
done

# Dataset sizes from PLAN.md: Sep 2025 has full payloads (~2.1 GB/day).
case "$mode" in
  sample) start=2025-09-01 end=2025-09-01 hours=12-14 ;;   # ~0.27 GB
  demo)   start=2025-09-07 end=2025-09-08 ;;               # ~4.2 GB, Sun + Mon (weekend vs weekday)
  large)  start=2025-09-01 end=2025-09-08 ;;               # ~17 GB (7 days + 1 for viral labels)
  "")     [ -n "$start" ] && [ -n "$end" ] || { echo "need --mode or --start/--end" >&2; exit 1; } ;;
  *) echo "mode must be sample|demo|large" >&2; exit 1 ;;
esac
h0=${hours%-*} h1=${hours#*-}

fetch_one() {   # $1 = YYYY-MM-DD-H
  local key=$1 dt=${1%-*}
  local target=/gh/raw/events/dt=$dt/$key.json.gz
  if hdfs dfs -test -e "$target" 2>/dev/null; then echo "skip    $key"; return 0; fi
  local tmp; tmp=$(mktemp -d)
  local code
  code=$(curl -sS --retry 5 --retry-delay 3 -o "$tmp/f.gz" -w '%{http_code}' "https://data.gharchive.org/$key.json.gz" || true)
  if [ "$code" != 200 ]; then
    echo "MISSING $key (HTTP $code)"
    printf '%s\t%s\traw\t-1\t0\t%s\tmissing\n' "$dt" "$key" "$(date -u +%FT%TZ)" \
      | hdfs dfs -put -f - "/gh/dq/stage_counts/stage=raw/$key.tsv"
    rm -rf "$tmp"; return 0
  fi
  if ! gzip -t "$tmp/f.gz" 2>/dev/null; then echo "CORRUPT $key" >&2; rm -rf "$tmp"; return 1; fi
  local lines bytes
  # awk counts records, wc -l counts newlines: some GH Archive hours have no newline after the last record
  lines=$(zcat "$tmp/f.gz" | awk 'END { print NR }')
  bytes=$(stat -c %s "$tmp/f.gz")
  hdfs dfs -mkdir -p "/gh/raw/events/dt=$dt"
  hdfs dfs -put -f "$tmp/f.gz" "$target._COPYING_" && hdfs dfs -mv "$target._COPYING_" "$target"
  printf '%s\t%s\traw\t%s\t%s\t%s\tok\n' "$dt" "$key" "$lines" "$bytes" "$(date -u +%FT%TZ)" \
    | hdfs dfs -put -f - "/gh/dq/stage_counts/stage=raw/$key.tsv"
  echo "loaded  $key  $lines events  $((bytes / 1048576)) MB"
  rm -rf "$tmp"
}
export -f fetch_one

hdfs dfs -mkdir -p /gh/dq/stage_counts/stage=raw
d=$start
while :; do
  for h in $(seq "$h0" "$h1"); do echo "$d-$h"; done
  [ "$d" = "$end" ] && break
  d=$(date -u -d "$d + 1 day" +%F)
done | xargs -P "$parallel" -I{} bash -c 'fetch_one {}'

echo "done: $(hdfs dfs -du -s -h /gh/raw/events)"
