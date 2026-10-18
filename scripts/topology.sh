#!/usr/bin/env bash
# (Re)start the Storm topology cleanly:  scripts/topology.sh [topic=gh.events]
# Kill any running copy, move the consumer group to the END of the topic, submit, wait for the spout.
# Why reset: windows use event time. Leftover events from an earlier replay would be read first, push the
# watermark forward, and a new replay of the same (or an earlier) hour would then be dropped as "late".
set -euo pipefail
export MSYS_NO_PATHCONV=1      # Git Bash on Windows: keep container paths (/opt/...) as-is
cd "$(dirname "$0")/.."
topic=${1:-gh.events}
name=$([ "$topic" = gh.events ] && echo pulse || echo pulse-live)
group="pulse-storm-$topic"

docker compose exec -T storm-nimbus storm kill "$name" -w 1 >/dev/null 2>&1 || true
sleep 8                        # let the killed worker stop committing offsets before the reset
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group "$group" \
  --reset-offsets --to-latest --topic "$topic" --execute >/dev/null 2>&1 || true
docker compose exec -T storm-nimbus storm jar /opt/pulse-jars/pulse-storm.jar pulse.storm.PulseTopology "$name" "$topic" 2>&1 \
  | grep -E "Finished submitting" || { echo "submit failed" >&2; exit 1; }

# ready = the Kafka spout has its partitions. storm-kafka-client assigns partitions manually (it never
# joins the group as a member), so the signal is its own worker-log line.
for _ in $(seq 1 60); do
  tid=$(curl -s http://localhost:8081/api/v1/topology/summary \
        | python3 -c "import sys,json; print(next((t['id'] for t in json.load(sys.stdin)['topologies'] if t['name']=='$name'), ''))" || true)
  if [ -n "$tid" ] && docker compose exec -T storm-supervisor sh -c \
       "grep -qs 'Partitions assignments has changed' /apache-storm/logs/workers-artifacts/$tid/*/worker.log"; then
    echo "topology $name ($tid) consuming $topic"; exit 0
  fi
  sleep 5
done
echo "topology submitted but the Kafka spout has no partitions yet" >&2; exit 1
