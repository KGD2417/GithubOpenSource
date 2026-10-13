#!/usr/bin/env bash
# Engine benchmark (PLAN.md §10): the same 3 queries in MapReduce, Pig, Hive, Spark and Impala.
#   bench/run_bench.sh <size-label> [reps=3] [engines="mr pig hive spark impala"]      (inside edge)
# Runs whichever engines the current stage has up (YARN for mr/pig/spark, HiveServer2 for hive,
# impalad for impala), one untimed warm-up, then <reps> timed runs. Appends to bench/results/timings.csv.
# Worker count = live NodeManagers (stop nodemanager2 for the 1-worker runs).
set -uo pipefail
cd /opt/pulse
SIZE=${1:?size label: sample|demo|large} REPS=${2:-3} ENGINES=${3:-"mr pig hive spark impala"}
CSV=bench/results/timings.csv JAR=java/hadoop-jobs/target/pulse-hadoop.jar IN=/gh/clean/events
[ -f "$CSV" ] || echo "run_id,engine,query,size,input_gb,workers,rep,seconds,rows_out,yarn_app_id,ts" > "$CSV"
run_id=$(date -u +%Y%m%dT%H%M%S)
input_gb=$(hdfs dfs -du -s $IN | awk '{printf "%.3f", $1 / 1073741824}')
workers=$( (yarn node -list 2>/dev/null || true) | awk -F: '/Total Nodes/ {gsub(/ /, "", $2); print $2}')
up() { nc -z "$1" "$2" 2>/dev/null; }

query() {   # engine q out -> runs it, prints output rows
  local e=$1 q=$2 out=/tmp/bench/$1_$2
  case $e in
    mr)
      hdfs dfs -rm -r -f -skipTrash "$out" >/dev/null 2>&1
      case $q in
        q1) hadoop jar $JAR pulse.mr.BenchJobs q1 $IN "$out" ;;
        q2) hadoop jar $JAR pulse.mr.TopReposByTypeDay -D pulse.topN=10 -D pulse.excludeBots=true \
              -D pulse.eventTypes=WatchEvent $IN "$out" ;;
        q3) hadoop jar $JAR pulse.mr.BenchJobs q3 $IN "$out" /gh/raw/catalogue/repos ;;
      esac >/tmp/bench.log 2>&1 && hdfs dfs -cat "$out/part-*" | wc -l ;;
    pig)
      hdfs dfs -rm -r -f -skipTrash "$out" >/dev/null 2>&1
      pig -useHCatalog -param out="$out" pig/bench/$q.pig >/tmp/bench.log 2>&1 && hdfs dfs -cat "$out/part-*" | wc -l ;;
    hive)
      beeline -u jdbc:hive2://hiveserver2:10000 -n root --silent=true --showHeader=false --outputformat=tsv2 \
        -f hive/bench/$q.sql 2>/tmp/bench.log | grep -c . ;;
    impala)
      beeline -u "jdbc:hive2://impalad:21050/;auth=noSasl" -n root --silent=true --showHeader=false \
        --outputformat=tsv2 -f hive/bench/$q.sql 2>/tmp/bench.log | grep -c . ;;
    spark)
      spark-sql -f hive/bench/$q.sql 2>/tmp/bench.log | grep -c . ;;
  esac
}

for e in $ENGINES; do
  case $e in
    mr|pig|spark) up resourcemanager 8032 || { echo "skip $e (YARN not running)"; continue; } ;;
    hive) up hiveserver2 10000 || { echo "skip hive (HiveServer2 not running)"; continue; } ;;
    impala) up impalad 21050 || { echo "skip impala (impalad not running)"; continue; } ;;
  esac
  [ "$e" = impala ] && w=NA || w=$workers
  for q in q1 q2 q3; do
    query "$e" "$q" >/dev/null                                    # warm-up (JIT, HDFS cache, metadata)
    for rep in $(seq 1 "$REPS"); do
      start=$EPOCHREALTIME
      rows=$(query "$e" "$q"); rc=$?
      secs=$(awk -v a="$start" -v b="$EPOCHREALTIME" 'BEGIN { printf "%.2f", b - a }')
      app=$(grep -oE 'application_[0-9]+_[0-9]+' /tmp/bench.log | tail -1)
      [ $rc -eq 0 ] || { echo "FAILED $e $q (see /tmp/bench.log)"; rows=-1; }
      echo "$run_id,$e,$q,$SIZE,$input_gb,$w,$rep,$secs,${rows:-0},${app:-},$(date -u +%FT%TZ)" | tee -a "$CSV"
    done
  done
done
