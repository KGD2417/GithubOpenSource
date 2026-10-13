#!/usr/bin/env bash
# Bulk-load the serving TSVs (spark/hbase_prep.py) into HBase.   pulse hbase-load
# ImportTsv is a MapReduce job that writes HFiles directly (-Dimporttsv.bulk.output) instead of doing
# millions of Puts through the RegionServer; completebulkload then moves the HFiles into the regions.
# On YARN when a ResourceManager is up; otherwise MapReduce's LocalJobRunner inside this container
# (the 8 GB dev Mac can't run YARN and HBase at the same time).
set -euo pipefail
T=/gh/serving/tsv H=/gh/serving/hfiles
if nc -z resourcemanager 8032 2>/dev/null; then MR=yarn; else MR=local; fi
echo "ImportTsv MapReduce mode: $MR"

load() {   # table, columns
  local t=$1 cols=$2
  hdfs dfs -rm -r -f -skipTrash "$H/$t" >/dev/null
  hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.framework.name=$MR \
    -Dimporttsv.separator=$'\t' -Dimporttsv.columns="$cols" -Dimporttsv.bulk.output="$H/$t" "$t" "$T/$t" 2>&1 \
    | { grep -E "Bad Lines|Map input records|ERROR|Exception" || true; }
  hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool "$H/$t" "$t" 2>&1 | { grep -E "ERROR|Exception" || true; }
  echo "loaded $t"
}

load repo_hourly  "HBASE_ROW_KEY,c:watch,c:fork,c:push,c:pr,c:issues,c:comment,c:create,c:release,c:other,c:bot,c:total"
load repo_similar "HBASE_ROW_KEY,m:name,$(seq -f 's:%02g' 1 20 | paste -sd, -)"
load user_recs    "HBASE_ROW_KEY,$(seq -f 'r:%02g' 1 10 | paste -sd, -)"
load repo_index   "HBASE_ROW_KEY,i:id"
