# Viva demo script (about 10 minutes)

**Before the viva:**
- The demo dataset has been run through the daily DAG, the ML/serving steps and `pulse export-extracts`.
- Tableau is open on the extracts.
- `bench/charts/` has been generated.

Every step also has a screenshot in `docs/screenshots/` as a backup.

## 0:00 HDFS and YARN (stage `warehouse` already up)
```bash
make status
open http://localhost:9870 http://localhost:8088          # NameNode: 2 live DataNodes · YARN: NodeManagers
docker compose exec edge hdfs fsck /gh/raw/events/dt=2025-09-07/2025-09-07-12.json.gz -files -blocks -locations
```
Expected: each hourly file (about 100 MB) is 2 blocks of 64 MB + the rest, each `Live_repl=2` on different DataNodes.

## 1:30 MapReduce, killing a DataNode mid-job
```bash
make pulse ARGS="mr-top 2025-09-07" &
sleep 20; docker stop bda-datanode2                       # kill a DataNode while maps are reading
wait                                                       # job still SUCCEEDS: reads fail over to the other replica
open http://localhost:19888                                # JobHistory: counters Pulse.RECORDS_IN, BOT_SKIPPED, combine in/out
docker start bda-datanode2                                 # NameNode UI: dead (after about 90 s) -> live; under-replicated blocks heal
```
Things to explain:
- The key is `(dt, type, repo)`.
- The combiner sums map-side.
- `TypeDayPartitioner` hashes only `(dt, type)`, so one reducer owns a whole group and computes an exact top-N in one job.

## 3:30 Hive, then the same query on Impala
```bash
docker compose exec edge bench/time_query.sh hive hive/analytics/q4_bots.sql 1      # about 40 s (Hive on MapReduce)
make stage STAGE=impala                                                               # YARN stops, Impala starts (about 40 s)
docker compose exec edge bench/time_query.sh impala hive/analytics/q4_bots.sql 1    # about 1 s, same rows
make pulse ARGS="bench-chart"; open bench/charts/hive_vs_impala.png
```
Point out: same Parquet files and same table definitions in one Postgres metastore DB. Impala reaches it through its own CDP metastore process (PLAN.md, *As built*).

## 5:30 Streaming: Kafka → Storm → HBase
```bash
make stage STAGE=streaming
make topology
make replay DT=2025-09-07 H=12-12 SPEED=60 &              # one GitHub hour per real minute
open http://localhost:8081                                 # Storm UI: spout -> parse -> windows -> sink, 0 failed
scripts/hlookup.sh live 3                                  # events per minute by type + bot/human, from HBase
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic gh.alerts --max-messages 3
```
Things to explain:
- The windows use **event time**, so a 60× replay still gives per-GitHub-minute counts.
- The fields grouping on `repo_id` lets the counting tasks own disjoint repos (partial top-K, then a global merge).
- Spikes are EWMA + 4σ.

## 7:30 HBase serving (salted keys)
```bash
scripts/hlookup.sh similar JetBrains/koog                  # Mahout item-CF neighbours: one Get
scripts/hlookup.sh hourly dockur/windows 20250907          # hourly series: one prefix Scan
scripts/hlookup.sh recs <a login from user_recs>           # "repos you may like"
```
Row key = `md5(id)[0:2] | id | yyyyMMddHH`. The salt spreads sequential ids over 16 pre-split regions and is recomputed on read.

## 8:30 Tableau
Walk through the views: Trending now → Activity clock (UTC/IST toggle) → Bots vs humans → Recommendations lookup → Viral watchlist.

## 9:30 Wrap-up
- `bench/charts/runtime_by_engine_*.png`: the same 3 queries on MapReduce, Pig, Hive, Spark and Impala.
- `bench/charts/dq_funnel.png`: record counts raw → clean → curated, enforced by `pulse dq`.
