-- RAW zone: external table straight over the gz JSON from GH Archive (PLAN.md §4.3).
-- The JSON SerDe maps only the fields declared here; everything else in the event is ignored.
CREATE DATABASE IF NOT EXISTS gh LOCATION '/gh/curated/warehouse/gh.db';

CREATE EXTERNAL TABLE IF NOT EXISTS gh.events_raw (
  id         STRING,
  type       STRING,
  actor      STRUCT<id:BIGINT, login:STRING>,
  repo       STRUCT<id:BIGINT, name:STRING>,
  org        STRUCT<id:BIGINT, login:STRING>,
  payload    STRUCT<
               action:STRING, ref:STRING, ref_type:STRING, size:INT, distinct_size:INT,
               pull_request:STRUCT<number:INT, merged:BOOLEAN, created_at:STRING, closed_at:STRING, merged_at:STRING,
                                   additions:INT, deletions:INT, changed_files:INT, `user`:STRUCT<login:STRING>>,
               issue:STRUCT<number:INT, state:STRING, created_at:STRING, closed_at:STRING, comments:INT,
                            `user`:STRUCT<login:STRING>>,
               review:STRUCT<state:STRING>,
               release:STRUCT<tag_name:STRING>,
               forkee:STRUCT<id:BIGINT, full_name:STRING>>,
  public     BOOLEAN,
  created_at STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/gh/raw/events';

MSCK REPAIR TABLE gh.events_raw;

-- Data-quality log: one row per (stage, file/partition) with record counts (PLAN.md §13)
CREATE EXTERNAL TABLE IF NOT EXISTS gh.dq_stage_counts (
  dt STRING, unit STRING, stage_name STRING, records BIGINT, bytes BIGINT, ts STRING, status STRING)
PARTITIONED BY (stage STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/gh/dq/stage_counts';

MSCK REPAIR TABLE gh.dq_stage_counts;
