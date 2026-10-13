-- CLEAN zone: one row per event, written by Pig (pig/etl_events.pig) through HCatStorer.
-- Column order = ParseGhEvent output (minus event_type, which is a partition) + Pig's bot tagging.
CREATE EXTERNAL TABLE IF NOT EXISTS gh.events_clean (
  event_id BIGINT, created_at TIMESTAMP, created_ts BIGINT, actor_id BIGINT, actor_login STRING,
  repo_id BIGINT, repo_name STRING, repo_owner STRING, org_login STRING, action STRING, ref_type STRING,
  push_size INT, push_distinct_size INT,
  pr_number INT, pr_merged BOOLEAN, pr_created_at TIMESTAMP, pr_closed_at TIMESTAMP, pr_merged_at TIMESTAMP,
  pr_additions INT, pr_deletions INT, pr_changed_files INT, pr_author STRING,
  issue_number INT, issue_created_at TIMESTAMP, issue_closed_at TIMESTAMP, issue_comments INT, issue_author STRING,
  review_state STRING, release_tag STRING, forkee_repo_id BIGINT, payload_version STRING,
  is_bot BOOLEAN, bot_reason STRING)
PARTITIONED BY (dt STRING, event_type STRING)
STORED AS PARQUET
LOCATION '/gh/clean/events'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'external.table.purge'='true');
