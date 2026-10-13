-- CURATED zone (PLAN.md §4.3): managed, non-ACID Parquet tables that Hive, Impala, Spark and Tableau share.
-- Daily tables are partitioned by dt and rebuilt per day (hive/curated/daily.sql);
-- window tables cover every loaded day and are rebuilt whole (hive/curated/window.sql).

-- Per repo per event type per day. Bucketed on repo_id: joins to the catalogue / other repo tables can
-- use bucket map joins, and TABLESAMPLE(BUCKET x OUT OF 8) gives cheap repo samples.
CREATE TABLE IF NOT EXISTS gh.repo_type_daily (
  repo_id BIGINT, repo_name STRING, event_type STRING, events BIGINT, human_events BIGINT, distinct_actors BIGINT)
PARTITIONED BY (dt STRING) CLUSTERED BY (repo_id) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- One row per repo per day: the activity fact table behind most views
CREATE TABLE IF NOT EXISTS gh.repo_daily_activity (
  repo_id BIGINT, repo_name STRING, stars BIGINT, forks BIGINT, pushes BIGINT, prs_opened BIGINT,
  prs_merged BIGINT, issues_opened BIGINT, issues_closed BIGINT, comments BIGINT, reviews BIGINT,
  releases BIGINT, distinct_actors BIGINT, bot_events BIGINT, human_events BIGINT, total_events BIGINT)
PARTITIONED BY (dt STRING) CLUSTERED BY (repo_id) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Human stars (WatchEvent): the user x repo matrix for Mahout / ALS. Bucketed by user for per-user splits.
CREATE TABLE IF NOT EXISTS gh.stars (actor_id BIGINT, actor_login STRING, repo_id BIGINT, starred_at TIMESTAMP)
PARTITIONED BY (dt STRING) CLUSTERED BY (actor_id) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.hourly_activity (
  hour_utc INT, weekday_utc INT, hour_ist INT, weekday_ist INT, date_ist STRING,
  event_type STRING, is_bot BOOLEAN, events BIGINT)          -- weekday: 1 = Monday ... 7 = Sunday
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- MapReduce top-N + names + day-over-day change (also exported to Postgres by Sqoop)
CREATE TABLE IF NOT EXISTS gh.trending_daily (
  event_type STRING, rnk INT, repo_id BIGINT, repo_name STRING, events BIGINT,
  prev_day_events BIGINT, growth_pct DOUBLE)
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Every closed PR / issue with its lifetime, straight from the payload timestamps
CREATE TABLE IF NOT EXISTS gh.pr_lifecycle (
  repo_id BIGINT, repo_name STRING, pr_number INT, pr_author STRING, author_is_bot BOOLEAN, merged BOOLEAN,
  created_at TIMESTAMP, closed_at TIMESTAMP, hours_open DOUBLE, additions INT, deletions INT, changed_files INT)
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.issue_lifecycle (
  repo_id BIGINT, repo_name STRING, issue_number INT, issue_author STRING, created_at TIMESTAMP,
  closed_at TIMESTAMP, hours_open DOUBLE, comments INT)
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Window tables
CREATE TABLE IF NOT EXISTS gh.trending_window (
  event_type STRING, rnk INT, repo_id BIGINT, repo_name STRING, events BIGINT, days_active INT,
  first_day_events BIGINT, last_day_events BIGINT, growth_pct DOUBLE)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.repo_health (
  repo_id BIGINT, repo_name STRING, prs_closed BIGINT, prs_merged BIGINT, merge_rate DOUBLE,
  median_hours_to_merge DOUBLE, issues_opened BIGINT, issues_closed BIGINT, issue_close_ratio DOUBLE,
  median_hours_to_close_issue DOUBLE, comments BIGINT, reviews BIGINT, responses_per_item DOUBLE,
  bot_pr_share DOUBLE)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.language_trends (
  language STRING, events BIGINT, stars BIGINT, forks BIGINT, active_repos BIGINT)
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.topic_trends (
  topic STRING, events BIGINT, stars BIGINT, active_repos BIGINT)
PARTITIONED BY (dt STRING) STORED AS PARQUET TBLPROPERTIES ('transactional'='false');
