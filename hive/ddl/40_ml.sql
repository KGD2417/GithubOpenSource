-- Tables written by Spark (spark/*.py) and Mahout (mahout/*.sh); same non-ACID Parquet convention.

-- Q6: who drives each repo? (Spark features.py)
CREATE TABLE IF NOT EXISTS gh.repo_profile (
  repo_id BIGINT, repo_name STRING, stars BIGINT, forks BIGINT, fork_star_ratio DOUBLE,
  contributors BIGINT, contribution_events BIGINT, top1_share DOUBLE, gini DOUBLE,
  active_days INT, bot_share DOUBLE, profile STRING)                       -- solo | small-team | community
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Q8: one row per new repository: first-window features + label (Spark features.py)
CREATE TABLE IF NOT EXISTS gh.repo_features (
  repo_id BIGINT, repo_name STRING, created_at TIMESTAMP, is_org INT, has_description INT,
  name_len INT, create_hour INT, create_weekday INT,
  f_stars BIGINT, f_forks BIGINT, f_stargazers BIGINT, f_star_velocity_last BIGINT, f_max_stars_hour BIGINT,
  f_pushes BIGINT, f_prs BIGINT, f_issues BIGINT, f_releases BIGINT, f_actors BIGINT, f_bot_share DOUBLE,
  label_stars BIGINT, label INT)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.viral_predictions (
  repo_id BIGINT, repo_name STRING, created_at TIMESTAMP, split STRING, model STRING,
  probability DOUBLE, label INT, label_stars BIGINT)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.model_metrics (
  run_ts STRING, task STRING, model STRING, metric STRING, value DOUBLE, notes STRING)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Q7: Mahout item-based CF output, with names (spark/eval_recs.py loads these)
CREATE TABLE IF NOT EXISTS gh.similar_repos (
  repo_id BIGINT, repo_name STRING, rnk INT, similar_repo_id BIGINT, similar_repo_name STRING, similarity DOUBLE)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS gh.user_recs (
  actor_id BIGINT, actor_login STRING, rnk INT, repo_id BIGINT, repo_name STRING, score DOUBLE, source STRING)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');

-- Q10 (bonus): Mahout k-means clusters of repos by activity mix
CREATE TABLE IF NOT EXISTS gh.repo_clusters (
  repo_name STRING, cluster_id INT, cluster_label STRING, distance DOUBLE, total_events BIGINT,
  star_share DOUBLE, push_share DOUBLE, pr_share DOUBLE, issue_share DOUBLE, comment_share DOUBLE, bot_share DOUBLE)
STORED AS PARQUET TBLPROPERTIES ('transactional'='false');
