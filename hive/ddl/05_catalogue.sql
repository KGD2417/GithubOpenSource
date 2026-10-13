-- Catalogue imported by Sqoop (incremental, merged on repo_id) into /gh/raw/catalogue/repos.
-- Column order = Postgres table order (sql/catalogue.sql). Timestamps stay strings (UTC text from Sqoop).
CREATE EXTERNAL TABLE IF NOT EXISTS gh.catalogue_repos (
  repo_id BIGINT, full_name STRING, owner_login STRING, owner_type STRING, primary_language STRING,
  topics STRING, license_spdx STRING, created_at STRING, stargazers_count INT, forks_count INT,
  open_issues_count INT, is_fork BOOLEAN, archived BOOLEAN, api_status INT, updated_at STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gh/raw/catalogue/repos';
