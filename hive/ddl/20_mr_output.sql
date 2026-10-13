-- Output of the MapReduce job TopReposByTypeDay (pulse mr-top <dt>), human events only.
CREATE EXTERNAL TABLE IF NOT EXISTS gh.top_repos_mr (event_type STRING, rnk INT, repo_id BIGINT, events BIGINT)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/gh/curated/mr/top_repos';
