-- Bench Q2 in Pig: top 10 repos by human stars per day
ev    = LOAD 'gh.events_clean' USING org.apache.hive.hcatalog.pig.HCatLoader();
stars = FILTER ev BY event_type == 'WatchEvent' AND NOT is_bot;
byrep = FOREACH (GROUP stars BY (dt, repo_id)) GENERATE FLATTEN(group) AS (dt, repo_id), COUNT_STAR(stars) AS n;
byday = GROUP byrep BY dt;
top   = FOREACH byday { s = ORDER byrep BY n DESC, repo_id; t = LIMIT s 10; GENERATE FLATTEN(t); };
STORE top INTO '$out' USING PigStorage('\t');
