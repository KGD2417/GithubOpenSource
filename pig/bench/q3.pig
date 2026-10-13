-- Bench Q3 in Pig: events and stars per day by language; replicated (map-side) join with the catalogue
ev   = LOAD 'gh.events_clean' USING org.apache.hive.hcatalog.pig.HCatLoader();
evp  = FOREACH ev GENERATE dt, repo_id, (event_type == 'WatchEvent' ? 1 : 0) AS star;
cat  = LOAD 'gh.catalogue_repos' USING org.apache.hive.hcatalog.pig.HCatLoader();
lang = FILTER (FOREACH cat GENERATE repo_id, primary_language) BY primary_language IS NOT NULL;
j    = JOIN evp BY repo_id, lang BY repo_id USING 'replicated';
g    = GROUP j BY (evp::dt, lang::primary_language);
out  = FOREACH g GENERATE FLATTEN(group), COUNT_STAR(j), SUM(j.evp::star);
STORE out INTO '$out' USING PigStorage('\t');
