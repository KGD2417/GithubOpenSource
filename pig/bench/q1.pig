-- Bench Q1 in Pig: events per day and type
ev  = LOAD 'gh.events_clean' USING org.apache.hive.hcatalog.pig.HCatLoader();
g   = GROUP ev BY (dt, event_type);
out = FOREACH g GENERATE FLATTEN(group), COUNT_STAR(ev);
STORE out INTO '$out' USING PigStorage('\t');
