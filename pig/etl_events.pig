-- Pig ETL: raw GH Archive JSON  ->  gh.events_clean (Parquet, partitioned by dt / event_type)
-- PLAN.md §6.2.   Cluster:  pig -useHCatalog -param dt=2025-09-01 pig/etl_events.pig   (via `pulse pig-etl`)
-- Local test:   pig -x local -param dt=... -param input=... -param bots_input=... -param bots_loader=PigStorage()
--               -param output=/tmp/out -param storer=PigStorage() -param dq_out=/tmp/dq pig/etl_events.pig
--
-- Steps: parse each line with the Java UDF (null = malformed) -> drop malformed -> dedup on event_id
--        -> tag bots ([bot] suffix, known_bots list from Sqoop, "-bot" name heuristic) -> store.
-- Data-quality counts for every step go to the dq_stage_counts table.

%default input       '/gh/raw/events/dt=$dt'
%default bots_input  'gh.known_bots'
%default bots_loader 'org.apache.hive.hcatalog.pig.HCatLoader()'
%default output      'gh.events_clean'
%default storer      'org.apache.hive.hcatalog.pig.HCatStorer(\'dt=$dt\')'
%default dq_out      '/gh/dq/stage_counts/stage=clean/$dt'
%default jar         '/opt/pulse/java/hadoop-jobs/target/pulse-hadoop.jar'

SET default_parallel 2;
SET job.name 'pulse-etl-$dt';
REGISTER $jar;
DEFINE Parse pulse.pig.ParseGhEvent();

raw    = LOAD '$input' USING TextLoader() AS (line:chararray);    -- .gz is decompressed transparently
parsed = FOREACH raw GENERATE Parse(line) AS ev;
good   = FOREACH (FILTER parsed BY ev IS NOT NULL) GENERATE FLATTEN(ev);

-- Dedup: GH Archive occasionally repeats an event; keep one row per event_id (shuffle on event_id)
by_id  = GROUP good BY event_id;
dedup  = FOREACH by_id { one = LIMIT good 1; GENERATE FLATTEN(one); };

-- Bot tagging. known_bots is tiny, so a replicated (map-side) join ships it to every mapper.
bots   = LOAD '$bots_input' USING $bots_loader;
botl   = FOREACH bots GENERATE (chararray) $0 AS login;
j      = JOIN dedup BY $5 LEFT OUTER, botl BY login USING 'replicated';
tagged = FOREACH j GENERATE $0 .. $31,
           ($5 MATCHES '.*\\[bot\\]$' ? 'suffix'
             : ($32 IS NOT NULL ? 'list'
               : ($5 MATCHES '(?i).*[-_]bot$' ? 'name' : (chararray) null))) AS bot_reason;

-- Final columns, named and ordered exactly like gh.events_clean; event_type last = dynamic partition
out = FOREACH tagged GENERATE
  $0 AS event_id, $2 AS created_at, $3 AS created_ts, $4 AS actor_id, $5 AS actor_login,
  $6 AS repo_id, $7 AS repo_name, $8 AS repo_owner, $9 AS org_login, $10 AS action, $11 AS ref_type,
  $12 AS push_size, $13 AS push_distinct_size,
  $14 AS pr_number, $15 AS pr_merged, $16 AS pr_created_at, $17 AS pr_closed_at, $18 AS pr_merged_at,
  $19 AS pr_additions, $20 AS pr_deletions, $21 AS pr_changed_files, $22 AS pr_author,
  $23 AS issue_number, $24 AS issue_created_at, $25 AS issue_closed_at, $26 AS issue_comments, $27 AS issue_author,
  $28 AS review_state, $29 AS release_tag, $30 AS forkee_repo_id, $31 AS payload_version,
  (bot_reason IS NOT NULL ? true : false) AS is_bot, bot_reason, $1 AS event_type;

STORE out INTO '$output' USING $storer;

-- Data-quality counts (dt, unit, stage_name, records, bytes, ts, status)
c_raw   = FOREACH (GROUP raw ALL)   GENERATE 'raw_lines'  AS k, COUNT_STAR(raw)   AS n;
c_good  = FOREACH (GROUP good ALL)  GENERATE 'parsed_ok'  AS k, COUNT_STAR(good)  AS n;
c_dedup = FOREACH (GROUP dedup ALL) GENERATE 'clean_rows' AS k, COUNT_STAR(dedup) AS n;
bot_out = FILTER out BY is_bot;
c_bot   = FOREACH (GROUP bot_out ALL) GENERATE 'bot_events' AS k, COUNT_STAR(bot_out) AS n;
counts  = UNION c_raw, c_good, c_dedup, c_bot;
dq      = FOREACH counts GENERATE '$dt', 'pig', k, n, 0L, ToString(CurrentTime()), 'ok';
STORE dq INTO '$dq_out' USING PigStorage('\t');
