-- DQ raw -> clean for one day (PLAN.md §13).  pulse dq clean <dt>
-- Prints the metrics, then assert_true() fails the query (and the Airflow task) if any rule breaks.
WITH c AS (
  SELECT count(*) AS n, count(DISTINCT event_id) AS n_distinct,
         sum(if(event_id IS NULL OR created_at IS NULL OR repo_id IS NULL, 1, 0)) AS n_null_keys,
         sum(if(to_date(created_at) <> dt, 1, 0)) AS n_wrong_day,
         sum(if(t.event_type IS NULL, 1, 0)) AS n_unknown_type,
         sum(if(is_bot, 1, 0)) AS n_bot,
         sum(if(payload_version = 'trimmed', 1, 0)) AS n_trimmed
  FROM gh.events_clean e LEFT JOIN gh.event_types t ON e.event_type = t.event_type
  WHERE e.dt = '${dt}'),
d AS (
  SELECT sum(if(stage = 'raw' AND status = 'ok', records, 0))   AS raw_downloaded,
         sum(if(stage_name = 'raw_lines', records, 0))         AS pig_raw_lines,
         sum(if(stage_name = 'parsed_ok', records, 0))         AS pig_parsed,
         sum(if(stage_name = 'clean_rows', records, 0))        AS pig_clean
  FROM gh.dq_stage_counts WHERE dt = '${dt}')
SELECT '${dt}' AS dt, d.raw_downloaded, d.pig_raw_lines, d.pig_raw_lines - d.pig_parsed AS malformed,
       d.pig_parsed - d.pig_clean AS duplicates, c.n AS clean_rows, c.n_bot AS bot_rows,
       round(100 * c.n_bot / c.n, 1) AS bot_pct, c.n_wrong_day, c.n_trimmed,
       assert_true(d.raw_downloaded = d.pig_raw_lines)  AS raw_matches_download,
       assert_true(c.n = d.pig_clean)                   AS clean_matches_pig,
       assert_true(c.n = c.n_distinct)                  AS no_duplicate_ids,
       assert_true(c.n_null_keys = 0)                   AS no_null_keys,
       assert_true(c.n_unknown_type = 0)                AS all_types_known,
       assert_true(c.n_wrong_day <= 0.001 * c.n)        AS dt_matches_created_at,
       assert_true((d.pig_raw_lines - d.pig_parsed) <= 0.001 * d.pig_raw_lines) AS malformed_below_0_1pct
FROM c CROSS JOIN d;
