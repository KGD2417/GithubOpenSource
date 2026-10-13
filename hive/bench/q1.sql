-- Bench Q1 (scan + small aggregation): events per day and type. Same file runs on Hive, Impala, Spark SQL.
SELECT dt, event_type, count(*) AS events
FROM gh.events_clean
GROUP BY dt, event_type;
