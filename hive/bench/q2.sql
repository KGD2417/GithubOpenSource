-- Bench Q2 (filter + group + top-N): top 10 repos by human stars per day.
SELECT dt, repo_id, stars, rnk FROM (
  SELECT dt, repo_id, count(*) AS stars,
         row_number() OVER (PARTITION BY dt ORDER BY count(*) DESC, repo_id) AS rnk
  FROM gh.events_clean
  WHERE event_type = 'WatchEvent' AND NOT is_bot
  GROUP BY dt, repo_id) t
WHERE rnk <= 10;
