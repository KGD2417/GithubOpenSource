-- P4 check: MapReduce top-25 must equal the same ranking computed in HiveQL (ties broken by repo_id).
WITH h AS (
  SELECT event_type, repo_id, events,
         row_number() OVER (PARTITION BY event_type ORDER BY events DESC, repo_id ASC) AS rnk
  FROM (SELECT event_type, repo_id, count(*) AS events FROM gh.events_clean
        WHERE dt = '${dt}' AND NOT is_bot GROUP BY event_type, repo_id) x)
SELECT count(*) AS mr_rows,
       sum(if(h.repo_id = m.repo_id AND h.events = m.events, 1, 0)) AS identical,
       assert_true(count(*) = sum(if(h.repo_id = m.repo_id AND h.events = m.events, 1, 0))) AS all_match
FROM gh.top_repos_mr m JOIN h ON m.event_type = h.event_type AND m.rnk = h.rnk
WHERE m.dt = '${dt}';
