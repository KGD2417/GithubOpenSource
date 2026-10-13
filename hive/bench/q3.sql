-- Bench Q3 (join with a dimension): events and stars per day by primary language of catalogued repos.
SELECT e.dt, c.primary_language, count(*) AS events,
       sum(CASE WHEN e.event_type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars
FROM gh.events_clean e JOIN gh.catalogue_repos c ON e.repo_id = c.repo_id
WHERE c.primary_language IS NOT NULL
GROUP BY e.dt, c.primary_language;
