-- Q6  Fork-to-star ratio and how many people touch each popular repo (community vs solo; Spark adds Gini)
SELECT repo_name, sum(stars) AS stars, sum(forks) AS forks,
       round(sum(forks) / sum(stars), 3) AS fork_star_ratio,
       max(distinct_actors) AS peak_daily_actors, sum(human_events) AS human_events
FROM gh.repo_daily_activity
GROUP BY repo_name
HAVING sum(stars) >= 20
ORDER BY stars DESC
LIMIT 20;
