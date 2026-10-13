-- Q2  Language trends: activity and stars of catalogued repos by primary language
SELECT language, sum(events) AS events, sum(stars) AS stars, sum(forks) AS forks, max(active_repos) AS repos
FROM gh.language_trends
GROUP BY language
ORDER BY events DESC
LIMIT 15;
