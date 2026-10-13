-- Q1  Most active and fastest-rising repositories (human events), latest day, top 5 per event type
SELECT dt, event_type, rnk, repo_name, events, prev_day_events, growth_pct
FROM gh.trending_daily
WHERE dt = (SELECT max(dt) FROM gh.trending_daily) AND rnk <= 5
  AND event_type IN ('WatchEvent', 'ForkEvent', 'PushEvent', 'PullRequestEvent', 'IssuesEvent')
ORDER BY event_type, rnk;
