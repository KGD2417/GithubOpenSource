-- Q4  Bot vs human share of events, and which event types bots dominate
SELECT event_type, sum(events) AS total_events,
       sum(CASE WHEN is_bot THEN events ELSE 0 END) AS bot_events,
       round(100.0 * sum(CASE WHEN is_bot THEN events ELSE 0 END) / sum(events), 1) AS bot_pct
FROM gh.hourly_activity
GROUP BY event_type
ORDER BY bot_pct DESC;
