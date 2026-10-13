-- Q3  Activity by hour of day in UTC and IST, plus weekend (Sat/Sun in that timezone) share
SELECT 'UTC' AS tz, hour_utc AS hr, sum(events) AS events,
       round(100.0 * sum(CASE WHEN weekday_utc >= 6 THEN events ELSE 0 END) / sum(events), 1) AS weekend_pct
FROM gh.hourly_activity GROUP BY hour_utc
UNION ALL
SELECT 'IST' AS tz, hour_ist AS hr, sum(events) AS events,
       round(100.0 * sum(CASE WHEN weekday_ist >= 6 THEN events ELSE 0 END) / sum(events), 1) AS weekend_pct
FROM gh.hourly_activity GROUP BY hour_ist
ORDER BY tz, hr;
