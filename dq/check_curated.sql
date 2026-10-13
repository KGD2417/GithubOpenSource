-- DQ clean -> curated for one day: curated tables must account for every clean event.   pulse dq curated <dt>
WITH c AS (SELECT count(*) AS n, sum(if(event_type = 'WatchEvent' AND NOT is_bot, 1, 0)) AS human_stars
           FROM gh.events_clean WHERE dt = '${dt}'),
     a AS (SELECT sum(total_events) AS n, sum(bot_events + human_events) AS n2 FROM gh.repo_daily_activity WHERE dt = '${dt}'),
     t AS (SELECT sum(events) AS n FROM gh.repo_type_daily WHERE dt = '${dt}'),
     h AS (SELECT sum(events) AS n FROM gh.hourly_activity WHERE dt = '${dt}'),
     s AS (SELECT count(*) AS n FROM gh.stars WHERE dt = '${dt}'),
     r AS (SELECT count(*) AS n FROM gh.trending_daily WHERE dt = '${dt}')
SELECT '${dt}' AS dt, c.n AS clean_rows, a.n AS repo_daily_sum, t.n AS repo_type_sum, h.n AS hourly_sum,
       s.n AS stars_rows, r.n AS trending_rows,
       assert_true(a.n = c.n AND a.n2 = c.n) AS repo_daily_complete,
       assert_true(t.n = c.n)                AS repo_type_complete,
       assert_true(h.n = c.n)                AS hourly_complete,
       assert_true(s.n = c.human_stars)      AS stars_complete,
       assert_true(r.n > 0)                  AS trending_present
FROM c CROSS JOIN a CROSS JOIN t CROSS JOIN h CROSS JOIN s CROSS JOIN r;
