-- Window tables over every loaded day.   pulse hive-window

-- Q1 (weekly view): biggest repos per event type across the window, and first-day vs last-day change
INSERT OVERWRITE TABLE gh.trending_window
SELECT event_type, rnk, repo_id, repo_name, events, days_active, first_day_events, last_day_events,
       if(first_day_events > 0, round(100.0 * (last_day_events - first_day_events) / first_day_events, 2), NULL)
FROM (
  SELECT event_type, repo_id, max(repo_name) AS repo_name, sum(human_events) AS events, count(*) AS days_active,
         sum(if(dt = d.first_dt, human_events, 0)) AS first_day_events,
         sum(if(dt = d.last_dt, human_events, 0))  AS last_day_events,
         row_number() OVER (PARTITION BY event_type ORDER BY sum(human_events) DESC, repo_id) AS rnk
  FROM gh.repo_type_daily
  CROSS JOIN (SELECT min(dt) AS first_dt, max(dt) AS last_dt FROM gh.repo_type_daily) d
  GROUP BY event_type, repo_id) x
WHERE rnk <= 50;

-- Q5: maintenance health per repo (only repos with enough closed PRs/issues to say something).
-- PR counts and merge times use HUMAN-authored PRs only: dependency bots open/close PRs in bulk and
-- would otherwise dominate the ranking; bot_pr_share keeps that signal as its own column.
INSERT OVERWRITE TABLE gh.repo_health
SELECT a.repo_id, a.repo_name, coalesce(p.prs_closed, 0), coalesce(p.prs_merged, 0),
       round(p.prs_merged / p.prs_closed, 3), p.median_hours_to_merge,
       a.issues_opened, a.issues_closed, if(a.issues_opened > 0, round(a.issues_closed / a.issues_opened, 3), NULL),
       i.median_hours_to_close, a.comments, a.reviews,
       round((a.comments + a.reviews) / greatest(a.prs_opened + a.issues_opened, 1), 2),
       round(p.bot_prs / p.all_prs, 3)
FROM (SELECT repo_id, max(repo_name) AS repo_name, sum(issues_opened) AS issues_opened,
             sum(issues_closed) AS issues_closed, sum(comments) AS comments, sum(reviews) AS reviews,
             sum(prs_opened) AS prs_opened
      FROM gh.repo_daily_activity GROUP BY repo_id) a
LEFT JOIN (SELECT repo_id, sum(if(author_is_bot, 0, 1)) AS prs_closed,
                  sum(if(merged AND NOT author_is_bot, 1, 0)) AS prs_merged,
                  percentile_approx(if(merged AND NOT author_is_bot, hours_open, NULL), 0.5) AS median_hours_to_merge,
                  sum(if(author_is_bot, 1, 0)) AS bot_prs, count(*) AS all_prs
           FROM gh.pr_lifecycle GROUP BY repo_id) p ON a.repo_id = p.repo_id
LEFT JOIN (SELECT repo_id, percentile_approx(hours_open, 0.5) AS median_hours_to_close
           FROM gh.issue_lifecycle GROUP BY repo_id) i ON a.repo_id = i.repo_id
WHERE coalesce(p.prs_closed, 0) + a.issues_opened + a.issues_closed >= 3;
