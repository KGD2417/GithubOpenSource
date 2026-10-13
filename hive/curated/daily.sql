-- Build every daily curated partition for one day from gh.events_clean.   pulse hive-curated <dt>
-- Each INSERT OVERWRITE replaces its partition, so re-running a day is safe.
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE gh.repo_type_daily PARTITION (dt = '${dt}')
SELECT repo_id, max(repo_name), event_type, count(*), sum(if(is_bot, 0, 1)), count(DISTINCT actor_id)
FROM gh.events_clean WHERE dt = '${dt}'
GROUP BY repo_id, event_type;

INSERT OVERWRITE TABLE gh.repo_daily_activity PARTITION (dt = '${dt}')
SELECT repo_id, max(repo_name),
       sum(if(event_type = 'WatchEvent', 1, 0)),
       sum(if(event_type = 'ForkEvent', 1, 0)),
       sum(if(event_type = 'PushEvent', 1, 0)),
       sum(if(event_type = 'PullRequestEvent' AND action = 'opened', 1, 0)),
       sum(if(event_type = 'PullRequestEvent' AND action = 'closed' AND pr_merged, 1, 0)),
       sum(if(event_type = 'IssuesEvent' AND action = 'opened', 1, 0)),
       sum(if(event_type = 'IssuesEvent' AND action = 'closed', 1, 0)),
       sum(if(event_type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent', 'CommitCommentEvent'), 1, 0)),
       sum(if(event_type = 'PullRequestReviewEvent', 1, 0)),
       sum(if(event_type = 'ReleaseEvent', 1, 0)),
       count(DISTINCT actor_id),
       sum(if(is_bot, 1, 0)), sum(if(is_bot, 0, 1)), count(*)
FROM gh.events_clean WHERE dt = '${dt}'
GROUP BY repo_id;

INSERT OVERWRITE TABLE gh.stars PARTITION (dt = '${dt}')
SELECT actor_id, actor_login, repo_id, created_at
FROM gh.events_clean WHERE dt = '${dt}' AND event_type = 'WatchEvent' AND NOT is_bot;

INSERT OVERWRITE TABLE gh.hourly_activity PARTITION (dt = '${dt}')
SELECT hour(created_at), cast(date_format(created_at, 'u') AS INT),
       hour(ist), cast(date_format(ist, 'u') AS INT), to_date(ist),
       event_type, is_bot, count(*)
FROM (SELECT created_at, from_utc_timestamp(created_at, 'Asia/Kolkata') AS ist, event_type, is_bot
      FROM gh.events_clean WHERE dt = '${dt}') e
GROUP BY hour(created_at), cast(date_format(created_at, 'u') AS INT), hour(ist),
         cast(date_format(ist, 'u') AS INT), to_date(ist), event_type, is_bot;

-- Needs `pulse mr-top <dt>` first (gh.top_repos_mr)
INSERT OVERWRITE TABLE gh.trending_daily PARTITION (dt = '${dt}')
SELECT m.event_type, m.rnk, m.repo_id, t.repo_name, m.events, p.human_events,
       if(p.human_events > 0, round(100.0 * (m.events - p.human_events) / p.human_events, 2), NULL)
FROM gh.top_repos_mr m
JOIN gh.repo_type_daily t ON t.dt = '${dt}' AND t.repo_id = m.repo_id AND t.event_type = m.event_type
LEFT JOIN gh.repo_type_daily p
       ON p.dt = cast(date_sub('${dt}', 1) AS STRING) AND p.repo_id = m.repo_id AND p.event_type = m.event_type
WHERE m.dt = '${dt}';

INSERT OVERWRITE TABLE gh.pr_lifecycle PARTITION (dt = '${dt}')
SELECT repo_id, repo_name, pr_number, pr_author, pr_author LIKE '%[bot]', pr_merged,
       pr_created_at, pr_closed_at,
       round((unix_timestamp(coalesce(pr_merged_at, pr_closed_at)) - unix_timestamp(pr_created_at)) / 3600.0, 2),
       pr_additions, pr_deletions, pr_changed_files
FROM gh.events_clean
WHERE dt = '${dt}' AND event_type = 'PullRequestEvent' AND action = 'closed' AND pr_created_at IS NOT NULL;

INSERT OVERWRITE TABLE gh.issue_lifecycle PARTITION (dt = '${dt}')
SELECT repo_id, repo_name, issue_number, issue_author, issue_created_at, issue_closed_at,
       round((unix_timestamp(issue_closed_at) - unix_timestamp(issue_created_at)) / 3600.0, 2), issue_comments
FROM gh.events_clean
WHERE dt = '${dt}' AND event_type = 'IssuesEvent' AND action = 'closed' AND issue_created_at IS NOT NULL;

-- Language / topic trends need the Sqoop-imported catalogue (only catalogued repos have a language)
INSERT OVERWRITE TABLE gh.language_trends PARTITION (dt = '${dt}')
SELECT c.primary_language, sum(a.total_events), sum(a.stars), sum(a.forks), count(*)
FROM gh.repo_daily_activity a JOIN gh.catalogue_repos c ON a.repo_id = c.repo_id
WHERE a.dt = '${dt}' AND c.primary_language IS NOT NULL
GROUP BY c.primary_language;

INSERT OVERWRITE TABLE gh.topic_trends PARTITION (dt = '${dt}')
SELECT topic, sum(total_events), sum(stars), count(*)
FROM (SELECT a.total_events, a.stars, c.topics
      FROM gh.repo_daily_activity a JOIN gh.catalogue_repos c ON a.repo_id = c.repo_id
      WHERE a.dt = '${dt}' AND c.topics IS NOT NULL) j
LATERAL VIEW explode(split(topics, ',')) t AS topic   -- Hive: LATERAL VIEW must follow a table/subquery
GROUP BY topic;
