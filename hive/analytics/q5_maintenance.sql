-- Q5  Maintenance health: PR merge rate and time-to-merge, issue close ratio, responses per PR/issue
SELECT repo_name, prs_closed, prs_merged, merge_rate, median_hours_to_merge,
       issues_opened, issues_closed, issue_close_ratio, responses_per_item, bot_pr_share
FROM gh.repo_health
WHERE prs_closed >= 5
ORDER BY prs_closed DESC
LIMIT 20;
