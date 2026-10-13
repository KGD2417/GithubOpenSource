#!/usr/bin/env bash
# Fallback for Tableau when the ODBC drivers aren't available: every dashboard table -> CSV.
#   pulse export-extracts            (warehouse stage; writes tableau/extracts/*.csv)
#   pulse export-live                (streaming stage; HBase live_minute + live_topk -> CSV, re-run to refresh)
set -euo pipefail
cd /opt/pulse
OUT=tableau/extracts
mkdir -p $OUT
url="jdbc:hive2://hiveserver2:10000"
extract() {   # name, query
  beeline -u "$url" -n root --silent=true --outputformat=csv2 -e "$2" 2>/dev/null > "$OUT/$1.csv"
  echo "$1.csv: $(($(wc -l < "$OUT/$1.csv") - 1)) rows"
}
if [ "${1:-batch}" = live ]; then
  # Storm's live counters straight from HBase (Tableau refreshes the CSV)
  echo "scan 'live_minute', {COLUMNS => ['t:PushEvent:toLong', 't:CreateEvent:toLong', 't:DeleteEvent:toLong', 't:PullRequestEvent:toLong', 't:PullRequestReviewEvent:toLong', 't:PullRequestReviewCommentEvent:toLong', 't:IssuesEvent:toLong', 't:IssueCommentEvent:toLong', 't:CommitCommentEvent:toLong', 't:WatchEvent:toLong', 't:ForkEvent:toLong', 't:ReleaseEvent:toLong', 't:MemberEvent:toLong', 't:PublicEvent:toLong', 't:GollumEvent:toLong', 't:DiscussionEvent:toLong', 'b:bot:toLong', 'b:human:toLong']}" | hbase shell -n 2>/dev/null | awk '/column=/ {
      split($1, k, "|"); split($2, c, "="); sub(/,$/, "", c[2]); v = $NF; sub(/value=/, "", v);
      print k[2] "," c[2] "," v }' | sort > $OUT/live_minute.raw
  { echo "minute,column,events"; cat $OUT/live_minute.raw; } > $OUT/live_minute.csv; rm $OUT/live_minute.raw
  echo "scan 'live_topk'" | hbase shell -n 2>/dev/null | awk '/column=/ {
      split($2, c, ":"); sub(/,$/, "", c[2]); v = $0; sub(/.*value=/, "", v); split(v, f, "|");
      print $1 "," c[2] "," f[2] "," f[3] }' > $OUT/live_topk.raw
  { echo "window_end,rank,repo_name,events"; cat $OUT/live_topk.raw; } > $OUT/live_topk.csv; rm $OUT/live_topk.raw
  wc -l $OUT/live_*.csv
  exit 0
fi
extract trending_daily   "SELECT * FROM gh.trending_daily"
extract trending_window  "SELECT * FROM gh.trending_window"
extract language_trends  "SELECT * FROM gh.language_trends"
extract topic_trends     "SELECT * FROM gh.topic_trends WHERE events >= 10"
extract hourly_activity  "SELECT * FROM gh.hourly_activity"
extract repo_health      "SELECT * FROM gh.repo_health"
extract pr_lifecycle     "SELECT repo_name, merged, hours_open, author_is_bot, dt FROM gh.pr_lifecycle"
extract repo_profile     "SELECT * FROM gh.repo_profile"
extract repo_clusters    "SELECT * FROM gh.repo_clusters"
extract similar_repos    "SELECT * FROM gh.similar_repos"
extract user_recs        "SELECT * FROM gh.user_recs"
extract viral_predictions "SELECT * FROM gh.viral_predictions WHERE split = 'test'"
extract top_bots         "SELECT actor_login, bot_reason, count(*) AS events, count(DISTINCT repo_id) AS repos
                          FROM gh.events_clean WHERE is_bot GROUP BY actor_login, bot_reason ORDER BY events DESC LIMIT 50"
