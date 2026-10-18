#!/usr/bin/env bash
# Millisecond lookups against the HBase serving tables, computing the salted keys for you (viva demo).
#   hlookup.sh similar <owner/repo>          Mahout "similar repos"
#   hlookup.sh hourly  <owner/repo> [yyyyMMdd] hourly activity series (one prefix scan)
#   hlookup.sh recs    <login>               Mahout "repos you may like"
#   hlookup.sh live    [n]                   last n minutes of Storm counters
# Runs on the laptop (wraps docker compose exec) or inside the edge container.
set -euo pipefail
export MSYS_NO_PATHCONV=1      # Git Bash on Windows: keep container paths (/opt/...) as-is
run() { if [ -f /.dockerenv ]; then hbase shell -n; else docker compose exec -T edge hbase shell -n; fi; }
salt() { local s; s=$(printf '%s' "$1" | (md5sum 2>/dev/null || md5) | cut -c1-2); echo "$s"; }
repo_id() {   # full_name -> repo_id via repo_index
  local n; n=$(echo "$1" | tr 'A-Z' 'a-z')
  # awk reads to the end (no early exit): an early exit would SIGPIPE the shell and, with pipefail, abort us
  echo "get 'repo_index', '$(salt "$n")|$n', 'i:id'" | run 2>/dev/null | awk '/value=/ {v = $0} END {sub(/.*value=/, "", v); print v}'
}
pad() { printf '%012d' "$1"; }
LIVE_COLS="'t:PushEvent:toLong', 't:CreateEvent:toLong', 't:DeleteEvent:toLong', 't:PullRequestEvent:toLong', 't:PullRequestReviewEvent:toLong', 't:PullRequestReviewCommentEvent:toLong', 't:IssuesEvent:toLong', 't:IssueCommentEvent:toLong', 't:CommitCommentEvent:toLong', 't:WatchEvent:toLong', 't:ForkEvent:toLong', 't:ReleaseEvent:toLong', 't:MemberEvent:toLong', 't:PublicEvent:toLong', 't:GollumEvent:toLong', 't:DiscussionEvent:toLong', 'b:bot:toLong', 'b:human:toLong'"

case "${1:-}" in
  similar)
    id=$(repo_id "$2"); [ -n "$id" ] || { echo "unknown repo $2"; exit 1; }
    echo "get 'repo_similar', '$(salt "$id")|$(pad "$id")'" | run ;;
  hourly)
    id=$(repo_id "$2"); [ -n "$id" ] || { echo "unknown repo $2"; exit 1; }
    echo "scan 'repo_hourly', {ROWPREFIXFILTER => '$(salt "$id")|$(pad "$id")|${3:-}', COLUMNS => ['c:total','c:watch','c:push','c:pr','c:bot']}" | run ;;
  recs)
    echo "get 'user_recs', '$(salt "$2")|$2'" | run ;;
  live)
    n=${2:-5}
    # counters are 8-byte longs (atomic Increments): ':toLong' decodes each column
    echo "scan 'live_minute', {REVERSED => true, LIMIT => $((n * 8)), COLUMNS => [$LIVE_COLS]}" \
      | run | grep 'column=' | sort -t'|' -k2 -r | head -$((n * 20)) ;;
  *) sed -n '3,8p' "$0"; exit 1 ;;
esac
