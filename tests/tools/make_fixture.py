"""Builds tests/fixtures/events_small.json from real GH Archive lines on stdin (PLAN.md §13).

  hdfs dfs -cat /gh/raw/events/dt=2025-09-01/2025-09-01-12.json.gz | zcat | python3 make_fixture.py <trimmed-2026-line-file>

Picks: up to 2 events of every type, merged / unmerged / opened PRs, opened / closed issues, a [bot]
actor, a curated-list bot; then appends a duplicate, a malformed line and one trimmed (2026) PR event.
Long free-text fields are cut to 200 chars to keep the fixture small.
"""
import json, sys

def shrink(o):
    if isinstance(o, dict):
        return {k: shrink(v) for k, v in o.items()}
    if isinstance(o, list):
        return [shrink(v) for v in o[:3]]
    if isinstance(o, str) and len(o) > 200:
        return o[:200]
    return o

wanted = {}
def want(key, e, n=1):
    if len(wanted.setdefault(key, [])) < n:
        wanted[key].append(e)

for line in sys.stdin:
    e = json.loads(line)
    t, p, login = e["type"], e["payload"], e["actor"]["login"]
    want(t, e, 2)
    if t == "PullRequestEvent":
        pr = p["pull_request"]
        if p["action"] == "closed":
            want("pr_merged" if pr.get("merged") else "pr_unmerged", e)
        elif p["action"] == "opened":
            want("pr_opened", e)
    if t == "IssuesEvent":
        want("issue_" + p.get("action", ""), e)
    if login.endswith("[bot]"):
        want("suffix_bot", e)
    if login in ("renovate-bot", "k8s-ci-robot", "pre-commit-ci", "mergify", "netlify"):
        want("list_bot", e)
    if t == "WatchEvent":
        want("watch_more", e, 4)

seen, out = set(), []
for evs in wanted.values():
    for e in evs:
        if e["id"] not in seen:
            seen.add(e["id"]); out.append(json.dumps(shrink(e), separators=(",", ":")))
out.append(out[0])                                     # duplicate event id
out.append('{"id":"1","type":"PushEvent","actor":')     # malformed (truncated) line
out.append(open(sys.argv[1]).read().strip())            # trimmed post-Oct-2025 PullRequestEvent
print("\n".join(out))
print(f"{len(out)} lines, {len(seen)} distinct real events, keys: {sorted(wanted)}", file=sys.stderr)
