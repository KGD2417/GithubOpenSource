#!/usr/bin/env python3
"""Seed the Postgres repo catalogue from the GitHub REST API (PLAN.md §4.2).

    seed_catalogue.py --repos repos.tsv [--bots bots.txt] [--max-wait 900]

repos.tsv: repo_id<TAB>full_name, most active first (produced by `pulse seed-catalogue` from Hive).
bots.txt:  one login per line (logins ending in [bot] seen in our data).

Rate limits: uses GITHUB_TOKEN if set (5,000 req/h, otherwise 60/h). Every response is cached in
data/cache/github/ for 7 days; stale entries are revalidated with ETag (a 304 does not use quota).
When remaining quota hits the floor it sleeps until X-RateLimit-Reset, unless that wait is longer
than --max-wait, in which case it stops cleanly: re-running continues where it left off.
Rows are upserted and updated_at only changes when a value actually changed, so Sqoop's
lastmodified incremental import picks up exactly the new and changed repos.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests

CACHE = Path(__file__).resolve().parent.parent / "data" / "cache" / "github"
TTL = 7 * 24 * 3600
API = "https://api.github.com"

# Well-known automation accounts that do not use the "[bot]" suffix
CURATED_BOTS = [
    "dependabot-preview", "renovate-bot", "greenkeeperio-bot", "snyk-bot", "imgbot", "codecov-io",
    "pre-commit-ci", "mergify", "allcontributors", "stale", "depfu", "pull", "k8s-ci-robot",
    "openshift-ci-robot", "google-oss-bot", "netlify", "vercel", "sonarcloud", "github-actions",
]


class RateLimited(Exception):
    pass


def session():
    s = requests.Session()
    s.headers["Accept"] = "application/vnd.github+json"
    s.headers["X-GitHub-Api-Version"] = "2022-11-28"
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s, bool(token)


def respect_limits(resp, floor, max_wait):
    """Sleep until reset when quota is (nearly) spent; raise RateLimited if that's too long."""
    if resp.status_code in (403, 429) and "Retry-After" in resp.headers:  # secondary rate limit
        wait = int(resp.headers["Retry-After"]) + 1
    elif int(resp.headers.get("X-RateLimit-Remaining", floor + 1)) <= floor:
        wait = int(resp.headers.get("X-RateLimit-Reset", time.time())) - int(time.time()) + 5
    else:
        return False
    if wait > max_wait:
        raise RateLimited(f"quota exhausted; resets in {wait // 60} min (set GITHUB_TOKEN or re-run later)")
    print(f"  rate limit: sleeping {wait}s", flush=True)
    time.sleep(max(wait, 1))
    return True


def fetch_repo(s, full_name, floor, max_wait):
    """Returns (status, body). Uses the on-disk cache; revalidates stale entries with ETag."""
    path = CACHE / (full_name.replace("/", "__") + ".json")
    cached = json.loads(path.read_text()) if path.exists() else None
    if cached and time.time() - cached["fetched_at"] < TTL:
        return cached["status"], cached["body"]
    headers = {"If-None-Match": cached["etag"]} if cached and cached.get("etag") else {}
    while True:
        resp = s.get(f"{API}/repos/{full_name}", headers=headers, timeout=30)
        if resp.status_code in (403, 429) and respect_limits(resp, floor, max_wait):
            continue
        break
    if resp.status_code == 304:
        status, body, etag = cached["status"], cached["body"], cached.get("etag")
    elif resp.status_code == 200:
        status, body, etag = 200, resp.json(), resp.headers.get("ETag")
    elif resp.status_code in (404, 451):          # deleted, private or blocked: remember, don't refetch
        status, body, etag = 404, None, None
    else:
        resp.raise_for_status()
        raise RuntimeError(f"unexpected HTTP {resp.status_code} for {full_name}")
    path.write_text(json.dumps({"status": status, "body": body, "etag": etag, "fetched_at": time.time()}))
    respect_limits(resp, floor, max_wait)
    return status, body


UPSERT = """
INSERT INTO repos (repo_id, full_name, owner_login, owner_type, primary_language, topics, license_spdx,
                   created_at, stargazers_count, forks_count, open_issues_count, is_fork, archived, api_status)
VALUES (%(repo_id)s, %(full_name)s, %(owner_login)s, %(owner_type)s, %(primary_language)s, %(topics)s,
        %(license_spdx)s, %(created_at)s, %(stargazers_count)s, %(forks_count)s, %(open_issues_count)s,
        %(is_fork)s, %(archived)s, %(api_status)s)
ON CONFLICT (repo_id) DO UPDATE SET
  full_name = EXCLUDED.full_name, owner_login = EXCLUDED.owner_login, owner_type = EXCLUDED.owner_type,
  primary_language = EXCLUDED.primary_language, topics = EXCLUDED.topics, license_spdx = EXCLUDED.license_spdx,
  created_at = EXCLUDED.created_at, stargazers_count = EXCLUDED.stargazers_count,
  forks_count = EXCLUDED.forks_count, open_issues_count = EXCLUDED.open_issues_count,
  is_fork = EXCLUDED.is_fork, archived = EXCLUDED.archived, api_status = EXCLUDED.api_status,
  updated_at = now()
WHERE (repos.full_name, repos.owner_type, repos.primary_language, repos.topics, repos.license_spdx,
       repos.stargazers_count, repos.forks_count, repos.open_issues_count, repos.archived, repos.api_status)
  IS DISTINCT FROM
      (EXCLUDED.full_name, EXCLUDED.owner_type, EXCLUDED.primary_language, EXCLUDED.topics, EXCLUDED.license_spdx,
       EXCLUDED.stargazers_count, EXCLUDED.forks_count, EXCLUDED.open_issues_count, EXCLUDED.archived,
       EXCLUDED.api_status)
"""


def to_row(repo_id, full_name, status, b):
    if status != 200:
        return dict(repo_id=repo_id, full_name=full_name, owner_login=full_name.split("/")[0], owner_type=None,
                    primary_language=None, topics=None, license_spdx=None, created_at=None, stargazers_count=None,
                    forks_count=None, open_issues_count=None, is_fork=None, archived=None, api_status=404)
    return dict(
        repo_id=repo_id, full_name=b["full_name"], owner_login=b["owner"]["login"], owner_type=b["owner"]["type"]
        if b["owner"]["type"] in ("User", "Organization") else None,
        primary_language=b.get("language"), topics=",".join(b.get("topics") or []) or None,
        license_spdx=(b.get("license") or {}).get("spdx_id"), created_at=b.get("created_at"),
        stargazers_count=b.get("stargazers_count"), forks_count=b.get("forks_count"),
        open_issues_count=b.get("open_issues_count"), is_fork=b.get("fork"), archived=b.get("archived"),
        api_status=200)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos", required=True)
    ap.add_argument("--bots")
    ap.add_argument("--max-wait", type=int, default=900, help="longest rate-limit sleep before stopping (s)")
    args = ap.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    s, authed = session()
    floor = 50 if authed else 2
    print(f"GitHub API: {'token' if authed else 'anonymous (60 req/h)'}", flush=True)

    db = psycopg2.connect(host="postgres", dbname="catalogue", user="catalogue",
                          password=os.environ["CATALOGUE_DB_PASSWORD"])
    db.autocommit = True
    cur = db.cursor()

    if args.bots:
        bots = [(l.strip(), "suffix") for l in open(args.bots) if l.strip()] + [(b, "curated") for b in CURATED_BOTS]
        cur.executemany("INSERT INTO known_bots (login, source) VALUES (%s, %s) ON CONFLICT DO NOTHING", bots)
        print(f"known_bots: {len(bots)} offered", flush=True)

    done = changed = 0
    try:
        for line in open(args.repos):
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            repo_id, full_name = int(parts[0]), parts[1]
            status, body = fetch_repo(s, full_name, floor, args.max_wait)
            try:
                cur.execute(UPSERT, to_row(repo_id, full_name, status, body))
                changed += cur.rowcount
            except psycopg2.errors.UniqueViolation:   # renamed repo now owns a name we already stored
                print(f"  skip {full_name}: name already used by another repo_id", flush=True)
            done += 1
            if done % 50 == 0:
                print(f"  {done} repos processed", flush=True)
    except RateLimited as e:
        print(f"stopped early: {e}", flush=True)
        print(f"repos: {done} processed, {changed} inserted or changed")
        sys.exit(3)
    print(f"repos: {done} processed, {changed} inserted or changed")


if __name__ == "__main__":
    main()
