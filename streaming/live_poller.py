#!/usr/bin/env python3
"""Optional near-live feed: GitHub public events API -> Kafka gh.events.live (PLAN.md §6.3).

Its own topic, never gh.events: the topology windows on event time, so mixing today's events into a
2025 replay would move the watermark past the replay and every replayed event would count as late.
Run the topology on it with:  storm jar pulse-storm.jar pulse.storm.PulseTopology pulse-live gh.events.live

  live_poller.py [--interval 60] [--pages 3]          (GITHUB_TOKEN from the environment)

Polite polling: honours X-Poll-Interval, sends If-None-Match (a 304 costs no quota), stops before the
rate limit runs out, and drops event ids it has already sent. The live API returns today's trimmed
payloads (Oct 2025 change), but Storm only uses top-level fields, so the topology handles both.
"""
import argparse
import json
import os
import time
from collections import OrderedDict

import requests
from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument("--rounds", type=int, default=0, help="stop after N polls (0 = forever)")
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="gh.events.live")
    a = ap.parse_args()

    s = requests.Session()
    s.headers.update({"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
    if os.environ.get("GITHUB_TOKEN"):
        s.headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    p = Producer({"bootstrap.servers": a.bootstrap})
    etags, seen, rounds = {}, OrderedDict(), 0
    while True:
        interval, sent = a.interval, 0
        for page in range(1, a.pages + 1):
            r = s.get("https://api.github.com/events", params={"per_page": 100, "page": page},
                      headers={"If-None-Match": etags.get(page, "")}, timeout=30)
            interval = max(interval, int(r.headers.get("X-Poll-Interval", 60)))
            remaining = int(r.headers.get("X-RateLimit-Remaining", 1))
            if r.status_code == 304:
                continue
            if r.status_code != 200:
                print(f"HTTP {r.status_code}: {r.text[:120]}", flush=True)
                break
            etags[page] = r.headers.get("ETag", "")
            for e in r.json():
                if e["id"] in seen:
                    continue
                seen[e["id"]] = None
                p.produce(a.topic, key=str(e["repo"]["id"]), value=json.dumps({
                    "id": e["id"], "type": e["type"], "created_at": e["created_at"],
                    "actor_id": e["actor"]["id"], "actor_login": e["actor"]["login"],
                    "repo_id": e["repo"]["id"], "repo_name": e["repo"]["name"],
                    "org": (e.get("org") or {}).get("login"), "source": "live"}))
                sent += 1
            if remaining < 10:
                print("rate limit nearly exhausted; waiting for reset", flush=True)
                interval = max(interval, int(r.headers.get("X-RateLimit-Reset", time.time() + 600)) - int(time.time()))
                break
        while len(seen) > 20000:
            seen.popitem(last=False)
        p.flush()
        rounds += 1
        print(f"poll {rounds}: {sent} new events; next in {interval}s", flush=True)
        if a.rounds and rounds >= a.rounds:
            return
        time.sleep(interval)


if __name__ == "__main__":
    main()
