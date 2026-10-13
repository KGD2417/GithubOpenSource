#!/usr/bin/env python3
"""Replay GH Archive hours into Kafka at an accelerated, configurable speed (PLAN.md §6.3).

  replay_producer.py --date 2025-09-01 --hours 12-14 --speed 60 [--loop] [--limit N]

Reads each hour from the HDFS raw zone, orders its events by created_at, and sends a slim JSON envelope
to topic gh.events keyed by repo_id (so one repo's events stay in order on one partition). The gap
between consecutive events is divided by --speed: 60 replays an hour in a minute. Storm windows on the
event's own created_at, so the analytics stay in "GitHub time" whatever the speed.
"""
import argparse
import gzip
import io
import json
import subprocess
import time

from confluent_kafka import Producer

SLIM = ("id", "type", "created_at")


def hour_events(date, hour):
    path = f"/gh/raw/events/dt={date}/{date}-{hour}.json.gz"
    raw = subprocess.run(["hdfs", "dfs", "-cat", path], check=True, capture_output=True).stdout
    events = []
    for line in io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw)), encoding="utf-8"):
        try:
            e = json.loads(line)
            events.append({
                "id": e["id"], "type": e["type"], "created_at": e["created_at"],
                "actor_id": e["actor"]["id"], "actor_login": e["actor"]["login"],
                "repo_id": e["repo"]["id"], "repo_name": e["repo"]["name"],
                "org": (e.get("org") or {}).get("login"), "action": (e.get("payload") or {}).get("action"),
                "source": "replay"})
        except (ValueError, KeyError, TypeError):
            continue                                   # malformed line: skip, like the batch ETL
    events.sort(key=lambda e: e["created_at"])
    return events


def epoch(ts):
    return time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--hours", default="12-12")
    ap.add_argument("--speed", type=float, default=60.0)
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    ap.add_argument("--bootstrap", default="kafka:9092")
    a = ap.parse_args()
    h0, h1 = (int(x) for x in a.hours.split("-"))

    p = Producer({"bootstrap.servers": a.bootstrap, "linger.ms": 50, "compression.type": "lz4",
                  "queue.buffering.max.messages": 200000})
    sent, started = 0, time.time()
    while True:
        for h in range(h0, h1 + 1):
            events = hour_events(a.date, h)
            print(f"hour {a.date}-{h}: {len(events)} events, replaying at {a.speed:g}x", flush=True)
            t_first, wall_first = epoch(events[0]["created_at"]), time.time()
            for e in events:
                due = wall_first + (epoch(e["created_at"]) - t_first) / a.speed
                if (delay := due - time.time()) > 0:
                    time.sleep(delay)
                p.produce("gh.events", key=str(e["repo_id"]), value=json.dumps(e, separators=(",", ":")))
                sent += 1
                if sent % 5000 == 0:
                    p.poll(0)
                    print(f"  sent {sent} ({sent / (time.time() - started):.0f}/s)", flush=True)
                if a.limit and sent >= a.limit:
                    p.flush()
                    print(f"done: {sent} events")
                    return
            p.flush()
        if not a.loop:
            break
    print(f"done: {sent} events in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
