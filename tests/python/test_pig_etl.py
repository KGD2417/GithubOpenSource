"""Runs pig/etl_events.pig in local mode on the fixture (PLAN.md §13). Run inside edge:  pytest tests/python"""
import csv
import glob
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_pig_etl_on_fixture(tmp_path):
    out, dq = tmp_path / "out", tmp_path / "dq"
    subprocess.run(
        ["pig", "-x", "local", "-param", "dt=2025-09-01",
         "-param", f"input={ROOT}/tests/fixtures/events_small.json",
         "-param", f"bots_input={ROOT}/tests/fixtures/bots.txt", "-param", "bots_loader=PigStorage()",
         "-param", f"output={out}", "-param", "storer=PigStorage()", "-param", f"dq_out={dq}",
         "-param", f"jar={ROOT}/java/hadoop-jobs/target/pulse-hadoop.jar",
         str(ROOT / "pig/etl_events.pig")],
        check=True, capture_output=True, cwd=tmp_path)

    counts = {r[2]: int(r[3]) for f in glob.glob(f"{dq}/part*") for r in csv.reader(open(f), delimiter="\t")}
    assert counts == {"raw_lines": 40, "parsed_ok": 39, "clean_rows": 38, "bot_events": 7}

    rows = [r for f in glob.glob(f"{out}/part*") for r in csv.reader(open(f), delimiter="\t")]
    assert len(rows) == 38 and len({r[0] for r in rows}) == 38          # deduplicated on event_id
    bots = {r[4]: r[32] for r in rows if r[31] == "true"}               # actor_login -> bot_reason
    assert bots["k8s-ci-robot"] == "list" and bots["dependabot[bot]"] == "suffix"
    assert {r[33] for r in rows} >= {"PushEvent", "WatchEvent", "PullRequestEvent", "ForkEvent"}
    assert sum(r[30] == "trimmed" for r in rows) == 1                   # the 2026-format PR event
