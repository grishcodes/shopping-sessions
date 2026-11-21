#!/usr/bin/env python3
"""Replay equivalent datasets against a baseline, the DuckDB reference, and a candidate.

Example: python scripts/replay.py --baseline ./baseline --candidate ./candidate
Both commands receive DATA_DIR, OUTPUT_DIR and SCRATCH_DIR as environment variables.
"""

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import random
import shlex
import subprocess
import sys
import tempfile

from run_fixtures import SCHEMAS, load_actual, rows

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2028, 2, 29)
START = "2028-02-29T00:00:00Z"
MS = int(datetime.fromisoformat(START[:-1]).replace(tzinfo=timezone.utc).timestamp()) * 1000
WEEK = 604800000
HEADERS = {
    "events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
    "clicks": "click_id,user_id,campaign_id,ts",
    "links": "user_id_a,user_id_b",
}


def data(duplicates):
    aliases = [f"u{i}" for i in range(320)]
    links = list(zip(aliases, aliases[1:])) + [(aliases[-1], "U-unused")]
    events = []
    for i in range(2048):
        purchase = i % 8 == 0
        product = "p1" if i % 16 == 0 else "p2"
        events.append((f"e{i}", aliases[i % len(aliases)], str(MS + 1000 * (i % 900)), START,
                       "purchase" if purchase else "view", product, "10" if purchase else ""))
    # The earliest, invalid copy of poison must suppress a later valid copy for a different user.
    later = "2028-02-29T01:00:05Z"
    events.extend([("poison", "quiet ", "bad", START, "cart", "p1", ""),
                   ("poison", aliases[0], str(MS), later, "cart", "p1", ""),
                   ("second", aliases[0], str(MS + 3600001), START, "view", "p1", ""),
                   ("solo", "quiet ", str(MS), START, "cart", "p1", ""),
                   ("late", "quiet ", str(MS), "2028-03-01T00:00:01Z", "view", "p2", "")])
    clicks = [("k10", aliases[1], "c_1", str(MS)), ("k2", aliases[4], "c_2", str(MS)),
              ("old", aliases[5], "c_3", str(MS - WEEK)), ("new", aliases[-1], "c_2", str(MS + 1))]
    if duplicates:
        # All extra copies must leave the result unchanged, including a different user
        # in a later copy of a purchased event and many exact-tie duplicates.
        for e in events[:256]:
            events.append((e[0], "quiet ", e[2], later, "cart", "p2", ""))
        events.extend(events[:64])
        clicks.extend(clicks * 8)
        links.extend([(right, left) for left, right in links] + [("U-unused", "U-unused")])
    return {"events": events, "clicks": clicks, "links": links}


def write_input(path, *, seed, parts=1, shuffle=False, quote=False, duplicates=False):
    path.mkdir()
    (path / "manifest.json").write_text(json.dumps({"window_start": START, "window_end": "2028-03-14T00:00:00Z"}))
    (path / "products.csv").write_text("product_id,category\np1,alpha\np2,beta\n")
    (path / "campaigns.csv").write_text("campaign_id,channel\nc_1,search\nc_2,social\nc_3,email\n")
    rng = random.Random(seed)
    for log, records in data(duplicates).items():
        (path / log).mkdir()
        if shuffle:
            rng.shuffle(records)
        for part in range(parts + 1):
            # Keep a header-only file even when every data shard has rows.
            chunk = records[part::parts] if part < parts else []
            with (path / log / f"part-{part:03}.csv").open("w", encoding="utf-8", newline="") as f:
                f.write(HEADERS[log] + "\n")
                for record in chunk:
                    fields = ['"' + field + '"' if quote and rng.getrandbits(1) else field for field in record]
                    f.write(",".join(fields) + "\n")


def run(command, dataset, output):
    output.mkdir()
    scratch = output / "scratch"
    scratch.mkdir()
    env = dict(os.environ, DATA_DIR=str(dataset), OUTPUT_DIR=str(output),
               SCRATCH_DIR=str(scratch), NCPU="2", MEM_GB="2")
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{shlex.join(command)} on {dataset.name} exited {result.returncode}\n{result.stderr}")
    return {table: rows(load_actual(output / table, schema)) for table, schema in SCHEMAS.items()}


def compare(label, actual, expected):
    for table in SCHEMAS:
        if actual[table] != expected[table]:
            raise ValueError(f"{label}, {table}\nexpected {expected[table]}\nactual   {actual[table]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", default="native/target/release/shopping-sessions")
    parser.add_argument("--baseline", help="previous executable on the original rows; defaults to the candidate")
    parser.add_argument("--seed", default=257, type=int, help="deterministic shuffle and quoting seed")
    args = parser.parse_args()
    candidate = shlex.split(args.candidate)
    baseline = shlex.split(args.baseline) if args.baseline else candidate
    reference = [sys.executable, str(ROOT / "pipeline/pipeline.py")]
    expected = {
        "user_daily": [("U-unused", DAY, 2, 2049, 2560, "alpha"), ("quiet ", DAY, 1, 1, 0, None)],
        "campaign_daily": [("c_1", "search", DAY, 1, 1, 2560, 2), ("c_2", "social", DAY, 1, 0, 0, 2),
                           ("c_3", "email", DAY, 0, 0, 0, 1), ("organic", "organic", DAY, 1, 0, 0, 0)],
    }
    variants = {"original": {}, "order": {"shuffle": True}, "shards": {"parts": 9},
                "quoting": {"quote": True}, "duplicate-copies": {"duplicates": True},
                "combined": dict(parts=23, shuffle=True, quote=True, duplicates=True)}
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        for name, options in variants.items():
            dataset = folder / name
            write_input(dataset, seed=args.seed, **options)
            if name == "original":
                baseline_rows = run(baseline, dataset, folder / "baseline")
                compare("original baseline vs hand-calculated totals", baseline_rows, expected)
            reference_rows = run(reference, dataset, folder / f"{name}-duckdb")
            compare(f"{name} DuckDB vs original baseline", reference_rows, baseline_rows)
            actual = run(candidate, dataset, folder / f"{name}-candidate")
            compare(f"{name} candidate vs DuckDB", actual, reference_rows)
            print(f"PASS {name} (all user and campaign rows)")
    print(f"Six replay cases passed, seed {args.seed}")


if __name__ == "__main__":
    main()
