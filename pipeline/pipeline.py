#!/usr/bin/env python3
"""Load Shopping Sessions inputs and write both daily Parquet tables."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

import duckdb


def records(path, width):
    field = rb'(?:[^,"\n]*|"[^,"\n]*")'
    row = re.compile(b",".join([field] * width))
    with path.open("rb") as source:
        next(source, None)
        for line in source:
            line = line.removesuffix(b"\n")
            if row.fullmatch(line):
                yield line


def load_log(con, data, temporary, name, columns, threads):
    source = data / name
    cleaned = temporary / name
    cleaned.mkdir()
    normalizer = os.environ.get("NORMALIZER")
    if normalizer:
        subprocess.run([normalizer, str(source), str(cleaned), str(len(columns)), str(threads)], check=True)
    else:
        for path in sorted(source.glob("*.csv")):
            with (cleaned / path.name).open("wb") as out:
                for line in records(path, len(columns)):
                    if b"\r" in line:
                        # Quote entire fields so DuckDB treats CR as data.
                        line = b",".join(b'"' + field.strip(b'"') + b'"' for field in line.split(b","))
                    out.write(line + b"\n")
    files = sorted(cleaned.glob("*.csv"))
    paths = [str(path) for path in files if path.stat().st_size]
    if paths:
        schema = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
        con.execute(f"""
            CREATE TABLE {name} AS
            SELECT * FROM read_csv(?, columns={{{schema}}}, auto_detect=false,
                                   header=false, delim=',', new_line='\\n')
        """, [paths])
    else:
        schema = ", ".join(f"{column} VARCHAR" for column in columns)
        con.execute(f"CREATE TABLE {name} ({schema})")
    for path in files:
        path.unlink()


def linked_ids(data, path):
    parents = {}
    sizes = {}
    names = {}

    def root(node):
        while node != parents[node]:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    for source in sorted((data / "links").glob("*.csv")):
        for line in records(source, 2):
            left, right = (field.strip(b'"') for field in line.split(b","))
            if not left or not right:
                continue
            for node in (left, right):
                if node not in parents:
                    parents[node], sizes[node], names[node] = node, 1, node
            left, right = root(left), root(right)
            if left == right:
                continue
            if sizes[left] < sizes[right]:
                left, right = right, left
            parents[right] = left
            sizes[left] += sizes[right]
            names[left] = min(names[left], names[right])

    with path.open("wb") as out:
        for node in parents:
            canonical = names[root(node)]
            if node != canonical:
                out.write(b'"' + node + b'","' + canonical + b'"\n')


def main():
    started = time.perf_counter()
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    threads = int(os.environ.get("NCPU", str(os.cpu_count() or 1)))
    memory_gb = int(os.environ.get("MEM_GB", "8"))
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as workspace:
        temporary = Path(workspace)
        with duckdb.connect() as con:
            con.execute("SET threads = ?", [threads])
            con.execute("SET memory_limit = ?", [f"{max(1, int(memory_gb * 0.7))}GB"])
            con.execute("SET temp_directory = ?", [str(temporary / "spill")])
            con.execute("SET preserve_insertion_order = false")
            con.execute("SET TimeZone = 'UTC'")
            manifest = json.loads((data / "manifest.json").read_text())
            con.execute("""
                CREATE TABLE limits AS SELECT epoch_ms(?::TIMESTAMPTZ) AS start_ms,
                                              epoch_ms(?::TIMESTAMPTZ) AS end_ms
            """, [manifest["window_start"], manifest["window_end"]])
            for name, columns in (("products", "product_id,category"), ("campaigns", "campaign_id,channel")):
                schema = ", ".join(f"'{column}': 'VARCHAR'" for column in columns.split(","))
                con.execute(f"""
                    CREATE TABLE {name} AS SELECT * FROM read_csv(?, columns={{{schema}}},
                                                                 header=true, auto_detect=false)
                """, [str(data / f"{name}.csv")])
            csv_path = temporary / "identities.csv"
            linked_ids(data, csv_path)
            if csv_path.stat().st_size:
                con.execute("""
                    CREATE TABLE identities AS SELECT * FROM read_csv(?, header=false, auto_detect=false,
                        columns={'id':'VARCHAR','person':'VARCHAR'}, new_line='\\n')
                """, [str(csv_path)])
            else:
                con.execute("CREATE TABLE identities(id VARCHAR, person VARCHAR)")
            csv_path.unlink()
            load_log(con, data, temporary, "events",
                     ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"), threads)
            load_log(con, data, temporary, "clicks", ("click_id", "user_id", "campaign_id", "ts"), threads)
            print(f"loaded inputs in {time.perf_counter() - started:.2f}s", file=sys.stderr)
            con.execute(Path(__file__).with_name("daily.sql").read_text())
            for name in ("user_daily", "campaign_daily"):
                target = output / name
                target.mkdir(parents=True, exist_ok=True)
                con.execute(f"COPY {name} TO ? (FORMAT PARQUET)", [str(target / "part-0.parquet")])
    print(f"wrote both tables in {time.perf_counter() - started:.2f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
