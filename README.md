# Shopping Sessions

The default engine reads UTF-8 CSV shards directly into typed Rust records, resolves linked IDs, selects the earliest copy of each event, and writes the two daily Parquet tables. Campaign assists use merged seven-day click intervals. The image also includes a DuckDB reference implementation. Select it with `ENGINE=duckdb` or select Rust with `ENGINE=native`.

To build the native executable locally, run `cargo build --release --locked --manifest-path native/Cargo.toml` with Rust 1.85 or newer. Set `DATA_DIR`, `OUTPUT_DIR` and `NCPU` before running `native/target/release/shopping-sessions`. Use an empty output directory for each run, as the scorer does. `PROFILE=1` prints cumulative elapsed time at each stage. `EVENT_DEDUP=sort` selects the default sort-based implementation and `EVENT_DEDUP=hash` selects the hash-table variant.

## Local verification

Install the runtime and fixture checker with Python 3.10 or newer.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt polars==2.1.0

SCRATCH_DIR="$PWD/scratch" NCPU=4 MEM_GB=6 \
  python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py

make run-dev PLATFORM=linux/amd64 CPUS=4 MEM=6g
make check-dev
```

Build the organizer's generator with Rust 1.85 or newer using `cargo build --release --locked`. Generate the public inputs with `generate --seed 1 --events 1000000 --clicks 250000 --output dev`. Extract the official dev answer into `dev-answer/`.

On Apple silicon, use `PLATFORM=linux/arm64` for fast container iteration and `linux/amd64` for the submission smoke test. Use distinct `IMAGE` values if building both architectures. Keep generated input, answers, and benchmark output outside Git.

To use the Rust scanner in a direct Python run, build it with Rust 1.85 or newer.

```sh
mkdir -p .local
rustc --edition=2024 -C opt-level=3 -o .local/normalize native/normalize.rs
export NORMALIZER="$PWD/.local/normalize"
```
