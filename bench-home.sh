#!/bin/bash
# bench-home.sh — same protocol as bench-po.sh (warmup + best-of-3, NCPU=2,
# PROFILE=1, mimalloc via env) but with out/scratch dirs under the workspace
# instead of /tmp, because /tmp (512M tmpfs) fills up with concurrent builders.
# Usage: BIN=/path/to/binary bench-home.sh <label>
set -u
cd ~/workspace/buildathon-shopping-sessions || exit 1
BIN="${BIN:-$PWD/bin/micro2-v3}"
LABEL="$1"; shift
WORK="$PWD/micro2/bench-tmp"
mkdir -p "$WORK"
export DATA_DIR="$PWD/practice20m" NCPU="${NCPU:-2}" PROFILE=1
run_once() {
  local outdir scratchdir errfile
  outdir=$(mktemp -d "$WORK/out.XXXXXX")
  scratchdir=$(mktemp -d "$WORK/scratch.XXXXXX")
  errfile=$(mktemp "$WORK/run.XXXXXX.err")
  env "$@" OUTPUT_DIR="$outdir" SCRATCH_DIR="$scratchdir" "$BIN" >/dev/null 2>"$errfile"
  local rc=$?
  cp "$errfile" "$WORK/last.err"
  rm -rf "$outdir" "$scratchdir" "$errfile"
  return $rc
}
run_once "$@" || { echo "$LABEL: WARMUP FAILED"; tail -3 "$WORK/last.err"; exit 1; }
BEST=999
BEST_STAGES=""
BEST_WALL=""
for i in 1 2 3; do
  run_once "$@" || { echo "$LABEL run $i: FAILED"; tail -3 "$WORK/last.err"; exit 1; }
  TOTAL=$(grep "wrote parquet" "$WORK/last.err" | awk '{print $NF}')
  CPU=$(grep "^cpu " "$WORK/last.err" | awk '{print $NF}')
  STAGES=$(tr '\n' '|' < "$WORK/last.err")
  if awk -v a="$CPU" -v b="$BEST" 'BEGIN{exit !(a < b)}'; then BEST="$CPU"; BEST_WALL="$TOTAL"; BEST_STAGES="$STAGES"; fi
  echo "$LABEL run $i: wall $TOTAL cpu $CPU"
done
echo "$LABEL BEST: cpu $BEST wall $BEST_WALL  [$BEST_STAGES]"
