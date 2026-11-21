#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
case "${ENGINE:-native}" in
    native)
        # Set LD_PRELOAD to an empty value to measure the system allocator.
        export LD_PRELOAD="${LD_PRELOAD-libmimalloc.so.2}"
        # Let mimalloc back its heap with large OS pages when available;
        # falls back silently when huge pages are not configured.
        export MIMALLOC_ALLOW_LARGE_OS_PAGES="${MIMALLOC_ALLOW_LARGE_OS_PAGES-1}"
        # x86-64-v3 binary (AVX2/BMI2) when the run CPU supports it, else the
        # baseline build. Both are the same source; only codegen differs.
        if grep -q " avx2 " /proc/cpuinfo && grep -q " bmi2 " /proc/cpuinfo; then
            exec /app/shopping-sessions-v3
        else
            exec /app/shopping-sessions
        fi
        ;;
    duckdb) exec python3 /app/pipeline/pipeline.py ;;
    *) echo "unknown ENGINE: $ENGINE" >&2; exit 2 ;;
esac
