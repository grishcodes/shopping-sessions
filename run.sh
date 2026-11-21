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
        export MIMALLOC_LARGE_OS_PAGES="${MIMALLOC_LARGE_OS_PAGES-1}"
        # Keep freed memory committed for reuse instead of returning it to
        # the OS after a few ms: every stage frees large buffers that the
        # next stage reallocates, and a purge makes each reuse take fresh
        # first-touch page faults (kernel zeroing). Peak RSS is unchanged.
        # mimalloc 2.0.x (the Debian bookworm libmimalloc2.0 in this image)
        # reads MIMALLOC_ALLOW_DECOMMIT; 2.1+ reads MIMALLOC_PURGE_DELAY.
        # Each version ignores the other's name. Override either to compare.
        export MIMALLOC_ALLOW_DECOMMIT="${MIMALLOC_ALLOW_DECOMMIT-0}"
        export MIMALLOC_PURGE_DELAY="${MIMALLOC_PURGE_DELAY--1}"
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
