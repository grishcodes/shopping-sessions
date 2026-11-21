FROM rust:1.85.1-slim-bookworm AS normalizer
COPY native/normalize.rs /src/normalize.rs
RUN rustc --edition=2024 -C opt-level=3 -o /normalize /src/normalize.rs

WORKDIR /src/engine
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src/ src/
RUN cargo build --release --locked && cp target/release/shopping-sessions /tmp/shopping-sessions-base
# Second build for x86-64-v3 (AVX2/BMI2/popcnt): enables the PEXT path in
# Keys::id and AVX2 auto-vectorization. RUSTFLAGS changes fingerprints, so
# this rebuilds the crates (~2-3 min). run.sh picks the v3 binary only when
# the run CPU supports it, falling back to the baseline otherwise.
RUN RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked && cp target/release/shopping-sessions /tmp/shopping-sessions-v3

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends libmimalloc2.0=2.0.9+ds-2 \
    && rm -rf /var/lib/apt/lists/*

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=normalizer /normalize /app/normalize
COPY --from=normalizer /tmp/shopping-sessions-base /app/shopping-sessions
COPY --from=normalizer /tmp/shopping-sessions-v3 /app/shopping-sessions-v3
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NORMALIZER=/app/normalize

WORKDIR /app
CMD ["/app/run.sh"]
