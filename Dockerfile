FROM rust:1.85.1-slim-bookworm AS normalizer
COPY native/normalize.rs /src/normalize.rs
RUN rustc --edition=2024 -C opt-level=3 -o /normalize /src/normalize.rs

WORKDIR /src/engine
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src/ src/
ARG TARGETARCH
RUN if [ "$TARGETARCH" = amd64 ]; then \
        cargo rustc --release --locked -- -C target-cpu=x86-64-v3; \
    else cargo build --release --locked; fi

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=normalizer /normalize /app/normalize
COPY --from=normalizer /src/engine/target/release/shopping-sessions /app/shopping-sessions
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
