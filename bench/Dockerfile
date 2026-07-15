# syntax=docker/dockerfile:1
# Builds all open-weave binaries for the network-impairment bench.
FROM rust:1.88-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --bins && \
    mkdir -p /out && \
    for b in weave weave-northbound weave-southbound weave-controller weave-adapter-strom; do \
        cp "target/debug/$b" /out/; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --create-home --uid 10001 weave && \
    mkdir -p /var/lib/weave && chown weave:weave /var/lib/weave
COPY --from=builder /out/ /usr/local/bin/
USER weave
