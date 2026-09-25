# syntax=docker/dockerfile:1
# Builds all open-weave binaries for the network-impairment bench.
FROM rust:1.88-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
# The target cache is shared by every build of this Dockerfile on the host,
# from any checkout, and cargo judges a workspace crate fresh by source mtimes.
# Touching the sources rebuilds the workspace crates from this checkout's code;
# dependencies stay cached.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    find crates examples -type f -exec touch {} + && \
    cargo build --bins && \
    mkdir -p /out && \
    for b in weave weave-northbound weave-southbound weave-controller weave-adapter-strom; do \
        cp "target/debug/$b" /out/; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --create-home --uid 10001 weave
COPY --from=builder /out/ /usr/local/bin/
USER weave
