# Network-impairment media test bench (docker-compose). See bench/README.md.
mod bench

default:
    @just --list

build:
    cargo build

test:
    cargo test
    just browser-check

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

lint:
    cargo clippy --all-targets --all-features

browser-check:
    node nodes/browser/check.mjs --check-only

run-north:
    cargo run -p weave-northbound

run-south:
    cargo run -p weave-southbound

run-controller:
    cargo run -p weave-controller

run-strom-adapter *ARGS:
    cargo run -p weave-adapter-strom -- {{ARGS}}

cli *ARGS:
    cargo run -p weave-cli -- {{ARGS}}

apply FILE="examples/stream.yaml":
    cargo run -p weave-cli -- apply -f {{FILE}}

get-streams:
    cargo run -p weave-cli -- get streams

delete-stream NAME:
    cargo run -p weave-cli -- delete stream {{NAME}}
