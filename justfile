# Network-impairment media test bench (docker-compose). See bench/README.md.
mod bench

default:
    @just --list

build:
    cargo build

test:
    cargo test

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

lint:
    cargo clippy --all-targets --all-features

run-north:
    cargo run -p weave-northbound

run-south:
    cargo run -p weave-southbound

run-controller:
    cargo run -p weave-controller

run-node *ARGS:
    cargo run -p weave-media-node -- {{ARGS}}

run-strom-adapter *ARGS:
    cargo run -p weave-adapter-strom -- {{ARGS}}

cli *ARGS:
    cargo run -p weave-cli -- {{ARGS}}
