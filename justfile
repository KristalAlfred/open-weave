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

cli *ARGS:
    cargo run -p weave-cli -- {{ARGS}}
