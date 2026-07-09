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

apply FILE="examples/stream.yaml":
    cargo run -p weave-cli -- apply -f {{FILE}}

get-streams:
    cargo run -p weave-cli -- get streams

# --- full-loop demo (docker bench + northbound + controller actuation) ---

# Build binaries, bring up the bench (egress-only), start northbound + controller.
demo-up:
    cargo build
    bash scripts/demo.sh up

# Apply the contribution intent; the controller creates+starts the Strom flow.
demo-apply:
    bash scripts/demo.sh apply

# Resolve the weave-managed flow by name and print srt-stats for both hops.
demo-stats:
    bash scripts/demo.sh stats

# Controller's own /status view (reconcile status + per-flow telemetry).
demo-status:
    bash scripts/demo.sh status

# Inject packet loss on the impaired ingress<->egress hop.
demo-loss pct:
    bash bench/scripts/netem.sh loss {{pct}}

# Clear all impairment.
demo-heal:
    bash bench/scripts/netem.sh clear

# Stop weave processes and tear the bench down.
demo-down:
    bash scripts/demo.sh down
