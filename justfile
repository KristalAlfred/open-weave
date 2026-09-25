# Network-impairment media test bench (docker-compose). See bench/README.md.
mod bench

default:
    @just --list

build:
    cargo build

test:
    cargo test --workspace
    just browser-check

fmt:
    cargo fmt

fmt-check:
    cargo fmt --all --check

contracts:
    cargo run -p weave-core --bin generate-contracts

contracts-check:
    cargo test -p weave-core contracts::tests

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

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

apply-set OWNER FILE="examples/stream-set.yaml":
    cargo run -p weave-cli -- apply-set {{OWNER}} -f {{FILE}}

plan FILE="examples/stream.yaml":
    cargo run -p weave-cli -- plan -f {{FILE}}

get-streams:
    cargo run -p weave-cli -- get streams

get-stream NAME:
    cargo run -p weave-cli -- get stream {{NAME}}

get-nodes:
    cargo run -p weave-cli -- get nodes

get-status:
    cargo run -p weave-cli -- get status

get-endpoints NAME:
    cargo run -p weave-cli -- get endpoints {{NAME}}

delete-stream NAME:
    cargo run -p weave-cli -- delete stream {{NAME}}

# Backlog items by status. See BACKLOG.md.
board:
    #!/usr/bin/env bash
    set -euo pipefail
    awk '
      FNR == 1 { fm = 0; files[++n] = FILENAME }
      /^---$/ { fm++; next }
      fm == 1 && /^[a-z_]+:/ {
        key = $0; sub(/:.*/, "", key)
        val = $0; sub(/^[^:]*:[ \t]*/, "", val); gsub(/^"|"$/, "", val)
        f[FILENAME, key] = val
      }
      END {
        split("in-progress blocked todo done dropped", order, " ")
        for (i in order) rank[order[i]] = i
        for (i = 1; i <= n; i++) status[f[files[i], "id"]] = f[files[i], "status"]
        for (i = 1; i <= n; i++) {
          file = files[i]; id = f[file, "id"]; st = f[file, "status"]
          deps = f[file, "depends_on"]; gsub(/[][ ]/, "", deps)
          waits = ""
          m = split(deps, d, ",")
          for (j = 1; j <= m; j++) if (status[d[j]] != "done") waits = waits " " d[j]
          note = (st == "todo" && waits != "") ? "  (waits on" waits ")" : ""
          assignee = f[file, "assignee"] == "" ? "-" : f[file, "assignee"]
          num = id; sub(/^[A-Z]+-/, "", num)
          printf "%d\t%d\t%-6s %-11s %-12s %-10s %s%s\n", rank[st] ? rank[st] : 9, num, id, st, f[file, "type"], assignee, f[file, "title"], note
        }
      }
    ' backlog/*.md | sort -t $'\t' -k1,1n -k2,2n | cut -f3-
