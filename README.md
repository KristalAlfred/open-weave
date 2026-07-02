# open-weave

open-weave is a software-defined media contribution orchestrator — the control-plane
"brain" that accepts declarative *definitions* of desired state and drives a fleet of
media nodes toward that state. Operators describe what they want on the northbound
side; the control plane reconciles it onto media nodes over the southbound side.

## Crates

- **`weave-core`** — shared domain types (definitions / desired state, node descriptors).
- **`weave-cli`** (`weave`) — operator CLI for applying definitions and inspecting state.
- **`weave-northbound`** — northbound API: accepts desired-state definitions from
  operators and systems.
- **`weave-southbound`** — southbound API: the media-node-facing side that drives nodes
  toward desired state.

## Why two binaries

Northbound and southbound run as separate binaries. The northbound surface is a
request/response HTTP API for operators. The southbound surface is stateless HTTP
today, but is expected to grow a persistent-connection transport (gRPC streaming or
WebSocket) for real-time reconciliation and telemetry. Keeping it separate lets that
transport evolve without touching the northbound binary.

## Quickstart

```sh
just build       # cargo build
just run-north   # start the northbound API on 127.0.0.1:8080
just run-south   # start the southbound API on 127.0.0.1:8081
just cli -- --help
```

> Phase 0 scaffolding: handlers are stubs and no reconciliation logic exists yet.
