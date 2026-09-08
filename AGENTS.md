# AGENTS.md

open-weave is a control plane for live media contribution. An operator applies a
stream manifest; the controller plans SRT/WHIP/WHEP hops across registered media
nodes and each node's adapter builds them. `README.md` is the reference for every
contract; this file is the short version plus the things that are easy to get
wrong.

## Commands

```sh
just build            # cargo build
just test             # cargo test --workspace, then node nodes/browser/check.mjs
just lint             # clippy --workspace --all-targets --all-features -D warnings
just fmt-check        # cargo fmt --all --check
```

CI runs `fmt-check`, `lint` and `test` on the workspace, plus the browser-node
check as a separate job. Run all four before proposing a change.

Tests are `#[cfg(test)]` modules beside the code they cover; there is no
`tests/` directory.

## Running it

Every service refuses to start without its surface token, so either export both
or opt out explicitly:

```sh
export WEAVE_NORTHBOUND_TOKEN=$(openssl rand -hex 32)
export WEAVE_SOUTHBOUND_TOKEN=$(openssl rand -hex 32)
# or, for local work only:
export WEAVE_AUTH_DISABLED=1
```

```sh
just run-north        # 127.0.0.1:9080
just run-south        # 127.0.0.1:8081
just run-controller   # 127.0.0.1:8082, dashboard at /ui
just run-strom-adapter --config examples/node.yaml
just apply            # examples/stream.yaml, through northbound
just get-streams
```

The adapter needs a node config from `--config` or `WEAVE_NODE_CONFIG` and will
not start without one.

## Layout

| Crate | What it is |
|---|---|
| `crates/core` (`weave-core`) | Shared domain types, `API_V1`, `PROTOCOL_VERSION`, auth middleware |
| `crates/cli` (`weave`) | Operator CLI |
| `crates/northbound` | Operator-facing desired-state API |
| `crates/controller` | Reconciler, sole owner of state, dashboard |
| `crates/southbound` | Adapter-facing API |
| `crates/adapter-strom` | Southbound adapter for Strom |
| `crates/strom` | HTTP client for Strom's own API |

`nodes/browser/` is a media node that runs in a page. `bench/` is a
docker-compose stack of real Strom instances behind per-node `netem` routers.

## What to get right

- **The controller owns all state.** Northbound and southbound are stateless
  proxies that call into it. The controller answers no request by calling out;
  its one outbound call is the node lifecycle webhook, which is fire-and-forget
  and off unless `WEAVE_WEBHOOK_URL` is set.
- **Version mismatches are refused, not smoothed over.** `API_V1` moves when
  routes change, `PROTOCOL_VERSION` when payloads do. A registration carrying
  the wrong `protocol_version` gets `409` and is not recorded. There are no
  back-compat aliases and no deprecation window at `0.1.0`.
- **Formats are declared, not discovered.** An absent `format` means unknown,
  not wrong, and nothing is inferred from it. A `format` is fixated; an
  `accepts` is a constraint set. A mismatch is reported on the stream and the
  media still flows.
- **Manifests name nodes, never addresses or transports between them.** The
  controller resolves addresses and socket roles at plan time from what each
  node declared about its own reachability.
- **Strom edits made outside open-weave are drift**, as with direct edits to
  Kubernetes-managed objects. Desired state is the source of truth.

## What is not built

`BACKLOG.md` is the authority on gaps, with the evidence behind each one. Do not
infer from the code that something works; if it is listed there, it does not.
Not implemented today: TLS, controller HA, per-node tokens, format conversion,
and every adapter except Strom.

Anything only verified on the `bench/` stack is verified there and nowhere else.
Say which one a claim rests on.

## Docs

`README.md` and `BACKLOG.md` are written plainly: short sentences, no hype, no
restated conclusions. Match that. State what the code does rather than why
someone chose it, and keep new comments to the ones a reader could not get from
the code itself.
