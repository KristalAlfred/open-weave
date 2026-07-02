# open-weave

open-weave is a software-defined media contribution orchestrator — the control-plane
"brain" that accepts declarative desired state and reconciles it onto a wide
southbound ecosystem of media nodes, adapters, and existing transport systems.

It does **not** define a new media data plane. The northbound side speaks operator
intent; the southbound side normalizes NMOS, MXL, MCM, managed edge nodes, and
future vendor adapters into one observed/control model.

## Crates and binaries

- **`weave-core`** — shared domain types: definitions, nodes, endpoints, adapters,
  capabilities, observed state, reconcile reports.
- **`weave-cli`** (`weave`) — operator CLI for applying definitions and inspecting
  state.
- **`weave-northbound`** — northbound API for desired-state CRUD.
- **`weave-controller`** — reconciler loop. It reads desired state from northbound,
  observed state from southbound, and will become the planner/command emitter.
- **`weave-southbound`** — adapter/media-node-facing API for registration,
  telemetry, endpoint discovery, and future command streams.
- **`weave-media-node`** — managed edge agent for unmanaged media endpoints:
  phones, browser ingest, microphones, SRT/RIST/WebRTC/ST 2110 gateways, and local
  monitor outputs.

Future adapter crates can be split out as needed, for example:

- `weave-adapter-nmos`
- `weave-adapter-mxl-domain`
- `weave-adapter-mxl-k8s`
- `weave-adapter-mcm`

## Runtime shape

```text
operator/system
  -> weave / weave-northbound
  -> weave-controller
  -> weave-southbound
  -> weave-media-node or adapter implementations
  -> existing media systems and transports
```

The core rule is: **wide southbound ecosystem, narrow adapter contract**. A
southbound implementation may only discover, only report health, or fully
connect/provision resources depending on its capabilities.

## Quickstart

```sh
just build
just run-north       # 127.0.0.1:8080
just run-south       # 127.0.0.1:8081
just run-controller  # 127.0.0.1:8082 health endpoint
just run-node        # registers local-media-node, then serves health on 127.0.0.1:8090
just cli -- --help
```

> Phase 0 scaffolding: APIs are in-memory and the controller only reports desired
> vs observed counts. Planning, persistence, command streams, and real adapter
> implementations come next.
