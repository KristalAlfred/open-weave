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
- **`weave-controller`** — reconciler loop. It reads desired streams from
  northbound and observed state from southbound, derives a per-stream hop path,
  and writes desired hops to southbound per node for the adapters to realise.
  Serves a live dashboard at `/ui` (backed by the `/view` JSON document) showing
  nodes, streams, and per-hop link conditions.
- **`weave-southbound`** — adapter/media-node-facing API for registration,
  telemetry, endpoint discovery, and future command streams.
- **`weave-adapter-strom`** — southbound adapter for existing
  [Strom](https://github.com/Eyevinn/strom) media runtimes.
- **`weave-media-node`** — future managed edge agent for unmanaged media endpoints
  that do not fit an existing runtime.

Adapter crates can be split out as needed, for example:

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
  -> weave-adapter-strom or other adapter implementations
  -> Strom, existing media systems, and transports
```

The core rule is: **wide southbound ecosystem, narrow adapter contract**. A
southbound implementation may only discover, only report health, or fully
connect/provision resources depending on its capabilities.

## Strom adapter and drift policy

Strom is the first media runtime target. `weave-adapter-strom` runs beside one
Strom instance, registers it with southbound, polls `/api/flows`, and reports
Strom flows as observed endpoints. It pulls its node's desired hops from
southbound and translates them into Strom flow create/start/delete calls.

Strom UI/API edits are **drift**, like direct edits to Kubernetes managed
objects. The source of truth is open-weave desired state; out-of-band Strom
changes should be reconciled back or explicitly adopted into desired state.

## Quickstart

```sh
just build
just run-north           # 127.0.0.1:9080
just run-south           # 127.0.0.1:8081
just run-controller      # 127.0.0.1:8082 health endpoint
just run-strom-adapter   # registers Strom via southbound http://127.0.0.1:8081
just run-node            # future first-party edge node stub
just cli -- --help
```
