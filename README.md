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
- **`weave-controller`** — reconciler loop and the single owner of all state.
  Northbound and southbound are stateless proxies that call *into* it; it makes no
  outbound calls of its own. It derives a per-stream hop path and serves each
  node's desired hops for that node's adapter to pull. Also serves a live
  dashboard at `/ui` (backed by the `/view` JSON document) showing nodes,
  streams, and per-hop link conditions.
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

Requests flow left to right: the CLI calls northbound, adapters call southbound,
and both of those call the controller. The controller never calls back out.

The core rule is: **wide southbound ecosystem, narrow adapter contract**. A
southbound implementation may only discover, only report health, or fully
connect/provision resources depending on its capabilities.

## API versioning

Both control-plane contracts are served under **`/v1`**:

| Contract | Served by | Routes |
|---|---|---|
| operator (northbound) | northbound, controller | `/v1/streams`, `/v1/streams/{name}`, `/v1/streams/{name}/endpoints`, `/v1/status` |
| adapter (southbound) | southbound, controller | `/v1/nodes/register`, `/v1/nodes/{id}/heartbeat`, `/v1/nodes/{id}/desired`, `/v1/nodes`, `/v1/endpoints`, `/v1/state` |

The controller serves the union of both, because northbound and southbound are
stateless proxies onto it. One prefix covers both contracts: they are two halves
of the same control plane and move to a `/v2` together. The prefix is defined once
as `weave_core::API_V1`; servers nest their routes behind it and clients build
their paths from it.

The adapter contract is the one that matters most: operators attach their own
media nodes, including third-party adapters open-weave does not ship, and those
bind to `/v1` southbound.

Deliberately **not** versioned:

- **`/health`** on all three services — infrastructure, not contract.
  Healthchecks and load balancers address it directly.
- **`/`, `/ui`, `/view`** on the controller — the dashboard and its data source
  ship inside the controller binary and version with it. **`/view` carries no
  stability guarantee**: its shape follows whatever the embedded UI needs, and it
  may change in any release. Script against `/v1/status`, not `/view`.

`/status` *is* versioned: it is a scriptable rollup that people automate against
(the bench justfile does), so it belongs to the operator contract rather than to
the dashboard — even though, like the dashboard, it is unauthenticated.

There are no back-compat aliases: the previously unprefixed paths now `404`.

### Protocol version negotiation

A URL prefix tells a client where to send a request; it does not let the server
notice a stale adapter. So `POST /v1/nodes/register` also carries a
`protocol_version` field, which adapters set from `weave_core::PROTOCOL_VERSION`:

```json
{ "protocol_version": 1, "node": { "id": "strom-node-1", "...": "..." } }
```

The controller accepts only the version it speaks. Anything else — including an
absent field, which reads as `0` and marks an adapter predating the handshake — is
refused with `409 Conflict`, a body naming both versions, and a `warn` log line
naming the node id:

```json
{
  "error": "incompatible southbound protocol version",
  "node_id": "strom-node-1",
  "reported_protocol_version": 2,
  "supported_protocol_version": 1
}
```

Nothing about a rejected node is recorded: a registration the controller cannot
serve correctly is worse than none. `weave-adapter-strom` treats the `409` as
fatal and exits — retrying never converges — so a version mismatch surfaces as a
stopped container with a clear reason instead of a node that looks alive.
`PROTOCOL_VERSION` and `API_V1` move together: a breaking southbound change bumps
both.

## Authentication

Each API surface is protected by one shared bearer token, supplied through the
environment. Requests present it as `Authorization: Bearer <token>`; anything else
gets `401` with a `WWW-Authenticate: Bearer` challenge. Tokens are compared in
constant time and never logged.

| Variable | Presented by | Accepted by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | operators, the `weave` CLI (`--token`), northbound → controller | northbound, controller |
| `WEAVE_SOUTHBOUND_TOKEN` | adapters and media nodes, southbound → controller | southbound, controller |

The controller backs both surfaces, so it needs both variables and requires the
one matching the surface a route belongs to — an adapter's southbound token
cannot create streams. Northbound and southbound each re-present their own
surface token on the hop to the controller, so one secret covers a surface end to
end. Nodes may instead carry the token in their config file as
`node.southbound_token`, which takes precedence over the environment.

**Services fail closed.** A service whose token variable is unset or empty
refuses to start rather than serve unauthenticated traffic. For local development
set `WEAVE_AUTH_DISABLED=1` to opt out explicitly; only `1` or `true` disable it,
so `WEAVE_AUTH_DISABLED=0` leaves authentication on.

Left unauthenticated on purpose:

- **`/health`** on every service — compose healthchecks and load balancers need it.
- **The controller dashboard** (`/`, `/ui`, `/view`) and the `/v1/status` rollup
  it shares its data with. The dashboard is browser-loaded and polls `/view`,
  which a bearer token cannot carry without a cookie/session mechanism or a
  reverse proxy. `/view` exposes topology and allocated ports, so **do not expose
  the controller port publicly** — keep it on a private network or put a reverse
  proxy in front of it. The controller's `/v1/streams` and `/v1/nodes` API routes
  *are* authenticated, so an exposed port leaks read-only dashboard data rather
  than write access.

There is no TLS: terminate it at a reverse proxy. Per-node tokens issued at
registration and mTLS are follow-ups, not implemented here.

## Strom adapter and drift policy

Strom is the first media runtime target. `weave-adapter-strom` runs beside one
Strom instance, registers it with southbound, polls `/api/flows`, and reports
Strom flows as observed endpoints. It pulls its node's desired hops from
southbound and translates them into Strom flow create/start/delete calls.

Strom UI/API edits are **drift**, like direct edits to Kubernetes managed
objects. The source of truth is open-weave desired state; out-of-band Strom
changes should be reconciled back or explicitly adopted into desired state.

## Quickstart

Every service needs its surface token (see [Authentication](#authentication)), so
export both first — or set `WEAVE_AUTH_DISABLED=1` to run without any:

```sh
export WEAVE_NORTHBOUND_TOKEN=$(openssl rand -hex 32)
export WEAVE_SOUTHBOUND_TOKEN=$(openssl rand -hex 32)
```

```sh
just build
just run-north           # 127.0.0.1:9080
just run-south           # 127.0.0.1:8081
just run-controller      # 127.0.0.1:8082 health endpoint
just run-strom-adapter   # registers Strom via southbound http://127.0.0.1:8081
just run-node            # future first-party edge node stub
just cli -- --help
```

The CLI picks `WEAVE_NORTHBOUND_TOKEN` up from the environment; `--token`
overrides it.
