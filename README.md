# open-weave

open-weave takes a file describing the live media streams you want and makes them
happen. It plans the SRT, WHIP, WHEP and RIST links between your media nodes,
tells each node's adapter what to build, and keeps reconciling as nodes come and
go. It works out which end of a link dials the other, and inserts a relay hop
when both ends sit behind NAT.

```yaml
name: cam1-to-studio
source:
  srt: { node: remote-site }
destinations:
  - id: studio
    srt: { node: studio }
```

`weave apply -f` that, and `weave get streams` reports it `flowing` once the
nodes it names have registered and their hops are up.

It does **not** define a new media data plane, and no media passes through
open-weave itself. The northbound side speaks operator intent; the southbound
side normalizes media runtimes into one observed/control model. One runtime is
implemented: [Strom](https://github.com/Eyevinn/strom), via
`weave-adapter-strom`.

## Use it for

- Live contribution feeds over SRT, WHIP or WHEP across more than a couple of
  sites, declared in a file instead of clicked into each box.
- Links where one or both ends sit behind NAT and you would rather not work out
  the relay yourself.
- Fronting [Strom](https://github.com/Eyevinn/strom) instances, or whatever else
  you run once it has an adapter.

## Not for

- Moving or converting media. Nothing transcodes, resamples or remuxes; a format
  mismatch is reported, not fixed.
- File-based or VOD work. Every contract here describes live links between nodes.
- Deciding what to route. Scheduling, bookings and who gets which feed belong to
  an application that drives open-weave through northbound.
- Production, yet. No TLS, and one Postgres under every controller — see
  [Status](#status).

## Status

Early, and version `0.1.0` means it. The HTTP contracts can change without a
deprecation window. Adapters declare `PROTOCOL_VERSION`, and a mismatch is
refused rather than smoothed over.

Built: the three control-plane services, the `weave` CLI, one southbound adapter
(`weave-adapter-strom`), and a browser node. Links carry SRT, WHIP, WHEP or
RIST, and the controller plans NAT traversal through relay nodes. SRT links
between nodes are encrypted with keys the controller derives. Several controllers
can share one Postgres: one leads and the others stand by. All of it is verified on
the docker-compose bench in `bench/`, which runs real Strom instances behind
per-node `netem` routers, and nowhere else. Two things are exceptions, covered
only by planner and status tests: redundant paths, since no shipped node merges
two paths, and WHIP senders and WHEP players outside open-weave, since the bench
has no such peer.

Not built: TLS, Postgres HA, format conversion, and any adapter other than
Strom. The items in `backlog/` list the known gaps with the
evidence behind each one.

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
- **`weave-southbound`** — adapter-facing API for registration, telemetry,
  and endpoint discovery.
- **`weave-adapter-strom`** — southbound adapter for existing
  [Strom](https://github.com/Eyevinn/strom) media runtimes.

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
and both of those call the controller. The controller answers no request by
calling back out; its one outbound call is the webhook below,
which is fire-and-forget and off unless configured. With Postgres, several
controllers can share one database, and only the one holding the lease serves
(see [Controller failover](#controller-failover)).

The core rule is: **wide southbound ecosystem, narrow adapter contract**. A
southbound implementation may only discover, only report health, or fully
connect/provision resources depending on its capabilities.

## HTTP API

Both control-plane contracts use flat routes:

| Contract | Served by | Routes |
|---|---|---|
| operator (northbound) | northbound, controller | `/streams`, `/streams/{name}`, `/stream-sets`, `/stream-sets/{owner}`, `/streams/{name}/endpoints`, `/stream-plans`, `/nodes`, `/status` |
| adapter (southbound) | southbound, controller | `/nodes/register`, `/nodes/{id}/heartbeat`, `/nodes/{id}/desired`, `/nodes`, `/endpoints`, `/state` |

The controller serves the union of both because northbound and southbound are
stateless proxies onto it. There is no URL version before the project declares a
stable release. Routes, payloads, and error shapes may change between pre-release
versions without aliases or a deprecation window.

`GET /nodes` lists registered nodes for operator and adapter reads. A node the
controller has not heard from is first marked `offline`, then removed from the
listing and from the store:

| Variable | Default | Meaning |
|---|---|---|
| `WEAVE_NODE_TTL_SECS` | `15` | Seconds without a heartbeat before a node reads `offline`. |
| `WEAVE_NODE_FORGET_SECS` | `300` | Seconds without a heartbeat before an offline node is removed. |

A node that a stored stream names as its source, a destination or a `via` relay
is never removed, and stays listed as `offline`. A removed node's next heartbeat
gets `404 node_not_found`, and both shipped nodes then register again. For a
node loaded from the store, both intervals count from the controller's start,
or from the moment it took the lease.

`GET /nodes/{id}/desired` returns the full list of hops the last reconcile tick
computed for that node. A node that tick did not cover, because it is unknown or
registered after the tick, gets `404 node_not_found`. A reconciled node with
nothing to run gets `200 []`, and its adapter removes every hop it manages. Only
a `2xx` is an answer: an adapter keeps what it runs on any other response, or
when southbound cannot be reached, and asks again on its next poll. Both shipped
nodes do this. The controller runs its first tick before it serves requests, so
after a restart with `DATABASE_URL` set it serves the stored hops from the first
request. The in-memory store starts empty, so after a restart without
`DATABASE_URL` each node is told to run nothing once it has re-registered.

`GET /streams` lists stream resources. `GET /streams/{name}` returns one
or `404 stream_not_found`. A resource contains its desired definition under
`spec`, its current `generation`, and an `owner` when it belongs to a stream
set. A single-resource response also carries an opaque `ETag`.

`POST /streams` creates with `If-None-Match: *` or replaces with the current
`If-Match` ETag. `DELETE /streams/{name}` also requires the current
`If-Match`. A missing precondition returns `428 precondition_required`; a stale
one returns `412 precondition_failed`. The server does not retry or merge a
conflicting write. Reapplying an identical spec returns `changed: false` and
preserves both generation and ETag.

`GET /stream-sets` lists ownership sets. `GET /stream-sets/{owner}`
returns one with its streams and a set ETag. `PUT /stream-sets/{owner}`
atomically applies its `streams` list. Create uses `If-None-Match: *`; updates
use the current set `If-Match` ETag. The owner follows the same identifier rule
as streams and nodes.

With `prune: false`, owned streams omitted from the request remain unchanged.
With `prune: true`, omitted streams owned by that set are deleted. Pruning never
deletes an unmanaged stream or one owned by another set. A requested name that
already exists outside the set returns `409 ownership_conflict`; stream sets do
not adopt existing resources. An owned stream cannot be changed or deleted
through the single-stream routes, which return `409 stream_owned`.

The set write and all member writes commit together. An identical apply returns
`changed: false` and preserves the set ETag plus every member's generation and
ETag. An empty `streams` list is accepted only with `prune: true`; the empty set
remains addressable for later conditional writes.

`weave apply-set OWNER -f FILE` applies this contract from YAML. The file is a
strict object with a required `streams` list and optional `prune` boolean, which
defaults to `false`; unknown fields are rejected. The CLI reads the set first,
then creates with `If-None-Match: *` or updates with its current ETag. It does not
retry a stale write or adopt a name outside the set. See
`examples/stream-set.yaml`, or run `just apply-set OWNER`.

`POST /stream-plans` accepts the same stream definition as apply and changes
no state. It validates the definition, plans it alongside the current desired
streams against the current nodes, and returns `placed`, `unplaced`, or
`disabled` with the resolved nodes, desired hops, endpoints, and any placement
reason. It reads the hops nodes report running, as a reconcile does, so a
preview keeps a bridge on the relay running it (see
[Capabilities and topology](#capabilities-and-topology)).
Use `weave plan -f examples/stream.yaml` or `just plan`.

The adapter contract is the one that matters most: operators attach their own
media nodes, including third-party adapters open-weave does not ship, and those
bind to the southbound routes.

Outside the control-plane contracts:

- **`/health`** on all three services — infrastructure, not contract.
  Healthchecks and load balancers address it directly.
- **`/`, `/ui`, `/view`** on the controller — the dashboard and its data source
  ship inside the controller binary and version with it. **`/view` carries no
  stability guarantee**: its shape follows whatever the embedded UI needs, and it
  may change in any release. Script against `/status`, not `/view`.

`/status` is a scriptable rollup in the operator contract. The unauthenticated
copy is the controller's, which the dashboard shares; northbound's copy sits
behind the bearer. Version-prefixed routes such as `/v1/status` are not aliases
and return `404`.

### Protocol version negotiation

`POST /nodes/register` carries a
`protocol_version` field, which adapters set from `weave_core::PROTOCOL_VERSION`:

```json
{ "protocol_version": 5, "node": { "id": "strom-node-1", "...": "..." } }
```

The controller accepts only the version it speaks. Anything else — including an
absent field, which reads as `0` and marks an adapter predating the handshake — is
refused with `409 Conflict`, a body naming both versions, and a `warn` log line
naming the node id:

```json
{
  "code": "incompatible_protocol_version",
  "message": "incompatible southbound protocol version",
  "details": [{
    "field": "protocol_version",
    "code": "unsupported",
    "message": "reported 4; supported 5"
  }]
}
```

Nothing about a rejected node is recorded: a registration the controller cannot
serve correctly is worse than none. `weave-adapter-strom` treats the `409` as
fatal and exits — retrying never converges — so a version mismatch surfaces as a
stopped container with a clear reason instead of a node that looks alive.
`PROTOCOL_VERSION` is a southbound handshake. It moves when adapter behavior or
payload semantics become incompatible, so the controller can reject a stale
process at registration rather than wait for a later request to fail. It is `5`:
SRT socket `params` carry a `passphrase` and `pbkeylen` (see
[SRT encryption](#srt-encryption)). An adapter at `4` would ignore both and
build its end of every keyed link in the clear, which the other end refuses.
The same version adds a hop profile's `merge`, a desired hop's
`merge_ingress`, and the hop status that reports it (see
[Redundant paths](#redundant-paths)), and the `rist` transport and socket (see
[Capabilities and topology](#capabilities-and-topology)), which an adapter at
`4` cannot read.

### Hop status and fan-out

Every destination has a stable resource id. The same id is the `branch_id` on
every hop that carries it. Receiver ids use the destination id; bridge ids use
the destination id and bridge position. Reordering the manifest list changes no
hop ids, ports, or desired snapshots. A destination's second path carries
`{id}.2` instead (see [Redundant paths](#redundant-paths)).

Hop ids join names with `-`, so two valid streams can spell the same id: the
sender of stream `x-receiver-a` and the receiver of stream `x` for destination
`a-sender` are both `weave-x-receiver-a-sender`. A hop id names one hop, since it
is the flow name on its node and the input to the key of the link feeding it.
`POST /streams` and `PUT /stream-sets/{owner}` refuse a stream that can plan an
id another stream can, on whatever nodes each lands, with
`409 hop_id_conflict`. The message names both streams and the detail names the
id. The stream being replaced, and streams the same stream-set write replaces
or prunes, do not count; another stream in the same write does. An unchanged
stream is not checked, so streams stored before this check that already collide
still reapply as no-ops. Streams whose sender a node reports running are
planned first, then the rest, each group in name order. A stream that would
plan an id an earlier stream holds stays unplaced, with reason
`placement_failed` and a detail naming the id and the stream holding it.

```json
{
  "id": "weave-cam1-to-studio-sender",
  "node_id": "strom-node-1",
  "profile_id": "srt-forward",
  "role": "sender",
  "ingress": { "transport": "srt", "role": "listen", "port": 20000 },
  "egresses": [
    {
      "branch_id": "studio",
      "transport": "srt",
      "role": "connect",
      "host": "172.27.0.10",
      "port": 20000
    },
    {
      "branch_id": "preview",
      "transport": "srt",
      "role": "connect",
      "host": "203.0.113.7",
      "port": 20001
    }
  ]
}
```

An adapter reports ingress separately and one status entry for every desired
branch. Branch order is not significant; `branch_id` is the join key. Missing,
duplicate, or unknown branch ids make the hop report incomplete, so it remains
`pending` instead of allowing one healthy destination to hide another. A hop
with a `merge_ingress` reports its status in a `merge_ingress` field of the same
shape as `ingress`, and is incomplete without it:

```json
{
  "id": "weave-cam1-to-studio-sender",
  "node_id": "strom-node-1",
  "state": "provisioned",
  "ingress": {
    "condition": "flowing",
    "resolved": { "host": "172.26.0.10", "port": 20000 },
    "stats": { "connections": 1, "rate_mbps": 8.1 }
  },
  "egresses": [
    {
      "branch_id": "studio",
      "condition": "flowing",
      "resolved": { "host": "172.27.0.10", "port": 20000 },
      "stats": { "connections": 1, "rate_mbps": 8.0 }
    },
    {
      "branch_id": "preview",
      "condition": "connecting",
      "resolved": { "host": "203.0.113.7", "port": 20001 },
      "stats": { "connections": 0, "rate_mbps": 0.0 }
    }
  ]
}
```

The second branch keeps the aggregate stream `degraded`. `/status` also carries
one entry per destination with its own status, nodes, conditions, and endpoint.
A fan-out is `flowing` only when every branch is flowing.

A placed stream stays placed when a node it names goes offline. Its hops stay
desired, and the stream reads `degraded` until the node heartbeats again. A
stream that names a node that has not registered is `pending`.

```json
{
  "name": "cam1-to-studio",
  "status": "degraded",
  "conditions": [],
  "destinations": [
    {
      "id": "studio",
      "status": "flowing",
      "nodes": ["strom-node-1", "strom-node-2"],
      "conditions": [],
      "endpoint": {
        "node": "strom-node-2",
        "host": "172.27.0.10",
        "port": 20001,
        "url": "srt://172.27.0.10:20001"
      }
    }
  ]
}
```

### Manifest validation

The controller is the authority for stream validation. Northbound runs the same
shared validator to give early feedback, but a client that posts directly to the
controller cannot bypass the rules. Invalid submissions return `400` and are not
stored. If persisted desired state no longer passes the current contract, the
controller refuses to start and names the stream, field, and validation error.

Validation returns every issue as a field-addressed detail. Top-level and
endpoint payloads reject unknown fields. A valid stream can still be refused
with `409 hop_id_conflict` when it can plan a hop id another stream can (see
[Hop status and fan-out](#hop-status-and-fan-out)).

### Error responses

Every API error is JSON with `code` and `message`
fields. Validation failures also carry all known field issues in `details`:

```json
{
  "code": "invalid_request",
  "message": "stream validation failed",
  "details": [{
    "field": "destinations[0].srt.node",
    "code": "invalid_characters",
    "message": "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
  }]
}
```

Clients branch on `code`, not `message`. `details` is absent when the error has
no field context.

### Machine-readable contracts

Generated OpenAPI 3.1 documents for the northbound and southbound surfaces are
in `contracts/openapi/`. JSON Schema 2020-12 documents for their request and
response payloads are in `contracts/json-schema/`. They are generated from
the Rust wire types, and `contracts/json-schema/webhook-event.json` describes a
webhook delivery. Resource-id patterns, required destination lists, non-empty
format constraints, unknown-field rejection, and enum values are present in the
schemas. Rules that depend on where a shared endpoint type appears, such as
source-only `format`, remain authoritative in the shared validator and return
field details at runtime.

Run `just contracts` after changing a route or wire type. Contract drift tests
compare every committed artifact with a fresh generation, and the route paths
come from the same constants used by the servers. `/health` and the controller
dashboard routes are not in OpenAPI because they are outside the API contracts.

### Resource identifiers

Stream and node ids are 1–63 characters and match
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`: lowercase ASCII letters, digits, and
interior hyphens. They are not trimmed or normalized. The same rule applies to
stream names, registered node ids, and every node reference in `node`, `device`,
`via`, destination ids, profile ids, attachment ids, and network ids. Invalid
request paths return `400` before a proxy constructs an upstream URL.

This keeps ids safe as URL segments, database keys, log fields, hop ids, and
Strom flow names. Invalid persisted stream definitions stop controller startup
and name the failing field. Stored node registrations are caches, so invalid
ones are dropped; a node must re-register with a valid id.

### Desired-state revisions

The controller stores a generation and an opaque revision with each stream.
Creating a stream starts generation 1. Reapplying the same definition preserves
both values; changing it increments the generation and assigns a new revision.
Deleting and recreating a name starts generation 1 again but does not reuse the
old revision. Reads expose the generation in the resource body and the revision
only as an ETag. Generation describes a spec; the ETag guards a mutation and
prevents a delete-and-recreate ABA race.

Each stream status has `generation` and `observed_generation`. The first is the
current desired spec. The second is the generation used by the last completed
reconcile, or `null` before one completes. Conditions are always keyed by stable
types: `placement_ready`, `nodes_available`, `hops_ready`, `format_compatible`,
and `media_flowing`. Each has `true`, `false`, or `unknown` status, a stable
reason code, a detail string, and an RFC 3339 `last_transition_time`. The time
changes when the condition status changes, not when only its reason or detail
changes.

## Authentication

Requests present a bearer token as `Authorization: Bearer <token>`. A missing or
invalid one gets `401` with a `WWW-Authenticate: Bearer` challenge. Tokens are
compared in constant time and never logged.

| Variable | Held by | Accepted by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | operators, the `weave` CLI (`--token`), northbound → controller | northbound (every operator route), controller (every operator route but `/status`) |
| `WEAVE_SOUTHBOUND_KEY` | southbound, controller | nothing: node tokens are derived from it, and it is never a token itself |
| `WEAVE_SOUTHBOUND_TOKEN` | each adapter and media node, holding its own node token | southbound, controller |
| `WEAVE_SOUTHBOUND_MIN_EPOCHS` | southbound, controller | nothing: the lowest token epoch each node it names may present |

The northbound token is one shared secret. On southbound every node has a token
of its own, at an epoch that is a non-negative integer:

```
<node id>.<epoch>.<lowercase hex HMAC-SHA256(WEAVE_SOUTHBOUND_KEY, "<node id>.<epoch>")>
```

Southbound and the controller hold the key, recompute the MAC, and read the node
id off the token. They keep no list of nodes, so adding a node needs a new token
and no restart. The key must be at least 32 characters, for example
`openssl rand -hex 32`: a token carries its own MAC input, so a leaked one is
enough to test guesses of a short key offline. Southbound and the controller refuse to
start with a shorter key, and `weave node-token` refuses one.

`weave node-token <id>` prints a node's token at epoch 0, and
`--epoch <n>` at another, from `WEAVE_SOUTHBOUND_KEY` (or `--key`) without
calling any service. openssl gives the same value:

```sh
id=strom-node-1 epoch=0
printf '%s.%s.%s\n' "$id" "$epoch" "$(printf %s "$id.$epoch" | openssl dgst -sha256 -hmac "$WEAVE_SOUTHBOUND_KEY" -r | cut -d' ' -f1)"
```

A node token acts only for its own node. `POST /nodes/register` whose
`node.id`, and `POST /nodes/{id}/heartbeat` or `GET /nodes/{id}/desired` whose
path id, is another node's gets `403`. So does a registration or heartbeat
carrying an endpoint whose `node_id` is another node's:

```json
{ "code": "forbidden", "message": "token does not belong to node strom-node-2" }
```

Southbound forwards the node's own `Authorization` header to the controller
rather than a credential of its own. Both check the token and the id, and
nothing about a refused registration is recorded. `weave-adapter-strom` treats a
`403` at registration as fatal and exits, as it does a protocol-version `409`.
The inventory reads `GET /nodes`, `GET /endpoints` and `GET /state` accept any
node's token. They list every node, endpoint and hop status, but no node's
desired hops.

To revoke one node's tokens, mint it a token at a higher epoch and raise its
minimum in `WEAVE_SOUTHBOUND_MIN_EPOCHS`, given to southbound and the controller
alike as comma-separated `<node id>=<epoch>` pairs such as
`strom-node-1=2,guest-1=1`. A token below its node's minimum gets `401`, as a
bad MAC does. A node the variable does not name accepts every epoch, and no
other node's token changes. Both services read the variable at startup, so a
change takes effect on restart, and a value that does not parse stops them from
starting. Rotating the key still replaces every node's token.

The controller backs both surfaces, so it needs `WEAVE_NORTHBOUND_TOKEN` and
`WEAVE_SOUTHBOUND_KEY`, and requires the credential matching the surface a route
belongs to — a node token cannot create streams. Northbound re-presents its
token on the hop to the controller. Nodes may instead carry their token in their
config file as `node.southbound_token`, which takes precedence over the
environment.

Controller `GET /nodes` accepts the northbound token or any node token, because
both surfaces expose the same read-only inventory. This does not cross the
mutation boundary: operator stream writes accept only the northbound token, and
node lifecycle writes accept only the node's own token.

A browser node (`nodes/browser/`) is a media node too: the page presents its
node token on every southbound call, passed in through the URL fragment so it
never reaches a server log. Without `#node=`, the page takes its node id from the
token; a `#node=` naming another id is shown as rejected and never registers.
Because the page runs on a different origin from southbound, southbound sends
CORS headers on its API routes when `WEAVE_SOUTHBOUND_CORS_ORIGIN` is set — an
exact origin such as `https://studio.example`, or `*` for development. Unset, no
CORS headers are sent and only non-browser adapters can register. The preflight
is answered before the bearer check and allows `Authorization` and
`Content-Type`. A page holds its own node's token and no other. A browser node
registers with a `browser://<id>` endpoint, which is a placeholder: the
controller never dials any node's endpoint.

The Strom adapter also presents a token that open-weave never accepts, so it
is not in the table. When Strom requires a bearer token, the adapter presents
`WEAVE_STROM_TOKEN`, or `strom.token` from its config, which takes precedence.
With neither set no `Authorization` header is sent, so an unauthenticated Strom
keeps working.

**Services fail closed.** A service whose secret (`WEAVE_NORTHBOUND_TOKEN`,
`WEAVE_SOUTHBOUND_KEY`, and for the controller `WEAVE_SRT_KEY_SECRET`, see
[SRT encryption](#srt-encryption)) is unset or empty refuses to start rather than
serve unauthenticated traffic, and so does an adapter without a node token. So
does a service whose `WEAVE_SOUTHBOUND_KEY` or `WEAVE_SRT_KEY_SECRET` is shorter
than 32 characters. For
local development set `WEAVE_AUTH_DISABLED=1` to opt out explicitly; only `1` or
`true` disable it, so `WEAVE_AUTH_DISABLED=0` leaves authentication on.

Left unauthenticated on purpose:

- **`/health`** on every service — compose healthchecks and load balancers need it.
- **The controller dashboard** (`/`, `/ui`, `/view`) and the `/status` rollup
  it shares its data with — on the controller only. Northbound's `/status` and
  `/streams/{name}/endpoints` require the northbound token, so an operator can
  read a stream's resolved address through northbound with the controller port
  unexposed. The dashboard is browser-loaded and polls `/view`, which a bearer
  token cannot carry without a cookie/session mechanism or a reverse proxy.
  `/view` exposes topology and allocated ports, so **do not expose the controller
  port publicly** — keep it on a private network or put a reverse proxy in front
  of it. The controller's `/streams` and `/nodes` API routes *are*
  authenticated, so an exposed port leaks read-only dashboard data rather than
  write access.

There is no TLS: terminate it at a reverse proxy. There is no mTLS.

## SRT encryption

Every SRT link between two nodes is encrypted. The controller derives its key as
HMAC-SHA256 of `WEAVE_SRT_KEY_SECRET` over the id of the hop the link feeds and
the ids of the two nodes it joins, written as 64 hex characters, and puts it
with `pbkeylen: 32` (AES-256) in both ends' desired hops. The same secret and
topology give the same key on every tick, so no key is stored and a replan, or a
link changing direction, does not rekey it. A link that moves to another node,
such as a swapped `via` relay or a destination re-applied on another node, gets
a new key, so the node it left holds no valid key for it. Changing the secret
rekeys every link once. So does upgrading from a controller that derived keys
from the hop id alone; each adapter rebuilds its keyed flows once.

`WEAVE_SRT_KEY_SECRET` must be at least 32 characters, for example
`openssl rand -hex 32`. Without it the controller refuses to start, like a
service without its token. With `WEAVE_AUTH_DISABLED=1` and no secret it
generates a random one and logs a warning: every link key then changes when the
controller restarts, and every adapter rebuilds its flows.

A socket that something outside open-weave dials, or that dials out to it, takes
its key from the manifest: the source ingress a producer dials, the output a
consumer dials, and a `remote` listener.

```yaml
source:
  srt:
    node: strom-node-1
    passphrase: producer-shared-passphrase
destinations:
  - id: uplink
    srt:
      remote: { host: 198.51.100.5, port: 9000, network: internet }
      passphrase: far-end-shared-passphrase
```

A passphrase is 10 to 80 bytes, the range libsrt accepts, with no control
characters. A socket without one runs in the clear. The peer must use the same
passphrase; its key length may differ, since SRT settles on one at connect.

Where keys appear:

- Adapters get them in `GET /nodes/{id}/desired`, and nothing else serves them.
- `/view`, `/status` and `/streams/{name}/endpoints` carry none.
  `POST /stream-plans` drops every passphrase from the hops it returns and keeps
  `pbkeylen`, which marks a keyed socket.
- `GET /streams`, `/streams/{name}` and the stream-set routes return the manifest
  as written, so a northbound-token holder can read manifest passphrases. Derived
  keys never reach northbound.
- The controller stores manifest passphrases in Postgres in the clear, as part of
  the stream spec.
- A node token reads only its own node's desired hops, so each node learns the
  keys of its own sockets and no others. Whoever holds `WEAVE_SOUTHBOUND_KEY` can
  make any node's token, and so read every key.
- A link between nodes that runs over RIST carries no key (`backlog/OW-41`).
- Strom returns the keys in its own `GET /api/flows`, and its SRT blocks log them
  at INFO. `WEAVE_STROM_TOKEN` guards that API when Strom requires a token;
  nothing in open-weave changes what Strom logs.

## Webhooks

The controller POSTs a JSON event to one configured receiver when a node
registers, goes offline, or comes back, and when a stream's conditions change.
It exists so a service that hosts guest pages can declare a stream for a guest's
seat on being told the page registered, and hear that the stream went `flowing`
or `degraded`, rather than polling `/nodes` and `/status`.

| Variable | Meaning |
|---|---|
| `WEAVE_WEBHOOK_URL` | Absolute URL receiving events. Unset or blank switches webhooks off. |
| `WEAVE_WEBHOOK_TOKEN` | Presented to the receiver as `Authorization: Bearer <token>`. Optional. |
| `WEAVE_WEBHOOK_EVENTS` | Comma-separated event types to deliver. Defaults to all of them. |

| Event | When |
|---|---|
| `node.registered` | Every accepted registration, including a re-registration of a node already online — a page reload does exactly this. |
| `node.online` | A node heartbeats after having been marked offline. |
| `node.offline` | A node crosses `WEAVE_NODE_TTL_SECS` without a heartbeat. |
| `node.forgotten` | An offline node crosses `WEAVE_NODE_FORGET_SECS` without a heartbeat and is removed. A node a stored stream names is never removed. |
| `stream.changed` | A reconcile tick computes a stream's conditions for the first time, or computes conditions that differ from the previous tick's in `type`, `status` or `reason`, on the stream or on any destination. |

```json
{
  "event_id": "guest-1-1788520800000003",
  "event_type": "node.registered",
  "occurred_at": "2026-09-04T11:22:33.123456789Z",
  "node": {
    "id": "guest-1",
    "status": "ready",
    "endpoint": "browser://guest-1",
    "capabilities": {
      "adapters": [],
      "hop_profiles": [{
        "id": "camera-to-whip",
        "ingress": { "device": "capture" },
        "egress": { "transport": "whip", "roles": ["connect"] },
        "max_egresses": 1
      }]
    },
    "topology": {
      "attachments": [{ "id": "client", "network": "internet", "dial": true }]
    }
  }
}
```

That is an abbreviated browser registration. Its hop profiles show it can
capture, while its dial-only attachment shows it exposes no media listener.

`node` is the registering node's descriptor and nothing else: `endpoints` and
`hop_status` describe hops rather than the node, and are not part of this
contract. `event_id` is the node id or stream name followed by a number. It is stable
across retries of one delivery, so a receiver can deduplicate, and it is not
reused, including by a restarted controller: the number counts up from the
controller's start time in microseconds. With `DATABASE_URL` it counts from the
moment the controller took the lease, on the Postgres clock, so a controller
taking over from another sends none of its ids either.

```json
{
  "event_id": "basic-1790330400000007",
  "event_type": "stream.changed",
  "occurred_at": "2026-09-25T10:15:02.412345678Z",
  "stream": {
    "name": "basic",
    "generation": 2,
    "observed_generation": 2,
    "status": "degraded",
    "conditions": [{
      "type": "nodes_available",
      "status": "false",
      "reason": "node_offline",
      "detail": "node strom-node-2 is offline",
      "last_transition_time": "2026-09-25T10:15:02.410123456Z"
    }],
    "destinations": [{
      "id": "studio",
      "status": "degraded",
      "conditions": [{
        "type": "media_flowing",
        "status": "false",
        "reason": "media_degraded",
        "detail": "media is not flowing across every branch",
        "last_transition_time": "2026-09-25T10:15:02.410123456Z"
      }]
    }]
  }
}
```

That is abbreviated too: a stream and each destination carry all five
conditions. `stream` has the stream's `name`, `generation`,
`observed_generation` and `status`, and every condition on the stream and on
each destination with its stable `reason` code. It leaves out the stream's nodes
and every address, which `/status` and `/streams/{name}/endpoints` carry. A tick
sends at most one event per stream. The controller stores a stream's
conditions whenever a tick changes them, and the first tick after a start or
takeover compares against those, so it sends only what changed since. With the
in-memory store a restart starts empty, and its first tick sends one for every
stream. A change of `detail`
alone sends nothing, and deleting a stream sends nothing. An event follows the
change it reports by up to one reconcile interval plus an adapter poll.

An empty `WEAVE_WEBHOOK_EVENTS` delivers every type, `stream.changed` included.
Set it to `node.registered,node.online,node.offline,node.forgotten` for node
events only.

Emitting never blocks a registration or a reconcile tick. Events are queued and
delivered by one background worker, in order, retried with backoff on a connect
error or 5xx and abandoned after four attempts; a 4xx is the receiver rejecting
the event and is not retried. A full queue drops rather than waits, and the queue is in memory, so
events do not survive a controller restart. A controller that loses the lease
keeps delivering what it queued before, so around a takeover a receiver can get
the two controllers' events interleaved.

**Delivery is at-least-once and incomplete by design.** A receiver reconciles
against `GET /nodes` and `/status` on boot and treats events as hints, not truth.

The bearer token authenticates the controller to the receiver; it does not let
the receiver tell a genuine event from anyone who has learned the token. A
receiver outside the trust boundary wants a body signature instead, which is not
implemented.

## Controller failover

With `DATABASE_URL` set, a controller serves only while it holds a lease, a row
in Postgres. Any number of controllers can share one database: one leads and
the others stand by. Without `DATABASE_URL` there is no lease and one
controller.

| Variable | Default | Meaning |
|---|---|---|
| `WEAVE_LEASE_TTL_SECS` | `10` | Seconds the lease lasts without a renewal. |

- A standby answers `503 not_leader` on every route except `/health`, `/` and
  `/ui`. It runs no reconcile tick and sends no webhook. `/health` answers `200`
  on both; `/status` tells them apart.
- The leader renews the lease every third of the TTL. When no renewal has
  succeeded for two thirds of the TTL, it stops serving and writing and stands
  by. The lease runs out on the Postgres clock a third of the TTL after that, so
  no standby can take over while the old leader still serves.
- A standby tries for the lease every second, or every third of the TTL if that
  is shorter. When it gets the lease it loads the stored state, runs one
  reconcile tick, and then serves. Hops are planned from the streams, node
  registrations and node reports in Postgres, so it serves the hops the old
  leader served and adapters keep their flows.
- Every write checks the lease in its own transaction and fails with
  `503 not_leader` unless the writer holds it. A takeover waits for a write
  already past that check, and the new leader loads it.
- On SIGTERM or SIGINT the leader gives the lease up, and a standby takes over
  on its next try. A leader that crashes keeps the lease until it runs out, so
  a lone controller restarting after a crash answers `503 not_leader` until
  then.
- Streams, stream sets, node registrations, generations, revisions and ETags
  live in Postgres and survive a takeover, as do what nodes report and what
  the last tick concluded. A heartbeat writes the node's registration when its
  status, a hop's state or a socket's condition changes; rates and addresses
  alone write nothing. A tick writes a node it marks `offline`, and each stream
  whose conditions changed, with their transition times. The new leader's first
  tick reads all of it, so a bridge stays on the relay that runs it, a flowing
  stream keeps reading `flowing`, and each condition keeps its transition time.
  Heartbeat times are not stored: the new leader counts every stored node as
  heard at the takeover. A restart with Postgres works the same way.
- Every controller needs the same `WEAVE_SRT_KEY_SECRET`,
  `WEAVE_SOUTHBOUND_KEY` and `WEAVE_SOUTHBOUND_MIN_EPOCHS`. With another SRT key
  secret, a new leader gives every link a new key and each adapter recreates
  its flows; with another southbound key it refuses every node's token; with
  other minimum epochs it accepts tokens the old leader refused, or the reverse.
- Postgres is one instance. While no controller can reach it, none leads.

Northbound and southbound take every controller in `WEAVE_CONTROLLER_URL`,
comma-separated; unset means `http://127.0.0.1:8082`. A request goes first to
the controller that answered last. The proxy moves to the next one only when it
cannot connect within two seconds or gets `503 not_leader`: neither controller
has acted on the request, so a write is never sent twice. Any other answer, a
timeout after connecting included, is passed back as it is. When no controller
leads, the proxy passes back `503 not_leader`; when none can be reached,
`502 controller_unreachable`.

While no controller leads, the Strom adapter and the browser node get `503` or
`502` from southbound, keep every hop they run, and register again once a
leader answers. Each registration sends `node.registered`.

## Capabilities and topology

Capabilities say which complete hop shapes an adapter can build. Each profile
has one ingress class, one homogeneous egress class, and an optional static
egress limit:

```yaml
capabilities:
  adapters: []
  hop_profiles:
    - id: srt-forward
      ingress: { transport: srt, roles: [listen, connect] }
      egress: { transport: srt, roles: [listen, connect] }
    - id: camera-to-whip
      ingress: { device: capture }
      egress: { transport: whip, roles: [connect] }
      max_egresses: 1
```

Strom advertises `srt-forward`, `whip-to-srt`, `srt-to-whep`, `srt-to-rist`
and `rist-to-srt`. The browser
advertises `camera-to-whip` and `whep-to-display`, both with one egress. It does
not advertise WHIP to WHEP or mixed-transport fan-out. The controller writes the
selected `profile_id` into every `DesiredHop`; adapters validate and dispatch on
that id.

A profile may declare `accepts`, the media its ingress takes, in the same shape
as a destination's `accepts` (see [Media formats](#media-formats)). Strom's
`whip-to-srt` declares H264 video and Opus audio, the codecs Strom's
`whip_input` negotiates:

```yaml
- id: whip-to-srt
  ingress: { transport: whip, roles: [listen] }
  egress: { transport: srt, roles: [listen, connect] }
  accepts:
    video: { codec: [h264] }
    audio: { codec: [opus] }
```

Topology says where a node can dial and what peers can reach. Attachment ids are
local to one node. Network ids name shared routing domains:

```yaml
topology:
  attachments:
    - id: wan
      network: internet
      dial: true
      listeners:
        srt:
          host: 203.0.113.10
          port_range: { start: 20000, end: 20100 }
        whip: { base_url: https://media.example/whip }
    - id: production
      network: studio-lan
      dial: true
      listeners:
        srt:
          host: 10.20.0.10
          port_range: { start: 21000, end: 21100 }
```

An attachment with `dial: true` and no listeners models a NAT client. A link is
possible when one end offers `connect`, has a dialing attachment, and the other
offers `listen` with a listener on the same network. Transport support comes
only from hop profiles. The controller chooses the lowest deterministic
transport, network, attachment, address, URL, and port that satisfies both.
A producer's or consumer's SRT socket, and the address
`GET /streams/{name}/endpoints` reports for it, use the same order: the
node's SRT listener on the lowest network, then the lowest attachment id,
among those on the endpoint's `network` when it names one.

A `rist` listener takes a `host` and a `port_range`, as an SRT one does. RIST
here is the simple profile: the receiver binds an even port for RTP and the port
after it for RTCP, and the sender pushes to both. Only the downstream end of a
link can host it, so a receiver behind NAT cannot take RIST. The planner tries
RIST after SRT, WHIP and WHEP, so it carries a link only where none of those
can, and a link that planned before RIST existed keeps its transport. Give RIST
its own port range: in a range it shares with SRT, an SRT port can take one half
of the last free pair. A RIST link carries no key (`backlog/OW-41`).

Planning first tries a direct link. If none works, it tries one online transit
node whose profile supports the required ingress-to-egress shape and whose
attachments carry both halves. Of those, it takes the lowest id that still has a
free port for each SRT listener the bridge would host, so a fan-out that fills
one relay's port range moves on to the next. When every such node is out of
ports, the stream stays unplaced and the reason names the lowest-id one. There
is no `relay` flag.

A bridge stays on the relay that reports running it, as long as that relay is
online, still qualifies, has the ports, and its report of the bridge is not
`failed`. So an earlier relay coming back does not move a working stream, and
a relay that goes offline or fails the bridge loses it. Streams a node already
runs are planned before new ones, and a stream plans the destinations some node
already carries before new ones, so a new stream or destination takes the next
relay instead of the ports of an existing bridge. Streams are still listed, and
each node's desired hops ordered, by stream name, and a stream's hops by
destination id. An explicit `via` chain remains an exact node constraint:

```yaml
destinations:
  - id: studio
    srt:
      node: studio-node
      via: [edge-relay]
```

Manifests still name terminal nodes, never inter-node transports. A remote SRT
listener includes its network because its reachability cannot be inferred, and
may carry the passphrase it expects:

```yaml
destinations:
  - id: uplink
    srt:
      remote: { host: 198.51.100.5, port: 9000, network: internet }
      passphrase: far-end-shared-passphrase
```

A source can be an outside WHIP sender, such as an encoder, and a destination
an outside WHEP player. Neither is a registered node. Each names the node whose
signalling listener the peer calls, as an SRT producer or consumer names the
node it dials:

```yaml
source:
  whip: { node: strom-node-1, network: internet }
destinations:
  - id: monitor
    whep: { node: strom-node-2 }
```

`whip` is for the source only and `whep` for destinations only. Either may pin
`network`. The planner takes the node's listener for the transport in the same
order as SRT producers and consumers: lowest network, then lowest attachment
id. A `whip` source may declare `format` and a `whep`
destination `accepts`, as SRT endpoints do. The sender is placed on the named
node with its ingress at that attachment's `whip` base, and the receiver on the
named node with its last egress at the `whep` base. Both URLs end in the hop id:
`{base}/weave-{stream}-sender` and `{base}/weave-{stream}-receiver-{destination}`.
A node that declares no base for the transport cannot take the peer, and the
stream stays unplaced. On Strom the sender uses `whip-to-srt` and the receiver
`srt-to-whep`, so a WHIP source and a WHEP destination on one node are two hops
joined over SRT. Strom's `whip_input` takes H264 video and Opus audio and one
session per endpoint; a `format` on the source outside those codecs is reported
as a format mismatch.

Strom serves `/whip` and `/whep` without authentication; its source puts those
routes outside its auth middleware. Anyone who can reach the node and knows an
endpoint id can publish to or play from it, and hop ids are predictable.

`GET /streams/{name}/endpoints` returns `ingress` plus a `destinations` list of
`{id, endpoint}` objects. An endpoint carries the `node` and the `url` a peer
dials. An SRT endpoint also carries `host` and `port`; a WHIP or WHEP endpoint's
`url` is the signalling URL above. A device end has a null endpoint. See
`nodes/browser/README.md` and `bench/README.md` for the shipped nodes.

### Redundant paths

A destination can ask for two paths to its node:

```yaml
destinations:
  - id: studio
    paths: 2
    srt: { node: studio-node }
```

`paths` is 1 or 2 and defaults to 1. A `remote` destination cannot ask for two,
since no receiver hop exists to merge them, and neither can one that pins `via`.

The planner places every destination's first path as it would with `paths: 1`,
then each second path. A second path passes through no relay the first one
uses, and at the sender and the receiver it uses no attachment the first one
may carry its link on. A dialing socket names no local address, so at an end
that dials, every dialing attachment on that network counts as used: two paths
that both dial out of one node leave it over different networks.

Merging the two copies is the receiver's job. A second path is placed only when
the receiver has a hop profile with `merge: true`. The receiver hop then gets a
`merge_ingress`, a second socket of that profile's ingress class:

```yaml
hop_profiles:
  - id: srt-merge
    ingress: { transport: srt, roles: [listen, connect] }
    egress: { transport: srt, roles: [listen] }
    merge: true
```

No shipped node advertises `merge`. The Strom adapter refuses a hop with a
`merge_ingress`.

The second path's egresses carry the branch id `{destination}.2`, and its
bridges are `weave-{stream}-bridge-{destination}.2-{position}`. `.` is not
allowed in a destination id, so these never equal another destination's ids.
Asking for a second path changes none of the first path's hop ids, ports, or
branch ids. A destination's status covers both paths, so one that is down
reads `degraded`.

When the second path cannot be placed, the stream is placed with one. Its
`placement_ready` condition and the destination's stay `true` with reason
`single_path`, and the detail names the destination and why. `POST
/stream-plans` returns the same detail as its `reason`.

## Media formats

A source may declare what its producer sends, and a destination what it accepts:

```yaml
source:
  srt:
    node: strom-node-1
    format:
      container: mpeg_ts
      video:
        codec: h264
        width: 1920
        height: 1080
        framerate: { numerator: 25, denominator: 1 }
        chroma_subsampling: yuv422
      audio: { codec: aac, sample_rate: 48000, channels: 1 }
destinations:
  - srt:
      node: studio-node
      accepts:
        audio: { sample_rate: [44100] }
```

Chroma subsampling is declared because it decides whether a receiving node
decodes on its GPU or falls back to CPU.

The two shapes are deliberately different. A `format` is fixated — every field
has one value and it describes media that exists. An `accepts` is partially
specified — each field lists the values the endpoint tolerates, and an absent
field constrains nothing, so an empty `accepts` accepts everything.

That split is borrowed from GStreamer caps, and **only the algebra transfers**.
GStreamer negotiates at runtime, in one process, downstream-first over a shared
bus; none of that exists across a control plane. What does carry over is caps as
constraint sets that a concrete format is checked against.

Formats are **declared, not discovered**. An SRT flow that only moves bytes never
parses its payload, so nothing in the path knows what is inside it — a Strom
endpoint reporting negotiated pad caps would faithfully report "some bytes".
Learning the real format would mean putting a parsing element in the pipeline.
An absent `format` means unknown, not wrong, and nothing is inferred from it.

When a declared source format does not satisfy a destination's `accepts`, the
stream places and the media flows — it just arrives somewhere it cannot be
decoded. That is reported rather than acted on:

```
degraded — destination studio cannot accept the source format:
           audio.sample_rate is 48000 but accepts 44100
```

The sender's hop profile is checked the same way. When the profile placed for
the sender declares `accepts` and the source's `format` falls outside it, the
mismatch names the node and profile:

```
degraded — node strom-node-1 cannot take the source format through profile
           whip-to-srt: video.codec is vp8 but accepts h264
```

The mismatch is known at plan time, so it is reported before any media exists.
A lost node outranks it: both read `Degraded`, and the reason distinguishes them.

**Nothing converts anything.** A mismatch is reported and no node is placed to
fix it.

## Strom adapter and drift policy

Strom is the first media runtime target. `weave-adapter-strom` runs beside one
Strom instance, registers it with southbound, polls `/api/flows`, and reports
Strom flows as observed endpoints. It pulls its node's desired hops from
southbound and translates them into Strom flow create/start/delete calls.

Strom UI/API edits are **drift**, like direct edits to Kubernetes managed
objects. The source of truth is open-weave desired state; out-of-band Strom
changes should be reconciled back or explicitly adopted into desired state.

Strom reports no RIST statistics, so the adapter reads a RIST socket's condition
from the SRT side of the same hop. A sender's RIST egress reads `flowing`
whenever its SRT ingress advances, whether or not anything receives what it
pushes. A receiver's RIST ingress reads `flowing` only while its SRT output has a
consumer pulling bytes. Strom advertises no RIST-to-RIST
profile, so no hop has RIST on both sides.

The adapter writes each SRT socket's latency and key into its `srt://` URI, since
setting an srt element's `uri` resets both and Strom sets element properties in no
fixed order. A flow whose SRT address, latency or key differs from its desired hop,
or whose WHIP/WHEP endpoint id does, is deleted and created again.

## Quickstart

The fastest way to see open-weave work is the bench: a docker-compose stack with
four Strom nodes behind emulated routers, which starts empty and takes stream
manifests. `just bench up` then `just bench stream-up basic` drives a stream end
to end. See [`bench/README.md`](bench/README.md).

To run the services directly instead, note that `run-north`, `run-south`,
`run-controller` and `run-strom-adapter` are each a long-running server and want
a terminal of their own.

Every service needs its secret (see [Authentication](#authentication)), and the
controller its SRT key secret, so export them first, and the adapter its node
token — or set
`WEAVE_AUTH_DISABLED=1` to run without any:

```sh
export WEAVE_NORTHBOUND_TOKEN=$(openssl rand -hex 32)
export WEAVE_SOUTHBOUND_KEY=$(openssl rand -hex 32)
export WEAVE_SRT_KEY_SECRET=$(openssl rand -hex 32)
export WEAVE_SOUTHBOUND_TOKEN=$(just cli node-token strom-node-1)
```

```sh
just build
just run-north           # 127.0.0.1:9080
just run-south           # 127.0.0.1:8081
just run-controller      # 127.0.0.1:8082 health endpoint
just run-strom-adapter --config examples/node.yaml
just cli --help
just apply               # examples/stream.yaml through northbound
just get-streams
just get-nodes
just get-status
```

The adapter needs a node config, from `--config` or `WEAVE_NODE_CONFIG`; it will
not start without one. `examples/node.yaml` names the bench's southbound and
Strom addresses, so point them at your own before running it anywhere else —
until they answer, the adapter serves `/health` and keeps retrying.

`just apply` and the `just get-*` commands call northbound, so it has to be up. The
applied stream stays `Pending` until the nodes it names register.

The CLI picks `WEAVE_NORTHBOUND_TOKEN` up from the environment; `--token`
overrides it.

CLI output defaults to a compact human-readable form. The global
`-o human|yaml|json` option selects an explicit format; YAML and JSON are useful
for scripts and for retaining every response field.

```sh
weave get nodes
weave get status
weave get endpoints STREAM
weave get streams
weave get stream NAME
weave get stream-sets
weave get stream-set OWNER
weave apply-set OWNER -f examples/stream-set.yaml
weave -o yaml get status
weave -o json get stream NAME
```

`get endpoints STREAM` returns the resolved addresses for that stream.

## License

MIT. See [`LICENSE`](LICENSE).
