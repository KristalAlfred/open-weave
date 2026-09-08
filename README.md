# open-weave

open-weave is a software-defined media contribution orchestrator — the control-plane
"brain" that accepts declarative desired state and reconciles it onto a
southbound ecosystem of media nodes, adapters, and existing transport systems.

It does **not** define a new media data plane. The northbound side speaks operator
intent; the southbound side normalizes media runtimes into one observed/control
model. One runtime is implemented: [Strom](https://github.com/Eyevinn/strom), via
`weave-adapter-strom`. NMOS, MXL and MCM are targets the adapter contract is
shaped for, not ones it ships with.

## Status

Early, and version `0.1.0` means it. The contracts move without deprecation
windows — `API_V1` and `PROTOCOL_VERSION` tell a client what it is talking to,
and a mismatch is refused rather than smoothed over.

Built: the three control-plane services, the `weave` CLI, one southbound adapter
(`weave-adapter-strom`), and a browser node. Links carry SRT, WHIP or WHEP, and
the controller plans NAT traversal through relay nodes. All of it is verified on
the docker-compose bench in `bench/`, which runs real Strom instances behind
per-node `netem` routers, and nowhere else.

Not built: TLS, controller HA, per-node tokens, format conversion, and any
adapter other than Strom. `BACKLOG.md` lists the known gaps with the evidence
behind each one.

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
  endpoint discovery, and future command streams.
- **`weave-adapter-strom`** — southbound adapter for existing
  [Strom](https://github.com/Eyevinn/strom) media runtimes.

Further adapters get their own crates as they arrive. None exist yet;
`weave-adapter-nmos`, `weave-adapter-mxl-domain` and `weave-adapter-mcm` are
names for unstarted work, listed to show where the seam falls.

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
calling back out; its one outbound call is the node lifecycle webhook below,
which is fire-and-forget and off unless configured.

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
the dashboard. The unauthenticated copy is the controller's, which the dashboard
shares; northbound's copy sits behind the bearer.

There are no back-compat aliases: the previously unprefixed paths now `404`.

### Protocol version negotiation

A URL prefix tells a client where to send a request; it does not let the server
notice a stale adapter. So `POST /v1/nodes/register` also carries a
`protocol_version` field, which adapters set from `weave_core::PROTOCOL_VERSION`:

```json
{ "protocol_version": 2, "node": { "id": "strom-node-1", "...": "..." } }
```

The controller accepts only the version it speaks. Anything else — including an
absent field, which reads as `0` and marks an adapter predating the handshake — is
refused with `409 Conflict`, a body naming both versions, and a `warn` log line
naming the node id:

```json
{
  "error": "incompatible southbound protocol version",
  "node_id": "strom-node-1",
  "reported_protocol_version": 1,
  "supported_protocol_version": 2
}
```

Nothing about a rejected node is recorded: a registration the controller cannot
serve correctly is worse than none. `weave-adapter-strom` treats the `409` as
fatal and exits — retrying never converges — so a version mismatch surfaces as a
stopped container with a clear reason instead of a node that looks alive.
`API_V1` moves when the routes change; `PROTOCOL_VERSION` moves when the payloads
behind them do. It is `2`: node capabilities changed shape when WebRTC transports
arrived, so an adapter built against `1` is refused rather than served hops whose
sockets it cannot read.

## Authentication

Each API surface is protected by one shared bearer token, supplied through the
environment. Requests present it as `Authorization: Bearer <token>`; anything else
gets `401` with a `WWW-Authenticate: Bearer` challenge. Tokens are compared in
constant time and never logged.

| Variable | Presented by | Accepted by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | operators, the `weave` CLI (`--token`), northbound → controller | northbound (every operator route), controller (every operator route but `/v1/status`) |
| `WEAVE_SOUTHBOUND_TOKEN` | adapters and media nodes, southbound → controller | southbound, controller |

The controller backs both surfaces, so it needs both variables and requires the
one matching the surface a route belongs to — an adapter's southbound token
cannot create streams. Northbound and southbound each re-present their own
surface token on the hop to the controller, so one secret covers a surface end to
end. Nodes may instead carry the token in their config file as
`node.southbound_token`, which takes precedence over the environment.

A browser node (`nodes/browser/`) is a media node too: the page presents
`WEAVE_SOUTHBOUND_TOKEN` on every southbound call, passed in through the URL
fragment so it never reaches a server log. Because the page runs on a different
origin from southbound, southbound sends CORS headers on its `/v1` routes when
`WEAVE_SOUTHBOUND_CORS_ORIGIN` is set — an exact origin such as
`https://studio.example`, or `*` for development. Unset, no CORS headers are
sent and only non-browser adapters can register. The preflight is answered
before the bearer check and allows `Authorization` and `Content-Type`. Per-node
tokens issued at registration remain a follow-up; today a page holds the shared
southbound secret. A browser node registers with a `browser://<id>` endpoint,
which is a placeholder: the controller never dials any node's endpoint.

The Strom adapter also presents a token that open-weave never accepts, so it
is not in the table. When Strom requires a bearer token, the adapter presents
`WEAVE_STROM_TOKEN`, or `strom.token` from its config, which takes precedence.
With neither set no `Authorization` header is sent, so an unauthenticated Strom
keeps working.

**Services fail closed.** A service whose token variable is unset or empty
refuses to start rather than serve unauthenticated traffic. For local development
set `WEAVE_AUTH_DISABLED=1` to opt out explicitly; only `1` or `true` disable it,
so `WEAVE_AUTH_DISABLED=0` leaves authentication on.

Left unauthenticated on purpose:

- **`/health`** on every service — compose healthchecks and load balancers need it.
- **The controller dashboard** (`/`, `/ui`, `/view`) and the `/v1/status` rollup
  it shares its data with — on the controller only. Northbound's `/v1/status` and
  `/v1/streams/{name}/endpoints` require the northbound token, so an operator can
  read a stream's resolved address through northbound with the controller port
  unexposed. The dashboard is browser-loaded and polls `/view`, which a bearer
  token cannot carry without a cookie/session mechanism or a reverse proxy.
  `/view` exposes topology and allocated ports, so **do not expose the controller
  port publicly** — keep it on a private network or put a reverse proxy in front
  of it. The controller's `/v1/streams` and `/v1/nodes` API routes *are*
  authenticated, so an exposed port leaks read-only dashboard data rather than
  write access.

There is no TLS: terminate it at a reverse proxy. Per-node tokens issued at
registration and mTLS are follow-ups, not implemented here.

## Node lifecycle webhooks

The controller POSTs a JSON event to one configured receiver when a node
registers, goes offline, or comes back. It exists so a service that hosts guest
pages can declare a stream for a guest's seat on being told the page registered,
rather than polling `/v1/nodes`.

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

```json
{
  "event_id": "guest-1-3",
  "event_type": "node.registered",
  "occurred_at": "2026-09-04T11:22:33.123456789Z",
  "node": {
    "id": "guest-1",
    "status": "ready",
    "endpoint": "browser://guest-1",
    "capabilities": {
      "adapters": [],
      "transports": [
        { "name": "whip", "roles": ["connect"] },
        { "name": "whep", "roles": ["connect"] }
      ],
      "devices": ["capture", "display"],
      "data_plane": { "default": { "host": "browser", "reachability": "outbound_only" } },
      "relay": false
    }
  }
}
```

That is a browser node's registration as `nodes/browser/` sends it. `devices`
and `transports` are what a consumer reads to decide the node is a capture
device worth declaring a stream for.

`node` is the registering node's descriptor and nothing else: `endpoints` and
`hop_status` describe hops rather than the node, and are not part of this
contract. `event_id` is stable across retries of one delivery, so a receiver can
deduplicate.

Emitting never blocks a registration. Events are queued and delivered by one
background worker, in order, retried with backoff on a connect error or 5xx and
abandoned after four attempts; a 4xx is the receiver rejecting the event and is
not retried. A full queue drops rather than waits, and the queue is in memory, so
events do not survive a controller restart.

**Delivery is at-least-once and incomplete by design.** A receiver reconciles
against southbound `GET /v1/nodes` on boot and treats events as hints, not truth.

The bearer token authenticates the controller to the receiver; it does not let
the receiver tell a genuine event from anyone who has learned the token. A
receiver outside the trust boundary wants a body signature instead, which is not
implemented.

## Reachability, link direction, and jump nodes

A node advertises each data-plane address with whether peers can dial it. A bare
host is dialable, the unremarkable case; spell the entry out to say otherwise:

```yaml
data_plane:
  default: 172.26.0.10
  wan:
    host: 203.0.113.7
    reachability: outbound_only
```

Planning reads this instead of assuming a fixed direction. Per link, upstream to
downstream:

| Dialable | Who listens | Who calls |
|---|---|---|
| downstream | downstream | upstream |
| upstream only | upstream | downstream |
| neither | a relay, on both sockets | both ends |

The first row is what a sender-calls-receiver template always did, so ordinary
streams plan exactly as before. The second reverses the link — useful whenever
the receiving side sits behind NAT — and costs nothing but a socket role, since
SRT listeners and callers both work as source or sink.

The third row is the jump node. When neither end can be dialled, the controller
inserts a **bridge hop** on a relay node that both ends call out to, sidestepping
the NAT boundary in the only direction it permits. It is not a special path: the
relay is dialable, so the two halves of the split link resolve under the same
rule as everything else.

A node offers itself as transit with `relay: true`; the controller draws the
lowest-id eligible relay so the choice stays stable across ticks. A node that has
gone offline is not eligible, so a stream moves to the next relay that is up. A
stream that needs transit and finds none stays `pending` and reports why, the same
as any other unplaceable stream.

Destinations may also pin transit themselves, upstream-first:

```yaml
destinations:
  - srt:
      node: studio-node
      via: [edge-relay]
```

A pin is policy — forcing traffic through a site or region — so it is honoured
even when the link would have resolved directly, and it does not consult
`relay: true`. Pins and automatic insertion compose: if a pinned relay cannot be
dialled from the hop before it, the controller relays into it as well. A pinned
node that goes offline is reported `degraded`, not swapped out: the manifest named
it, so no other node stands in for it.

A relay carries bytes and terminates nothing. Consumers still attach at the
destination node, and `GET /v1/streams/{name}/endpoints` is unchanged by transit.
One consequence worth stating: a consumer output on an `outbound_only` node is
only dialable from inside that node's network, because that is what the node
declared about itself.

## Transports

Manifests name nodes, never transports between them. `srt: { node: X }` says the
media enters or leaves X through an SRT socket a producer or consumer dials;
`device: { node: X }` says it starts at X's own camera or ends on X's own screen:

```yaml
name: alice-cam
source:
  device:
    node: browser-a1b2          # the node's own camera
destinations:
  - srt:
      node: strom-node-2        # a consumer dials this receiver's output
```

Nodes declare what they can carry when they register. A `transports` list holds
the link transports — `srt`, `whip`, `whep` — each with the socket roles the
node can take over it; a bare name offers both, which is what every Strom node
has always said. A device is not a transport but a terminal, so it sits in its
own `devices` list. A browser page offers `whip` and `whep` in the `connect`
role only, and declares `devices: [capture, display]`. A Strom node hosting
WebRTC for it offers `whip` and `whep` in the `listen` role:

```yaml
transports:
  - srt
  - { name: whip, roles: [listen] }
  - { name: whep, roles: [listen] }
```

A WebRTC link needs a signalling URL, and only the node serving it knows its
path. So each data-plane address may carry the base URL per transport, and the
controller appends `/<hop id>` and assumes nothing else:

```yaml
data_plane:
  default:
    host: 172.26.0.10
    signalling:
      whip: http://172.26.0.10:8080/whip
      whep: http://172.26.0.10:8080/whep
```

A node normally does not write that itself — its adapter fills it in at
registration. `weave-adapter-strom` builds it from `strom.signalling_base`, a
data-plane alias to base URL map, plus the routes its own Strom serves.

The controller chooses the transport per link from both ends' capabilities and
writes the concrete sockets into each desired hop. Preference is `srt`, then
`whip`, then `whep`; a candidate needs the listening end to offer `listen`, be
dialable, and the other end to offer `connect`. WHIP is only ever hosted
downstream (the connecting end pushes media), WHEP only upstream (the
connecting end pulls). Two ends with no transport in common get a relay that
can carry both halves, or the stream stays `pending` with the reason. A WebRTC
socket carries that signalling URL and the endpoint id it was built from
instead of a host and port, and claims no port.

Adapters map the sockets they are given. The Strom adapter has one flow shape
per (ingress, egress) transport pair: `srt → srt` is the byte relay it always
built; `whip → srt` is `whip_input → videoenc → mpegtssrt_output`; `srt → whep`
is `mpegtssrt_input → whep_output`. A hop with WebRTC on both sides is refused,
because the adapter reads media progress from a hop's SRT byte counters and
such a hop has none. The browser node realises `device → whip` and
`whep → device` and nothing else. Neither adapter ever decides a transport.

`GET /v1/streams/{name}/endpoints` lists only what an external peer can dial,
so a `device` end reads `null` in its place (`ingress: null` for a camera
source, a `null` output for a screen destination). See `nodes/browser/README.md`
for the page and `bench/README.md` for running it against the bench.

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
Learning the real format means putting a parsing element in the pipeline, which
is a separate piece of work. Until then an absent `format` means unknown, not
wrong, and nothing is inferred from it.

When a declared source format does not satisfy a destination's `accepts`, the
stream places and the media flows — it just arrives somewhere it cannot be
decoded. That is reported rather than acted on:

```
degraded — destination 0 cannot accept the source format:
           audio.sample_rate is 48000 but accepts 44100
```

The mismatch is known at plan time, so it is reported before any media exists.
A lost node outranks it: both read `Degraded`, and the reason distinguishes them.

**Nothing converts anything yet.** Placing a resampler needs nodes to advertise
which transforms they can perform, and a cost model so the planner does not
silently insert a transcode farm to rescue a mistyped manifest. Naming the
problem precisely is what comes first, and it is what a conversion planner will
read when it arrives.

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
just run-strom-adapter --config examples/node.yaml
just cli --help
just apply               # examples/stream.yaml through northbound
just get-streams
```

The adapter needs a node config, from `--config` or `WEAVE_NODE_CONFIG`; it will
not start without one. `examples/node.yaml` names the bench's southbound and
Strom addresses, so point them at your own before running it anywhere else —
until they answer, the adapter serves `/health` and keeps retrying.

`just apply` and `just get-streams` call northbound, so it has to be up. The
applied stream stays `Pending` until the nodes it names register.

The CLI picks `WEAVE_NORTHBOUND_TOKEN` up from the environment; `--token`
overrides it.

## License

MIT. See [`LICENSE`](LICENSE).
