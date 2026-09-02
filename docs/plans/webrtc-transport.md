# Plan: WebRTC (WHIP/WHEP) as a second transport

## Why

Browsers cannot speak SRT. A browser can only contribute media over WHIP and
receive it over WHEP. open-weave has one transport (`Transport::Srt`) and the
planner assumes SRT socket roles throughout (`BACKLOG.md`, "Not scheduled").
Adding WebRTC lets a browser be the edge of a weave path, which is the demo:
open a page, the webcam becomes a contribution, and the programme comes back.

Delivery into open-live stays SRT. open-live dials a weave receiver output
with its `builtin.mpegtssrt_input` block and needs nothing from this plan.
WebRTC is for the browser edge only: WHIP into a Strom node on the way in,
WHEP out of a Strom node on the way back.

## How the pieces fit

Strom already has the blocks (verified on the bench Strom, `GET /api/blocks`):

| Block | Role | Address |
|---|---|---|
| `builtin.whip_input` | hosts a WHIP endpoint; decodes to raw A/V | `/whip/{endpoint_id}` (path used by open-live; verify on Strom) |
| `builtin.whep_output` | hosts a WHEP endpoint from raw A/V | `/api/whep/{endpoint_id}` (from the block description) |
| `builtin.videoenc` | raw → H.264 | |
| `builtin.mpegtssrt_output` | mux + SRT; auto-encodes audio to AAC | `srt_uri`, `latency` |
| `builtin.mpegtssrt_input` | SRT → demux → decode | `srt_uri`, `latency`, `decode` |

A hop whose ingress and egress transports differ is a gateway. The adapter
maps it to a block flow instead of the element flow it emits for SRT→SRT.
Nothing else in the model changes: a gateway is still one hop on one node.

## Phase 1 — WHIP ingress and WHEP egress hosted on Strom nodes

Result: a manifest can say "media enters over WHIP at node X" and "media
leaves over WHEP at node Y". The browser is an external peer, exactly as the
bench's ffmpeg containers are today. This alone gives the webcam demo.

Manifest:

```yaml
name: alice-cam
source:
  whip:
    node: strom-node-1          # hosts the WHIP endpoint
destinations:
  - srt:
      node: strom-node-2        # open-live dials this receiver's output
```

```yaml
name: alice-return
source:
  srt:
    node: strom-node-2          # open-live sends its PGM here
destinations:
  - whep:
      node: strom-node-1        # the browser plays this
```

### Core (`crates/core`)

- `Transport` gains `Whip` and `Whep`.
- `SocketSpec` gains `url: Option<String>`. WebRTC sockets carry the full
  signalling URL; `host`/`port` stay `None`. `SocketRole::Listen` is the
  hosting side, `Connect` the client side, so the role vocabulary is reused.
- `StreamTransport` gains `Whip(WebRtcEndpoint)` and `Whep(WebRtcEndpoint)`.
  `WebRtcEndpoint { node, network, format/accepts }` — no `remote`, no `via`
  on a source (same rule as SRT), no latency.
- `params: SrtParams` becomes `params: SocketParams` (enum by transport) or
  stays and is ignored for WebRTC. Decide when touching it; the enum is
  cleaner, the field is only read by the Strom mapping.
- `DataPlaneAddr` gains `webrtc_base_url: Option<String>` (e.g.
  `http://172.26.0.10:8080`). WHIP/WHEP URLs are `<base>/whip/<hop-id>` and
  `<base>/api/whep/<hop-id>`. Browsers on an `https` page cannot fetch an
  `http` URL, so this must be a configurable full URL, not host+port.
- `TransportDescriptor` gains `roles: Vec<SocketRole>` with a default of both,
  so existing `transports: [srt]` configs read unchanged.
- Protocol version: no bump. Only nodes advertising `whip`/`whep` are ever
  handed those sockets, and old adapters advertise `srt` only, so nothing
  they deserialize changes.

### Planner (`crates/controller/src/path.rs`)

- `source_socket`: for a `whip` source, the sender hop's ingress is
  `Listen`/`Whip` with URL `<base>/whip/<sender-hop-id>`. No port claimed.
  Requires the node to advertise `whip` with role `listen`; otherwise
  `PlacementError::TransportUnsupported { node, transport }` and the stream
  stays `pending` with that reason.
- Receiver terminal egress: for a `whep` destination, the consumer socket is
  `Listen`/`Whep` with URL `<base>/api/whep/<receiver-hop-id>`, instead of
  the SRT listener on a claimed port.
- `plan_link` is untouched in this phase. Every link between hops is still SRT.
- `stream_endpoints`: `EndpointAddr` gains `transport`; `url` is the
  WHIP/WHEP URL for those endpoints; `host`/`port` are taken from the URL.
- Format conflicts: a WHIP source's format is whatever the browser negotiates;
  leave `format` undeclared and let the existing "unknown, not wrong" rule
  apply.

### Strom adapter (`crates/strom`, `crates/adapter-strom`)

- `FlowSpec` gains `blocks: Vec<Block>` (id, `block_definition_id`, name,
  properties, position). Strom flows already carry the field.
- Mapping by (ingress transport, egress transports):
  - srt → srt: unchanged element flow.
  - whip → srt: `whip_input(endpoint_id=hop id)` → `videoenc(codec=h264)` →
    `mpegtssrt_output(srt_uri, latency)`; `whip_input:audio_out` →
    `mpegtssrt_output:audio_in_0`. Fan-out tees after the encoder.
  - srt → whep: `mpegtssrt_input(srt_uri, latency, decode=true)` →
    `whep_output(endpoint_id=hop id)` on both video and audio.
  - whip → whep: `whip_input` → `whep_output` (browser to browser on one node;
    falls out of the two above).
- Hop ids become `endpoint_id`s. Verify Strom's allowed charset first; open-live
  strips non-alphanumerics from ids "that Strom may reject" and hop ids contain
  `-`.
- Drift detection (`flow_drifted`) compares SRT URIs parsed from element
  properties; extend it to compare `endpoint_id`s on blocks.
- Status: `hop_statuses` reads `srt-stats` and finds elements by the
  `srtsrc`/`srtsink` id prefix. Block-internal element ids differ. Check what
  `srt-stats` returns for a block-based flow, and how Strom exposes WHIP/WHEP
  session state (connected clients, bytes). Map to `LinkCondition` the same
  way: no session → `Idle` for a host, session without bytes → `Connected`,
  bytes advancing → `Flowing`, frozen → `Stalled` via `StallTracker`.
- Registration: node config `transports: [srt, whip, whep]` with roles;
  `data_plane.default.webrtc_base_url`.

### Bench

- Node configs advertise `whip`/`whep` and a `webrtc_base_url` on node 1.
- Manifests `whip-ingress` (whip source → srt destination) and `whep-egress`
  (srt source → whep destination), with rows in the observed-behaviour table.
- A minimal static page under `bench/browser/` that takes a WHIP URL and
  publishes the webcam, and a WHEP URL and plays it. Served by a small nginx
  service on `net_core`. No weave awareness; this is phase 2's page without
  the node logic.
- Docker on macOS is the risk: WebRTC media is UDP and Strom's ICE candidates
  will name container addresses the host browser cannot reach. Strom's own
  guide recommends `network_mode: host` for WHIP/WHEP, which is Linux-only.
  Check whether Strom can be told an external address and a UDP port range;
  if not, run the WebRTC edge Strom natively on the Mac (Strom ships macOS
  builds) or run the bench on a Linux host. Settle this before phase 2.

### Done when

- `cargo test --workspace` and clippy clean, with serde tests for the new
  manifest variants, planner tests for whip source / whep destination /
  unsupported node, and mapping snapshot tests for the two gateway flows.
- On the bench, a browser publishes to the `whip-ingress` WHIP URL and the
  stream reports `flowing` with ffmpeg on the SRT output; `whep-egress` plays
  the bench producer in the browser.
- `README.md` transport section and `BACKLOG.md` "Transports other than SRT"
  updated to say what is and is not covered.

## Phase 2 — the browser as a node

Result: the page registers with southbound, shows up on the dashboard with
link conditions, and realises hops itself. The operator's manifest names the
browser node; the planner picks WebRTC for the link and puts the hosting side
on the Strom node.

Manifest:

```yaml
name: alice-cam
source:
  device:
    node: browser-a1b2          # the node's own camera
destinations:
  - srt:
      node: strom-node-2
```

```yaml
name: alice-return
source:
  srt:
    node: strom-node-2
destinations:
  - device:
      node: browser-a1b2        # the node's own screen
```

### Core

- `Transport::Device` for a socket the node terminates itself (camera in,
  video element out). No address. `StreamTransport::Device(NodeEndpoint)`.
  This is the small edge of the backlog's "Capture inputs" item: one
  node-local device the node picks, no enumeration. Say so in `BACKLOG.md`.
- The browser registers `transports: [{whip: [connect]}, {whep: [connect]},
  {device: [listen, connect]}]`, `data_plane.default` with
  `reachability: outbound_only` (host is a placeholder; nothing dials it).

### Planner

- `plan_link` picks a transport per link instead of assuming SRT: the set of
  transports where the upstream has one role and the downstream the
  complementary one, and the side that would `Listen` is dialable. Prefer
  `srt`, then `whip`/`whep`. This is the same dialability rule with a
  transport column added; relays still come from `splice_relays`.
- A link whose upstream is a `device` sender and downstream is a Strom node
  resolves to WHIP with Strom hosting. A link into a `device` receiver
  resolves to WHEP with Strom hosting.
- The receiver hop on a browser has ingress `Connect`/`Whep` and egress
  `Device`; the sender hop has ingress `Device` and egress `Connect`/`Whip`.
- `stream_endpoints` reports nothing dialable for a `device` end.

### Southbound

- CORS. The page calls `/v1/nodes/register`, `/heartbeat`, and `/desired`
  cross-origin. Add a CORS layer with a configurable allowed origin
  (`WEAVE_SOUTHBOUND_CORS_ORIGIN`), off by default.
- Token. The page needs `WEAVE_SOUTHBOUND_TOKEN`. For the demo, the serving
  page takes it from a URL fragment or a tiny minting endpoint. Per-node
  tokens are already listed as a follow-up in `README.md`; this makes them
  worth scheduling, not part of this plan.

### The page (`bench/browser/`, plain JS, no build step)

- Generates `browser-<8 hex>` once per tab (sessionStorage), registers with
  `protocol_version`, heartbeats every 5 s with `hop_status`, polls
  `/desired` every 2 s.
- Realises hops: `Device → Whip`: `getUserMedia`, `RTCPeerConnection`,
  WHIP `POST application/sdp`, keep the `Location` for `DELETE` on teardown.
  `Whep → Device`: WHEP `POST`, attach the answer's tracks to a `<video>`.
- Reports `LinkCondition` from `getStats()` bytes sent/received across polls,
  mirroring `StallTracker`, and `LinkStats` from RTCP loss counters.
- Shows its node id and a ready-to-apply manifest snippet, so the operator
  (or a `just browser-stream <node-id>` recipe) can apply it.

### Done when

- Planner tests: device sender → Strom picks WHIP hosted on Strom; Strom →
  device receiver picks WHEP; two browsers with no Strom between them stay
  `pending` with `NoRelayAvailable`; SRT-only links plan exactly as before.
- On the bench: open the page, apply `alice-cam`, the dashboard shows the
  browser node and the path `flowing`; open-live (or ffmpeg) receives it.
  Apply `alice-return`, the page plays it.

## Phase 3 — round trip with open-live

Nothing new in open-weave. Configure an open-live SRT output to dial
`alice-return`'s ingress (`GET /v1/streams/alice-return/endpoints` → `ingress.url`).
Handing that URL to open-live is a second provider-shaped seam on the
open-live side (see `open-live-source-providers.md`, out of scope there);
for the demo, set it by hand through open-live's outputs API.

## Before starting phase 1, check on the bench Strom

1. `endpoint_id` allowed characters (create a `whip_input` with a `weave-…` id).
2. Actual WHIP and WHEP URL paths (`/whip/…` vs `/api/whip/…`).
3. What `srt-stats` returns for a flow whose SRT sockets live inside blocks.
4. How WHIP/WHEP sessions are exposed for status.
5. ICE: whether Strom accepts an external address and a UDP port range, and
   whether a host browser on macOS can reach a container Strom at all.

## Not in this plan

- Weave pushing WHIP *into* open-live's per-production WHIP endpoints. Strom
  has `builtin.whip_output`, so it is possible, but open-live creates those
  endpoints at activation and the URL is dynamic. SRT in is simpler.
- WebRTC between two Strom nodes. SRT is the right link there.
- TLS. Remote attendees need `https` for `getUserMedia` and for the WHIP URL;
  that is a reverse proxy in front of the bench, not a weave change.
- Per-node tokens and mTLS (already listed follow-ups).
