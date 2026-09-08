# Bench stream manifests

A library of `StreamDefinition` manifests for exercising the bench. Each file's
name is also the stream `name`, so the recipes take a bare name:

```sh
just bench stream-ls          # list manifests + one-line description
just bench stream-up basic    # apply + drive end to end, wait for `flowing`
just bench stream-down basic  # detach endpoints and delete the stream

just bench stream basic       # apply only: placed but unfed -> `awaiting_input`
just bench stream-rm basic    # delete the stream; controller tears down its hops
```

The bundled `producer`/`consumer` verification endpoints take a stream name so
any scenario can be driven end to end
(`just bench producer-up <stream>` / `consumer-up <stream>`, default `basic`).
Addresses are not hardcoded: the recipes resolve them from the controller's
discovery API (`GET :29082/v1/streams/<name>/endpoints`, or the
`scripts/endpoints.sh` helper) — the producer dials the reported `ingress` and
each consumer dials an `outputs[]` entry. Fan-out has one output per destination,
so a consumer attaches to each: `consumer-up <stream>` (output 0) and
`consumer-2-up <stream>` (output 1).

`stream-up` does all of that in one step — it reads the output count from
discovery (`endpoints.sh <stream> outputs`) and attaches that many consumers, so
the "With producer + consumer" column below is what a bare `stream-up <name>`
produces. The media endpoints are singletons, so driving a second stream takes
them from the first.

## Observed behaviour

Statuses below were observed on the live bench (controller `/status`), not
inferred. Media columns use the bundled `producer`/`consumer`; `—` marks a cell
not measured.

| Manifest | Scenario | No media | With producer | With producer + consumer |
|----------|----------|----------|---------------|--------------------------|
| `basic` | listener node-1 → node-2 (canonical contribution) | `awaiting_input` | `degraded` | `flowing` |
| `reverse` | listener node-2 → node-1 (matrix directionality) | `awaiting_input` | — | `flowing` |
| `same-node` | source and destination both on node-1 | `awaiting_input` | `degraded` | `flowing` |
| `fanout` | one source, two destinations | `awaiting_input` | `degraded` | `flowing` |
| `unplaceable` | `source.node: strom-node-404` (never registers) | `pending` | `pending` | `pending` |
| `disabled` | `enabled: false` | `idle` | `idle` | `idle` |
| `srt-latency` | non-default SRT latency (120ms / 2000ms) | `awaiting_input` | `degraded` | `flowing` |
| `via` | pinned transit: node-1 → bridge on node-2 → node-1 | `awaiting_input` | — | `flowing` |
| `nat-egress` | NAT'd node-3 contributes out to node-1 | `awaiting_input` | — | `flowing` |
| `nat-ingress` | node-1 delivers into NAT'd node-3 (link reverses) | `awaiting_input` | — | `flowing` |
| `nat-relay` | both ends on NAT'd node-3; bridged via node-1 | `awaiting_input` | — | `flowing` |
| `format-ok` | declared source format the destination accepts | `awaiting_input` | — | `flowing` |
| `format-mismatch` | 48 kHz source into a 44.1 kHz-only destination | `degraded` | — | `degraded` |
| `browser-cam` | page camera → node-1 over WHIP, consumer pulls SRT | `degraded` (see note) | — | `degraded` (see note) |
| `browser-cam-host` | camera of a page on the docker host → node-1 via the `docker-host` alias | `degraded` (see note) | — | — |
| `browser-return` | producer → node-1 → page screen over WHEP | `awaiting_input` | `flowing` | `flowing` |

Notes:

- **`unplaceable`**: northbound accepts it (a listener source with an explicit
  node is valid), but the sender hop is pinned to `strom-node-404`, which never
  registers, so it never provisions and the stream stays `pending`. Register a
  node with that id and it would converge — Kubernetes-style unschedulable.
- **`fanout`**: the sender tees to both destinations (one srtsink per
  destination) and there is one receiver hop per destination —
  `weave-fanout-receiver-0` on node-2 and `weave-fanout-receiver-1` on node-1,
  co-located with the source. The `flowing` cell requires a consumer on each
  receiver output at the same time (`consumer-up` + `consumer-2-up`); with the
  producer but no consumers it is `degraded`, and detaching either consumer drops
  it back to `degraded` (that receiver's egress falls to `connected`) — observed,
  so per-destination health is real.
- **`disabled`**: listed by northbound but reconciled to `idle`; no hops are
  provisioned.
- **`format-*`**: the declared format in both is what the bench producer actually
  sends, read off `ffprobe` against a receiver output rather than assumed — the
  audio is **mono**, which `-i sine=...` gives you unless told otherwise.
  `format-mismatch` is degraded from the moment it is applied, before any media
  exists, with `destination 0 cannot accept the source format: audio.sample_rate
  is 48000 but accepts 44100`. Driving it changes the reason not at all: every
  hop reports `flowing` on both sockets, because the bytes do arrive — at an
  endpoint that cannot use them. Nothing converts anything yet; the diagnosis is
  the feature.

- **`nat-*`**: node 3 sits behind a real NAT — router-3 masquerades its outbound
  traffic and nothing outside net_node3 is given a route back in. Verified
  directly, not assumed: a TCP connect from the controller and from node 1 to
  `172.29.0.10:8080` both fail, while node 3 reaches `172.26.0.10:8080` and its
  adapter registers through the same path.
  - **`nat-egress`** needs no relay and no reversal: the destination is dialable,
    so the sender calls out, which is the direction a NAT allows anyway.
  - **`nat-ingress`** is the reversal. Observed sockets: the sender's egress on
    node 1 is `listen`, and node 3's receiver ingress is `connect` to
    `172.26.0.10`. Delivery into a NAT'd site costs a socket role, not a relay.
  - **`nat-relay`** is the jump node. Observed: `weave-nat-relay-bridge-0-0` on
    node 1 listens on *both* sockets while both node-3 hops dial out to it.
    Removing `relay: true` from node 1 makes the stream unplaceable with
    `no route from strom-node-3 to strom-node-3: neither can be dialled and no
    relay node is available`, so the bridge is doing the work rather than
    decorating a path that would have worked anyway.

  Source and destination are the same node in `nat-relay` because the bench has
  one NAT'd site. The media still crosses the NAT twice, outbound each time,
  which is the mechanism two separate NAT'd sites would rely on — but two sites
  genuinely unable to reach each other is not what this covers.

  Media endpoints for these live *inside* net_node3 (`producer-3`,
  `consumer-3`). That is not a bench workaround: a socket on a NAT'd node can
  only be dialled from inside its network, which is why such a site runs its own
  encoder and decoder. `producer-up`/`consumer-up` pick the right container from
  the resolved address via `scripts/inside.sh`.

- **`via`**: three hops — `weave-via-sender` on node-1, `weave-via-bridge-0-0`
  on node-2, `weave-via-receiver-0` back on node-1 — so the media crosses both
  routers twice. Observed with every hop `flowing` on both sockets. The bridge is
  an ordinary Strom flow (`srtsrc` listener → `srtsink` caller); relaying needed
  no adapter change. Both nodes here are dialable, so this covers a `via` pin
  rather than the NAT case that makes the controller insert a relay by itself —
  that one needs a node the bench cannot dial, which the topology does not yet
  have.
- **`browser-*`**: templates, `BROWSER_NODE` is the page's node id and
  `just bench browser-stream` fills it in; applying one directly leaves it
  `pending` on an unregistered node. The media for `browser-cam` comes from the
  page itself, so its "No media" column is the page with no consumer attached.
  It is `degraded` because the page's Chromium sends no H264 and Strom's WHIP
  input accepts nothing else, so only audio arrives — the page's own hop reads
  `flowing` on both sockets while node-1's reads `idle → flowing` at a few
  kb/s. `bench/README.md` has the detail and `BACKLOG.md` the item.
  `browser-return` is the mirror and flows end to end (VP9 + Opus into the page
  at ~8 Mb/s).
- **`browser-cam-host`**: `browser-cam` for a page in a browser on the docker
  host, which `just bench host-cam <seat>` fills in. Its destination names node
  1's `docker-host` alias, so the page is told to signal at `localhost:28080`
  while the SRT output stays on `172.26.0.10` (`bench/README.md`, "A page in
  your own browser"). Applied for the in-bench page it stays `pending`: that
  page's hop fails with `Failed to fetch`, because `localhost` in its container
  is itself. From the host, Google Chrome put H264 640x480 and AAC on the SRT
  output through it; with nothing dialling that output the stream reads
  `degraded`. The bundled consumer has not been attached to it, so the last
  column is unmeasured.
