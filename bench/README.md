# open-weave bench

A full-system docker-compose stack: the three control-plane services, a
Postgres, and three Strom media nodes each sitting behind its own emulated
router, so any node's network can be impaired with `netem`. It starts **empty**
— you drive it with stream manifests and watch what the control plane does.

This is where open-weave is verified. If you want to see the system work, start
here.

## Prerequisites

| Need | Why | Check |
|---|---|---|
| Docker Engine + Compose v2 | runs the stack | `docker compose version` |
| [`just`](https://github.com/casey/just) | the recipes below | `just --version` |
| A Rust toolchain (stable) | `just bench stream` runs the `weave` CLI from source | `cargo --version` |
| `jq` | the recipes parse JSON with it | `jq --version` |
| `curl`, `bash`, `python3` | health polling, `just bench page`, `just bench hook-sink` | — |

Roughly 4 GB of disk for images, and a first `just bench up` takes several
minutes because it compiles the workspace into the bench image. Later runs
reuse the cache.

## Quickstart

Every recipe is run from the **repository root** as `just bench <recipe>`. From
inside `bench/` you can drop the `bench` prefix.

```sh
just bench up                  # build + start + wait for healthy
just bench status              # health, registered nodes, streams
just bench stream-up basic     # apply a stream and drive it end to end
```

`stream-up basic` should end with `basic: flowing` in a few seconds. That means
the controller placed a two-hop path across Strom nodes 1 and 2, both adapters
provisioned their flows, an ffmpeg producer is pushing SRT into the source, and
an ffmpeg consumer is pulling the receiver output.

Open <http://localhost:29082/ui> to watch it live: registered nodes, each
stream's path across them with per-hop link conditions and rates, and the
addresses external peers dial.

Then take it apart:

```sh
just bench netem node1 delay 200ms loss 5%   # impair node 1's network
just bench status                            # per-hop rates and conditions
just bench netem-clear node1

just bench stream-down basic   # detach the endpoints and delete the stream
just bench down                # tear down containers, networks, volumes
```

`just bench` on its own lists every recipe, grouped.

## Stream manifests

`manifests/` is a library of scenarios — fan-out, NAT traversal, pinned transit,
format mismatch, reversed links. Each file's name is the stream name, so the
recipes take a bare name:

```sh
just bench stream-ls           # list them with one-line descriptions
just bench stream-up fanout    # one source, two destinations
just bench stream-up nat-relay # both ends behind NAT, bridged through node 1
```

`manifests/README.md` describes each one and records the status it was observed
to reach.

Two manifests are not drivable, and `stream-up` says so rather than waiting for
an ingress that will never exist: `disabled` reports that it is disabled and
exits `0`, `unplaceable` reports that nothing matched its hops and exits `1`.

## Recipes

```sh
just bench up          # build + start + wait for healthy
just bench status      # health, registered nodes, controller view, streams
just bench ps          # container states
just bench logs controller   # follow one service's logs
just bench down        # tear down (containers, networks, volumes)
```

`stream-up` is the one to reach for: it applies the manifest, attaches the
producer to the stream's ingress and **one consumer per receiver output**, and
waits until the controller reports `flowing`. It reads the output count from
discovery, so fan-out gets the right number of consumers without being told.

The lower-level pieces are there when a scenario wants them separately —
applying a stream without media, or detaching a producer mid-run to watch the
status transition:

```sh
just bench stream basic         # control plane only: placed but unfed -> `awaiting_input`
just bench stream-rm basic      # delete the stream; controller tears down its hops

just bench producer-up basic    # feed a stream's source ingress
just bench producer-down        # stop it (drives source -> no-source transitions)
just bench consumer-up basic    # pull output 0
just bench consumer-2-up fanout # pull output 1 (fan-out's second destination)
just bench consumer-down
just bench consumer-2-down

just bench stream-wait basic flowing   # poll the controller until a status is reached
```

The `producer`, `consumer`, and `consumer-2` containers are **singletons**, so
only one stream can be driven with media at a time. `stream-up <other>`
re-points them, and the previously driven stream loses its source — it reports
`degraded` (a path that was flowing and went quiet is a fault, distinct from one
that never received input, which is `awaiting_input`). Applying several streams
at once is fine; only the media endpoints are shared.

## Topology

```
        net_core 172.25.0.0/24
   northbound  southbound  controller
        |          |           |          \
   router-1     router-2       |        router-3  (NAT)
  (172.25.0.11) (172.25.0.12)  |       (172.25.0.13)
        |            |                      |
  net_node1      net_node2            net_node3
  172.26.0.0/24  172.27.0.0/24        172.29.0.0/24
  strom-1        strom-2              strom-3
  adapter-1      adapter-2            adapter-3
  (relay)                             (outbound-only)
```

Each node subnet reaches everything else only through its router. Applying netem
on a router impairs both directions of that node's traffic: SRT media between
Stroms, adapter heartbeats, and controller→Strom API calls.

**Node 3 sits behind a NAT.** Its router masquerades outbound traffic, and no
route to `172.29.0.0/24` is installed anywhere outside net_node3 — not in the
core containers, not in routers 1 and 2. Docker's inter-network isolation blocks
the bridge-level path, so node 3 can open connections outward and nothing can
open one toward it. That asymmetry is load-bearing: adding a return route would
silently delete the boundary and the `nat-*` manifests would start passing for
the wrong reason.

Node 1 is the only node advertising `relay: true`, so it is what the controller
picks when a link needs transit.

Because a NAT'd node's sockets can only be dialled from inside its network, the
bench carries a second pair of media endpoints (`producer-3`, `consumer-3`) on
net_node3. The producer/consumer recipes choose between them and the net_core
pair from the resolved address, so `just bench stream-up <name>` works
uniformly.

Those cross-subnet routes are installed by the `route-manager` service, which
re-asserts them every few seconds inside each container's network namespace
(some images, Strom and ffmpeg among them, have no `ip` tool of their own).
Because routes live in the netns, they are lost whenever a container restarts or
is recreated — so asserting them on a loop is what makes `docker compose
restart`, `stop`/`start`, and a rebuild+recreate all recover on their own. It
also picks up the profiled producer/consumer whenever they come up.

## Impairment

```sh
just bench netem node1 delay 200ms loss 5%   # any raw `tc netem` spec
just bench netem node1 delay 200ms 20ms      # delay + jitter
just bench netem-show node1
just bench netem-clear node1
```

Nodes are `node1`, `node2`, `node3`. The spec is applied to both legs of that
node's router, so it impairs the node's traffic in both directions.

## External verification endpoints

`producer` and `consumer` are ffmpeg containers that live **outside** the system
and only exist to exercise it. They are off by default (compose `profiles`) and
started explicitly by the recipes above — never as part of `just bench up`. Both
sit on `net_core` and route to the node subnets through the netem routers, so
their SRT traffic crosses the same impaired hops as real external peers.

- `producer` pushes `testsrc2 + sine` as MPEG-TS over SRT (caller) into a source
  ingress listener. `producer-up <stream>` resolves that stream's ingress address
  from discovery (default stream `basic`).
- `consumer` pulls a receiver flow's output (SRT caller) and discards it
  (`-f null -`). `consumer-up <stream>` resolves the stream's first output address
  from discovery; `consumer-2-up <stream>` resolves the second output (fan-out).

Data-plane addresses are never hardcoded in the recipes: they are resolved from
the controller's discovery API.

```sh
curl -s -H "Authorization: Bearer bench-northbound-token" \
  localhost:29082/v1/streams/basic/endpoints | jq
# { "ingress": {node,host,port,url}, "outputs": [{node,host,port,url}] }
```

`200` once placed, `503` while known-but-unplaced, `404` if unknown, `401`
without the northbound token. The
`scripts/endpoints.sh <stream> ingress|output|outputs [index]` helper polls this
until placed and prints `host:port` — or, for `outputs`, how many receiver
outputs the stream has, which is how `stream-up` knows how many consumers to
attach. It reads `WEAVE_NORTHBOUND_TOKEN` from its environment and fails fast on
`401` rather than polling a rejected token.

## Browser node

A web page can be a node (`nodes/browser/`). The bench runs one as a headless
Chromium inside `net_core`, because WebRTC media has to reach Strom's ICE
candidates on the node subnets and a browser on a macOS host generally has no
route there. The service is off by default (profile `browser`):

```sh
just bench browser-up      # build + start the page as node `browser-bench`
just bench browser-stream  # apply browser-cam + browser-return, drive both
just bench browser-down    # detach, delete both streams, stop the page
```

`browser-cam` sends the page's camera to node-1 (the planner picks WHIP hosted
on Strom; the consumer pulls the SRT output). `browser-return` feeds the
producer into node-1 and plays it on the page (the planner picks WHEP hosted on
Strom). Both manifests are templates: `browser-stream` fills in the node id,
which `docker-compose.yml` pins to `browser-bench` so the manifests keep
pointing at the page across restarts.

`browser-return` reaches `flowing`. **`browser-cam` does not** — Strom's
`whip_input` accepts only H264 and the Playwright image's Chromium has no H264
encoder on arm64, so only Opus audio negotiates and the gateway flow stalls.
`browser-stream` prints its status rather than waiting on it. `BACKLOG.md` has
the item and what would fix it.

Node 1 offers `whip [listen]` and `whep [listen]` and names the base URL
browsers reach its signalling at (`strom.signalling_base` in
`config/adapter-1.yaml`, one entry per data-plane alias; the adapter appends
`/whip` and `/whep` and advertises the result on that alias). Southbound allows
the page's origin with `WEAVE_SOUTHBOUND_CORS_ORIGIN=*`, a development value
like the tokens.

`just bench logs browser` shows one line per 5 s with every hop's conditions and
negotiated codecs.

### A page in your own browser

To use a real browser on your machine instead of the in-bench Chromium:

```sh
just bench page                # serve nodes/browser on localhost:8000, print the URL
just bench host-cam            # apply browser-cam-host for that seat
just bench host-cam-down       # delete it again
```

`just bench page` pins the page's node id with `#node=<seat>` (default
`guest-1`), so a page that reloads keeps its name and the applied stream keeps
pointing at it. Unpinned, every tab is a new node. Pass
`just bench page 8000 guest-2` for a second, concurrent guest.

A browser on the host generally cannot reach `172.26.0.10:8080`, so node 1
advertises a second data-plane alias, `docker-host`, with the same SRT address
as `default` and signalling at `localhost:28080` instead. The
`browser-cam-host` manifest selects it with `network: docker-host` on the
destination, and the planner resolves both the WHIP URL the page dials and the
SRT output host from that one alias. With nothing dialling the SRT output the
stream reads `degraded`, which is the roll-up for a source that flows and a
destination that does not.

## Node lifecycle webhooks

The `controller` service sets `WEAVE_WEBHOOK_URL` to
`http://host.docker.internal:29099` and `WEAVE_WEBHOOK_TOKEN` to
`bench-webhook-token`, so it delivers `node.registered`, `node.online` and
`node.offline` to a sink on this host. `just bench hook-sink` is that sink: a
few lines of Python that print each event and the `Authorization` header it
arrived with. Export `WEAVE_WEBHOOK_URL=` (empty) before `just bench up` to
switch webhooks off.

Run the sink in its own terminal *before* `just bench up` — the controller logs
a connection-refused line and gives up on anything emitted while it is down.

```sh
just bench hook-sink            # terminal 1
just bench up                   # terminal 2
just bench page 8000 guest-1    # terminal 3, then open the printed URL
```

Opening the page gives one `node.registered` for `guest-1`. Reloading the tab
gives a second `node.registered` for the same id, because `#node=` pins the seat
and a re-registration is not reported as `node.online`. Closing the tab and
waiting `WEAVE_NODE_TTL_SECS` (15) gives `node.offline`.

`host.docker.internal` resolves through the `extra_hosts: host-gateway` entry on
the controller service. See the root README's "Node lifecycle webhooks" for the
payload and the delivery guarantees.

## Host ports

| Port | Service |
|------|---------|
| 29080 | northbound (`WEAVE_NORTHBOUND_URL=http://localhost:29080 weave ...`) |
| 29081 | southbound (`/v1/nodes` shows registered capabilities) |
| 29082 | controller: dashboard at `/ui`, `/view`; API at `/v1/status`, `/v1/streams/{name}/endpoints` |
| 28080 | strom-1 API |
| 28081 | strom-2 API |
| 28082 | strom-3 API (host→container; grants no route into net_node3) |
| 29099 | `just bench hook-sink` on this host, where the controller delivers node lifecycle webhooks |

## Authentication

The API surfaces require a bearer token (see the root README for the model). The
bench defaults to development values so `just bench up` stays a single command:

| Variable | Default | Used by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | `bench-northbound-token` | northbound, controller, CLI, `endpoints.sh` |
| `WEAVE_SOUTHBOUND_TOKEN` | `bench-southbound-token` | southbound, controller, the three adapters, the browser page |

Export either variable to override it; `docker-compose.yml` and the recipes read
the same defaults, so both stay in step. The adapter configs leave
`node.southbound_token` unset and inherit the env var instead, keeping the value
out of the repo.

The `just` recipes add the right header for you. Calling the APIs by hand needs
it explicitly:

```sh
curl -s -H "Authorization: Bearer bench-southbound-token" localhost:29081/v1/nodes | jq
curl -s -H "Authorization: Bearer bench-northbound-token" localhost:29080/v1/streams | jq
```

Without a valid token these return `401` and `WWW-Authenticate: Bearer`. Every
service **refuses to start** if its token variable is missing, so a
`docker compose up` that exits immediately with a `WEAVE_..._TOKEN is unset`
error is the fail-closed default working, not a bug. `WEAVE_AUTH_DISABLED=1`
opts out for local runs.

`/health` on all three services, the controller's dashboard (`/ui`, `/view`),
and the `/v1/status` rollup need no token, so anyone who can reach port 29082
can read the full topology and allocated ports. Compose publishes it on all
interfaces: fine on a laptop, but **do not expose a controller port on a shared
or public host.**

## API versioning

Both contracts live under `/v1` (see the root README). `/health` and the
controller's `/`, `/ui`, `/view` sit outside it. There are no unprefixed
aliases, so `curl localhost:29081/nodes` returns `404` — add the prefix. The
recipes carry it in the `v` variable at the top of the `justfile`.

The adapters declare `protocol_version` when they register; a mismatch is
refused with `409` and the adapter exits rather than retrying, so
`just bench logs adapter-1` naming an incompatible protocol version means the
adapter image and the controller image are out of step. Rebuild with
`just bench up`.

## Troubleshooting

**`error: Justfile does not contain recipe 'up'`** — you are in the repository
root without the prefix. Use `just bench up`, or `cd bench` first.

**`just bench up` times out waiting for the stack.** `just bench ps` shows which
container is unhealthy and `just bench logs <service>` says why. A service
exiting immediately with `WEAVE_..._TOKEN is unset` is the fail-closed default,
not a crash.

**A stream stays `pending`.** Nothing matched its hops. `just bench status`
lists the registered nodes; the manifest names one that is not among them, or
the node it names has gone offline.

**A stream stays `awaiting_input`.** It placed, but nothing is feeding it.
`just bench stream` applies without media on purpose — use `just bench stream-up`
to attach the producer and consumer too.

**A stream reads `degraded` after driving another one.** The producer and
consumer are singletons, so `stream-up <other>` took them from the first stream.

**Ports already in use.** The bench publishes 29080–29082, 28080–28082 and
29099. Another stack holding one of those makes `up` fail; stop it or change the
`ports:` entries in `docker-compose.yml`.

**A container came back with no route to another subnet.** `route-manager`
repairs it within a few seconds; `just bench logs route-manager` shows a line
per repair and is otherwise quiet.

## How it works

- Any service can be restarted, stopped/started, or rebuilt and recreated
  individually; `route-manager` repairs its routes within a tick and the system
  reconverges without manual steps.
- The controller never talks to Strom. Per stream it derives an ordered hop chain
  — a **sender** hop and a **receiver** hop — places each on a node (sender by
  `source.node` or host-match on the source URL; receiver by host-match on the
  destination), and groups the desired hops per node. The per-node **adapters**
  pull their own hops (`GET /v1/nodes/{id}/desired` via southbound, full replace of
  what that node should run) and create/start/delete the `weave-…` Strom flows.
  Hops for a node that has not registered yet just wait until it does.
- Per-stream status is rolled up from adapter-reported hop conditions:
  `awaiting_input` (no source media) → `degraded` (source flowing, not end to end)
  → `flowing`. See the controller `/v1/status` endpoint.
- The receiver hop listens on the destination port and re-exposes the media on
  `port + 1` for a downstream consumer.
- No pre-configured flows are shipped — create them through the CLI.
