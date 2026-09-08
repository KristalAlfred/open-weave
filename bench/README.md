# open-weave bench

Full-system docker-compose stack with per-node emulated routers so each Strom
node's network can be impaired with `netem`. The stack starts **empty** — drive
it with the `weave` CLI against the host-published northbound.

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
open one toward it. That asymmetry is deliberate and load-bearing: adding a
return route would silently delete the boundary and the `nat-*` manifests would
start passing for the wrong reason.

Node 1 is the only node advertising `relay: true`, so it is what the controller
picks when a link needs transit.

Because a NAT'd node's sockets can only be dialled from inside its network, the
bench carries a second pair of media endpoints (`producer-3`, `consumer-3`) on
net_node3. The producer/consumer recipes choose between them and the net_core
pair from the resolved address, so `just stream-up <name>` works uniformly.

Those cross-subnet routes are installed by the `route-manager` service, which
re-asserts them every few seconds inside each container's network namespace
(some images, Strom and ffmpeg among them, have no `ip` tool of their own).
Because routes live in the netns, they are lost whenever a container restarts or
is recreated — so asserting them on a loop is what makes `docker compose
restart`, `stop`/`start`, and a rebuild+recreate all recover on their own. It
also picks up the profiled producer/consumer whenever they come up.

## Host ports

| Port | Service |
|------|---------|
| 29080 | northbound (`WEAVE_NORTHBOUND_URL=http://localhost:29080 weave ...`) |
| 29081 | southbound (`/v1/nodes` shows registered capabilities) |
| 29082 | controller: dashboard at `/ui`, `/view`; API at `/v1/status`, `/v1/streams/{name}/endpoints` |
| 28080 | strom-1 API |
| 28081 | strom-2 API |
| 28082 | strom-3 API (host→container; grants no route into net_node3) |
| 28083 | open-live's Strom API, published by open-live's compose file, not this one (see "Feeding open-live") |
| 29099 | `just hook-sink` on this host, where the controller delivers node lifecycle webhooks (see "Node lifecycle webhooks") |

Open <http://localhost:29082/ui> to watch the system live: registered nodes,
every stream's path across them (per-hop link conditions and rates), and the
addresses external peers dial. It polls the controller's `/view` endpoint,
which serves the same joined picture as JSON.

Ports are offset (29xxx/28xxx) to avoid colliding with a stale prior bench that
may still hold 9080/8082/18080/18081. Point the CLI at northbound with
`WEAVE_NORTHBOUND_URL`.

## API versioning

Both contracts live under `/v1` (see the root README). `/health` and the
controller's `/`, `/ui`, `/view` sit outside it and are unchanged. There are no
unprefixed aliases, so `curl localhost:29081/nodes` now returns `404` — add the
prefix. The recipes carry it in the `v` variable at the top of the `justfile`.

The adapters declare `protocol_version` when they register; a mismatch is refused
with `409` and the adapter exits rather than retrying, so `docker compose logs
adapter-1` naming an incompatible protocol version means the adapter image and the
controller image are out of step. Rebuild with `just up`.

## Authentication

The API surfaces require a bearer token (see the root README for the model). The
bench defaults to development values so `just up` stays a single command:

| Variable | Default | Used by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | `bench-northbound-token` | northbound, controller, CLI, `endpoints.sh` |
| `WEAVE_SOUTHBOUND_TOKEN` | `bench-southbound-token` | southbound, controller, the three adapters, the browser page |

Export either variable to override it; `docker-compose.yml` and the recipes read
the same defaults, so both stay in step. The adapter configs deliberately leave
`node.southbound_token` unset and inherit the env var instead, keeping the value
out of the repo.

The `just` recipes add the right header for you. Calling the APIs by hand needs it
explicitly:

```sh
curl -s -H "Authorization: Bearer bench-southbound-token" localhost:29081/v1/nodes | jq
curl -s -H "Authorization: Bearer bench-northbound-token" localhost:29080/v1/streams | jq
```

Without a valid token these return `401` and `WWW-Authenticate: Bearer`. Every
service **refuses to start** if its token variable is missing, so a
`docker compose up` that exits immediately with a `WEAVE_..._TOKEN is unset`
error is the fail-closed default working, not a bug. `WEAVE_AUTH_DISABLED=1`
opts out for local runs.

`/health` on all three services, the controller's dashboard (`/ui`, `/view`), and
the `/v1/status` rollup need no token, so anyone who can reach port 29082 can read the full
topology and allocated ports. Compose publishes it on all interfaces: fine on a
laptop, but **do not expose a controller port on a shared or public host.**

## Usage

```sh
just up            # build + start + wait for healthy
just status        # health, registered nodes, controller view, streams

just stream-ls        # list the stream manifests (see manifests/README.md)
just stream-up basic  # apply + drive end to end, wait for `flowing`
just stream-down basic  # detach the endpoints and delete the stream

just netem node1 delay 200ms loss 5%   # impair node 1's network
just netem-show node1
just netem-clear node1

just hook-sink     # print node lifecycle webhooks (run in its own terminal)

just down          # tear down (containers, networks, volumes)
```

`stream-up` is the recipe to reach for: it applies the manifest, attaches the
external producer to the stream's ingress and **one consumer per receiver
output**, and then waits until the controller reports `flowing`. Fan-out needs
two consumers to be fully driven, and `stream-up` reads the output count from
discovery, so it attaches the right number without being told.

The lower-level pieces are still there when a scenario wants them separately —
applying a stream without media, or detaching a producer mid-run to watch the
status transition:

```sh
just stream basic         # control plane only: placed, but unfed -> `awaiting_input`
just stream-rm basic      # delete the stream; controller tears down its hops

just producer-up basic    # feed a stream's source ingress
just producer-down        # stop it (drives source -> no-source transitions)
just consumer-up basic    # pull output 0
just consumer-2-up fanout # pull output 1 (fan-out's second destination)
just consumer-down
just consumer-2-down

just stream-wait basic flowing   # poll the controller until a status is reached
```

The `producer`, `consumer`, and `consumer-2` containers are **singletons**, so
only one stream can be driven with media at a time. `stream-up <other>` re-points
them, and the previously driven stream loses its source — it reports `degraded`
(a path that was flowing and went quiet is a fault, distinct from one that never
received input, which is `awaiting_input`). Applying several streams at once is
fine; only the media endpoints are shared.

Two manifests are deliberately not drivable, and `stream-up` says so instead of
waiting for an ingress that will never exist: `disabled` reports that it is
disabled and exits `0`, `unplaceable` reports that nothing matched its hops and
exits `1`.

Data-plane addresses are never hardcoded in the recipes: they are resolved from
the controller's discovery API. Query it directly with:

```sh
curl -s -H "Authorization: Bearer bench-northbound-token" \
  localhost:29082/v1/streams/basic/endpoints | jq
# { "ingress": {node,host,port,url}, "outputs": [{node,host,port,url}] }
```

`200` once placed, `503` while known-but-unplaced, `404` if unknown, `401` without
the northbound token. The `scripts/endpoints.sh <stream> ingress|output|outputs [index]`
helper polls this until placed and prints `host:port` — or, for `outputs`, how many
receiver outputs the stream has, which is how `stream-up` knows how many consumers
to attach. The producer/consumer recipes use it and pass the token in. It reads
`WEAVE_NORTHBOUND_TOKEN` from its environment and fails fast on `401` rather than
polling a rejected token.

## Node lifecycle webhooks

The `controller` service sets `WEAVE_WEBHOOK_URL` to
`http://host.docker.internal:29099` and `WEAVE_WEBHOOK_TOKEN` to
`bench-webhook-token`, so it delivers `node.registered`, `node.online` and
`node.offline` to a sink on this host. `just hook-sink` is that sink: a few lines
of Python that print each event and the `Authorization` header it arrived with.
Export `WEAVE_WEBHOOK_URL=` (empty) before `just up` to switch webhooks off.

Run the sink in its own terminal *before* `just up` — the controller logs a
connection-refused line and gives up on anything emitted while it is down.

```sh
just hook-sink                 # terminal 1
just up                        # terminal 2
just page 8000 guest-1         # terminal 3, then open the printed URL
```

Opening the page gives one `node.registered` for `guest-1`. Reloading the tab
gives a second `node.registered` for the same id, because `#node=` pins the seat
and a re-registration is not reported as `node.online`. Closing the tab and
waiting `WEAVE_NODE_TTL_SECS` (15) gives `node.offline`.

`host.docker.internal` resolves through the `extra_hosts: host-gateway` entry on
the controller service. See the README's "Node lifecycle webhooks" for the
payload and the delivery guarantees.

## External verification endpoints

`producer` and `consumer` are ffmpeg containers that live **outside** the system
and only exist to exercise it. They are off by default (compose `profiles`) and
started explicitly by the recipes above — never as part of `just up`. Both sit on
`net_core` and route to the node subnets through the netem routers, so their SRT
traffic crosses the same impaired hops as real external peers.

- `producer` pushes `testsrc2 + sine` as MPEG-TS over SRT (caller) into a source
  ingress listener. `producer-up <stream>` resolves that stream's ingress address
  from discovery (default stream `basic`).
- `consumer` pulls a receiver flow's output (SRT caller) and discards it
  (`-f null -`). `consumer-up <stream>` resolves the stream's first output address
  from discovery; `consumer-2-up <stream>` resolves the second output (fan-out).

Both sit on `net_core` and route to either node subnet through the netem routers.
Addresses come from the controller's discovery API, not hardcoded tables — see the
`/v1/streams/{name}/endpoints` note above and `manifests/README.md` for the manifest
library.

## Browser node

A web page can be a node (`nodes/browser/`). The bench runs one as a headless
Chromium inside `net_core`, because WebRTC media has to reach Strom's ICE
candidates on the node subnets and a browser on a macOS host generally has no
route there. The service is off by default (profile `browser`):

```sh
just browser-up             # build + start the page as node `browser-bench`, print its id
just browser-stream         # apply browser-cam + browser-return, attach the media endpoints, wait
just browser-down           # detach, delete both streams, stop the page
```

`browser-cam` sends the page's camera to node-1 (the planner picks WHIP hosted
on Strom; the consumer pulls the SRT output). `browser-return` feeds the
producer into node-1 and plays it on the page (the planner picks WHEP hosted on
Strom). Both manifests are templates: `browser-stream` fills in the node id,
which `docker-compose.yml` pins to `browser-bench` so the manifests keep
pointing at the page across restarts. The recipe then waits for
`browser-return` to reach `flowing`, then prints `browser-cam`'s status without
waiting on it — that stream does not settle here, see below.

Node 1 offers `whip [listen]` and `whep [listen]` and names the base URL
browsers reach its signalling at (`strom.signalling_base` in
`config/adapter-1.yaml`, one entry per data-plane alias; the adapter appends
`/whip` and `/whep` and advertises the result on that alias). The `default`
alias signals at `172.26.0.10:8080` for the page inside the bench; the
`docker-host` alias signals at `localhost:28080` for a page on this machine,
see "Feeding open-live". Southbound allows the page's origin with
`WEAVE_SOUTHBOUND_CORS_ORIGIN=*`, a development value like the tokens.

The container serves the page to itself and opens it as `http://127.0.0.1:8000`,
because `getUserMedia` exists only in a secure context. `just logs browser`
shows one line per 5 s with every hop's conditions and negotiated codecs.

Observed on this bench (arm64, Docker via colima):

- `browser-return` reaches `flowing` end to end: producer SRT → node-1
  `mpegtssrt_input → whep_output` → the page plays VP9 + Opus at ~8 Mb/s, and
  both hops report `flowing` on both sockets.
- `browser-cam` cycles between `degraded` and `pending` and never reaches
  `flowing` (8 `degraded`, 16 `pending` in 24 samples over two minutes). The
  page sends audio + video over WHIP (~0.5 Mb/s) and its hop reads
  `device flowing → whip flowing`, but Strom's `whip_input` accepts only H264
  video (`video-codecs = ["H264"]` in `backend/src/blocks/builtin/whip.rs`) and
  the Playwright image's Chromium (Chromium 151, arm64) has no H264 encoder, so
  the session carries only the Opus audio. Strom then emits an audio-only
  trickle on the SRT output (`egress flowing` at ~5 kb/s), the flow never leaves
  `gst_state: Paused` (its WHIP ingress reads `idle`), and Strom's inactivity
  monitor tears the session down every ~20 s. The page reconnects, reports
  `pending` for its first attempts, and the roll-up follows it — which is where
  the `pending` samples come from. `BACKLOG.md` has the item and what would fix
  it.
- A page restart is answered with `503` for 10–20 s: `whip_input` allows one
  session and the page cannot release the one it left behind. Also in
  `BACKLOG.md`.

## Feeding open-live

[open-live](https://github.com/Eyevinn/open-live), with a `weave` source
provider added in a fork, lists every placed weave stream's node-hosted SRT
output as a read-only source and dials it from its own Strom. That Strom is the
`strom` service in open-live's `docker-compose.yml`, not a service here. It
joins this bench's net_core as an external network (`ow-bench_net_core`, address
`172.25.0.50`, host port 28083) under the container name `ow-open-live-strom`,
and `scripts/route-manager.sh` lists that name so it gets the routes into the
node subnets the way the bundled producer and consumer do. It is another
system's engine, not a weave node: no adapter fronts it. Two consequences: the
bench must be up before open-live's stack, and `just down` removes the network
from under it.

The feed is a browser on this machine, which cannot reach `172.26.0.10:8080`:
colima routes no container IP to the host, and only the published ports are
reachable. So node 1 advertises a second data-plane alias, `docker-host`, with
the same SRT address as `default` and signalling at `localhost:28080` instead.
The `browser-cam-host` manifest selects it with `network: docker-host` on the
destination; the planner then resolves both the WHIP URL the page dials and the
SRT host open-live dials from that one alias.

```sh
just page                          # serve nodes/browser pinned to seat guest-1; open the printed URL
just host-cam                      # apply browser-cam-host for that seat
just host-cam-down                 # delete it again
```

`just page` pins the page's node id with `#node=<seat>`, the way the in-bench
page is pinned on its `command:`. The seat is the identity the whole chain hangs
off: the manifest names it as the source device, and open-live keys its source
doc on the stream, so a guest who reloads or rejoins that seat comes back on the
mixer input the operator already assigned. Unpinned, every tab is a new node and
the applied stream is left pointing at one that is gone. Pass `just page 8000
guest-2` for a second, concurrent guest.

Then start open-live's stack with `just up-weave`, which layers
`docker-compose.weave.yml` on and passes `SOURCE_PROVIDERS`,
`WEAVE_NORTHBOUND_URL` and `WEAVE_NORTHBOUND_TOKEN` through from open-live's
`.env` (a plain `docker compose up` leaves the provider off); from inside a
container the northbound is at `host.docker.internal`, not `localhost`. The
`weave` provider then lists the stream's SRT output as a source within one
poll:

```sh
cd /path/to/open-live
cat >> .env <<'EOF'
SOURCE_PROVIDERS=weave
WEAVE_NORTHBOUND_URL=http://host.docker.internal:29080
WEAVE_NORTHBOUND_TOKEN=bench-northbound-token
EOF
docker compose up -d --build
curl -s localhost:3000/api/v1/sources | jq
```

Observed:

- After `adapter-1` restarts with the config, `GET /v1/nodes` shows node 1's
  `docker-host` alias with `signalling.whip = http://localhost:28080/whip`
  beside the unchanged `default`.
- `just host-cam browser-bench` planned the page's hop as `device → whip connect
  http://localhost:28080/whip/weave-browser-cam-host-receiver-0` and the output
  as `srt://172.26.0.10:20665`; node 1 ran the receiver as `whip_input →
  videoenc → mpegtssrt_output`. The in-bench page's hop read `failed · Failed to
  fetch`, since `localhost` inside its container is itself; the recipe exists
  for a page on this machine.
- `ow-open-live-strom` gets its routes from `route-manager` within one interval
  and opens a TCP connection to `172.26.0.10:8080`.
- With `browser-cam`, `browser-cam-host` and `browser-return` applied, the fork's
  provider lists the first two with `?mode=caller` and skips `browser-return`,
  whose only output is a device end reported as `null`. Before the fork's fix
  that `null` made the whole listing throw, so no weave source appeared while
  any device destination existed.
- Google Chrome on this machine, registered as its own node and given
  `browser-cam-host`, sent camera and microphone into node 1 through the
  `docker-host` alias. Strom's log shows one WHIP session from peer
  `172.26.0.1` with both `Pad audio_0` and `Pad video_0`, the first video pad
  seen on this bench; the gateway flow reached `gst_state: Playing`; and
  `ffprobe` on the SRT output found `h264 (Constrained Baseline) 640x480 30 fps`
  and `aac 48 kHz stereo`. Strom logged a burst of `mpegtsmux` warnings
  (`Impossible to configure latency: max 0 < min 40 ms. Add queues`) at
  session start; the media flowed regardless. With nothing dialling the SRT
  output the stream reads `degraded`, which is the roll-up for a source that
  flows and a destination that does not.
- open-live activated a production with `browser-cam-host` assigned:
  `ow-open-live-strom` dialled node 1's output (a caller from `172.25.0.50` on
  the SRT sink, 4.3 Mb/s) and the weave stream read `flowing`, the first time a
  stream on this bench reached that state with another system as the consumer.
- An assigned open-live input with no media holds its whole production at
  `gst_state: Paused`. With `browser-cam` (the in-bench page's audio-only
  trickle) or an unpublished WHIP input beside the camera, the flow stayed
  `Paused, pending Playing`; with only the camera assigned it reached `Playing`
  in 2 s.
- A camera page without a microphone starves node 1's gateway flow: Strom 1 saw
  `Pad video_0` alone, the hop read `whip stalled → srt idle`, and a 15 s SRT
  probe of the output returned nothing. That is the backlog item on video-only
  WHIP senders; the page sends both tracks unless opened with `media=video`.
- open-live's WHEP previews never connected to a browser on this machine until
  a TURN relay existed. From the studio's origin, a browser without camera or
  microphone permission offers only mDNS `.local` host candidates, which Strom
  cannot resolve, and the browser cannot reach Strom's container addresses, so
  every candidate pair stayed `in-progress`. open-live's compose now runs
  `coturn` on TCP 3478 and Strom lists it; the same browser then connected over
  a relay pair and decoded the PGM at 1280x720.

## Notes

- Any service can be restarted, stopped/started, or rebuilt and recreated
  individually; `route-manager` repairs its routes within a tick and the system
  reconverges without manual steps. `docker compose logs route-manager` shows a
  line per repair and is otherwise quiet.
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
