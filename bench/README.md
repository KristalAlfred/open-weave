# open-weave bench

A full-system docker-compose stack: the three control-plane services, a
Postgres, and four Strom media nodes each sitting behind its own emulated
router, so any node's network can be impaired with `netem`. The Stroms run
`eyevinntechnology/strom:0.6.10`. It starts **empty**
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

Open the leading controller's dashboard, <http://localhost:29082/ui> or
<http://localhost:29083/ui> (`just bench leader` says which), to watch it live:
registered nodes, each stream's path across them with per-hop link conditions
and rates, and the addresses external peers dial. The other one reads
"standby".

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
just bench stream-up nat-transit # two NAT'd sites, relay chosen by the controller
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
just bench auth-check  # a node token is refused for another node's id
just bench leader      # which controller holds the lease
just bench tls-check   # the TLS endpoints verify against the bench CA, and only it
just bench ps          # container states
just bench logs controller   # follow one service's logs
just bench down        # tear down (containers, networks, volumes)
```

`stream-up` is the one to reach for: it applies the manifest, attaches the
producer to the stream's ingress and **one consumer per receiver output**, and
waits until the controller reports `flowing`. It reads the output count from
discovery, so fan-out gets the right number of consumers without being told, and
reads each endpoint's `passphrase` from the stream as northbound returns it, so a
keyed manifest such as `encrypted` is driven with the keys it declares.

The lower-level pieces are there when a scenario wants them separately —
applying a stream without media, or detaching a producer mid-run to watch the
status transition:

```sh
just bench stream basic         # control plane only: placed but unfed -> `awaiting_input`
just bench stream-rm basic      # delete the stream; controller tears down its hops

just bench producer-up basic    # feed a stream's source ingress
just bench producer-up encrypted some-passphrase  # the same, with a passphrase in its URL
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

The bench runs two controllers on one Postgres, `controller` and
`controller-2`. One holds the lease and serves; the other answers
`503 not_leader`. Northbound and southbound list both and send each request to
the one that leads. `controller-restart` restarts, kills or stops the leading
controller under a flowing stream and checks that nothing on the media side
noticed:

```sh
just bench stream-up basic
just bench controller-restart basic          # docker compose restart
just bench controller-restart basic kill     # SIGKILL, then start
just bench controller-restart basic stop     # stop it, check, start it again
```

It records every `weave-` flow on the four Stroms with its id and SRT byte
count, and the stream's generation and ETag. It then acts on the leader, waits
until a controller leads, and waits 20 s for every adapter to poll it. It fails
if a flow was deleted or recreated (Strom gives a recreated flow a new id), if a
flow moved no bytes, if an adapter logged a delete, if a producer or consumer
restarted ffmpeg, if a Strom node reads `offline`, if the generation or ETag
changed, if re-applying the manifest fails, or if the stream does not read
`flowing` again. With `stop`, the old leader stays down through the checks, the
other controller must be the one leading, and the old one must come back as a
standby. A restarted or killed leader can take the lease again: after `kill`
both controllers wait for the lease to run out (`WEAVE_LEASE_TTL_SECS`, 10 s)
and either may get it.

`just bench topology rist` restarts adapter-2 with `config/adapter-2-rist.yaml`,
where node 2 dials nothing and offers only a RIST listener to the internet, its
SRT listener serving consumers on its own LAN. `just bench stream-up rist` then
drives a stream whose link from node 1 to node 2 the planner puts on RIST, which
it does because the manifest sets `allow_cleartext_links`.
`just bench topology default` puts adapter-2 back, and so does the next
`just bench up`.

## Topology

```
                     net_core 10.97.25.0/24
        northbound  southbound  controller  controller-2  postgres  tls
     +---------------+-----------------+------------------+
     |               |                 |                  |
  router-1        router-2         router-3 (NAT)     router-4 (NAT)
  10.97.25.11     10.97.25.12      10.97.25.13        10.97.25.14
     |               |                 |                  |
  net_node1       net_node2        net_node3          net_node4
  10.97.26.0/24   10.97.27.0/24    10.97.29.0/24      10.97.30.0/24
  strom-1         strom-2          strom-3            strom-4
  adapter-1       adapter-2        adapter-3          adapter-4
  (dialable)      (dialable)       (outbound-only)    (outbound-only)
```

The subnets are `/24`s in `10.97.0.0/16`, outside Docker's default address
pools (`172.17.0.0/16`–`172.31.0.0/16` and `192.168.0.0/16`), so a compose
project that lets Docker pick its subnet does not take one of them.

Each node subnet reaches everything else only through its router. Applying netem
on a router impairs both directions of that node's traffic: SRT media between
Stroms, adapter heartbeats, and controller→Strom API calls.

**Nodes 3 and 4 each sit behind a NAT.** Their routers masquerade outbound
traffic, and no route to `10.97.29.0/24` or `10.97.30.0/24` is installed
anywhere outside that subnet — not in the core containers, not in routers 1 and
2, and not on the other NAT'd site. Docker's inter-network isolation blocks the
bridge-level path, so each of nodes 3 and 4 can open connections outward and
nothing can open one toward it, including the other. That asymmetry is
load-bearing: adding a return route would silently delete the boundary and the
`nat-*` manifests would start passing for the wrong reason.

Nodes 1 and 2 declare SRT listeners on the `internet` network that nodes 3 and 4
dial out on, so either can carry transit between the two NAT'd sites. The
controller picks the eligible node with the lowest id, so node 1 unless it is
offline or out of ports.

Because a NAT'd node's sockets can only be dialled from inside its network, the
bench carries a pair of media endpoints on each NAT'd subnet (`producer-3` and
`consumer-3` on net_node3, `producer-4` and `consumer-4` on net_node4). The
producer/consumer recipes choose between them and the net_core pair from the
resolved address, so `just bench stream-up <name>` works uniformly.

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

Nodes are `node1`, `node2`, `node3`, `node4`. The spec is applied to both legs of that
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
the discovery API, through northbound.

```sh
curl -s -H "Authorization: Bearer bench-northbound-token" \
  localhost:29080/streams/basic/endpoints | jq
# { "ingress": {node,host,port,url}, "destinations": [{id,endpoint}] }
```

`200` once placed, `503` while known-but-unplaced, `404` if unknown, `401`
without the northbound token. The
`scripts/endpoints.sh <stream> ingress|destination|destinations [id]` polls this
until placed and prints `host:port` — or, for `destinations`, how many receiver
endpoints the stream has, which is how `stream-up` knows how many consumers to
attach. It reads `WEAVE_NORTHBOUND_TOKEN` from its environment and fails fast on
`401` rather than polling a rejected token.

## Browser node

A web page can be a node (`nodes/browser/`). The bench runs one as a headless
Chromium inside `net_core`, because WebRTC media has to reach Strom's ICE
candidates on the node subnets and a browser on a macOS host generally has no
route there. The Chromium is Debian's `chromium` package, driven by Playwright
(`Dockerfile.browser`): Playwright's own Chromium and Firefox builds have no
H264 encoder on arm64, and Strom's `whip_input` takes H264 video only. The
services are off by default (profile `browser`):

```sh
just bench browser-up        # build + start the page as node `browser-bench`
just bench browser-up video  # the same, sending the camera without the microphone
just bench browser-stream    # apply browser-cam + browser-return, drive both
just bench browser-2-up      # start a second page as node `browser-bench-2`
just bench browser-b2b       # the first page's camera to the second page's screen
just bench browser-down      # detach, delete the browser streams, stop the pages
```

`browser-cam` sends the page's camera to node-1 (the planner picks WHIP hosted
on Strom; the consumer pulls the SRT output). `browser-return` feeds the
producer into node-1 and plays it on the page (the planner picks WHEP hosted on
Strom). Both manifests are templates: `browser-stream` fills in the node id.
The page takes that id from its token, which `docker-compose.yml` sets to
`browser-bench`'s, so the manifests keep pointing at the page across restarts.

Both reach `flowing`. browser-cam's SRT output carries H264 640x480 and AAC;
with `browser-up video` the page declares video alone, node 1 builds its gateway
flow without audio, and the output carries H264 only. On Strom 0.6.6 the gateway
flow never decoded this Chromium's H264 (no video pad linked, the flow stayed
`Paused`), so the output carried audio alone.

A page restart (`docker restart -t 0 ow-browser`) reconnects without a `503`
when the page sends audio: Strom 0.6.10 displaces the dead session after about
3 s without media. A video-only page is refused until Strom's inactivity monitor
frees the slot after about 10 s (`backlog/OW-46-video-only-page-restart-waits.md`).

`browser-b2b` names two pages and no Strom node. Neither page listens, so the
planner places node 1, the one node declaring WHIP and WHEP listeners, between
them with its `whip-to-whep` profile: the first
page sends WHIP to node 1 and the second pulls WHEP from it. It reaches
`flowing`, with H264 and Opus arriving on the second page.

Node 1 advertises `whip-to-srt`, `srt-to-whep` and `whip-to-whep` profiles and
declares the signalling listener bases in `config/adapter-1.yaml`. Southbound
allows the page's origin with `WEAVE_SOUTHBOUND_CORS_ORIGIN=*`, a development
value like the tokens.

`just bench logs browser` shows one line per 5 s with every hop's conditions and
negotiated codecs.

### A page in your own browser

To use a real browser on your machine instead of the in-bench Chromium:

```sh
just bench page                # serve nodes/browser on localhost:8000, print the URL
just bench host-cam            # apply browser-cam-host for that seat
just bench host-cam-down       # delete it again
```

`just bench page` gives the page the token for `<seat>` (default `guest-1`) and
pins its node id with `#node=<seat>`, so a page that reloads keeps its name and
the applied stream keeps pointing at it. Pass `just bench page 8000 guest-2` for
a second, concurrent guest. `just bench node-token <seat>` prints a seat's token
on its own.

A browser on the host generally cannot reach `10.97.26.10:8080`, so node 1
advertises a second attachment on the `docker-host` network, with the same SRT
address and signalling at `localhost:28080`. The
`browser-cam-host` manifest selects it with `network: docker-host` on the
destination, and the page registers on that network. With nothing dialling the SRT output the
stream reads `degraded`, which is the roll-up for a source that flows and a
destination that does not.

## Webhooks

Both controller services set `WEAVE_WEBHOOK_URL` to
`http://host.docker.internal:29099` and `WEAVE_WEBHOOK_TOKEN` to
`bench-webhook-token`, so the leading one delivers every event type to a sink on
this host.
`just bench hook-sink` is that sink: a few lines of Python that print each event
and the `Authorization` header it arrived with. Export `WEAVE_WEBHOOK_URL=`
(empty) before `just bench up` to switch webhooks off.

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
waiting `WEAVE_NODE_TTL_SECS` (15) gives `node.offline`, and waiting
`WEAVE_NODE_FORGET_SECS` (300) gives `node.forgotten`, unless a stream names
`guest-1`. Applying a stream gives a `stream.changed` on the next reconcile
tick, and another each time its conditions change.

`host.docker.internal` resolves through the `extra_hosts: host-gateway` entry on
the controller services. See the root README's "Webhooks" for the
payload and the delivery guarantees.

## Host ports

| Port | Service |
|------|---------|
| 29443 | northbound over TLS, which the recipes use (`SSL_CERT_FILE=bench/tls/public/ca.pem WEAVE_NORTHBOUND_URL=https://localhost:29443 weave ...`) |
| 29444 | southbound over TLS, which the recipes use |
| 29080 | northbound, plaintext |
| 29081 | southbound, plaintext (`/nodes` shows registered capabilities; `just bench page` uses it) |
| 29082 | controller: dashboard at `/ui`, `/view`; API at `/status`, `/streams/{name}/endpoints` |
| 29083 | controller-2, the same routes; whichever does not lead answers `503 not_leader` |
| 28080 | strom-1 API |
| 28081 | strom-2 API |
| 28082 | strom-3 API (host→container; grants no route into net_node3) |
| 28083 | strom-4 API (host→container; grants no route into net_node4) |
| 29099 | `just bench hook-sink` on this host, where the controller delivers webhooks |

## Authentication

The API surfaces require a bearer token (see the root README for the model).
Northbound takes one shared token. Southbound and the controller hold a key and
take from each node a token derived from it for that node's id. The bench
defaults to development values so `just bench up` stays a single command:

| Variable | Default | Used by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | `bench-northbound-token` | northbound, both controllers, CLI, `endpoints.sh` |
| `WEAVE_SOUTHBOUND_KEY` | `bench-southbound-key-for-local-use-only` | southbound, both controllers, `just bench node-token` |
| `WEAVE_ADAPTER_{1,2,3,4}_TOKEN` | `strom-node-{1,2,3,4}`'s epoch-0 token under the default key | adapter-1 to adapter-4; the recipes present node 1's for southbound reads |
| `WEAVE_BROWSER_TOKEN` | `browser-bench`'s epoch-0 token under the default key | the in-bench browser page, which takes its node id from it |
| `WEAVE_BROWSER_2_TOKEN` | `browser-bench-2`'s epoch-0 token under the default key | the second in-bench page (`browser-2`) |
| `WEAVE_SRT_KEY_SECRET` | `bench-srt-key-secret-for-local-use-only` | both controllers, to derive the keys of SRT links between nodes |
| `WEAVE_SOUTHBOUND_MIN_EPOCHS` | unset | southbound, both controllers: `<id>=<epoch>` pairs that revoke a node's older tokens |

`docker-compose.yml` passes each adapter its token as `WEAVE_SOUTHBOUND_TOKEN`.
The adapter configs leave `node.southbound_token` unset and inherit it. The node
tokens are written out in `docker-compose.yml` and the bench `justfile`, so
exporting `WEAVE_SOUTHBOUND_KEY` means exporting the six token variables too;
`just bench node-token <id>` prints each one under the exported key, and
`just bench node-token <id> <epoch>` a token at a later epoch.

The `just` recipes add the right header for you. Calling the APIs by hand needs
it explicitly:

```sh
curl -s -H "Authorization: Bearer $(just bench node-token strom-node-1)" localhost:29081/nodes | jq
curl -s -H "Authorization: Bearer bench-northbound-token" localhost:29080/streams | jq
```

Without a valid token these return `401` and `WWW-Authenticate: Bearer`. A node
token used for another node's id gets `403`; `just bench auth-check` tries that
against southbound and the leading controller. Every service **refuses to start** if its
secret is missing, so a `docker compose up` that exits immediately with a
`WEAVE_... is unset` error is the fail-closed default working, not a bug.
`WEAVE_AUTH_DISABLED=1` opts out for local runs.

`/health` on all three services, the controller's dashboard (`/ui`, `/view`),
and the `/status` rollup need no token, so anyone who can reach port 29082 or
29083 can read the full topology and allocated ports. Compose publishes it on all
interfaces: fine on a laptop, but **do not expose a controller port on a shared
or public host.**

## TLS

The `tls` service is nginx on net_core at `10.97.25.25`. It terminates TLS for
northbound (container port 9443, host 29443) and southbound (8443, host 29444)
and proxies plain HTTP to them. The services themselves serve no TLS, and their
plaintext ports stay published.

On every `up`, the one-shot `tls-certs` service runs `scripts/tls-certs.sh` in
the netshoot image, so the host needs no openssl. It generates a private CA and
a server certificate for `10.97.25.25`, `127.0.0.1` and `localhost` into
`bench/tls/` (gitignored). It keeps them while the server certificate names
`10.97.25.25` and is more than a day from expiring. Delete `bench/tls/` for a
new CA.

Who dials TLS, and how each trusts the bench CA:

| Client | Dials | Trusts the CA through |
|---|---|---|
| adapter-1 to adapter-4 | `https://10.97.25.25:8443` (`southbound_url` in `config/`) | `SSL_CERT_FILE=/tls/ca.pem` |
| the in-bench browser page | `https://10.97.25.25:8443` | certutil adds the CA to Chromium's NSS database before the page opens; `NODE_EXTRA_CA_CERTS` for `check.mjs`'s own polling |
| `weave` in the recipes | `https://localhost:29443` | `SSL_CERT_FILE=bench/tls/public/ca.pem` |
| curl in the recipes | both, on localhost | `--cacert bench/tls/public/ca.pem` |

Certificate verification stays on everywhere; nothing is told to skip it.
`just bench tls-check` shows that curl and `weave` verify against the bench CA
and refuse the certificate without it. A browser on the host does not trust the
bench CA, so `just bench page` points the page at southbound's plaintext port.

## API compatibility

Both contracts use flat routes (see the root README). There is no URL version
before a stable release, and version-prefixed aliases are not served.

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
lists the registered nodes; the manifest names one that is not among them. A
stream whose node has gone offline reads `degraded` instead.

**A stream stays `awaiting_input`.** It placed, but nothing is feeding it.
`just bench stream` applies without media on purpose — use `just bench stream-up`
to attach the producer and consumer too.

**A stream reads `degraded` after driving another one.** The producer and
consumer are singletons, so `stream-up <other>` took them from the first stream.

**Ports already in use.** The bench publishes 29080–29083, 28080–28083 and
29099. Another stack holding one of those makes `up` fail; stop it or change the
`ports:` entries in `docker-compose.yml`.

**`Pool overlaps with other one on this address space`.** Another Docker network
holds part of `10.97.25.0/24`–`10.97.30.0/24`. `docker network inspect` on each
network in `docker network ls` shows which.

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
  pull their own hops (`GET /nodes/{id}/desired` via southbound, full replace of
  what that node should run) and create/start/delete the `weave-…` Strom flows.
  Hops for a node that has not registered yet just wait until it does.
- Per-stream status is rolled up from adapter-reported hop conditions:
  `awaiting_input` (no source media) → `degraded` (source flowing, not end to end)
  → `flowing`. See the controller `/status` endpoint.
- The receiver hop listens on the destination port and re-exposes the media on
  `port + 1` for a downstream consumer.
- No pre-configured flows are shipped — create them through the CLI.
