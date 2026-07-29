# open-weave bench

Full-system docker-compose stack with per-node emulated routers so each Strom
node's network can be impaired with `netem`. The stack starts **empty** — drive
it with the `weave` CLI against the host-published northbound.

## Topology

```
        net_core 172.25.0.0/24
   northbound  southbound  controller
        |          |           |
     router-1 (172.25.0.11) router-2 (172.25.0.12)
        |                       |
  net_node1 172.26.0.0/24   net_node2 172.27.0.0/24
   strom-1 + adapter-1       strom-2 + adapter-2
```

Each node subnet reaches everything else only through its router. Applying netem
on a router impairs both directions of that node's traffic: SRT media between
Stroms, adapter heartbeats, and controller→Strom API calls.

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
| `WEAVE_SOUTHBOUND_TOKEN` | `bench-southbound-token` | southbound, controller, adapter-1, adapter-2 |

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

just stream-ls     # list the stream manifests (see manifests/README.md)
just stream basic  # apply a manifest; `just stream-rm basic` removes it

just netem node1 delay 200ms loss 5%   # impair node 1's network
just netem-show node1
just netem-clear node1

just producer-up          # feed the basic stream's source ingress
just producer-up reverse  # feed another stream (resolves its ingress via discovery)
just producer-down # stop it (drives no-source -> source -> no-source transitions)
just consumer-up          # pull the basic stream's receiver output
just consumer-up reverse  # pull another stream's output (resolved via discovery)
just consumer-down

just down          # tear down (containers, networks, volumes)
```

Data-plane addresses are never hardcoded in the recipes: they are resolved from
the controller's discovery API. Query it directly with:

```sh
curl -s -H "Authorization: Bearer bench-northbound-token" \
  localhost:29082/v1/streams/basic/endpoints | jq
# { "ingress": {node,host,port,url}, "outputs": [{node,host,port,url}] }
```

`200` once placed, `503` while known-but-unplaced, `404` if unknown, `401` without
the northbound token. The `scripts/endpoints.sh <stream> ingress|output [index]`
helper polls this until placed and prints `host:port`; the producer/consumer
recipes use it and pass the token in. It reads `WEAVE_NORTHBOUND_TOKEN` from its
environment and fails fast on `401` rather than polling a rejected token.

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
