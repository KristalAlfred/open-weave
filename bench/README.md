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

Each node subnet reaches everything else only through its router
(`route-*` sidecars install the routes; the Strom image has no `ip` tool, so the
sidecars share its netns). Applying netem on a router impairs both directions of
that node's traffic: SRT media between Stroms, adapter heartbeats, and
controller→Strom API calls.

## Host ports

| Port | Service |
|------|---------|
| 29080 | northbound (`WEAVE_NORTHBOUND_URL=http://localhost:29080 weave ...`) |
| 29081 | southbound (`/nodes` shows registered capabilities) |
| 29082 | controller `/status` and `/streams/{name}/endpoints` (discovery) |
| 28080 | strom-1 API |
| 28081 | strom-2 API |

Ports are offset (29xxx/28xxx) to avoid colliding with a stale prior bench that
may still hold 9080/8082/18080/18081. Point the CLI at northbound with
`WEAVE_NORTHBOUND_URL`.

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
curl -s localhost:29082/streams/basic/endpoints | jq
# { "ingress": {node,host,port,url}, "outputs": [{node,host,port,url}] }
```

`200` once placed, `503` while known-but-unplaced, `404` if unknown. The
`scripts/endpoints.sh <stream> ingress|output [index]` helper polls this until
placed and prints `host:port`; the producer/consumer recipes use it.

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
`/streams/{name}/endpoints` note above and `manifests/README.md` for the manifest
library.

## Notes

- The controller never talks to Strom. Per stream it derives an ordered hop chain
  — a **sender** hop and a **receiver** hop — places each on a node (sender by
  `source.node` or host-match on the source URL; receiver by host-match on the
  destination), and writes the grouped desired hops to southbound per node
  (`PUT /nodes/{id}/desired`, full replace). The per-node **adapters** pull their
  desired hops and create/start/delete the `weave-…` Strom flows. Hops for a node
  that has not registered yet just wait until it does.
- Per-stream status is rolled up from adapter-reported hop conditions:
  `awaiting_input` (no source media) → `degraded` (source flowing, not end to end)
  → `flowing`. See the controller `/status` endpoint.
- The receiver hop listens on the destination port and re-exposes the media on
  `port + 1` for a downstream consumer.
- No pre-configured flows are shipped — create them through the CLI.
