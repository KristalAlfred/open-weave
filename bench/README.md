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
| 29081 | southbound |
| 29082 | controller `/status` |
| 28080 | strom-1 API |
| 28081 | strom-2 API |

Ports are offset (29xxx/28xxx) to avoid colliding with a stale prior bench that
may still hold 9080/8082/18080/18081. Point the CLI at northbound with
`WEAVE_NORTHBOUND_URL`.

## Usage

```sh
just up            # build + start + wait for healthy
just status        # health, registered nodes, controller view, streams
weave apply -f ../examples/contribution.yaml   # drive it yourself

just netem node1 delay 200ms loss 5%   # impair node 1's network
just netem-show node1
just netem-clear node1

just producer-up   # external ffmpeg feed -> strom-1 ingress :7001 (SRT caller)
just producer-down # stop it (drives no-source -> source -> no-source transitions)
just consumer-up   # external ffmpeg sink <- receiver output :7003 (SRT caller)
just consumer-down

just down          # tear down (containers, networks, volumes)
```

## External verification endpoints

`producer` and `consumer` are ffmpeg containers that live **outside** the system
and only exist to exercise it. They are off by default (compose `profiles`) and
started explicitly by the recipes above — never as part of `just up`. Both sit on
`net_core` and route to the node subnets through the netem routers, so their SRT
traffic crosses the same impaired hops as real external peers.

- `producer` pushes `testsrc2 + sine` as MPEG-TS over SRT (caller) into the
  contribution feed's ingress listener on `strom-1:7001`.
- `consumer` pulls the receiver flow's output from `strom-2:7003` (SRT caller)
  and discards it (`-f null -`).

They target the ports used by `examples/contribution.yaml`; apply that stream
first (`weave apply -f ../examples/contribution.yaml`) so the flows exist.

## Notes

- The controller places the **sender** flow on `WEAVE_STROM_URL` (strom-1) and,
  for each enabled stream, a matching **receiver** flow (`<name>-recv`) on the
  destination node's Strom. It maps the destination SRT host to a registered node
  via southbound (`WEAVE_SOUTHBOUND_URL`); with no match it logs and places only
  the sender. The receiver listens on the destination port and re-exposes the media
  on `port + 1` for a downstream consumer.
- No pre-configured flows are shipped — create them through the CLI.
