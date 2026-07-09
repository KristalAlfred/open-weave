# Bench stream manifests

A library of `StreamDefinition` manifests for exercising the bench. Each file's
name is also the stream `name`, so the recipes take a bare name:

```sh
just bench stream-ls          # list manifests + one-line description
just bench stream basic       # apply bench/manifests/basic.yaml
just bench stream-rm basic    # delete the stream; controller tears down its hops
```

Apply **one at a time**. Most manifests bind SRT `:7001` on node-1 and `:7002`
on node-2, so applying several together causes port collisions and `failed`
hops. `stream-rm` between manifests; the controller clears the node's desired
hops on the next tick (~5s).

The bundled `producer`/`consumer` verification endpoints (`just bench producer-up`
/ `consumer-up`) are hardwired to the `basic` flow's addresses: producer pushes
to `strom-1:7001`, consumer pulls from `strom-2:7003`. They only drive flows that
listen on those exact sockets.

## Observed behaviour

Statuses below were observed on the live bench (controller `/status`), not
inferred. "With media" columns record only what the bundled producer/consumer
can actually drive.

| Manifest | Scenario | No media | With producer | With producer + consumer | Exercises a limitation |
|----------|----------|----------|---------------|--------------------------|------------------------|
| `basic` | listener node-1 → node-2 (canonical contribution) | `awaiting_input` | `degraded` | `flowing` | no |
| `reverse` | listener node-2 → node-1 (matrix directionality) | `awaiting_input` | not drivable — bundled producer targets node-1 | not drivable | no |
| `same-node` | source and destination both on node-1 | `awaiting_input` | `degraded` | not reachable — bundled consumer targets node-2 | no |
| `fanout` | one source, two destinations | `awaiting_input` | `degraded` (first dest only) | `flowing` (first dest only) | **yes — first-destination-only** |
| `unplaceable` | `source.node: strom-node-404` (never registers) | `pending` | `pending` | `pending` | **yes — pending-until-registered** |
| `disabled` | `enabled: false` | `idle` | `idle` | `idle` | no |
| `srt-latency` | non-default SRT latency (120ms / 2000ms) | `awaiting_input` | `degraded` | `flowing` | no |

Notes:

- **`fanout`**: the second destination is silently dropped — the controller
  derives a hop chain for the first destination only. Verified: node-1 receives
  one `sender` hop, node-2 one `receiver` hop; no hop is placed for the second
  destination.
- **`unplaceable`**: northbound accepts it (a listener source with an explicit
  node is valid), but the sender hop is pinned to `strom-node-404`, which never
  registers, so it never provisions and the stream stays `pending`. Register a
  node with that id and it would converge — Kubernetes-style unschedulable.
- **`srt-latency`**: verified the values reach the desired hops — sender ingress
  `120`, sender egress `2000`; receiver ingress `2000`, receiver egress `200`
  (the receiver→consumer default).
- **`disabled`**: listed by northbound but reconciled to `idle`; no hops are
  provisioned.
