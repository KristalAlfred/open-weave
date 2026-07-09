# Bench stream manifests

A library of `StreamDefinition` manifests for exercising the bench. Each file's
name is also the stream `name`, so the recipes take a bare name:

```sh
just bench stream-ls          # list manifests + one-line description
just bench stream basic       # apply bench/manifests/basic.yaml
just bench stream-rm basic    # delete the stream; controller tears down its hops
```

The bundled `producer`/`consumer` verification endpoints take an optional
`host:port` so any scenario can be driven end to end
(`just bench producer-up <host:port>` / `consumer-up <host:port>`). With no
argument they default to the `basic` scenario (producer `172.26.0.10:7001`,
consumer `172.27.0.10:7003`). The producer target is the scenario's source
listener; the consumer source is the receiver output, which is the destination
port + 1 on the node hosting the destination.

| Manifest | `producer-up` target | `consumer-up` source |
|----------|----------------------|----------------------|
| `basic` | `172.26.0.10:7001` (default) | `172.27.0.10:7003` (default) |
| `reverse` | `172.27.0.10:7001` | `172.26.0.10:7003` |
| `same-node` | `172.26.0.10:7001` | `172.26.0.10:7003` |
| `fanout` | `172.26.0.10:7001` | `172.27.0.10:7003` |
| `srt-latency` | `172.26.0.10:7001` | `172.27.0.10:7003` |

## Observed behaviour

Statuses below were observed on the live bench (controller `/status`), not
inferred. Media columns use the bundled `producer`/`consumer`; `—` marks a cell
not measured.

| Manifest | Scenario | No media | With producer | With producer + consumer |
|----------|----------|----------|---------------|--------------------------|
| `basic` | listener node-1 → node-2 (canonical contribution) | `awaiting_input` | `degraded` | `flowing` |
| `reverse` | listener node-2 → node-1 (matrix directionality) | `awaiting_input` | — | `flowing` |
| `same-node` | source and destination both on node-1 | `awaiting_input` | `degraded` | `flowing` |
| `fanout` | one source, two destinations | `awaiting_input` | `degraded` | — |
| `unplaceable` | `source.node: strom-node-404` (never registers) | `pending` | `pending` | `pending` |
| `disabled` | `enabled: false` | `idle` | `idle` | `idle` |
| `srt-latency` | non-default SRT latency (120ms / 2000ms) | `awaiting_input` | `degraded` | `flowing` |

Notes:

- **`unplaceable`**: northbound accepts it (a listener source with an explicit
  node is valid), but the sender hop is pinned to `strom-node-404`, which never
  registers, so it never provisions and the stream stays `pending`. Register a
  node with that id and it would converge — Kubernetes-style unschedulable.
- **`srt-latency`**: verified the values reach the desired hops — sender ingress
  `120`, sender egress `2000`; receiver ingress `2000`, receiver egress `200`
  (the receiver→consumer default).
- **`disabled`**: listed by northbound but reconciled to `idle`; no hops are
  provisioned.
