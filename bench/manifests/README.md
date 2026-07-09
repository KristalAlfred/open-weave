# Bench stream manifests

A library of `StreamDefinition` manifests for exercising the bench. Each file's
name is also the stream `name`, so the recipes take a bare name:

```sh
just bench stream-ls          # list manifests + one-line description
just bench stream basic       # apply bench/manifests/basic.yaml
just bench stream-rm basic    # delete the stream; controller tears down its hops
```

The bundled `producer`/`consumer` verification endpoints (`just bench producer-up`
/ `consumer-up`) exercise the `basic` scenario: producer pushes to `strom-1:7001`,
consumer pulls from `strom-2:7003`.

## Observed behaviour

Statuses below were observed on the live bench (controller `/status`), not
inferred. Media columns use the bundled `producer`/`consumer`; `—` marks a cell
not measured.

| Manifest | Scenario | No media | With producer | With producer + consumer |
|----------|----------|----------|---------------|--------------------------|
| `basic` | listener node-1 → node-2 (canonical contribution) | `awaiting_input` | `degraded` | `flowing` |
| `reverse` | listener node-2 → node-1 (matrix directionality) | `awaiting_input` | — | — |
| `same-node` | source and destination both on node-1 | `awaiting_input` | `degraded` | — |
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
