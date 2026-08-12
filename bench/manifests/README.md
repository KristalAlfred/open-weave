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
- **`via`**: three hops — `weave-via-sender` on node-1, `weave-via-bridge-0-0`
  on node-2, `weave-via-receiver-0` back on node-1 — so the media crosses both
  routers twice. Observed with every hop `flowing` on both sockets. The bridge is
  an ordinary Strom flow (`srtsrc` listener → `srtsink` caller); relaying needed
  no adapter change. Both nodes here are dialable, so this covers a `via` pin
  rather than the NAT case that makes the controller insert a relay by itself —
  that one needs a node the bench cannot dial, which the topology does not yet
  have.
