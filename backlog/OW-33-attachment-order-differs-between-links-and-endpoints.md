---
id: OW-33
title: "Link planning and consumer endpoints order a node's attachments differently"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Two functions in `crates/controller/src/path.rs` pick one of a node's
attachments, and they sort in different orders:

- `ResolvedEnd::attachments`, which `link_transport` walks to choose the
  listener a link uses, sorts by `(network, id)`.
- `srt_listener_attachment`, which `claim_port` and `endpoint_addr` use for a
  producer's or consumer's SRT socket and the address `stream_endpoints`
  reports, sorts by `(id, network)`.

When a node has more than one attachment with an SRT listener and the
endpoint sets no `network`, the two orders can disagree. The test
`path::tests::link_and_consumer_endpoint_pick_the_same_attachment` shows it.
Node 2 has `a-site` on `zeta` (10.9.0.2, ports 7000–7099) and `z-lan` on
`alpha` (10.1.0.2, ports 8000–8099), and node 1 dials both networks. The
sender dials 10.1.0.2, so the link takes the lowest network. The receiver's
consumer socket is claimed from `a-site`'s range, and `GET
/streams/{name}/endpoints` reports 10.9.0.2, the lowest attachment id. The test
is `#[ignore]`d against this item. `cargo test -p weave-controller
link_and_consumer -- --include-ignored` fails with `left: "10.9.0.2"`,
`right: "10.1.0.2"`.

`README.md` ("Capabilities and topology") says the controller chooses "the
lowest deterministic transport, network, attachment, address, URL, and port".
That sentence is about links. Nothing says which attachment a producer or
consumer socket uses when the manifest names no network.

`bench/config/adapter-1.yaml` gives node 1 two attachments whose orders
disagree (`a-routed` on `internet`, `z-docker-host` on `docker-host`). Both
carry the same SRT host and port range, so no SRT endpoint on the bench changes.
Not checked on the bench.

## Done when

- [ ] One ordering rule picks the attachment for links and for producer and
      consumer sockets, or the difference is intended and `README.md` says
      which attachment an endpoint without a `network` uses.
- [ ] `link_and_consumer_endpoint_pick_the_same_attachment` is un-ignored and
      passes, or is rewritten to the documented rule.

## Easy to break

- Changing either order moves hosts and ports for every node whose attachments
  sort differently under the two orders, so its desired snapshots and
  consumer URLs change on upgrade.

## Log

- 2026-09-25: filed by claude-tests from a planner test written for OW-18.
