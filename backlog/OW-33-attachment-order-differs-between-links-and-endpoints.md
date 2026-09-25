---
id: OW-33
title: "Link planning and consumer endpoints order a node's attachments differently"
type: bug
status: done
depends_on: []
assignee: claude-tests
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

- [x] One ordering rule picks the attachment for links and for producer and
      consumer sockets, or the difference is intended and `README.md` says
      which attachment an endpoint without a `network` uses.
- [x] `link_and_consumer_endpoint_pick_the_same_attachment` is un-ignored and
      passes, or is rewritten to the documented rule.

## Easy to break

- Changing either order moves hosts and ports for every node whose attachments
  sort differently under the two orders, so its desired snapshots and
  consumer URLs change on upgrade.

## Log

- 2026-09-25: filed by claude-tests from a planner test written for OW-18.
- 2026-09-25: started by claude-tests.
- 2026-09-25: what the mismatch breaks. No link dials an address nobody
  listens on. Every SRT address the planner hands out takes its host and its
  port from one attachment: a link from `LinkChoice::attachment` in
  `plan_link`, and a producer or consumer socket from `srt_listener_attachment`
  in both `claim_port` and `endpoint_addr`. Strom listeners bind `srt://:port`
  on every interface (`crates/strom/src/spec.rs`), and `PortAllocator` keeps
  ports unique per node, not per attachment. The only effect was that a
  producer or consumer could be given an address on a different attachment
  than the node's links use when the endpoint names no `network`. The test
  `every_srt_address_handed_out_pairs_a_listener_host_with_its_own_range`
  checks this on the item's fixture and passes before and after the fix.
- 2026-09-25: fixed as the lead decided. `srt_listener_attachment` now sorts by
  `(network, id)`, the order `ResolvedEnd::attachments` uses for links. Nothing
  in the code or docs marks an attachment as the external-facing one.
  `link_and_consumer_endpoint_pick_the_same_attachment` is un-ignored and
  passes; `README.md` ("Capabilities and topology") now says which listener a
  producer or consumer socket uses. Hop ids do not depend on attachments and
  are unchanged. A node's ports and endpoint hosts change only if its SRT
  listener attachments sort differently under the two orders and differ in
  host or port range. On the bench only node 1 has two, and after the fix its
  producer and consumer sockets come from `z-docker-host` instead of
  `a-routed`. Both have host 10.97.26.10 and range 20000–20999, so no bench
  endpoint or port changes; this comes from reading `bench/config/`, not from
  a bench run. The Strom adapter still reports the resolved host of every
  listen socket from its first SRT listener in config order (`listener_host` in
  `crates/adapter-strom/src/main.rs`). That is observability only, and I did
  not change it. Checked with `just test`, unit tests only.
