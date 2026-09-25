---
id: OW-56
title: "Ports move when a hop starts running or reports failed"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

A read-only review of `crates/controller/src/path.rs` at `f5e2fdd` found this.
OW-42 made `derive_stream` plan the destinations a node reports carrying before
new ones, and OW-45 made `reconcile` (`crates/controller/src/main.rs`) plan
streams whose sender reports running before the rest. Both orders skip reports
whose state is `failed`. Ports are an FNV preference plus linear probing in
planning order, so the order change moves them. The reviewer's case: two public
nodes with range 20000–20099, destination `b` running on 20073, and a new
destination `a32`. On the first tick `b` keeps 20073 and `a32` gets 20074. Once
`a32` reports running, it plans first and takes 20073, `b` moves to 20074, and
both flows are rebuilt. When `a32`'s receiver later reports `failed`, the
healthy `b` moves back. `README.md` said a new stream or destination takes the
next relay "instead of the ports of an existing bridge"; that held only until
the new one ran.

`crates/controller/src/port_hold_tests.rs` runs this through `reconcile` for
200 candidate ids `a0`–`a199`. For each one it adds the destination, lets it
run, then fails its receiver, and checks that nothing already running moved.
Against `1064f57`:

- two public nodes: 9 of 200 moved (`a26`, `a32`, `a88`, `a90`, `a110`,
  `a164`, `a170`, `a185`, `a192`);
- two NAT'd nodes bridged by one relay: 6 of 200 (`a32`, `a65`, `a90`, `a97`,
  `a101`, `a164`);
- a new stream `a{n}` next to a running stream: 11 of 200.

## Done when

- [x] A hop that starts running or reports `failed` moves no other hop's port.
- [x] An existing bridge still keeps its relay and ports when a new stream or
      destination arrives, and the new one takes the next relay.

## Log

- 2026-09-26: filed by claude-tests from the planner review, with the counts
  above.
- 2026-09-26: fixed. `HeldPorts::from_reports` (`path.rs`) reads, once per
  tick, the port each running hop's listening sockets report. A report counts
  unless it is `failed`. A socket counts when its resolved host is `0.0.0.0`
  or one of the node's own listener hosts, which is how the Strom adapter
  reports a listener; a caller resolves to the address it dials. A RIST
  listener also holds the RTCP port after it. `reconcile` builds the port
  allocator with them (`PortAllocator::holding`), so `POST /stream-plans`
  gets them too.
- 2026-09-26: each claim now says which socket it is for, as the owning hop
  and its ingress, merge ingress or egress branch (`SocketAt`). That socket
  takes its held port when it is free and still in range, and no other socket
  is given a held port; relay capacity checks (`can_claim`) apply the same
  rule to the bridge they would place. OW-42's destination order and OW-45's
  stream order are gone: streams plan in name order and destinations in id
  order, whatever runs. OW-42's rule that a bridge stays on the relay
  reporting it is unchanged. Hop-id collisions between stored streams go back
  to name order, and OW-45's `a_running_stream_keeps_its_hop_ids_against_an_earlier_name`
  is removed with that behaviour.
- 2026-09-26: both boxes ticked on unit tests. The three `port_hold_tests`
  pass with no candidate moving. The OW-42 and OW-45 relay tests in
  `relay_choice_tests.rs` still pass, with reports that now resolve ports as
  the adapter does; with the old unresolved reports, nothing is held. A hop
  whose node does not report `resolved` holds no port, and gets the FNV port
  in fixed order. `README.md` ("Hop status and fan-out", "Capabilities and
  topology") updated.
