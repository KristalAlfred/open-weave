---
id: OW-26
title: "Every relayed destination lands on one relay until its ports run out"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`pick_relay` in `crates/controller/src/path.rs` picks the lowest-id eligible
relay without looking at its free ports. A relayed destination whose sender and
receiver both sit behind NAT claims two SRT ports on the relay, one for each
half. With the 1000-port range the bench and `examples/node.yaml` declare
(`20000-20999`), a scratch copy of `relayed_fan_out_to_three_hundred_nodes`
(`crates/controller/src/scale_tests.rs`) placed 500 receivers and failed at
501: `derive_path` returned `PortRangeExhausted { node: "relay-a" }` and the
whole stream stayed `pending`, while `relay-b` was online, eligible and had no
hops. Unit test only; not run on the bench.

## Done when

- [ ] A test shows a relayed fan-out that exceeds one relay's port range placed
      over a second eligible relay, or reported as unplaceable with a reason
      that names the port range.

## Easy to break

- Relay choice is stable across ticks; moving a destination between relays
  moves its ports.
- A failed destination fails the whole stream today (`derive_path` is
  all-or-nothing).

## Unchecked

- Whether spreading over relays should also weigh bandwidth. Nothing reports
  it today.

## Log

- 2026-09-25: filed from OW-12 by claude-planner.
