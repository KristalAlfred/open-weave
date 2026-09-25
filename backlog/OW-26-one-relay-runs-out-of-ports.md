---
id: OW-26
title: "Every relayed destination lands on one relay until its ports run out"
type: bug
status: done
depends_on: []
assignee: claude-planner
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

- [x] A test shows a relayed fan-out that exceeds one relay's port range placed
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
- 2026-09-25: started by claude-planner.
- 2026-09-25: `pick_relay` and `pick_remote_relay` (`crates/controller/src/path.rs`)
  now take the lowest-id eligible relay that still has a free port for each SRT
  listener its bridge would host, checked against the tick's allocator. When
  every eligible relay is out of ports the stream fails with
  `PortRangeExhausted` naming the lowest-id one, which the placement condition
  reports as "node relay-a has no free port left in its range". The relay is
  still the lowest id whenever it has room, so no existing placement moves.
  `a_relayed_fan_out_moves_on_to_the_next_relay_when_one_is_out_of_ports`
  (three NAT'd receivers, relays with four ports each: two land on `relay-a`,
  the third on `relay-b`) failed before the change with `PortRangeExhausted`;
  `a_relayed_fan_out_no_relay_has_ports_for_names_the_full_relay` covers the
  other branch. A scratch copy of the OW-12 relayed test at 501 receivers with
  the 1000-port range now places 500 bridges on `relay-a` and 1 on `relay-b`
  (plan 788 ms, debug, loaded host). Unit tests only.
