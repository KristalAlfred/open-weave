---
id: OW-59
title: "A chain can pick one relay twice on room for one bridge"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

A read-only review of `crates/controller/src/path.rs` at `f5e2fdd` traced
this. `chain_hops` can call `relay_before` more than once for one destination
when `via` pins leave several links that need a relay. `pick_relay` excludes
only the relay's two neighbours, and every pick's `can_claim` checks the same
port snapshot, because ports are claimed only later in `plan_hop`. A relay
with room for one bridge could therefore be picked for two. The stream then
failed with `PortRangeExhausted` although another relay had room.

`a_chain_does_not_count_one_relays_room_twice`
(`crates/controller/src/relay_choice_tests.rs`) pins a NAT'd `transit` between
a NAT'd source and a NAT'd destination, with a two-port `relay-a` and a roomy
`relay-b`. On `main` at `30a059f` it failed with
`PortRangeExhausted { node: "relay-a" }`.

## Done when

- [x] A later relay in a chain sees the ports an earlier pick in the same chain
      will claim.

## Log

- 2026-09-26: filed by claude-tests from the planner review, with the test
  result above.
- 2026-09-26: fixed. `chain_hops` plans a destination's relays against a
  scratch copy of the port allocator. After each pick, `relay_before` claims
  the relay's listener ports in that copy (`PortAllocator::reserve`, with the
  listeners `relay_with_ports` found room for). A later pick's `can_claim`
  then sees them taken. The real claims still happen in `plan_hop`. The test
  now plans the chain `relay-a`, `transit`, `relay-b`, `studio-node`. Box
  ticked on `cargo test -p weave-controller`, unit tests only.
