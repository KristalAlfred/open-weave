---
id: OW-25
title: "Relayed fan-out planning grows with the cube of its size"
type: bug
status: done
depends_on: []
assignee: claude-planner
---

## Evidence

`pick_relay` in `crates/controller/src/path.rs` scans every registered node for
each relayed link, and `station_link` resolves both halves with `find_node`, a
linear scan. One stream fanned out to N NAT'd receivers through relays does N
such picks over N nodes: O(N³). `reconcile_tick` holds `streams.read()` for the
whole tick, so stream writes wait for it.

Measured with `relayed_fan_out_to_three_hundred_nodes` in
`crates/controller/src/scale_tests.rs` and a scratch copy of it with more
receivers, debug build, Apple M4 Pro, other builds running (load average 15-22):

| receivers | plan | reconcile |
|---|---|---|
| 300 | 316 ms | 350 ms (`reconcile_tick`) |
| 500 | 1.19 s | 1.24 s (`reconcile`) |
| 1000 | 4.5 s, failed after 500 (OW-26) | 4.5 s (`reconcile`) |

The research for OW-12 measured 3.75 s debug and 1.34 s release at 1000
receivers with a port range wide enough to place them all. Direct fan-out to
300 receivers plans in 3-4 ms.

## Done when

- [x] Relayed fan-out to 1000 receivers plans within 10x the time of direct
      fan-out to 1000 receivers on the same build, recorded in this Log.

## Easy to break

- Relay choice is the lowest-id eligible online node and must stay stable
  across ticks.
- Planning allocates against the full candidate stream set (`BACKLOG.md`,
  "Scope guards").

## Log

- 2026-09-25: filed from OW-12 by claude-planner.
- 2026-09-25: started by claude-planner.
- 2026-09-25: `crates/controller/src/scale_tests.rs` gains
  `direct_fan_out_to_a_thousand_nodes` and `relayed_fan_out_to_a_thousand_nodes`
  (`#[ignore]`, about a second together; the reason says how to run them).
  Before the change, debug, Apple M4 Pro, load average 8-12: direct 1000 plan
  30 ms, reconcile tick 226 ms; relayed 1000 plan 6.78 s, reconcile tick 7.12 s
  (relayed 1000 now places, 500 bridges per relay, since OW-26).
- 2026-09-25: `pick_relay` and `pick_remote_relay` now take their candidates
  from a `RelayCache` in `crates/controller/src/path.rs`: the nodes an upstream
  station can link to, found once per upstream station per stream, so each
  relayed destination checks only those nodes' link to its receiver instead of
  every node's link from both ends. Choice is unchanged (lowest id with ports,
  offline and avoided nodes excluded); every existing planner test passes. After,
  same build and host, three runs: direct 1000 plan 31 ms, reconcile tick
  228-234 ms; relayed 1000 plan 138-140 ms, reconcile tick 405-484 ms, 4.5x
  direct. Relayed 300 now plans in 12 ms and ticks in 49 ms, so
  `relayed_fan_out_to_three_hundred_nodes` runs by default. Unit tests only.
