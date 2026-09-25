---
id: OW-25
title: "Relayed fan-out planning grows with the cube of its size"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] Relayed fan-out to 1000 receivers plans within 10x the time of direct
      fan-out to 1000 receivers on the same build, recorded in this Log.

## Easy to break

- Relay choice is the lowest-id eligible online node and must stay stable
  across ticks.
- Planning allocates against the full candidate stream set (`BACKLOG.md`,
  "Scope guards").

## Log

- 2026-09-25: filed from OW-12 by claude-planner.
