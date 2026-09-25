---
id: OW-20
title: "A heartbeat can race the offline check"
type: bug
status: done
depends_on: []
assignee: claude-lifecycle
---

## Evidence

`node_heartbeat` in `crates/controller/src/main.rs` releases the nodes lock
before it writes `last_seen`. A reconcile tick that runs between the two can
mark a node offline that has just heartbeated. Found by reading the code; not
reproduced.

## Done when

- [x] A test shows whether the race exists.
- [x] If it does, a heartbeat and the offline check can no longer interleave
      that way.

## Log

- 2026-09-25: filed from research on OW-8.
- 2026-09-25: started by claude-lifecycle.
- 2026-09-25: box 1 checked with a unit test: the race exists.
  `a_tick_cannot_see_a_heartbeat_half_applied` holds `last_seen`, lets a
  heartbeat from a node last seen 60 s ago run until it waits on it, then takes
  the nodes lock and runs `mark_offline`, as the start of a tick does. On the
  old code the lock was free and the node that had just heartbeated came out
  `offline` (`Ok(["strom-node-1"])`). By my reading of the handler, two real
  tasks can only meet in that gap on separate runtime threads, so the test
  stands in for the tick rather than running one.
- 2026-09-25: box 2 checked with the same test. `node_heartbeat` now writes
  `last_seen` before it drops the nodes lock, so the test finds that lock held.
  The tick takes the nodes lock before `last_seen`, as the handlers now do.
  `register_node` had the same gap; it is OW-30, fixed alongside. Unit tests
  only.
