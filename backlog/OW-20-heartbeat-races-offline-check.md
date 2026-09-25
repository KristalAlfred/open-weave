---
id: OW-20
title: "A heartbeat can race the offline check"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`node_heartbeat` in `crates/controller/src/main.rs` releases the nodes lock
before it writes `last_seen`. A reconcile tick that runs between the two can
mark a node offline that has just heartbeated. Found by reading the code; not
reproduced.

## Done when

- [ ] A test shows whether the race exists.
- [ ] If it does, a heartbeat and the offline check can no longer interleave
      that way.

## Log

- 2026-09-25: filed from research on OW-8.
