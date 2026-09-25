---
id: OW-30
title: "A re-registration can race the offline check"
type: bug
status: done
depends_on: []
assignee: claude-lifecycle
---

## Evidence

`register_node` in `crates/controller/src/main.rs` released the nodes lock
before it wrote `last_seen`, the same gap OW-20 describes for heartbeats. A
node that re-registers after missing heartbeats past `WEAVE_NODE_TTL_SECS`, and
a reconcile tick that runs between the two writes, marks the node `offline`
straight after its registration was accepted.

## Done when

- [x] A tick can no longer run between a registration's node write and its
      `last_seen` write.

## Log

- 2026-09-25: filed by claude-lifecycle while working OW-20, and fixed with it.
  `a_tick_cannot_see_a_registration_half_applied` holds `last_seen`, lets a
  re-registration of a node last seen 60 s ago run to its wait on it, then takes
  the nodes lock and runs `mark_offline` as a tick would. Before the fix it got
  the lock and marked the node offline (`Ok(["strom-node-1"])`); after it, the
  lock is still held by the registration. Unit tests only.
