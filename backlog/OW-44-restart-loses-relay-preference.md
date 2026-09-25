---
id: OW-44
title: "A controller restart can move a relayed stream back to its first relay"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Since OW-42, `derive_stream` (`crates/controller/src/path.rs`) keeps a bridge on
the relay whose hop report says it runs it. The reports come from
`observed_state`, which reads each registration's `hop_status`. A heartbeat
updates `hop_status` in memory only (`node_heartbeat` in
`crates/controller/src/main.rs`; `heartbeat_updates_memory_but_never_the_store`).
The store holds the `hop_status` a node sent when it registered. On a
restart, `AppState::hydrate` loads those registrations and
`router_after_first_tick` runs a tick before any heartbeat arrives. If a bridge
moved to a relay after that relay registered, the first tick sees no report of
it, gives the bridge to the lowest-id relay again, and serves that desired
state. Found by reading the code while working OW-42; not reproduced.

## Done when

- [ ] A test shows whether a relayed stream keeps its relay across a
      controller restart.
- [ ] If it does not, it does, without planning keeping state between ticks.

## Easy to break

- Planning stays side-effect free (`BACKLOG.md`, "Scope guards"). Whatever the
  first tick reads has to be an input, as the hop reports are.
- A relay that went offline during the restart must still lose the bridge.

## Log

- 2026-09-25: filed by claude-tests from OW-42.
