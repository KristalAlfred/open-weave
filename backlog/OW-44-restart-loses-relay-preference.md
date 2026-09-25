---
id: OW-44
title: "A controller restart can move a relayed stream back to its first relay"
type: bug
status: done
depends_on: []
assignee: claude-ha
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

- [x] A test shows whether a relayed stream keeps its relay across a
      controller restart.
- [x] If it does not, it does, without planning keeping state between ticks.

## Easy to break

- Planning stays side-effect free (`BACKLOG.md`, "Scope guards"). Whatever the
  first tick reads has to be an input, as the hop reports are.
- A relay that went offline during the restart must still lose the bridge.

## Log

- 2026-09-25: filed by claude-tests from OW-42.
- 2026-09-25: started by claude-ha, together with OW-39.
- 2026-09-25: both boxes checked with unit tests, together with OW-39.
  `a_restart_keeps_a_relayed_stream_on_the_relay_that_runs_it` moves a bridge
  to relay-b while relay-a is offline, brings relay-a back by registering it
  again, and starts a second controller from the same `MemStore`. With hop
  reports stored only at registration (the heartbeat write disabled), the
  second controller gave the bridge back to relay-a; the test fails there. Now
  a heartbeat that changes a hop's state or a socket's condition writes the
  node's registration, so the first tick reads the report that relay-b runs
  the bridge and serves every node the same hops and ports. Planning is
  unchanged: the stored report is an input. The same test then lets relay-b
  stop heartbeating, and the bridge moves to relay-a. A tick stores a node it
  marks `offline` too (`a_node_marked_offline_is_still_offline_after_a_restart`),
  so a relay offline before a restart is not taken as ready after it. Filed
  OW-48: after a failed desired fetch the Strom adapter reports no hops, which
  is now stored as well.
