---
id: OW-1
title: "A controller restart probably tears down running media"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

- `get_desired` serves an empty list for any node without a snapshot
  (`crates/controller/src/main.rs`).
- `spawn_api_server` starts the API before the reconcile loop runs its first
  tick, so the desired map is empty for a moment after every start.
- The Strom adapter deletes managed flows that are not desired (`diff_hops` in
  `crates/adapter-strom/src/provision.rs`).

Reading that code, an in-memory controller that restarts empties every node,
and one backed by Postgres (`DATABASE_URL`) does the same to any node that polls
before the first tick finishes. None of this has been run.

## Done when

- [ ] A test shows whether a controller restart deletes running Strom flows,
      with the in-memory store and with Postgres.
- [ ] If it does, it no longer does.
- [ ] A bench case restarts the controller with media flowing and no flow drops.

## Easy to break

- An adapter that ignores an empty desired list can no longer be told to remove
  its last hop.

## Log

- 2026-09-25: filed from a code reading during broadcaster research.
