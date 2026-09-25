---
id: OW-1
title: "A controller restart probably tears down running media"
type: bug
status: in-progress
depends_on: []
assignee: claude-lifecycle
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

- [x] A test shows whether a controller restart deletes running Strom flows,
      with the in-memory store and with Postgres.
- [x] If a restart with the Postgres store does, it no longer does.
- [ ] A bench case restarts the controller with media flowing and no flow drops.

## Easy to break

- An adapter that ignores an empty desired list can no longer be told to remove
  its last hop.

## Log

- 2026-09-25: filed from a code reading during broadcaster research.
- 2026-09-25: started by claude-lifecycle.
- 2026-09-25: box 2 changed from "If it does, it no longer does" to cover the
  Postgres store only. The in-memory store loses every stream on a restart, so
  once the nodes re-register the next tick tells them to run nothing and their
  adapters remove the flows. That is desired state being applied. Keeping the
  flows would need desired state the controller no longer has.
- 2026-09-25: box 1 checked with unit tests. Before the fix, a probe test that
  hydrated two `AppState`s from one `MemStore` (standing in for Postgres) got
  node 1's sender hop from the first, and `200 []` from the second before its
  first tick. A fresh store also answered `200 []` for a node that had not
  registered. The Strom adapter deletes every `weave-` flow on `200 []`
  (`an_empty_desired_list_deletes_only_managed_flows` in
  `crates/adapter-strom/src/main.rs`), so a restart deleted running flows with
  both stores. The Postgres half of the probe was not run on the old code; it
  goes through the same `hydrate` and `get_desired`.
- 2026-09-25: box 2 checked with unit tests. The controller runs its first tick
  before it serves (`router_after_first_tick`), and `GET /nodes/{id}/desired`
  answers `404 node_not_found` for a node without a snapshot.
  `a_restart_serves_the_stored_hops_on_its_first_request` (MemStore) and the
  ignored `a_restart_on_postgres_serves_the_stored_hops_on_its_first_request`
  (run with `--ignored` against `postgres:16` in docker, passed twice) show the
  restarted controller serving the same hops on its first request.
  `a_node_the_last_tick_did_not_cover_gets_404_for_its_desired_hops` shows the
  in-memory case: `404` until the node has re-registered and a tick has run,
  then `200 []`. `deleting_the_last_stream_tells_its_nodes_to_run_nothing`
  keeps last-hop removal. The adapter test
  `a_desired_response_other_than_2xx_leaves_every_flow_alone` covers `404`,
  `502`, `503` and an unreachable southbound. The browser page's `pollDesired`
  throws on any non-2xx and keeps its hops (`nodes/browser/node.js`); read, not
  run. No `PROTOCOL_VERSION` bump: both shipped nodes already treated a
  non-2xx this way.
