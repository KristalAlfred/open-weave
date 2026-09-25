---
id: OW-8
title: "A node that stops heartbeating is never forgotten"
type: bug
status: done
depends_on: []
assignee: claude-lifecycle
---

## Evidence

Southbound has no deregistration route, and the controller's node TTL only
changes a status: `mark_offline` sets the entry to `Offline` after
`WEAVE_NODE_TTL_SECS` (15s by default) and nothing ever removes it
(`crates/controller/src/main.rs`), so the node stays in `GET /nodes` and
`/status` as `offline`. Each browser page start without `--node` picks a fresh
id, and one bench run left three stale `browser-…` nodes beside `browser-bench`
(`7 node(s)` in `/status`).

## Done when

- [x] A node that has not heartbeated for some interval leaves the listing,
      unless a stored stream names it; a named node stays listed as `offline`.

## Easy to break

- Dropping an entry replans every stream placed on it.
- `pick_relay` skips `Offline` nodes, and a pinned relay that goes offline is
  reported `degraded` rather than swapped out. Both read the entry that would
  disappear.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-lifecycle.
- 2026-09-25: Done-when changed from "leaves the listing, or a node can
  deregister itself". Removing a node that a stream names as source,
  destination or `via` makes the stream `pending`, and every other node on it
  tears its hops down until the node returns. A named node now stays listed as
  `offline`. No deregistration route was added, and the browser page does not
  deregister on unload.
- 2026-09-25: checked with unit tests. A tick removes an offline node that no
  stored stream names (disabled streams included) once it has gone
  `WEAVE_NODE_FORGET_SECS` (default 300) without a heartbeat: from `nodes`,
  `last_seen` and the store (`StateStore::delete_node`, a `DELETE` in
  `PgStore`), and emits `node.forgotten`. On a store error it keeps the node and
  tries again next tick; that path has no test. Tests:
  `only_unnamed_offline_nodes_past_the_interval_are_forgettable`,
  `a_stream_names_its_source_destinations_and_via_relays`,
  `a_tick_forgets_an_offline_node_no_stream_names` (events, `GET /nodes`, the
  store, `404` on desired and heartbeat, no hop moved, re-registration works),
  `a_node_a_stream_names_stays_listed_as_offline`, `memstore_nodes_round_trip`,
  and the ignored `pgstore_round_trips_streams_and_nodes`, run against
  `postgres:16` in docker. Not run on the bench.
