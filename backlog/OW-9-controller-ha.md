---
id: OW-9
title: "One controller, no failover"
type: feature
status: done
depends_on: [OW-1]
assignee: claude-ha
---

## Evidence

One controller owns all state, with no leader election and no failover. While it
is down nothing can be applied, replanned or observed. Station groups and event
distributors run links around the clock: Tegna's Charlotte hub serves 64
stations
([NewscastStudio, 2021](https://www.newscaststudio.com/2021/03/16/tegna-adds-stream-center-master-control-hub-for-station-group/)).

## Done when

- [x] A second controller takes over when the first stops, with no operator step.
- [x] Nodes and running media carry on through the switch, shown on the bench.

## Easy to break

- The controller is the sole owner of state. Two controllers must never both
  serve desired hops from state that has diverged.
- Generations, revisions and stream-set ETags must survive a takeover
  (`BACKLOG.md`, "Scope guards").

## Log

- 2026-09-25: moved from "Not scheduled" after broadcaster research.
- 2026-09-25: started by claude-ha.
- 2026-09-25: controller half landed, checked with unit tests and with the
  ignored Postgres tests run against `postgres:16` in docker. With
  `DATABASE_URL`, a controller serves only while it holds a lease row with an
  epoch, renewed every third of `WEAVE_LEASE_TTL_SECS`; it stands by when no
  renewal succeeds for two thirds of it, a third before the lease can run out
  on the Postgres clock. Every store write checks the epoch under a share lock
  in its own transaction (`pg_every_write_is_fenced_by_the_lease`,
  `pg_a_takeover_waits_for_a_write_past_its_fence`, which fails with the lock
  removed). A standby answers `503 not_leader` on every route but `/health`,
  `/` and `/ui` (`a_standby_answers_not_leader_on_every_api_route`).
  `a_standby_on_postgres_takes_over_when_the_leader_stops` runs two
  controllers on one database: the second waits while the first renews, then
  serves the same desired hops, stream generation, stream ETag and stream-set
  ETag once the first stops. `a_leader_whose_lease_is_taken_stops_serving_and_stands_by`
  covers a leader losing the lease and taking it again later. A takeover
  counts every stored node as heard at the takeover
  (`a_takeover_counts_every_stored_node_as_heard_at_the_takeover`), and
  webhook ids count from the lease's start on the Postgres clock. Conditions
  still start over, as on a restart: filed as OW-39. Northbound and southbound
  do not fail over yet.
- 2026-09-25: northbound and southbound take a comma-separated
  `WEAVE_CONTROLLER_URL` (`weave_core::upstream`). A request goes first to the
  controller that answered last, and moves on only on a connect error (connect
  timeout 2 s) or `503 not_leader`. Unit tests in `crates/core/src/upstream.rs`
  cover both, and show a `500`, a `412`, a `503 stream_not_ready` and a bare
  `503` passed back without trying the next; each proxy has a test sending a
  request past a standby to the leader. The Strom adapter and the browser node
  call only southbound, so neither changed.
- 2026-09-25: both boxes checked on `bench/`, which now runs `controller` and
  `controller-2` on one Postgres, with northbound and southbound listing both.
  With `basic` flowing, `just bench controller-restart basic stop` stopped the
  leader, controller-2. Controller-1 took the lease 111 ms after the SIGTERM
  and no adapter logged a failed sync. Both `weave-basic-*` flows kept their
  Strom ids and kept moving bytes (sender 10.1 MB before, 24.9 MB after), no
  Strom node read `offline`, generation 1 and ETag `"revision-1"` were
  unchanged, re-applying the manifest gave `changed: false`, and controller-2
  came back answering `503 not_leader`. `kill` (SIGKILL, then start):
  controller-2 took the lease 8 s after the kill, once it ran out; the adapters
  got `503 not_leader` for two polls, kept their flows and registered again,
  and the same checks passed. `restart`: the leader gave the lease up on
  SIGTERM and took it back 220 ms later, before the standby's next try; the
  same checks passed. The first kill run failed in the recipe itself, whose
  wait loop ended on empty output; fixed and rerun. Webhooks from the new
  leader counted from its lease start (`basic-1790338795338185`), and its
  first tick sent `stream.changed` with `basic` `pending` (`hops_pending`,
  from the hop status stored at registration), then `flowing` 5 s later;
  recorded on OW-39. The standby's dashboard read "standby: another controller
  leads" (Playwright).
