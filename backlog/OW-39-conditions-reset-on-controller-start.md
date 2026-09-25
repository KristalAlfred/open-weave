---
id: OW-39
title: "Stream conditions start over on every controller start or takeover"
type: bug
status: done
depends_on: []
assignee: claude-ha
---

## Evidence

Stream conditions and their `last_transition_time` live only in
`ControllerView` (`crates/controller/src/main.rs`, `reconcile_tick` and
`stamp_condition_transition_times`). A controller that starts, or takes the
lease from another (OW-9), has none. Its first tick stamps every condition with
the current time and sends one `stream.changed` per stream, whatever the
previous controller last computed. README "Webhooks" says so.

Until a node heartbeats the new controller, the tick reads the hop status the
node stored when it last registered. On `bench/` (OW-9), after each takeover the
new leader's first tick sent `stream.changed` with `basic` `pending`
(`hops_ready` `false`, `hops_pending`) while its flows kept moving bytes, and
`flowing` again 5 s later. Every condition got a new transition time.

The scope guard in `BACKLOG.md` says a condition's transition time changes only
when its status changes.

## Done when

- [x] A controller start or takeover keeps each condition's
      `last_transition_time` while its status is unchanged.
- [x] Its first tick sends `stream.changed` only for streams whose conditions
      differ from the ones last computed before the start or takeover.
- [x] A stream whose media keeps flowing through a takeover does not read
      `pending` in between.

## Easy to break

- Conditions are written on every tick that changes them. Stored, they are a
  write per change, and with Postgres that write must go through the lease
  fence like every other.

## Log

- 2026-09-25: filed from OW-9. A takeover restarts conditions the same way a
  restart already did.
- 2026-09-25: the stored hop status question checked on `bench/` during OW-9:
  it does make the first tick differ. Moved to Evidence and a box added.
- 2026-09-25: started by claude-ha, together with OW-44.
- 2026-09-25: all three boxes checked with unit tests, together with OW-44. The
  store keeps each stream's status as the last tick that changed its
  conditions computed it (`stream_status`, removed with its stream), and each
  node's registration as the last heartbeat that changed its status, a hop's
  state or a socket's condition left it; rates and addresses alone write
  nothing (`a_heartbeat_writes_the_store_only_when_its_reports_change`). A
  tick also stores a node it marks `offline`. `AppState::hydrate` loads the
  statuses as the previous tick's, so the first tick stamps and compares
  against them. `a_restart_keeps_unchanged_conditions_and_reports_no_change`
  runs a flowing stream on one controller, then starts a second from the same
  `MemStore`: its `/status` equals the first's, transition times included, and
  its first tick sends no `stream.changed`. Without either half (reports or
  statuses) the test fails. `pg_stream_statuses_go_with_their_stream_and_are_fenced`
  covers the Postgres table, run against `postgres:16` in docker. A takeover
  loads state through the same `hydrate`. Not yet rerun on `bench/`.
- 2026-09-25: on `bench/`, with `basic` flowing: `just bench controller-restart
  basic stop` and then `... kill` each moved the lease to the other controller.
  `/status` gave `basic` `flowing` with the same five transition times before
  and after both takeovers, and the webhook sink got no `stream.changed` from
  either takeover; the OW-9 run before this change got `pending`, then
  `flowing`. After the kill the adapters registered again, which sent four
  `node.registered`. Every `weave-` flow kept its id and kept moving bytes.
