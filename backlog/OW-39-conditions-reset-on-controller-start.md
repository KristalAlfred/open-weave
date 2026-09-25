---
id: OW-39
title: "Stream conditions start over on every controller start or takeover"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] A controller start or takeover keeps each condition's
      `last_transition_time` while its status is unchanged.
- [ ] Its first tick sends `stream.changed` only for streams whose conditions
      differ from the ones last computed before the start or takeover.
- [ ] A stream whose media keeps flowing through a takeover does not read
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
