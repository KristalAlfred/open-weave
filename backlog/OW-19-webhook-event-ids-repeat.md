---
id: OW-19
title: "Webhook event ids repeat after a controller restart"
type: bug
status: done
depends_on: []
assignee: claude-lifecycle
---

## Evidence

`event_id` is `{id}-{seq}` with `seq` starting at 0 on every controller start
(`crates/controller/src/webhook.rs`), so a restarted controller sends ids it
has sent before. `README.md`, "Node lifecycle webhooks", invites receivers to
deduplicate by id, and a receiver that does can drop new events.

## Done when

- [x] Event ids do not repeat across controller restarts.

## Log

- 2026-09-25: filed from research on OW-7.
- 2026-09-25: started by claude-lifecycle.
- 2026-09-25: checked with a unit test. `event_id` keeps its
  `{node id or stream name}-{n}` shape, and `n` now counts up from the
  controller's start time in microseconds instead of from 0
  (`first_sequence` in `crates/controller/src/webhook.rs`). A restarted
  controller repeats an id only if the earlier run averaged more than one event
  per microsecond or the clock went back. `a_restarted_emitter_sends_ids_the_earlier_one_did_not`
  emits three events from one emitter, drops it, and checks that a new emitter's
  first id is above all three; with the old counter it would send `-0` again.
  README "Webhooks" says so. Unit tests only.
