---
id: OW-32
title: "A hop id collision is caught only at plan time"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

Since OW-21, `reconcile` in `crates/controller/src/main.rs` holds each placed
stream's hop ids in a `HopIds` (`crates/controller/src/path.rs`) and leaves a
stream unplaced when an earlier stream holds one of its ids. Streams are planned
in name order, so applying a new stream whose name sorts first unplaces an
existing stream that holds the same id. `validate_stream` accepts both, and
`POST /streams` and `PUT /stream-sets/{owner}` do not compare a stream's hop ids
with other stored streams'. `hop_id_tests.rs` shows the name-order rule; nothing
tests an apply against a stored stream.

## Done when

- [x] Applying a stream whose hop ids could equal a stored stream's is refused
      with a reason naming both streams, and the stored stream stays placed.

## Unchecked

- What the controller should do at startup with stored streams that already
  collide.

## Log

- 2026-09-25: filed from OW-21 by claude-planner.
- 2026-09-25: started by claude-tests.
- 2026-09-25: `POST /streams` and `PUT /stream-sets/{owner}` now refuse a
  stream that can plan a hop id another stream can, with
  `409 hop_id_conflict`. The message names both streams; the one detail names
  the field, the id and both streams. "Can plan" is independent of nodes:
  `shared_hop_id` (`crates/controller/src/path.rs`) compares each stream's
  sender and receiver ids, and its bridge ids as a prefix followed by any
  position, including a second path's `{id}.2` bridges. A POST is compared with
  every stored stream but itself. A set write is compared with every stored
  stream it does not replace or prune, and with the other streams in the same
  write. The check runs under the `streams` write lock, before the store
  write, so a refused write stores nothing and the set write stays one
  transaction. A stream whose spec equals its stored spec is not checked, so a
  semantic no-op keeps its generation and the set ETag. The plan-time `HopIds`
  check from OW-21 is unchanged. `ApiErrorCode::HopIdConflict` is new;
  `contracts/` regenerated, and the OpenAPI 409 descriptions for both routes
  name the new cause. `README.md` ("Hop status and fan-out", "Manifest
  validation") updated.
- 2026-09-25: Unchecked question: stored streams that already collide at
  startup are hydrated and planned as before, in name order, with the later
  one `placement_failed`. The controller still starts. Reapplying either one
  unchanged is a no-op, and changing either one is refused until the other is
  deleted or renamed.
- 2026-09-25: box ticked. Tests: `a_stream_that_can_plan_a_stored_streams_hop_id_is_refused`
  (409, nothing stored, the stored stream stays placed after a tick),
  `a_stream_set_write_that_can_plan_a_kept_streams_hop_id_writes_nothing`,
  `two_streams_in_one_set_write_that_can_plan_the_same_hop_id_are_refused`,
  `a_set_write_may_prune_the_stream_it_would_share_a_hop_id_with` and
  `streams_that_already_collide_reapply_unchanged_and_plan_in_name_order` in
  `crates/controller/src/main.rs`. With the two checks switched off, the first
  four fail with 202. `the_apply_check_names_the_id_each_colliding_pair_plans_twice`
  and `streams_whose_hop_ids_cannot_meet_are_not_flagged` are in
  `hop_id_tests.rs`. `every_planned_hop_id_has_a_form_its_stream_declares` in
  `path.rs` plans direct, relayed, pinned, remote-via and second-path streams
  and checks that every planned hop id matches one of its stream's forms. Unit
  tests only.
- 2026-09-25: not covered here: a second path's merge link is keyed by
  `weave-{stream}-receiver-{id}.2`, which is not a hop id, so neither check
  sees two streams spelling the same one. Filed as OW-43.
