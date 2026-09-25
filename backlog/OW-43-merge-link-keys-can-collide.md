---
id: OW-43
title: "Two streams' second-path merge links can derive the same SRT key"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

A destination with `paths: 2` gets a second link into its receiver.
`place_second_path` in `crates/controller/src/path.rs` plans that link with
`receiver_hop_id(stream, "{destination}.2")` as the downstream id, and
`plan_link` passes that id to `LinkKeys::link` (`crates/controller/src/keys.rs`),
which derives the SRT passphrase from the id alone. The id is not a hop in the
path, so neither the plan-time `HopIds` check (OW-21) nor the apply-time
`shared_hop_id` check (OW-32) looks at it.

Names joined with `-` can spell the same merge id: stream `x-receiver-a` with
destination `b`, and stream `x` with destination `a-receiver-b`, both give
`weave-x-receiver-a-receiver-b.2`. Two such links would share a key. Found by
reading the code; not reproduced. No shipped node advertises a `merge` profile,
so no shipped deployment plans a second path.

## Done when

- [x] A test shows whether two valid streams that ask for two paths can derive
      the same merge-link key.
- [x] If they can, they no longer can, and existing hop ids and keys stay the
      same for streams that collide with nothing.

## Log

- 2026-09-25: filed by claude-tests while working OW-32.
- 2026-09-25: started by claude-tests.
- 2026-09-25: box 1.
  `two_streams_that_spell_one_merge_link_id_never_both_carry_its_key`
  (`crates/controller/src/redundant_paths_tests.rs`) plans stream `x` to
  `a-receiver-b` and stream `x-receiver-a` to `b`, both with `paths: 2`.
  Planned apart with `derive_stream`, their merge links carry the same key.
  Reconciled together, `x` is placed and `x-receiver-a` is not, and each
  derived key in the served hops sits on exactly two sockets, the two ends of
  one link. `shared_hop_id` flags the pair. With the plan-time `HopIds` check
  switched off, the test fails on the shared key. Unit tests only.
- 2026-09-25: box 2: no change was needed, and this item's Evidence was wrong.
  The merge id is `receiver_hop_id(stream, "{destination}.2")`, the stream's
  receiver id with `.2` appended. `.` is outside the resource-id alphabet, and
  no hop id ends in `.2`: receivers end in a destination id, senders in
  `-sender`, bridges in `-{position}`. So a merge id never equals a hop id, and
  two merge ids are equal only when the two receiver hop ids are, which the
  plan-time check (OW-21) and the apply-time check (OW-32) already refuse. The
  example in Evidence is such a pair: its two receivers are both
  `weave-x-receiver-a-receiver-b`. No hop ids or keys changed.
  `crates/controller/src/keys.rs` is untouched, so OW-37 is not affected.
