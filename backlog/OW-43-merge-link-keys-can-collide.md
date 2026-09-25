---
id: OW-43
title: "Two streams' second-path merge links can derive the same SRT key"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] A test shows whether two valid streams that ask for two paths can derive
      the same merge-link key.
- [ ] If they can, they no longer can, and existing hop ids and keys stay the
      same for streams that collide with nothing.

## Log

- 2026-09-25: filed by claude-tests while working OW-32.
