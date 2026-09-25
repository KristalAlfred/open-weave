---
id: OW-32
title: "A hop id collision is caught only at plan time"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] Applying a stream whose hop ids could equal a stored stream's is refused
      with a reason naming both streams, and the stored stream stays placed.

## Unchecked

- What the controller should do at startup with stored streams that already
  collide.

## Log

- 2026-09-25: filed from OW-21 by claude-planner.
