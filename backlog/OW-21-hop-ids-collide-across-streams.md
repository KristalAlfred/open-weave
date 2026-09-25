---
id: OW-21
title: "Hop ids can collide across streams on one node"
type: bug
status: done
depends_on: []
assignee: claude-planner
---

## Evidence

Sender and receiver hop ids are built by joining names with `-`
(`crates/controller/src/path.rs`), so `sender_hop_id("x-receiver-a")` equals
`receiver_hop_id("x", "a-sender")`. Two streams on one node can then plan the
same hop id. Found by reading the code; not reproduced.

## Done when

- [x] A test shows whether two valid streams can plan the same hop id on one
      node.
- [x] If they can, they no longer can, and hop ids of existing streams stay
      stable (`README.md`, "Hop status and fan-out").

## Log

- 2026-09-25: filed from research on OW-5.
- 2026-09-25: started by claude-planner.
- 2026-09-25: box 1. `two_valid_streams_can_spell_the_same_hop_id_on_one_node`
  (`crates/controller/src/hop_id_tests.rs`) plans four pairs of streams that
  pass `validate_stream`: a sender and a receiver, two receivers, a receiver and
  a bridge, and two bridges, each pair spelling the same id on node `edge` when
  planned apart. Planned together before the fix, the first pair put
  `weave-x-receiver-a-sender` in `edge`'s desired hops twice; with the fix
  switched off the test fails there. Unit test only.
- 2026-09-25: box 2. `reconcile` plans each stream on a copy of the port
  allocator and keeps it only when `HopIds::claim` finds none of the stream's
  hop ids held by an earlier stream (name order). Otherwise the stream stays
  pending with `placement_failed` and "hop id X is already planned for stream
  Y". The check is across all nodes, not per node, because
  `LinkKeys::link` derives the SRT key from the hop id alone, so two streams
  sharing an id on different nodes would share a link key. No id format
  changed, so ids of every stream that collides with none stay the same; the
  same test checks the earlier stream's hops are those it plans alone.
  `a_refused_stream_claims_no_port` fails if a refused stream's ports are kept
  (checked by breaking that). A new stream that sorts first can unplace a
  stored one; filed as OW-32. Unit tests only.
