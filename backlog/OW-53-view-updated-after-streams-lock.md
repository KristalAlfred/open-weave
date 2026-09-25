---
id: OW-53
title: "A stream write updates the view after releasing the streams lock"
type: bug
status: done
depends_on: []
assignee: claude-ha
---

## Evidence

`submit_stream`, `put_stream_set` and `delete_stream`
(`crates/controller/src/main.rs`) wrote the store and the `streams` map, released
the `streams` lock, and only then updated the view. Another request or a tick
could run in that gap. Two writes to one stream could then update the view in
the opposite order, so the view's `generation` went back. With a tick in
between, the view showed `generation` equal to `observed_generation` for a spec
no tick had reconciled, against the scope guard in `BACKLOG.md`. A delete
followed by a create of the same name could remove the new stream's placeholder,
so `/streams/{name}/endpoints` answered `404` until the next tick. Found in a
review of the controller code; the reordering itself was not reproduced.

## Done when

- [x] A test shows whether another request can read the streams between a
      write and its view update.
- [x] If it can, it cannot.

## Log

- 2026-09-25: filed from a review of the controller code, and fixed by
  claude-ha. `a_stream_write_holds_the_streams_until_the_view_shows_it` holds
  the view lock, sends a create, a stream-set apply and a delete in turn, and
  checks whether the `streams` lock is free while the write waits for the view.
  Before the fix it was, for the create. Now each handler updates the view
  before it releases `streams`, the same order a tick takes the two locks in.
  Unit tests only. The reordered updates were reasoned from the code, not
  seen.
