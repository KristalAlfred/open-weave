---
id: OW-52
title: "A failed write of a node report or stream status is never retried"
type: bug
status: done
depends_on: [OW-39]
assignee: claude-ha
---

## Evidence

`reconcile_tick` (`crates/controller/src/main.rs`) stores the streams whose
conditions changed, but works out what changed against the in-memory view,
which it has already updated. When the store write failed, the next tick found
no change and wrote nothing, so Postgres kept the older status. A heartbeat's
report write and a tick's offline mark were the same: logged, never retried.
After a restart or takeover the first tick then compares against those older
rows, stamps new transition times and sends `stream.changed`, which README
"Controller failover" says it does not. Found in a review of the OW-39 code.

## Done when

- [x] A test shows whether a failed status, report or offline write is written
      on a later tick.
- [x] If it is not, it is.

## Log

- 2026-09-25: filed from a review of OW-39, and fixed by claude-ha.
  `a_failed_status_save_is_retried_on_the_next_tick` and
  `failed_node_writes_are_retried_on_the_next_tick` make `MemStore` fail the
  next writes (`MemStore::fail_writes`, test only). Before the fix both failed:
  after the second tick the store held no status, and node 1 was still `ready`
  and node 2 without its hop report. Now `AppState` keeps the nodes and streams
  whose write failed, and each tick writes them again as they stand then. A
  stream deleted meanwhile drops out, since the tick writes only streams it
  still has, and the Postgres write skips a stream that is gone. Unit tests
  only.
