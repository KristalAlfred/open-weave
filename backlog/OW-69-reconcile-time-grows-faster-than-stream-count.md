---
id: OW-69
title: "Reconcile time grows faster than the number of streams"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

After OW-58, the ignored timing test for `reconcile` with hop reports, one
destination per stream, debug build, took 147 ms for 1000 streams and 487 ms
for 2000 (`backlog/OW-58`, Log). Doubling the streams took 3.3 times as long.
Nothing has profiled where the time goes. `backlog/OW-58` names one candidate:
the port allocator is cloned for every stream. `streams.read()` is held for the
whole tick (`backlog/OW-25`), so stream writes wait for it.

## Done when

- [ ] A profile or timing breakdown says where the growth comes from.
- [ ] Reconcile time at 1000 and 2000 streams grows no faster than linearly,
      or this item's Log says why it cannot.

## Log

- 2026-09-26: filed by the lead from the OW-58 report.
