---
id: OW-69
title: "Reconcile time grows faster than the number of streams"
type: bug
status: done
depends_on: []
assignee: claude-perf
---

## Evidence

After OW-58, the ignored timing test for `reconcile` with hop reports, one
destination per stream, debug build, took 147 ms for 1000 streams and 487 ms
for 2000 (`backlog/OW-58`, Log). Doubling the streams took 3.3 times as long.
Nothing has profiled where the time goes. `backlog/OW-58` names one candidate:
the port allocator is cloned for every stream. `streams.read()` is held for the
whole tick (`backlog/OW-25`), so stream writes wait for it.

## Done when

- [x] A profile or timing breakdown says where the growth comes from.
- [x] Reconcile time at 1000 and 2000 streams grows no faster than linearly,
      or this item's Log says why it cannot.

## Log

- 2026-09-26: filed by the lead from the OW-58 report.
- 2026-09-26: started by claude-perf.
- 2026-09-26: timing breakdown. `xctrace` was not allowed to run and no other
  profiler is installed, so I timed sections of `reconcile` and `derive_on`
  with `Instant` in temporary instrumentation, since removed. Same test
  (`a_reconcile_with_reports_at_one_and_two_thousand_streams`), debug build,
  Apple Silicon Mac with other builds running (load average 4-8).
  Uninstrumented: 145 ms at 1000 streams, 485 ms at 2000. Instrumented, 1000
  to 2000 streams: `HopReports::held_by` from `derive_on` 76 to 301 ms,
  `path_status` 12.5 to 45 ms, `destination_path_status` 13.4 to 47 ms.
  `held_by` walked every reported socket of the tick once per stream, and the
  other two walked every report once per hop. The rest about doubled, the
  largest part being the receiver's `plan_hop` at 14.5 to 29 ms. All port
  allocator clones together took 0.5 to 1.3 ms a tick, so they were not the
  cause. Box ticked on that breakdown. Unit tests only.
- 2026-09-26: fixed. `HopReports` keys listening sockets by socket, then node,
  in a `BTreeMap`. `second_path_held` replaces `held_by`: it finds a stream's
  second-path sockets by exact lookup and one bridge-prefix range per
  destination that asks for two paths, so a stream with none looks nothing up.
  `reconcile` maps each hop id and node to its first report once per tick
  (`reports_by_hop`) and passes each stream only its own reports
  (`path_reports`). `path_status` and `destination_path_status` take a slice
  of anything that borrows as a `HopStatus`, so their tests are unchanged, as
  is every other test. After, same test and host, three runs: 1000 streams
  44-46 ms, 2000 streams 90-92 ms, 2.0 to 2.1 times. The port allocator is
  still cloned per stream and those clones still grow faster than the stream
  count, 0.4 to 1.2 ms a tick in total, about 1% of the 2000-stream tick.
  The ignored fan-out tests in `scale_tests.rs` gave the same times before
  and after, two alternating runs each: direct 1000 plan 90 ms, tick 286-292
  ms; relayed 1000 plan 350 ms, tick 614-618 ms.
  Box ticked on those runs and on `just fmt-check`, `just lint` and
  `just test` passing. Unit tests only. `get_view` has the same scan as
  `path_status` had, filed as OW-70.
