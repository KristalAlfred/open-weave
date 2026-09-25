---
id: OW-58
title: "Each stream rebuilds the running-hop map from every hop report"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

A read-only review of `crates/controller/src/path.rs` at `f5e2fdd` found that
OW-42's `RelayCache::new(observed)` walks every hop report once for each stream
planned. `reconcile` therefore costs streams × reports, and it runs while the
controller holds `streams.read()` for the whole tick. The reviewer measured, in
a debug build, 1.54 s at 1000 streams with reports (77 ms with the rebuild
patched out) and 6.35 s at 2000 (214 ms).

`a_reconcile_with_reports_at_one_and_two_thousand_streams` in
`crates/controller/src/port_hold_tests.rs` (`#[ignore]`, run with
`cargo test -p weave-controller reconcile_with_reports -- --ignored --nocapture --test-threads=1`)
reconciles 1000 and 2000 direct streams against their own reports, two per
stream. At `06e794b`, in a debug build on this host: 1000 streams took
1.42 s and 2000 took 5.91 s.

## Done when

- [x] The running-hop map is built once per tick, not once per stream.

## Log

- 2026-09-26: filed by claude-tests from the planner review, with the numbers
  above.
- 2026-09-26: fixed. The map of which nodes run each hop is now part of
  `HopReports` (renamed from OW-56's `HeldPorts`). `reconcile` builds it once
  per tick into the port allocator, and each stream's `RelayCache` shares it
  through an `Arc`. `derive_stream` no longer reads its `observed` argument;
  hop reports reach it through `PortAllocator::holding`. Same test, same
  host, after the change: 1000 streams 147 ms, 2000 streams 487 ms. The
  remaining growth is faster than linear. The review did not attribute it and
  I did not profile it; the port allocator is cloned per stream, which is one
  candidate. Box ticked on that timing and on `cargo test -p
  weave-controller` passing, unit tests only.
