---
id: OW-70
title: "The dashboard view scans every hop report for each hop"
type: bug
status: done
depends_on: []
assignee: claude-perf
---

## Evidence

`get_view` (`crates/controller/src/main.rs`, served at `GET /view`) collects
every node's hop reports into one list, then for each hop of each stream runs
`observed.iter().find(...)` over that whole list. Its cost grows with hops
times reports, so with streams, as `reconcile` did before OW-69. It holds read
locks on `nodes`, `last_seen` and `view` while it runs. Found while working
OW-69 and not measured.

## Done when

- [x] `get_view` finds each hop's report without scanning every report.
- [x] A timing of `/view` at 1000 and 2000 streams with hop reports is in the
      Log.

## Log

- 2026-09-26: filed by claude-perf from OW-69.
- 2026-09-26: started by claude-perf.
- 2026-09-26: `the_view_with_reports_at_one_and_two_thousand_streams` in
  `crates/controller/src/scale_tests.rs` (`#[ignore]`, the reason says how to
  run it) registers two nodes, places N streams with one destination each,
  sends both nodes' heartbeats with every hop reported running, and times
  `GET /view` through the router. Before the change, debug build, Apple
  Silicon Mac with other builds running, three runs: 1000 streams 35.6-35.8
  ms, 2000 streams 88.2-89.0 ms, 2.5 times.
- 2026-09-26: fixed. `get_view` maps each hop id and node to its first report
  once with `reports_by_hop`, the map `reconcile` uses since OW-69, which now
  takes any iterator of reports. After, same test and host, three runs: 1000
  streams 26.6-26.9 ms, 2000 streams 53.9-55.3 ms, 2.0 to 2.1 times. Both
  boxes ticked on those runs and on `just fmt-check`, `just lint` and
  `just test` passing, including `view_joins_desired_hops_with_reported_status`.
  Unit tests only. Each hop's egress reports are still found by scanning that
  hop's egresses, so a hop with many egresses costs egresses squared; this
  test has one egress per hop and does not measure that.
