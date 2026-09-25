---
id: OW-72
title: "The dashboard view scans a hop's egress reports for each egress"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`get_view` (`crates/controller/src/main.rs`, `GET /view`) finds each egress's
report with `status.egresses.iter().find(...)` over every egress report of
that hop. For a sender with N egresses, that is N scans of N reports.

A temporary probe test, not committed, placed one stream from `source` to N
direct receivers (the `direct_nodes` fan-out in
`crates/controller/src/scale_tests.rs`), sent each node's heartbeat with every
hop reported running, and timed `GET /view`. Debug build, Apple Silicon Mac
with other builds running, two runs each:

| Egresses on the sender | As built | Egress reports found by map |
|---|---|---|
| 1000 | 22.1-22.6 ms | 20.1-20.7 ms |
| 2000 | 48.6-50.9 ms | 40.2-40.4 ms |

With a map the view took 2.0 times as long at 2000 egresses as at 1000. As
built it took 2.2 to 2.3 times as long, and the difference grew from about
2 ms to about 9 ms. Found while working OW-71.

## Done when

- [ ] `get_view` finds each egress's report without scanning the hop's egress
      reports.
- [ ] A timing test for `/view` with one hop of 1000 and 2000 egresses is in
      `scale_tests.rs`, with before and after numbers in the Log.

## Log

- 2026-09-26: filed by claude-perf from OW-70 and OW-71, with the numbers
  above.
