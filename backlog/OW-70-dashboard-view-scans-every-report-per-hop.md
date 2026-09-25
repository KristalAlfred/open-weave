---
id: OW-70
title: "The dashboard view scans every hop report for each hop"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`get_view` (`crates/controller/src/main.rs`, served at `GET /view`) collects
every node's hop reports into one list, then for each hop of each stream runs
`observed.iter().find(...)` over that whole list. Its cost grows with hops
times reports, so with streams, as `reconcile` did before OW-69. It holds read
locks on `nodes`, `last_seen` and `view` while it runs. Found while working
OW-69 and not measured.

## Done when

- [ ] `get_view` finds each hop's report without scanning every report.
- [ ] A timing of `/view` at 1000 and 2000 streams with hop reports is in the
      Log.

## Log

- 2026-09-26: filed by claude-perf from OW-69.
