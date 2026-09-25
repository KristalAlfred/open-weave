---
id: OW-9
title: "One controller, no failover"
type: feature
status: todo
priority: 2
depends_on: [OW-1]
assignee:
branch:
pr:
---

## Evidence

One controller owns all state, with no leader election and no failover. While it
is down nothing can be applied, replanned or observed. Station groups and event
distributors run links around the clock: Tegna's Charlotte hub serves 64
stations
([NewscastStudio, 2021](https://www.newscaststudio.com/2021/03/16/tegna-adds-stream-center-master-control-hub-for-station-group/)).

## Done when

- [ ] A second controller takes over when the first stops, with no operator step.
- [ ] Nodes and running media carry on through the switch, shown on the bench.

## Easy to break

- The controller is the sole owner of state. Two controllers must never both
  serve desired hops from state that has diverged.
- Generations, revisions and stream-set ETags must survive a takeover
  (`BACKLOG.md`, "Scope guards").

## Log

- 2026-09-25: moved from "Not scheduled" after broadcaster research.
