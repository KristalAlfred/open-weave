---
id: OW-12
title: "Nothing runs at distribution scale"
type: verification
status: in-progress
depends_on: []
assignee: claude-planner
---

## Evidence

Everything is verified on three Strom nodes. Distribution runs to hundreds of
receivers: PBS to more than 330 stations, BBC World Service to hundreds of
partners
([Zixi, 2025, vendor](https://zixi.com/news/encompass-and-zixi-partner-to-transform-bbc-world-service-to-ip-distribution/)).
Nothing measures plan time, desired-state size or heartbeat load at that size.

## Done when

- [ ] A test registers a few hundred nodes and fans one stream out to all of
      them.
- [ ] Plan and reconcile times at that size are recorded in this item's Log.

## Easy to break

- Planning allocates against the full candidate stream set (`BACKLOG.md`, "Scope
  guards").

## Unchecked

- Whether the planner should spread a fan-out over transit nodes when a sender
  reaches `max_egresses`. That would be a new item.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-planner.
