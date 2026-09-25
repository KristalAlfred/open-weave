---
id: OW-8
title: "A node that stops heartbeating is never forgotten"
type: bug
status: todo
priority: 2
depends_on: []
assignee:
branch:
pr:
---

## Evidence

Southbound has no deregistration route, and the controller's node TTL only
changes a status: `mark_offline` sets the entry to `Offline` after
`WEAVE_NODE_TTL_SECS` (15s by default) and nothing ever removes it
(`crates/controller/src/main.rs`), so the node stays in `GET /nodes` and
`/status` as `offline`. Each browser page start without `--node` picks a fresh
id, and one bench run left three stale `browser-…` nodes beside `browser-bench`
(`7 node(s)` in `/status`).

## Done when

- [ ] A node that has not heartbeated for some interval leaves the listing, or a
      node can deregister itself.

## Easy to break

- Dropping an entry replans every stream placed on it.
- `pick_relay` skips `Offline` nodes, and a pinned relay that goes offline is
  reported `degraded` rather than swapped out. Both read the entry that would
  disappear.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
