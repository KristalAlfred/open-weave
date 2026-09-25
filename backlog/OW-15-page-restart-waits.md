---
id: OW-15
title: "A restarted page waits out Strom's inactivity timeout"
type: bug
status: todo
priority: 3
depends_on: []
assignee:
branch:
pr:
---

## Evidence

`whip_input` is created with `max_sessions: 1` (`crates/strom/src/spec.rs`) and
the page cannot release the session it left behind: Strom's CORS exposes only
`mcp-session-id`, so the `Location` header of the WHIP `201` is unreadable in a
browser and there is nothing to `DELETE`. Observed: Strom answers `503` for
10–20 s until its inactivity monitor frees the slot, then the new session flows.

## Done when

- [ ] A page restart reconnects without that window.

## Easy to break

- `max_sessions: 1` is what makes one page own one endpoint, and what a second
  connection to the same endpoint does with a higher limit is untested.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
