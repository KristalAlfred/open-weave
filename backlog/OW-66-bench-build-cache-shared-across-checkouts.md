---
id: OW-66
title: "Bench builds from two checkouts share one cargo target cache"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`bench/Dockerfile` builds with `--mount=type=cache,target=/build/target`. The
cache id defaults to the target path, so every build of that Dockerfile on the
host, from any checkout, shares one target directory. Cargo decides freshness by
file mtimes, so an artifact built later from another checkout's sources can
count as fresh for a checkout whose sources are older.

Seen on 2026-09-25 with several worktrees building the bench in turn: `just
bench up` from a worktree whose `weave-core` had `Track` failed in
`weave-strom` with ``unresolved import `weave_core::Track` `` and
``no field `tracks` on type `&DesiredHop` ``. `main` at 7b2ce9d, with the same
`weave-core`, had built and run on the bench 15 minutes earlier, before other
worktrees built.

## Done when

- [ ] A bench build uses only the checkout it was started from.

## Log

- 2026-09-25: filed by claude-webrtc.
