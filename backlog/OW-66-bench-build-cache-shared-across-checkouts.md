---
id: OW-66
title: "Bench builds from two checkouts share one cargo target cache"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
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

- [x] A bench build uses only the checkout it was started from.

## Log

- 2026-09-25: filed by claude-webrtc.
- 2026-09-25: started by claude-webrtc.
- 2026-09-25: ticked. `bench/Dockerfile` touches every file under `crates` and
  `examples` before `cargo build`, so the workspace crates rebuild from the
  checkout being built and the dependencies stay cached. Checked with four
  `docker build -f bench/Dockerfile` runs from `git archive` copies (sources
  carry the commit time, as an older checkout's do), each with
  `AUTH_DISABLED_VAR` in `weave-core` renamed to a marker, `…_MARKA` or
  `…_MARKB`, and `grep` for the marker in the image's `weave-adapter-strom`.
  Without the touch: B built in 23 s, then A in 1 s, and A's image carried
  `MARKB`, B's code. With it: B in 21 s carried `MARKB`, then A in 20 s carried
  `MARKA`. Bench image builds only; the stack was not started.
