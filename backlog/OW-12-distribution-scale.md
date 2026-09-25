---
id: OW-12
title: "Nothing runs at distribution scale"
type: verification
status: done
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

- [x] A test registers a few hundred nodes and fans one stream out to all of
      them.
- [x] Plan and reconcile times at that size are recorded in this item's Log.

## Easy to break

- Planning allocates against the full candidate stream set (`BACKLOG.md`, "Scope
  guards").

## Unchecked

- Whether the planner should spread a fan-out over transit nodes when a sender
  reaches `max_egresses`. That would be a new item.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-planner.
- 2026-09-25: `crates/controller/src/scale_tests.rs` registers every node
  through `POST /nodes/register`, applies one stream to 300 destinations through
  `POST /streams`, times `derive_path` and one `reconcile_tick`, and asserts
  placement, 300 sender egresses and one receiver hop per receiver node.
  `direct_fan_out_to_three_hundred_nodes` (301 nodes on one network) runs by
  default. `relayed_fan_out_to_three_hundred_nodes` (NAT'd source and receivers,
  two public relays, 303 nodes) takes about 0.7 s, so it is `#[ignore]` and its
  reason says how to run it. Unit tests only.
- 2026-09-25: times from `cargo test -p weave-controller scale_tests --
  --include-ignored --nocapture --test-threads=1`, debug build, Apple M4 Pro
  (12 cores), other builds running (load average 10-22), four runs. Direct 300,
  301 hops: plan 3.3-4.3 ms, reconcile tick 28-34 ms. Relayed 300, 601 hops:
  plan 315-320 ms, reconcile tick 350-356 ms; every bridge lands on `relay-a`.
  A scratch copy at 500 and 1000 receivers found relayed planning cubic (OW-25)
  and the relay out of ports at 501 destinations (OW-26).
- 2026-09-25: the first three runs above used the shared target dir, where
  cargo could link another worktree's `weave-core`; the fourth used a target dir
  of this worktree only and agreed with them. Re-run in this worktree's own
  target (`debug = "line-tables-only"`) with the OW-5 changes, load average
  14-22, two runs: direct plan 2.7-2.8 ms, reconcile tick 24 ms; relayed plan
  234-246 ms, reconcile tick 271-277 ms.
