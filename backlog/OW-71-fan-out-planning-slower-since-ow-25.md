---
id: OW-71
title: "Planning a large fan-out got slower after OW-25"
type: bug
status: in-progress
depends_on: []
assignee: claude-perf
---

## Evidence

The ignored fan-out tests in `crates/controller/src/scale_tests.rs` plan more
slowly on `main` than at OW-25's commit `c0ab00f`. I built every commit that
touches `crates/controller` from `c0ab00f` to `4a85e59` in one scratch copy
and ran `cargo test -p weave-controller fan_out_to_a_thousand -- --ignored
--nocapture --test-threads=1` twice on each, one commit after another.
Debug build, line-table debuginfo, incremental off, Apple Silicon Mac with
other builds running. Each pair of runs agreed within 3 ms on plan time. The
time changed at three commits and nowhere else:

| Commit | Direct 1000 plan / tick | Relayed 1000 plan / tick |
|---|---|---|
| `c0ab00f` to `8227bbc` | 32-34 / 222-232 ms | 138-141 / 397-404 ms |
| `9260045` (OW-49) | 34 / 230 ms | 213 / 481 ms |
| `06e794b` (OW-57) | 34-35 / 229 ms | 278-279 / 539 ms |
| `d218b57` (OW-59) | 90-93 / 285-290 ms | 351-352 / 612-613 ms |

Three alternating runs of `c0ab00f` and `main` at load average 13 gave the
same numbers, so host load does not explain the difference.

`d218b57` added `let mut ports = ports.clone();` to `chain_hops`
(`crates/controller/src/path.rs`), so every destination copies the whole port
allocator. In a fan-out, the allocator holds an entry for every receiver
planned before it, so the copying grows with destinations squared. In the
direct fan-out, no chain needs a relay, so none of these copies is written to.
I did not time inside `9260045` or `06e794b`. Both changed
`PortAllocator::claim_at` and `claim_rist_at` from one loop into up to three
probe passes, and `06e794b` added a `yielding` pass to `can_claim`. I have not
checked whether those changes are why they are slower.

## Done when

- [ ] A timing breakdown says where the added plan time goes at each of the
      three commits.
- [ ] Direct and relayed 1000 plan no slower than at `8227bbc`, or this item's
      Log says why they cannot.

## Log

- 2026-09-26: filed by claude-perf from OW-69, with the numbers above.
- 2026-09-26: started by claude-perf.
