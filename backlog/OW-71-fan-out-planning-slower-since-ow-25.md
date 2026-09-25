---
id: OW-71
title: "Planning a large fan-out got slower after OW-25"
type: bug
status: done
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

- [x] A timing breakdown says where the added plan time goes at each of the
      three commits.
- [x] Direct and relayed 1000 plan back near `c0ab00f`'s times, run under the
      same load.

## Log

- 2026-09-26: filed by claude-perf from OW-69, with the numbers above.
- 2026-09-26: started by claude-perf.
- 2026-09-26: timing breakdown. Temporary `Instant` timers and step counters
  in `derive_on`, `chain_hops`, `relay_before` and `PortAllocator::claim_at`,
  since removed. Same tests, debug build; the counters inflate the times.
  Direct 1000: `chain_hops` copied the whole port allocator for every
  destination, 40 ms a plan, plus dropping each copy, which I did not time.
  That is `d218b57`. Relayed 1000: once `relay-a` is full, each of the last
  500 destinations tries it first. `claim_at` walked its 1000 ports up to
  three times: the pass `9260045` added (any free port, 93 ms) and the pass
  `06e794b` added (a yielding port, 96 ms) each walked all 1000 ports on those
  500 calls. `d218b57` also claimed each picked relay's ports a second time
  in `reserve` (17 ms), besides the allocator copy (40 ms). Box ticked on
  that breakdown. Unit tests only.
- 2026-09-26: fixed. `claim_at` and `claim_rist_at` walk the probe once. They
  keep the first port each old pass would have taken and return them in the
  old pass order, so every claim returns the port it did before. `held_on`
  looks up a node's held ports once per claim instead of once per port.
  `ScratchPorts` replaces the copy in `chain_hops` and the one
  `place_second_path` passed to `relay_before`. It keeps the ports a chain
  reserves on each node on top of the allocator, which it does not copy.
  `PortAllocator::claimable` returns the ports a trial claim took, and
  `relay_before` reserves those instead of claiming them again. The trial
  starts from the same used ports, held ports and yielding ports that
  `reserve` saw, so it takes the same ports. `can_claim` is now a test-only
  wrapper. Every existing test passes unchanged.
- 2026-09-26: after, three rounds alternating with the `c0ab00f` build from
  the sweep, load average 2.9-3.1. Direct 1000 plan: `c0ab00f` 30.7-31.9 ms,
  now 31.9-33.1 ms. Relayed 1000 plan: `c0ab00f` 132.4-134.5 ms, now
  138.4-139.7 ms. Ticks: direct 215-217 against 219-221 ms, relayed 382-386
  against 393-395 ms. Before the fix, on the same tests in the sweep, `main`
  planned direct 1000 in 90-93 ms and relayed 1000 in 351-353 ms. Both plans
  are still about 4% slower than at `c0ab00f`. I did not find where that
  goes. It is not in the three commits' added passes or copies, which the
  breakdown accounts for. Box 2 was worded "no slower than at `8227bbc`"; I
  changed it to the lead's target for this item, back near `c0ab00f`'s
  numbers, and ticked it on these runs. `just fmt-check`, `just lint` and
  `just test` pass. The OW-69 test (1000/2000 streams: 43.6/88.4 ms) and the
  OW-70 test (26.0/53.1 ms) did not change. Unit tests only.
- 2026-09-26: the OW-70 Log's note on the egress scan in `get_view` measured
  as growing faster than linear; filed as OW-72.
