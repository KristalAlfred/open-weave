---
id: OW-49
title: "An SRT port can split the last free RIST pair in a shared range"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`PortAllocator::claim` in `crates/controller/src/path.rs` takes the first free
port from an FNV-preferred offset in the SRT listener's range. `claim_rist` needs
an even port and the next one both free (`PortRange::rist_pairs` in
`crates/core/src/lib.rs`). Ports are held per node, so where a node's SRT and
RIST ranges overlap an SRT claim can take one half of a free pair while other
ports are free, and a RIST receiver then finds no pair. OW-11 saw this in
`rist_tests`, and `README.md` ("Capabilities and topology") told operators to
give RIST its own range.

## Done when

- [x] An SRT claim takes half of a free RIST pair only when no other port in its
      range is free.
- [x] SRT ports on a node whose SRT and RIST ranges do not overlap are the ports
      the allocator gave before.
- [x] `README.md` no longer says to give RIST its own range.

## Log

- 2026-09-25: filed from claude-transport's OW-11 report; started by
  claude-security.
- 2026-09-25: `claim` first probes, in the same order as before, for a free port
  that `splits_rist_pair` clears: outside every RIST pair on the node, or the
  other half of a pair already broken. Only when none is free does it take the
  first free port, as before. Unit tests:
  `srt_ports_leave_rist_pairs_whole_while_other_ports_are_free` in `path.rs`
  (SRT 20000-20004 over RIST 20000-20003: claims preferring 20001, 20003 and
  20001 get 20004, 20003 and 20002, and a RIST pair is left at 20000) and
  `rist_and_srt_can_fill_a_range_they_share` in `rist_tests.rs` (three RIST
  receivers and their SRT consumer sockets fill a shared 9-port range). Both
  fail with the old probe; the second with `PortRangeExhausted`.
- 2026-09-25: a port outside every RIST range never splits a pair, so where the
  ranges do not overlap the probe is the old one and every existing port test,
  which asserts exact ports, passes unchanged. In an overlapping range an SRT
  port whose old pick split a free pair moves once on upgrade. There it also
  depends on whether a pair's other half is held, so freeing a port can move an
  SRT port that lies in a RIST pair, as freeing a port on its probe path already
  could. Unit tests only.
