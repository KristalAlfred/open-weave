---
id: OW-57
title: "A first path moves or grows onto a running second path's relay"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

A read-only review of `crates/controller/src/path.rs` at `f5e2fdd` found that
first paths are planned without regard to running second paths. `chain_hops`
calls `relay_before` with no relays to avoid, and second paths are placed
after every first path.

- (a) A NAT pair with a relay per uplink network plus a third relay: path 1 on
  `relay-a`, path 2 on `relay-b`. When `relay-a` goes offline, path 1 moves
  onto `relay-b` and pushes the healthy path 2 to the third relay, or drops it
  with `single_path` when there is none.
- (b) Two-port relays with `studio` (`paths: 2`) on `relay-a` and `relay-b`.
  Adding a destination `preview` (`paths: 1`) took `relay-b`, and `studio`
  dropped to `single_path`.

`crates/controller/src/second_path_tests.rs` checks both on `main` at
`4eae947`, after OW-56:

- (a) fails: path 1 moves to `relay-b` rather than the free `relay-c`.
- (b) with a free third relay passes: OW-56's held ports already keep
  `preview` off the second path's relay.
- (b) with no free relay fails worse than the review saw. OW-56 holds the
  second path's ports for it, so `preview`'s first path finds no room and the
  whole stream goes unplaced (`PortRangeExhausted`).

## Done when

- [x] A first path that loses its relay takes a relay its running second path
      does not use when one has room.
- [x] A new destination takes a relay with free ports before a running second
      path's, and takes the second path's only rather than leave the stream
      unplaced.

## Log

- 2026-09-26: filed by claude-tests from the planner review, with the test
  results above.
- 2026-09-26: fixed in `path.rs`. For a destination with `paths: 2`,
  `derive_on` passes the nodes reporting its second-path bridges to
  `chain_hops` as relays to shun. `relay_with_ports` takes, in order: the relay
  already running the bridge; the lowest-id one not shunned; the lowest-id
  one; and only then one whose room is ports this stream's own running second
  paths hold. While a stream plans its first paths, those ports
  (`PortAllocator::yielding`, picked by `is_second_path_socket`) are a last
  resort for a claim. They are cleared before the second paths are planned and
  before the allocator is committed, so no other stream can take them.
- 2026-09-26: choice made against the review's suggestion. The review suggested
  reserving running second-path bridges ahead of new first paths. First paths
  are all-or-nothing and second paths are best effort (`single_path`), so when
  no relay has other room, a new first path in the same stream takes the
  second path's relay and the destination keeps one path. The stream is not
  left unplaced. Across streams the second path's ports stay held (OW-56).
- 2026-09-26: both boxes ticked on the four tests in `second_path_tests.rs`,
  unit tests only: (a) with and without a third relay, (b) with and without a
  free relay. `README.md` ("Redundant paths") updated.
