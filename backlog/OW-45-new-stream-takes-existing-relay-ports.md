---
id: OW-45
title: "A new stream that sorts first can take an existing stream's relay ports"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

`reconcile` (`crates/controller/src/main.rs`) plans streams in name order
(`streams.sort_by(|a, b| a.name.cmp(&b.name))`) against one shared port
allocator. OW-42 made one stream plan the destinations a node already carries
before new ones, so a new destination does not take an existing bridge's relay
or ports. Nothing does the same across streams. A new stream whose name sorts
before an existing relayed stream, and that needs the same relay, claims that
relay's ports first. When the relay's range then has too few free ports,
`relay_with_ports` passes it over for the existing stream's bridge, which moves
to the next relay and interrupts its media. This is the same mechanism as the
destination case covered by
`a_new_destination_takes_the_next_relay_and_leaves_an_existing_bridge_alone`
(`crates/controller/src/relay_choice_tests.rs`). Found by reading the code
while working OW-42; not reproduced across streams.

## Done when

- [x] A test shows whether applying a new stream that sorts first moves an
      existing stream's bridge or its ports.
- [x] If it does, the existing bridge keeps its relay and ports, and the new
      stream takes the next relay.

## Easy to break

- Hop id collisions are resolved in name order: the earlier name keeps its
  hops (`README.md`, "Hop status and fan-out"). Changing the stream order
  changes which stream a collision leaves unplaced unless that rule is kept
  apart.
- Plans must allocate against the full candidate stream set, and a preview
  must match the next reconcile (`BACKLOG.md`, "Scope guards").

## Log

- 2026-09-25: filed by claude-tests from OW-42.
- 2026-09-25: started by claude-tests.
- 2026-09-25: box 1.
  `a_new_stream_that_sorts_first_takes_the_next_relay_and_leaves_an_existing_one_alone`
  (`crates/controller/src/relay_choice_tests.rs`) runs `reconcile`
  with two-port relays. Stream `feed` runs through `relay-a`, and a new stream
  `alpha` also needs a relay. Before the fix, `feed`'s bridge moved to `relay-b`
  and the test failed on `feed`'s hops.
- 2026-09-25: box 2. `reconcile` in `crates/controller/src/main.rs` now plans the
  streams whose sender hop a node reports running, in a report not `failed`,
  before the rest, and each group in name order. The input is the hop reports
  planning already receives; nothing is kept between ticks. The port
  allocator is still shared across the whole candidate set, and
  `POST /stream-plans` goes through the same `reconcile`. Output order is
  unchanged: statuses are sorted by name, and each node's desired hops are
  filled in stream name order after planning, so desired snapshot revisions
  only change when hops do. The test checks that `feed`'s hops (ports and link
  keys included) equal those it planned alone, that `alpha` is on `relay-b`,
  and that statuses and `source`'s desired hops are in name order. With
  nothing reported, the earlier name still takes the first relay.
- 2026-09-25: the new order changes the hop-id collision rule in the Easy to
  break note above. When two stored streams collide, the one a node reports
  running keeps its hop ids and the other stays unplaced; with no reports,
  name order decides, as before.
  `a_running_stream_keeps_its_hop_ids_against_an_earlier_name`
  (`hop_id_tests.rs`) covers it. Since OW-32, such pairs can only come from
  streams stored before that check. `README.md` ("Hop status and fan-out",
  "Capabilities and topology") updated. Unit tests only.
- 2026-09-26: OW-56 replaced this item's planning order with ports held
  from hop reports and a fixed planning order; see there.
