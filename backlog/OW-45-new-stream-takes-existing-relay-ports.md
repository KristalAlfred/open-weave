---
id: OW-45
title: "A new stream that sorts first can take an existing stream's relay ports"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] A test shows whether applying a new stream that sorts first moves an
      existing stream's bridge or its ports.
- [ ] If it does, the existing bridge keeps its relay and ports, and the new
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
