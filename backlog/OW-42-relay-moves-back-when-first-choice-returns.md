---
id: OW-42
title: "A relayed stream moves back to its first relay as soon as it returns"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

`pick_relay` (`crates/controller/src/path.rs`) takes the first online node, in
node order, that can bridge both ends. Nothing prefers the relay a stream is
already using. On the bench on 2026-09-25, with `nat-transit` flowing through
strom-node-1, stopping adapter-1 moved the bridge to strom-node-2 and the stream
was `flowing` again in about 30 s. Starting adapter-1 again moved the bridge
back to strom-node-1 within about 20 s, and node 2's flow was removed. Each move
interrupted media, including the second one, where the path through node 2 was
working.

## Done when

- [x] A stream whose relay is online and working keeps it when a node earlier
      in the order comes back.
- [x] A stream still moves off a relay that goes offline.

## Easy to break

- Planning is side-effect free and allocates against the full candidate stream
  set (`BACKLOG.md`, "Scope guards"). Preferring the current relay needs the
  current placement as an input to planning, not state the planner keeps.
- A `via` pin still wins over any preference.

## Log

- 2026-09-25: filed from the OW-6 bench run.
- 2026-09-25: started by claude-tests.
- 2026-09-25: the current placement comes from the hop reports planning
  already receives. `derive_stream` in `crates/controller/src/path.rs` used to
  ignore its `observed` argument. It now reads which node reports running each
  hop, leaving out reports whose state is `failed`, and the planner keeps no
  state between ticks. When a bridge needs a relay (`pick_relay`, and
  `pick_remote_relay` for a remote destination), a node that reports running
  that bridge's id is taken first if it still qualifies: online, both halves
  reachable, a matching profile, and free ports. Otherwise the lowest id wins,
  as before. `via` pins never go through the relay pickers, so a pin still
  wins.
- 2026-09-25: rule chosen for "working": the relay's report of the bridge hop
  is anything but `failed`, so `provisioned` and `pending` keep it. Link
  conditions (`flowing`, `connecting`, `stalled`) are not used. A bridge's
  conditions follow its upstream, so a source that stops sending would
  otherwise move every relay it feeds. An offline relay is never a candidate,
  so its stale report does not keep a bridge.
- 2026-09-25: the lead added a case. A new destination must not take an
  existing bridge's relay or ports. `derive_stream` now plans the
  destinations whose receiver or first bridge some node reports running before
  new ones, and puts first paths, then second paths, back in destination id
  order in the sender's egresses and the hop list. Hop ids and output order are
  unchanged. `POST /stream-plans` no longer drops hop reports, so a preview
  keeps the same relays and ports as the next reconcile. `README.md` ("HTTP API",
  "Capabilities and topology") updated.
- 2026-09-25: boxes ticked on unit tests in `relay_choice_tests.rs`:
  - `a_bridge_stays_on_its_relay_when_an_earlier_relay_comes_back`: the whole
    path is unchanged with a `provisioned` or `pending` report, and goes to
    `relay-a` with none.
  - `a_bridge_moves_off_a_relay_that_goes_offline`.
  - `a_relay_that_reports_the_bridge_failed_does_not_keep_it`.
  - `a_via_pin_wins_over_the_relay_running_the_bridge`.
  - `a_new_destination_takes_the_next_relay_and_leaves_an_existing_bridge_alone`:
    two-port relays; the existing bridge and receiver hops are equal, ports
    included, the new destination is on `relay-b`, and egresses and hops are in
    id order.

  Also `a_plan_keeps_a_bridge_on_the_relay_that_reports_running_it` in
  `main.rs`. Against the planner without this change, the first and fifth
  fail and the others pass. No bench run: the bench lock was held by
  claude-ha.
- 2026-09-25: not covered here: a bridge's relay is still lost when the
  controller restarts, since the first tick sees only the hop reports stored at
  each node's registration. Between a bridge moving and its new relay's first
  report (one adapter poll), an earlier relay coming back still takes it.
  Streams, as opposed to destinations, are still planned in name order, so a
  new stream that sorts first can still take a relay's last ports from an
  existing stream.
- 2026-09-26: OW-56 replaced this item's planning order with ports held
  from hop reports and a fixed planning order; see there.
