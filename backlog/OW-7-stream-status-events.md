---
id: OW-7
title: "Stream status reaches an application only by polling"
type: feature
status: done
depends_on: []
assignee: claude-lifecycle
---

## Evidence

The webhook carries `node.registered`, `node.online` and `node.offline` and
nothing about streams (`crates/core/src/webhook.rs`). The application that
decides what to route learns that a stream went `flowing` or `degraded` by
polling `/status` or `GET /streams`.

## Done when

- [x] Stream condition changes go out on the webhook with the stream name,
      generation and reason code.
- [x] `README.md`, "Node lifecycle webhooks", describes them.

## Easy to break

- The webhook is fire-and-forget with a bounded queue
  (`crates/controller/src/webhook.rs`). A slow receiver must not hold up
  reconciliation, and the controller answers no request by calling out.
- Condition reason codes are stable API values.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-lifecycle.
- 2026-09-25: box 1 checked with unit tests. One `stream.changed` event type.
  `changed_streams` in `crates/controller/src/main.rs` picks every stream whose
  (type, status, reason) triples, on the stream and on each destination, differ
  from the previous tick's, or that no earlier tick computed; the tick emits
  them through the existing queue after it drops its locks. The payload is
  `StreamSummary` in `crates/core/src/webhook.rs`: name, generation,
  observed_generation, status, and the stream's and each destination's
  conditions, with no nodes or addresses. `Event.node` became a flattened
  `subject`, so a node event's JSON is unchanged
  (`a_node_event_keeps_its_node_field`). Tests: `changed_streams` pure tests
  for first computation, placeholder, identical, detail-only, generation-only,
  status, reason-only, destination and added-destination cases; tick tests
  `the_first_tick_after_a_start_reports_every_stream`,
  `a_hop_status_change_reports_the_stream_once` and
  `a_tick_does_not_wait_on_an_unreachable_receiver`; webhook tests for the
  allowlist and the event JSON. Not run on the bench.
- 2026-09-25: box 2 checked by reading. The README section is now "Webhooks",
  since it covers streams too; bench/README.md follows. It has the event row,
  an example, the trigger rule and what an empty `WEAVE_WEBHOOK_EVENTS` now
  delivers. `contracts/json-schema/webhook-event.json` is new, generated from
  `webhook::Event` by `just contracts`.
- 2026-09-25: seen on `bench/` as well, with `just bench hook-sink` running:
  `producer-down` and `producer-up` on a flowing `basic` gave three
  `stream.changed` events, `awaiting_input`, then `degraded` (a reason-only
  change to `media_degraded`), then `flowing`. None carried an address.
