---
id: OW-7
title: "Stream status reaches an application only by polling"
type: feature
status: todo
depends_on: []
assignee:
---

## Evidence

The webhook carries `node.registered`, `node.online` and `node.offline` and
nothing about streams (`crates/core/src/webhook.rs`). The application that
decides what to route learns that a stream went `flowing` or `degraded` by
polling `/status` or `GET /streams`.

## Done when

- [ ] Stream condition changes go out on the webhook with the stream name,
      generation and reason code.
- [ ] `README.md`, "Node lifecycle webhooks", describes them.

## Easy to break

- The webhook is fire-and-forget with a bounded queue
  (`crates/controller/src/webhook.rs`). A slow receiver must not hold up
  reconciliation, and the controller answers no request by calling out.
- Condition reason codes are stable API values.

## Log

- 2026-09-25: filed from broadcaster research.
