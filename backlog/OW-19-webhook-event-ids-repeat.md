---
id: OW-19
title: "Webhook event ids repeat after a controller restart"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`event_id` is `{id}-{seq}` with `seq` starting at 0 on every controller start
(`crates/controller/src/webhook.rs`), so a restarted controller sends ids it
has sent before. `README.md`, "Node lifecycle webhooks", invites receivers to
deduplicate by id, and a receiver that does can drop new events.

## Done when

- [ ] Event ids do not repeat across controller restarts.

## Log

- 2026-09-25: filed from research on OW-7.
