---
id: OW-21
title: "Hop ids can collide across streams on one node"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Sender and receiver hop ids are built by joining names with `-`
(`crates/controller/src/path.rs`), so `sender_hop_id("x-receiver-a")` equals
`receiver_hop_id("x", "a-sender")`. Two streams on one node can then plan the
same hop id. Found by reading the code; not reproduced.

## Done when

- [ ] A test shows whether two valid streams can plan the same hop id on one
      node.
- [ ] If they can, they no longer can, and hop ids of existing streams stay
      stable (`README.md`, "Hop status and fan-out").

## Log

- 2026-09-25: filed from research on OW-5.
