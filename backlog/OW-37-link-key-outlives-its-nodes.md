---
id: OW-37
title: "A node that leaves a link keeps its key"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`LinkKeys::link` in `crates/controller/src/keys.rs` derives a link key from
`WEAVE_SRT_KEY_SECRET` and the id of the hop the link feeds. Hop ids are built
from the stream and destination names (`receiver_hop_id` and `bridge_hop_id` in
`crates/controller/src/path.rs`), so a link keeps its key when its ends change: a
`via` relay swapped, a decommissioned node replaced, a stream re-applied on other
nodes. The node that left still holds a key that is valid for that hop id until
the secret rotates.

## Done when

- [ ] A link's key changes when either end's node changes, and stays when the
      link reverses direction.
- [ ] Both ends of a link still get the same key.

## Easy to break

- With controller HA (OW-9) every controller must derive the same key from the
  shared secret.

## Log

- 2026-09-25: filed from a security review of OW-2 by claude-security.
