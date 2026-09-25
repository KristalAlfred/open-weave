---
id: OW-28
title: "One node's token cannot be revoked on its own"
type: feature
status: todo
depends_on: [OW-3]
assignee:
---

## Evidence

A node token is derived from `WEAVE_SOUTHBOUND_KEY` and the node id
(`crates/core/src/auth.rs`, `NodeKey`), and southbound and the controller keep
no list of issued tokens. A token stays valid for as long as the key does. The
only way to shut out one node that leaked its token, or left, is to rotate the
key, which invalidates every node's token and means re-provisioning all of them.
`README.md`, "Authentication", says so.

## Done when

- [ ] One node's token can be made invalid without changing any other node's.

## Log

- 2026-09-25: filed from OW-3, which left revocation out of scope.
