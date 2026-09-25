---
id: OW-38
title: "A node can publish endpoints under another node's id"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`register_node` and `node_heartbeat` in `crates/controller/src/main.rs` check
`endpoints[].node_id` with `validate_endpoint_node_ids`, which checks the id's
syntax only. A registration from `strom-node-1`, made with its own token, can
carry an endpoint whose `node_id` is `strom-node-2`, and `/endpoints` and
`/state` then list that endpoint as node 2's.

## Done when

- [ ] Registration and heartbeat refuse, with `403 forbidden`, an endpoint whose
      `node_id` is not the id the caller's token belongs to.
- [ ] With `WEAVE_AUTH_DISABLED=1` they accept any valid id, as before.

## Log

- 2026-09-25: filed from a security review of OW-3 by claude-security.
