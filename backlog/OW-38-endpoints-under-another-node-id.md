---
id: OW-38
title: "A node can publish endpoints under another node's id"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`register_node` and `node_heartbeat` in `crates/controller/src/main.rs` check
`endpoints[].node_id` with `validate_endpoint_node_ids`, which checks the id's
syntax only. A registration from `strom-node-1`, made with its own token, can
carry an endpoint whose `node_id` is `strom-node-2`, and `/endpoints` and
`/state` then list that endpoint as node 2's.

## Done when

- [x] Registration and heartbeat refuse, with `403 forbidden`, an endpoint whose
      `node_id` is not the id the caller's token belongs to.
- [x] With `WEAVE_AUTH_DISABLED=1` they accept any valid id, as before.

## Log

- 2026-09-25: filed from a security review of OW-3 by claude-security.
- 2026-09-25: started by claude-security.
- 2026-09-25: `register_node` and `node_heartbeat` in
  `crates/controller/src/main.rs` call `refuse_other_nodes_endpoints` after the
  syntax check, which answers `refuse_other_node`'s `403 forbidden` for the
  first `endpoints[].node_id` the caller may not act as. An endpoint with no
  `node_id` is accepted. Southbound forwards the node's own token, so the
  controller's check covers both surfaces. Unit tests in `node_auth_tests`:
  `a_node_publishes_endpoints_only_under_its_own_id` (strom-node-1's token gets
  403 for a registration and a heartbeat listing a strom-node-2 endpoint, the
  refused registration is not recorded and the refused heartbeat leaves
  `/endpoints` as it was; it fails without the check) and
  `without_auth_a_node_may_publish_endpoints_under_any_id` (202 for both with
  `NodeGuard::Disabled`). No contract change: both routes already document
  `403`. Unit tests only.
