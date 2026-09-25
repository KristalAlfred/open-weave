---
id: OW-37
title: "A node that leaves a link keeps its key"
type: bug
status: done
depends_on: []
assignee: claude-security
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

- [x] A link's key changes when either end's node changes, and stays when the
      link reverses direction.
- [x] Both ends of a link still get the same key.

## Easy to break

- With controller HA (OW-9) every controller must derive the same key from the
  shared secret.

## Log

- 2026-09-25: filed from a security review of OW-2 by claude-security.
- 2026-09-25: started by claude-security.
- 2026-09-25: `LinkKeys::link` in `crates/controller/src/keys.rs` now takes the
  hop id and both end node ids, sorts the ids, and MACs the hop id and the two
  ids, each followed by a NUL. `plan_link` in `path.rs` passes the upstream and
  downstream stations' node ids, derives the key once, and puts the same
  `SrtParams` on both sockets. Unit tests:
  `a_link_key_changes_with_either_end_node_but_not_their_order` in `keys.rs`;
  `a_link_rekeys_when_either_end_moves_to_another_node` in `path.rs` (a `via`
  swapped from `relay-a` to `relay-b` rekeys the links into and out of the
  relay, a destination moved to another node rekeys its link, and both ends of
  each link still agree; it fails with the hop-id-only derivation);
  `a_reversed_link_keeps_its_key` and
  `both_ends_of_a_link_share_a_key_no_other_link_has` still pass.
- 2026-09-25: every existing link rekeys once when the controller is upgraded,
  and adapters rebuild those flows (`a_flow_whose_key_differs_is_recreated` in
  `crates/adapter-strom/src/provision.rs`); `README.md` says so. The key is
  still a function of the secret and the plan only, so controllers sharing
  `WEAVE_SRT_KEY_SECRET` (OW-9) derive the same keys. Unit tests only; not run on
  `bench/`.
