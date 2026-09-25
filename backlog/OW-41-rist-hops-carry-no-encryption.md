---
id: OW-41
title: "RIST hops carry no encryption"
type: feature
status: done
depends_on: []
assignee: claude-security
---

## Evidence

OW-2 keys every SRT link between nodes. A link the planner puts on RIST
(OW-11) carries no key: `RistSocket` in `crates/core/src/lib.rs` has no
parameters, and the Strom adapter builds RIST hops from GStreamer's `ristsrc`
and `ristsink`, which have no passphrase or PSK property (`gst-inspect-1.0` in
`eyevinntechnology/strom:latest`, gst-plugins-bad 1.26.5). RIST main profile
defines encryption; the simple profile these elements implement does not.

## Done when

- [x] A RIST link between nodes is encrypted, or the planner refuses to put a
      link on RIST unless the stream allows it in the clear.

## Unchecked

- Whether any GStreamer or Strom element a node could use speaks RIST main
  profile with a PSK.

## Log

- 2026-09-25: filed from work on OW-11.
- 2026-09-25: started by claude-security. The user chose the second half of
  the box: a stream-level opt-in for links in the clear.
- 2026-09-25: the field is `allow_cleartext_links` (bool, default `false`,
  omitted when false, as a hop profile's `merge` is), snake_case like the other
  manifest fields; it names links because terminal sockets are already keyed
  by each endpoint's `passphrase`. `derive_stream` in
  `crates/controller/src/path.rs` plans a stream without it against the nodes
  with their RIST listeners and RIST hop profiles removed. If that fails and
  planning with RIST would succeed, the error is `PlacementError::CleartextLink`,
  naming the node the RIST link runs into. `placement_ready` then reads `false`
  with a new stable reason, `cleartext_not_allowed`
  (`StreamConditionReason` in `crates/core/src/api.rs`), since
  `placement_failed` does not tell an application what to change. A stream that
  RIST could not place either keeps its own error. With the field, planning is
  what it was. SRT key derivation does not read the field. Validation needed no
  new rule: the field is a bool, and `deny_unknown_fields` accepts it once it
  exists. `StreamDefinition`'s `PartialEq` compares it, so changing it is not a
  semantic no-op.
- 2026-09-25: unit tests in `rist_tests.rs`:
  `a_link_only_rist_can_carry_is_not_planned_unless_the_stream_allows_cleartext`
  (`CleartextLink { node: "studio" }` from the planner; through `reconcile`, a
  `pending` stream with `cleartext_not_allowed`, a detail naming the field, and
  no hops), `allowing_cleartext_leaves_srt_links_keyed` (the SRT link carries a
  key and `pbkeylen: 32` either way), and `the_sender_never_listens_for_rist`
  now checks both settings keep `NoRelayAvailable`. The existing RIST tests set
  the field. `cleartext_links_are_off_unless_a_stream_allows_them` in
  `weave-core` covers the default, omission and equality. Locally, not on
  `bench/`: a controller with strom-node-1 and strom-node-2 registered as the
  bench `rist` topology declares them placed `bench/manifests/rist.yaml` (now
  setting the field) with a RIST link from node 1 to node 2, and left
  `bench/manifests/basic.yaml` `pending` with `cleartext_not_allowed`.
  `just contracts` regenerated.
- 2026-09-25: bench (`bench/`, this change on f5e2fdd, before b8a0808 moved
  the bench to Strom 0.6.10): `just bench up`, `just bench topology rist`,
  `just bench stream-up rist` read `flowing`, with strom-node-2's desired
  `weave-rist-receiver-output` taking a RIST ingress on port 21948. `just bench
  stream basic` under the same topology read `pending`, `placement_ready`
  `false` with reason `cleartext_not_allowed` and the detail "the link into node
  strom-node-2 can only go over RIST, which carries no encryption; set
  allow_cleartext_links to plan it".
