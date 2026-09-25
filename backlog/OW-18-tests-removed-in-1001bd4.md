---
id: OW-18
title: "Tests removed with the stable-destinations change were not replaced"
type: verification
status: in-progress
depends_on: []
assignee: claude-tests
---

## Evidence

Commit `1001bd4` ("add stable destinations and topology-aware hop profiles")
removed most of the unit tests in five files, and nothing replaced them.
`#[test]` and `#[tokio::test]` counts before and after:

| File | Before | After |
|---|---|---|
| `crates/controller/src/main.rs` | 43 | 0 |
| `crates/controller/src/path.rs` | 65 | 7 |
| `crates/core/src/lib.rs` | 47 | 4 |
| `crates/core/src/validation.rs` | 9 | 0 |
| `crates/adapter-strom/src/main.rs` | 5 | 0 |

Among the removed controller tests: offline marking, `node.offline` emitted
once, condition transition times changing only with status, replanning off an
offline relay, and serving desired hops for an offline node. `path.rs` lost the
tests that put a relay between two dial-only NAT'd nodes, which `README.md`'s
Status section and the header of `bench/manifests/nat-relay.yaml` still cite.

## Done when

- [ ] Each removed test has been compared against current behaviour, and a
      test for every behaviour that still exists is back.
- [ ] Behaviour a removed test covered that no longer exists is listed in this
      item's Log.

## Easy to break

- Old tests encode old contracts. A test that fails against the current code
  is evidence of a regression or of a contract change; decide which before
  rewriting its assertions.

## Log

- 2026-09-25: filed from research on OW-1, OW-7, OW-8 and OW-6.
- 2026-09-25: started by claude-tests.
- 2026-09-25: `crates/core/src/lib.rs`: 41 tests back for the old 47. 40 are
  one-to-one ports to the v4 types (destination ids, `profile_id`, topology
  attachments); `data_plane_addr_rejects_a_misspelled_field` and
  `data_plane_addr_carries_signalling_bases_per_webrtc_transport` now test
  `NetworkAttachment` and `NetworkListeners`. The node config invariant test
  lost its missing-`default`-alias case, since there is no default alias.
  `capabilities_without_transports_read_as_srt_in_both_roles` and
  `devices_parse_from_a_list_and_are_omitted_when_empty` became one hop-profile
  test. Removed behaviour, not ported: the `data_plane` shorthand, reachability
  and `relay` flag (replaced by attachments); a pre-reachability registration
  hydrating (a stored row that no longer parses is dropped at boot by
  `decode_registrations` in `store.rs`); the bare-name `transports` offer and a
  role-less stored offer (replaced by `hop_profiles` with explicit roles); the
  SRT fallback for capabilities that declare no transports (a bare
  `NodeCapabilities` now offers nothing); `Signalling::set` (replaced by
  `NetworkListeners`, which has no setter). `crates/core/src/validation.rs`: 9
  of 9 back; the id-grammar test also covers destination and network ids. No
  ported test failed. Checked with `cargo test -p weave-core`, unit tests only.
