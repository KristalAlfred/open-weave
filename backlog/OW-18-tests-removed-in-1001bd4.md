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
- 2026-09-25: claude-lifecycle restored these `crates/controller/src/main.rs`
  tests with OW-1: `desired_reflects_computed_hops_after_a_reconcile_tick`,
  `status_distinguishes_current_and_observed_generations`,
  `condition_transition_time_changes_only_when_status_changes`,
  `hydration_drops_persisted_nodes_with_invalid_ids`,
  `heartbeat_updates_memory_but_never_the_store`,
  `mark_offline_marks_stale_preserves_fresh_and_spares_exact_ttl`,
  `mark_offline_reports_only_the_nodes_it_transitioned`,
  `registering_a_node_emits_node_registered`,
  `a_node_past_its_ttl_emits_node_offline_once`,
  `a_heartbeat_from_an_offline_node_emits_node_online`,
  `a_heartbeat_from_a_live_node_emits_nothing`,
  `registration_is_accepted_while_the_receiver_refuses_connections`,
  `reconcile_degrades_stream_when_a_hop_node_is_offline`,
  `reconcile_replans_a_stream_off_an_offline_relay` and
  `tick_marks_offline_node_but_still_serves_its_desired_hops`. Helpers back:
  `open_router`, `mem_state`, `node_registration`, `nat_registration`,
  `stream`, `stored_stream`, `send`, `send_with_headers`, `response_etag`,
  `webhook_state`. Not back: `guarded_router`, `send_auth`, the token
  constants, and `relay_registration`, since a relay is now any node with a
  matching hop profile. Adapted to current types: the receiver id is
  `weave-basic-receiver-studio`, and a NAT'd node is a dial-only `internet`
  attachment plus a site listener. The relay test also checks that `relay-a`
  carries the stream while it is online. All 15 pass on the current code.
- 2026-09-25: `crates/adapter-strom/src/main.rs`: 2 of 5 back
  (`hop_status_reports_each_fanout_branch_independently`,
  `reconcile_deletes_before_creating_on_same_ports`), ported to `profile_id`
  and destination branch ids. The other 3 tested removed behaviour: the adapter
  expanded `strom.signalling_base` into Strom's `/whip` and `/whep` routes and
  advertised signalling only for offered WebRTC transports. Now the operator
  writes each `base_url` on a topology attachment, `registration` copies the
  topology verbatim, and Strom always advertises the three fixed hop profiles.
  The Evidence table misses `crates/adapter-strom/src/config.rs`, which
  `1001bd4` also took from 9 tests to 0: 5 are back; the other 4 tested removed
  behaviour (`signalling_base` parsing, its alias check and default, the node
  `transports` list, and the required `default` data-plane alias). No ported
  test failed. Checked with `cargo test -p weave-adapter-strom`, unit tests
  only.
- 2026-09-25: `crates/controller/src/path.rs`: 64 of 65 back in a `tests`
  module beside `contract_tests`, ported to topology attachments and
  destination ids. A dialable node is an `internet` attachment with an SRT
  listener; a NAT'd node is a dial-only `internet` attachment plus a listener on
  its own site network, so each NAT'd node is its own routing domain. The
  NAT-pair tests are back: `outbound_only_pair_relays_through_a_node_both_dial`,
  `outbound_only_pair_without_a_relay_is_unplaceable`,
  `an_outbound_only_relay_is_never_chosen`, the three relay-choice tests,
  `an_offline_relay_alone_leaves_the_pair_unplaceable`,
  `a_pinned_via_still_gets_a_relay_when_its_own_link_is_undialable` and
  `fanout_relays_only_the_destination_that_needs_it`. Renamed where the old
  name was a removed concept: alias became network, port range became SRT
  listener, signalling base became signalling listener. Three contract changes
  show in the ports. A WebRTC link whose host declares no WHIP or WHEP listener
  now fails as `NoRelayAvailable` instead of `NoSignalling`; `NoSignalling` is
  no longer produced by `derive_path`, since `has_listener` and
  `NetworkListeners::signalling` read the same field. Two browsers no longer
  bridge through a relay carrying only Strom's profiles (OW-16); the ported
  test checks that, then adds a `whip-to-whep` profile no shipped adapter
  advertises and checks the planner bridges them. `srt_only_endpoints_json_shape_is_unchanged`
  now checks the `destinations` list that replaced `outputs`. Removed
  behaviour: `via_pins_a_node_that_need_not_advertise_as_a_relay`, since there
  is no `relay` flag and every hop, pinned or not, needs a matching hop profile
  in `select_profile`. No ported test failed. Checked with
  `cargo test -p weave-controller path::tests`, unit tests only.
- 2026-09-25: `crates/controller/src/main.rs` not started by claude-tests: it
  waits on OW-1 (`in-progress`) and OW-8 (`todo`). At `337cf40`, 28 of its 43
  removed tests are still missing. Auth: `api_routes_reject_missing_and_wrong_tokens`,
  `dashboard_and_health_stay_open`, `each_surface_accepts_its_own_token`,
  `each_surface_rejects_the_other_surfaces_token`,
  `node_inventory_accepts_either_surface_token`. Streams and stream sets:
  `post_stream_then_get_returns_it_and_writes_through`,
  `delete_stream_removes_and_writes_through`,
  `invalid_stream_is_rejected_before_persistence`,
  `stream_writes_require_and_enforce_etag_preconditions`,
  `owned_streams_reject_single_resource_mutations`,
  `stream_set_apply_retains_noops_and_prunes_atomically`,
  `stream_set_conflicts_do_not_partially_apply`,
  `hydration_refuses_invalid_persisted_streams`. Plans:
  `plan_allocates_ports_alongside_existing_streams`,
  `plan_distinguishes_unplaced_and_disabled_streams`,
  `plan_places_without_changing_desired_state`. Registration:
  `registration_with_an_incompatible_protocol_version_is_rejected`,
  `registration_with_an_invalid_node_id_is_rejected`,
  `node_cannot_report_another_nodes_hop_status`,
  `a_browser_endpoint_is_stored_verbatim_and_never_dialled`. Views:
  `endpoints_route_pending_then_placed`,
  `reconcile_reports_why_a_stream_is_pending`,
  `view_before_first_tick_has_no_report`,
  `view_joins_desired_hops_with_reported_status`. HTTP:
  `invalid_json_has_a_structured_error`, `invalid_resource_paths_are_rejected`,
  `ui_is_served_at_root_and_ui`, `versioned_api_paths_are_not_served`.
- 2026-09-25: `crates/controller/src/main.rs`, streams and stream sets: 8 of 8
  back (`post_stream_then_get_returns_it_and_writes_through`,
  `delete_stream_removes_and_writes_through`,
  `invalid_stream_is_rejected_before_persistence`,
  `stream_writes_require_and_enforce_etag_preconditions`,
  `owned_streams_reject_single_resource_mutations`,
  `stream_set_apply_retains_noops_and_prunes_atomically`,
  `stream_set_conflicts_do_not_partially_apply`,
  `hydration_refuses_invalid_persisted_streams`). The only change is a
  `network` on the invalid stream's remote. All pass; `cargo test -p
  weave-controller`, unit tests only.
- 2026-09-25: `crates/controller/src/main.rs`, plans: 3 of 3 back
  (`plan_places_without_changing_desired_state`,
  `plan_distinguishes_unplaced_and_disabled_streams`,
  `plan_allocates_ports_alongside_existing_streams`). The old port test gave
  each node one port, so the preview was unplaceable with or without the
  existing stream. The ported test sizes node 2 for one receiver, first checks
  the preview alone is `placed`, and then that it is `unplaced` with "no free
  port" once `existing` is applied. All pass; unit tests only.
- 2026-09-25: `crates/controller/src/main.rs`, registration: 4 of 4 back
  (`registration_with_an_incompatible_protocol_version_is_rejected`,
  `registration_with_an_invalid_node_id_is_rejected`,
  `node_cannot_report_another_nodes_hop_status`,
  `a_browser_endpoint_is_stored_verbatim_and_never_dialled`). The browser test
  now registers the page with a `camera-to-whip` profile and a dial-only
  attachment, and the Strom with a `whip-to-srt` profile and a WHIP listener.
  All pass; unit tests only.
- 2026-09-25: `crates/controller/src/main.rs`, views: 4 of 4 back
  (`endpoints_route_pending_then_placed`,
  `reconcile_reports_why_a_stream_is_pending`,
  `view_before_first_tick_has_no_report`,
  `view_joins_desired_hops_with_reported_status`). The joined-view test's two
  destinations are now `preview` and `studio` rather than a duplicated entry,
  and the sender's egresses are asserted by those branch ids. All pass; unit
  tests only.
- 2026-09-25: `crates/controller/src/main.rs`, HTTP: 4 of 4 back
  (`invalid_json_has_a_structured_error`, `invalid_resource_paths_are_rejected`,
  `ui_is_served_at_root_and_ui`, `versioned_api_paths_are_not_served`), with
  the route lists the last one walks. No changes to their assertions. All
  pass; unit tests only.
