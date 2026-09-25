---
id: OW-5
title: "One destination has one path"
type: feature
status: done
depends_on: []
assignee: claude-planner
---

## Evidence

Nothing plans a second copy of a destination; a grep for redundancy, 2022-7 and
failover in `crates/` finds nothing. Broadcasters send the same feed over two
routes and merge them at the receiver: Eurovision sends SRT over the internet as
two streams combined with SMPTE 2022-7
([Panorama, 2026](https://www.panoramaaudiovisual.com/en/2026/01/22/nuevas-necesidades-distribucion-grandes-eventos-deportivos-eurovision-services/)).

Merging is the receiving node's job (2022-7, libsrt socket groups). Choosing two
paths that share no relay or network is routing.

## Done when

- [x] A destination can ask for two paths.
- [x] The planner places them over disjoint relays and attachments when the
      topology allows.
- [x] A hop profile declares that the receiver can merge.
- [x] A stream that gets only one path reports it.

## Easy to break

- Hop ids and ports are stable across manifest edits (`README.md`, "Hop status
  and fan-out"). The second path needs ids of its own without renumbering the
  first.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-planner.
- 2026-09-25: box 1. `StreamDestination.paths` (1 or 2, default 1, omitted
  when 1). `validate_stream` rejects any other value (`invalid_range`) and two
  paths on a `remote` destination or one that pins `via` (`not_allowed`).
  Rejecting `via` was my call; the research left it open, and allowing it later
  breaks no manifest. Tests: `destination_paths_default_to_one_and_serialize_only_when_two`
  (`crates/core/src/lib.rs`), `two_paths_need_a_receiver_and_no_via`
  (`crates/core/src/validation.rs`).
- 2026-09-25: box 2. `derive_stream` (`crates/controller/src/path.rs`) places
  every first path, claims the sender ingress, selects profiles, then places
  each second path on a copy of the port allocator. A second path uses no relay
  of the first and, at the sender and the receiver, no attachment the first may
  carry its link on; at a dialing end that is every dialing attachment on the
  network. Its branch id is `{destination}.2` and its bridges
  `weave-{stream}-bridge-{destination}.2-{position}`. Strom v0.6.8-2-gccb8593
  (`~/git/strom`, read only) accepts `.` in a flow name: `create_flow` in
  `backend/src/api/flows.rs` checks only that the trimmed name is 1 to 255
  characters, and the GStreamer pipeline is named `flow-{id}`
  (`backend/src/gst/pipeline/construction.rs`). Tests in
  `crates/controller/src/redundant_paths_tests.rs`:
  `two_paths_over_two_networks_share_no_attachment_at_either_end`,
  `a_nat_pair_gets_its_second_path_through_a_second_relay`,
  `a_shared_uplink_or_a_single_relay_leaves_one_path` (fails if a dialing end
  may reuse the first path's uplink; checked by breaking that check),
  `asking_for_a_second_path_leaves_the_first_as_it_was`,
  `a_second_path_that_fails_claims_no_port`.
- 2026-09-25: box 3. `HopProfile.merge`, plus `merge_ingress` on `DesiredHop`
  and `HopStatus`. A second path is placed only when the receiver has a merging
  profile whose ingress class matches both ingresses. A hop status without the
  desired merge ingress leaves the hop pending. Strom does not advertise
  `merge`, and `flow_spec_from_hop` refuses a merge ingress. OW-2 had already
  moved `PROTOCOL_VERSION` to 5 this session, so there is no second bump; the
  README protocol paragraph names both changes. Tests: `a_receiver_that_cannot_merge_gets_one_path`,
  `hop_profile_merge_defaults_off_and_is_omitted_when_off`,
  `a_merge_ingress_is_reported_exactly_when_desired`,
  `a_merge_ingress_is_an_error` (`crates/strom/src/spec.rs`).
- 2026-09-25: box 4. New reason `single_path` on `placement_ready`, whose
  status stays `true`, on the stream and on the destination, with a detail
  naming the destination and the placement error. This is one new reason value
  and no new condition type, so the five condition types stay as they are.
  `last_transition_time` does not move when only the reason changes;
  `stream.changed` does fire. `POST /stream-plans` returns the detail as
  `reason`. A destination's status rolls up both paths, so a second path that
  is down reads `degraded`; a single-path destination's status is unchanged.
  Tests: `a_destination_with_one_path_of_two_says_so_in_its_placement_condition`,
  `a_plan_with_one_path_of_two_gives_the_reason`,
  `a_destination_rolls_up_both_of_its_paths_and_no_other`.
- 2026-09-25: every box rests on planner and status unit tests. No shipped node
  merges, so nothing ran on `bench/`.
