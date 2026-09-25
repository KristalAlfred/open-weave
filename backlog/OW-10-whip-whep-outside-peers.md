---
id: OW-10
title: "WHIP and WHEP for outside peers"
type: feature
status: done
depends_on: []
assignee: claude-planner
---

## Evidence

WHIP and WHEP exist only as link transports the planner picks for browser
nodes. A manifest has no `whip:` or `whep:` variant for an encoder or player
open-weave does not manage.

- WHIP is [RFC 9725](https://www.rfc-editor.org/rfc/rfc9725.html) (March 2025).
  OBS has sent it since version 30, and Haivision added it to the Makito X4 in
  2025 (vendor).
- WHEP is still a draft (`draft-ietf-wish-whep-04`, June 2026).
- Broadcast use is mostly low-latency return and browser monitoring beside SRT
  contribution
  ([Dolby case study, 2024](https://optiview.dolby.com/resources/customer-stories/new-remote-production-hub-in-uk-revolutionizes-remote-broadcasting/)).

## Done when

- [x] A manifest can take a source from an outside WHIP sender.
- [x] A manifest can deliver a destination to an outside WHEP player.
- [x] `GET /streams/{name}/endpoints` returns the URLs.
- [x] An outside WHIP sender whose declared `format` the node's WHIP ingest
      cannot take is reported as a format mismatch.

## Easy to break

- Strom's `whip_input` accepts H264 only and one session per endpoint (OW-13,
  OW-15). An outside sender with other codecs is a format mismatch to report.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-planner.
- 2026-09-25: added the fourth box. The lead decided that an outside sender
  with a declared `format` Strom cannot take is a format mismatch reported as
  mismatches are today; it was only under Easy to break.
- 2026-09-25: boxes 1 and 2. New manifest variants `whip: { node, network?,
  format? }` (source only) and `whep: { node, network?, accepts? }`
  (destination only), `SignallingEndpoint` in `crates/core/src/lib.rs`.
  `validate_stream` rejects `whip` on a destination (`source_only`) and `whep`
  on the source (`destination_only`). The planner puts the sender's ingress, or
  the receiver's last egress, at the named node's `whip` or `whep` base, taking
  the listener in the order OW-33 set for SRT producers and consumers (lowest
  network, then attachment id, within a pinned network), with the hop id as
  endpoint id; `NoSignalling` when the node declares no base. Strom's existing
  `whip-to-srt` and `srt-to-whep` profiles take the hops, so WHIP in and WHEP
  out on one node are two hops joined over SRT. Tests in
  `crates/controller/src/outside_peer_tests.rs` (six) and
  `whip_is_a_source_and_whep_a_destination`,
  `whip_and_whep_fields_are_validated`,
  `a_whip_source_parses_from_its_manifest_tag` (`crates/core/src/validation.rs`),
  `a_whep_player_that_cannot_accept_a_whip_senders_format_is_reported`
  (`crates/core/src/lib.rs`). Unit tests only.
- 2026-09-25: box 3. `EndpointAddr.host` and `port` are now optional and
  absent for a WHIP or WHEP endpoint, whose `url` is the signalling URL; SRT
  endpoints serialize as before. `a_placed_stream_reports_the_urls_outside_peers_call`
  checks the reconcile outcome that `GET /streams/{name}/endpoints` serves.
  Unit tests only.
- 2026-09-25: no bench run. The bench's only WHIP sender is Playwright
  Chromium, and the OW-10 research (throwaway Strom 0.6.6 and 0.6.10, not the
  bench) found its VP8 offer breaks Strom's `whip_input` session and an
  audio-only sender leaves a 0.6.6 gateway flow paused (OW-13, OW-14). A media
  check would fail for those reasons, not for this change.
- 2026-09-25: box 4. `HopProfile.accepts` (optional, same shape as a
  destination's `accepts`, checked by `validate_node`). Strom's `whip-to-srt`
  declares video codec `h264` and audio codec `opus`, which is what `whip_input`
  sets in the `audio_video` mode the gateway flow uses (`whip.rs` in Strom
  v0.6.6 and `~/git/strom`, read only). When the sender's selected profile
  declares `accepts` and the source's `format` falls outside it,
  `format_compatible` is `false` with `format_mismatch`, naming node and
  profile, and the stream reads `degraded`, as destination mismatches do. The
  constraint names both tracks, so a declared format without video or without
  audio is reported too. `VideoCodec` gains `vp8` so a WebRTC sender's usual
  codec can be declared. No `PROTOCOL_VERSION` change; it is 5 for the session.
  Tests: `a_whip_sender_declaring_a_codec_the_ingest_cannot_take_is_a_format_mismatch`,
  `a_whip_sender_declaring_what_the_ingest_takes_is_compatible`
  (`outside_peer_tests.rs`, both fail with the check switched off),
  `only_the_whip_gateway_constrains_its_ingress` (adapter),
  `a_hop_profile_constraint_follows_the_accepts_rules` (core). Unit tests only.
