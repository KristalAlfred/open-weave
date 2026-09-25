---
id: OW-2
title: "SRT hops carry no encryption"
type: feature
status: in-progress
depends_on: []
assignee: claude-transport
---

## Evidence

Nothing in `crates/` sets an SRT passphrase or key length, so every planned SRT
hop runs in the clear. Contribution and distribution over the public internet
are the common case:

- ESPN sent every camera of a 2023 college game over SRT into AWS
  ([SVG](https://www.sportsvideo.org/2023/01/24/espn-dmed-pull-off-first-end-to-end-cloud-based-live-production-in-u-s-with-a-10-college-hoops-game/)).
- Vivid Broadcast produces up to six Women's Super League matches a weekend over
  the public internet
  ([Intinor, 2026, vendor](https://intinor.com/securing-remote-production-for-the-womens-super-league/)).

The controller plans both ends of every hop, so it can give both the same key.

## Done when

- [x] Each planned SRT hop carries a key, the same in both ends' desired hops.
- [x] The Strom adapter sets it on both ends.
- [x] A `remote` destination takes its key from the manifest.
- [ ] A bench caller with the wrong key is refused.

## Easy to break

- The key must stay out of `/view`, `/status`, logs, webhook events and every
  unauthenticated route.
- With one shared southbound token any node can read any node's desired hops, so
  the key is only as private as OW-3 makes it.

## Unchecked

- Whether Strom's SRT blocks expose GStreamer's `passphrase` property.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-transport.
- 2026-09-25: the Unchecked question is answered by research on a throwaway
  Strom container: Strom's SRT blocks have no passphrase property, and a key in
  `srt_uri` works for both blocks.
- 2026-09-25: on a throwaway `eyevinntechnology/strom:latest` (0.6.6, GStreamer
  1.26.6), not the bench: libsrt took passphrases of 10 and 80 bytes and refused
  9 and 81 ("failed to set passphrase"); ffmpeg in `linuxserver/ffmpeg` connected
  with 80 although its help says 10..64; `pbkeylen=32` on one end against 16 or
  none on the other connected with either end listening; a passphrase
  percent-encoded into the URI matched the same literal set as a property.
  Validation takes 10 to 80 bytes.
- 2026-09-25: ticked "Each planned SRT hop carries a key". Read as the links
  between nodes: each gets HMAC-SHA256 of `WEAVE_SRT_KEY_SECRET` over the id of
  the hop it feeds, with `pbkeylen: 32`, on both sockets
  (`crates/controller/src/keys.rs`, `plan_link` in `path.rs`). Terminal sockets
  take the manifest `passphrase` or stay in the clear. Unit tests in `path.rs`:
  `both_ends_of_a_link_share_a_key_no_other_link_has`,
  `a_reversed_link_keeps_its_key`,
  `adding_a_destination_leaves_existing_link_keys_alone`. `key_exposure_tests`
  in the controller's `main.rs` shows the keys in `/nodes/{id}/desired` and in
  none of `/view`, `/status`, `/streams/{name}/endpoints` or `/stream-plans`.
  Unit tests only.
- 2026-09-25: ticked "The Strom adapter sets it on both ends". Every element
  `uri` and block `srt_uri` carries `passphrase` and `pbkeylen`, and the latency
  too (`SrtUri` in `crates/strom/src/spec.rs`, test
  `every_flow_shape_puts_the_key_in_its_srt_uris`); a running flow whose key
  differs is recreated (`a_flow_whose_key_differs_is_recreated` in
  `provision.rs`). Unit tests; the URI form rests on the throwaway-container
  runs above.
- 2026-09-25: ticked "A `remote` destination takes its key from the manifest":
  `terminal_sockets_carry_the_manifest_passphrase_or_none` in `path.rs` covers
  the remote egress, the source ingress and the consumer socket. Unit tests only.
  `PROTOCOL_VERSION` is 5.
