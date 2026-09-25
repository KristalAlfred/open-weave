---
id: OW-11
title: "RIST as a transport"
type: feature
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

The planner knows SRT, WHIP and WHEP (`TRANSPORT_PREFERENCE` in
`crates/controller/src/path.rs`). RIST is the other standard contribution
protocol (VSF TR-06). AWS MediaConnect offers it beside SRT, and Spalk takes
commentary ingest over SRT, Zixi or RIST
([AWS, 2023](https://aws.amazon.com/blogs/media/remote-sports-commentary-made-easy-with-spalk-and-aws/)).
The evidence is thinner than for SRT: none of the broadcaster cases found named
RIST as their link.

## Done when

- [x] Hop profiles can declare RIST.
- [x] The planner resolves which end connects for RIST as it does for SRT.
- [x] One adapter builds RIST hops on the bench.

## Easy to break

- Where RIST goes in `TRANSPORT_PREFERENCE` decides whether existing streams
  change transport.

## Unchecked

- Whether Strom builds RIST flows.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-transport.
- 2026-09-25: the Unchecked question is answered: Strom has no RIST block, and
  builds RIST hops from raw `ristsrc`/`ristsink` elements with no Strom change
  (below).
- 2026-09-25: ticked "Hop profiles can declare RIST". `Transport::Rist`,
  `RistSocket` and a `rist` listener (`host`, `port_range`) are in
  `crates/core/src/lib.rs`; the Strom adapter advertises `srt-to-rist` (RIST
  egress, `connect`) and `rist-to-srt` (RIST ingress, `listen`). Unit tests
  `a_rist_socket_round_trips_and_carries_no_params`,
  `a_rist_listener_needs_a_host_and_a_port_pair`,
  `rist_pairs_are_even_ports_whose_next_port_is_in_range`. Unit tests only.
  `PROTOCOL_VERSION` stays 5: the session's one bump came with OW-2.
- 2026-09-25: ticked "The planner resolves which end connects for RIST".
  `link_transport` in `path.rs` treats RIST as it does SRT, except that RIST
  simple profile has no mode where the receiver dials, so only the downstream
  end listens (the rule WHIP follows) and the sender pushes. RIST is last in
  `TRANSPORT_PREFERENCE`; every existing planner test passed unchanged but one
  that used `rist` as its example of an unknown transport name, now `zixi`.
  A RIST listener takes an even port and the one after it from its range
  (`claim_rist`), and relay choice checks RIST listeners for free pairs too.
  Unit tests in `crates/controller/src/rist_tests.rs`
  (`a_link_srt_can_carry_stays_srt`, `a_link_only_rist_can_carry_goes_over_rist`,
  `the_sender_never_listens_for_rist`,
  `rist_takes_even_port_pairs_that_srt_never_shares`) and
  `a_relay_is_checked_for_a_free_rist_pair` in `path.rs`. Unit tests only.
- 2026-09-25: ticked "One adapter builds RIST hops on the bench", on `bench/`.
  `just bench topology rist` restarted adapter-2 with
  `config/adapter-2-rist.yaml` (node 2 dials nothing; its internet attachment
  offers only a RIST listener; its SRT listener is on its own LAN), and
  `just bench stream-up rist` read `flowing`. strom-1 ran `weave-rist-sender` as
  srtsrc → capsfilter → queue → rtpmp2tpay → ristsink to 10.97.27.10:21948, and
  strom-2 ran `weave-rist-receiver-output` as ristsrc on 21948 → rtpmp2tdepay →
  queue → srtsink. tcpdump on router-2 showed RTP from 10.97.26.10 to
  10.97.27.10:21948 and RTCP both ways on 21949; the receiver's srtsink
  `bytes_sent` went from 7.4 to 9.2 MB in 5s, fed by nothing but the ristsrc; and
  `/view` showed every socket of both hops `flowing`. After
  `just bench topology default`, `basic` planned `srt-forward` on both hops and
  read `flowing`. Earlier, on a throwaway Strom container (not the bench): the
  golden flows in `crates/strom/src/testdata/` carried 642 buffers end to end,
  and a sender with two RIST egresses fed two receivers.
- 2026-09-25: Strom reports no RIST statistics (`srt-stats` lists only srt
  elements, and the element-properties API leaves out `stats`), so a RIST
  socket's condition is read from the SRT side of its hop: a sender's RIST
  egress reads `flowing` whenever its SRT ingress advances, whether or not
  anything receives, and a receiver's RIST ingress only while a consumer pulls
  its SRT output. A RIST-to-RIST transit hop would have no progress signal at
  all. Strom advertises no such profile, so the planner cannot place one on
  Strom, and no item was filed for it.
- 2026-09-25: RIST links carry no key; filed OW-41. In a port range RIST
  shares with SRT, the SRT allocator can take one half of the last free pair
  (seen in `rist_tests` with a 4-port range); README says to give RIST its own
  range.
