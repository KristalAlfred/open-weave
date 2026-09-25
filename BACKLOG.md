# Backlog

open-weave is an open-source layer 7 router for live media, offered as a free
building block. It decides which node dials which, over which standard protocol
and through which relays, and tells each node's adapter. What to route, when and
for whom belongs to an application that drives open-weave through northbound:
scheduling, bookings, feed entitlements. Media nodes and the data plane belong to
the runtimes behind the adapters: encoding, transcoding, bonding, merging
redundant copies. An item that needs open-weave to do either does not go here.
That a commercial product already does something is not a reason to leave it out.

A new item should state the evidence, what done looks like, and any constraint
that is easy to break while fixing it.

## Bugs

- **`browser-cam` carries audio and no video.** Strom's `whip_input` sets
  `video-codecs = ["H264"]` (`backend/src/blocks/builtin/whip.rs` in Strom) and
  the bench's Playwright Chromium on arm64 has no H264 encoder, so the session
  negotiates Opus alone. Node 1's gateway hop then emits a ~5 kb/s audio-only
  trickle on its SRT output, never leaves `gst_state: Paused`, reads `idle` on
  its WHIP ingress, and Strom's inactivity monitor drops the session every ~20 s
  before the page reconnects. The stream reports `degraded`. Done:
  `just bench browser-stream` reaches `flowing` on `browser-cam` with video in
  the SRT output. Two routes: a Chromium that encodes H264 (Google Chrome on
  x86_64 — there is no Linux arm64 build), or VP8/VP9 accepted by `whip_input`,
  which is a change to Strom. The gateway flow itself is fine: Google Chrome on
  the macOS host, through the `docker-host` attachment and `browser-cam-host`, put
  H264 640x480 plus AAC on the SRT output with the flow `Playing`
  (`bench/README.md`, "A page in your own browser"). Easy to break:
  `bench/justfile` prints `browser-cam`'s status instead of waiting on it, and
  `bench/README.md` records that it does not settle, so both change with the fix.
- **A video-only WHIP sender leaves the gateway flow paused.**
  `whip_to_srt_flow` always links `whip_in:audio_out` into
  `mpegtssrt_output`'s `audio_in_0` (`crates/strom/src/spec.rs`). A run with
  H264 available and no microphone left the flow at `gst_state: Paused` with 0
  bytes out and the audio pad linked to nothing. Done: a sender with video and
  no audio reaches `flowing`. That needs the adapter to know which tracks the
  sender has, so it can set `whip_input.mode` or leave the audio pad unlinked; a
  `DesiredHop` carries no track list today. Easy to break: leaving the audio pad
  unlinked unconditionally drops the audio of every sender that does send some.
- **A node that stops heartbeating is never forgotten.** Southbound has no
  deregistration route, and the controller's node TTL only changes a status:
  `mark_offline` sets the entry to `Offline` after `WEAVE_NODE_TTL_SECS` (15s by
  default) and nothing ever removes it (`crates/controller/src/main.rs`), so the
  node stays in `GET /nodes` and `/status` as `offline`. Each browser
  page start without `--node` picks a fresh id, and one bench run left three
  stale `browser-…` nodes beside `browser-bench` (`7 node(s)` in `/status`).
  Done: a node that has not heartbeated for some interval leaves the listing, or
  a node can deregister itself. Easy to break: dropping an entry replans every
  stream placed on it. `pick_relay` skips `Offline` nodes and a pinned relay
  that goes offline is reported `degraded` rather than swapped out, and both
  behaviours read the entry that would disappear.
- **A restarted page waits out Strom's inactivity timeout.** `whip_input` is
  created with `max_sessions: 1` (`crates/strom/src/spec.rs`) and the page
  cannot release the session it left behind: Strom's CORS exposes only
  `mcp-session-id`, so the `Location` header of the WHIP `201` is unreadable in
  a browser and there is nothing to `DELETE`. Observed: Strom answers `503` for
  10–20 s until its inactivity monitor frees the slot, then the new session
  flows. Done: a page restart reconnects without that window. Easy to break:
  `max_sessions: 1` is what makes one page own one endpoint, and what a second
  connection to the same endpoint does with a higher limit is untested.
- **Browser-to-browser media has no supported Strom profile.** Strom can build
  `whip_input → whep_output`, but the adapter reads media progress from SRT byte
  counters and this shape has no SRT side. Strom therefore does not advertise a
  `whip → whep` hop profile, and planning fails before desired state is sent.
  Done: expose a per-session media signal and add the profile. Easy to break:
  advertising it without that signal makes a working path read `degraded`.

## To build

Evidence marked with a link comes from web research on broadcaster use done on
2026-09-25. Most of it is trade press; vendor sources are named as such.

- **SRT hops carry no encryption.** Nothing in `crates/` sets an SRT passphrase
  or key length, so every planned SRT hop runs in the clear. Contribution and
  distribution over the public internet are the common case: ESPN sent every
  camera of a 2023 college game over SRT into AWS
  ([SVG](https://www.sportsvideo.org/2023/01/24/espn-dmed-pull-off-first-end-to-end-cloud-based-live-production-in-u-s-with-a-10-college-hoops-game/)),
  and Vivid Broadcast produces up to six Women's Super League matches a weekend
  over the public internet
  ([Intinor, 2026, vendor](https://intinor.com/securing-remote-production-for-the-womens-super-league/)).
  The controller plans both ends of every hop, so it can give both the same key.
  Done: each SRT hop carries a key in both ends' desired hops, the Strom adapter
  sets it, a `remote` destination takes its key from the manifest, and a bench
  caller with the wrong key is refused. Unchecked: whether Strom's SRT blocks
  expose GStreamer's `passphrase` property. Easy to break: the key must stay out
  of `/view`, `/status`, logs, webhook events and every unauthenticated route.
  With one shared southbound token any node can read any node's desired hops, so
  the key is only as private as per-node tokens make it.
- **Every node shares one southbound token.** One `WEAVE_SOUTHBOUND_TOKEN` covers
  every adapter and browser page (`README.md`, "Authentication"), so any node can
  register as, or read the desired hops of, any other. Distribution sends feeds
  to nodes run by other organisations: Eurovision to national broadcasters, PBS
  to more than 330 member stations
  ([TV Tech, 2026](https://www.tvtechnology.com/infrastructure/ip-networking/pbs-selects-ltn-to-power-nationwide-ip-video-network)).
  Done: each node authenticates as itself, and registration, heartbeat and
  `/nodes/{id}/desired` refuse any other id. Easy to break: a browser page gets
  its token through the URL fragment (`nodes/browser/`), and controller
  `GET /nodes` accepts either surface token.
- **TLS has never been exercised.** `README.md` says to terminate TLS at a
  reverse proxy, and the HTTP clients are built with `rustls-tls`, but nothing in
  `bench/` or CI puts a surface behind TLS. Nodes outside one network send bearer
  tokens and desired hops, which name peer addresses, across it. Done: the bench
  runs northbound and southbound behind TLS, and the adapter, the CLI and a
  browser page work against them. Whether the services serve TLS themselves is a
  separate decision.
- **A controller restart probably tears down running media.** `get_desired`
  serves an empty list for any node without a snapshot
  (`crates/controller/src/main.rs`), the API starts before the first reconcile
  tick, and the Strom adapter deletes managed flows that are not desired. Reading
  that code, an in-memory controller that restarts empties every node, and one
  backed by Postgres (`DATABASE_URL`) does the same to any node that polls before
  the first tick finishes. Not tested. Beyond restarts, one controller owns all
  state and nothing can be applied or replanned while it is down; station groups
  and event distributors run links around the clock. Done: a bench case that
  restarts the controller with media flowing and shows no flow dropped; then a
  second controller that takes over. Easy to break: an adapter that ignores an
  empty desired list can no longer be told to remove its last hop.
- **One destination has one path.** Nothing plans a second copy of a
  destination; a grep for redundancy, 2022-7 and failover in `crates/` finds
  nothing. Broadcasters send the same feed over two routes and merge them at the
  receiver: Eurovision sends SRT over the internet as two streams combined with
  SMPTE 2022-7
  ([Panorama, 2026](https://www.panoramaaudiovisual.com/en/2026/01/22/nuevas-necesidades-distribucion-grandes-eventos-deportivos-eurovision-services/)).
  Merging is the receiving node's job (2022-7, libsrt socket groups); choosing
  two paths that share no relay or network is routing. Done: a destination can
  ask for two paths, the planner places them over disjoint relays and attachments
  when the topology allows, a hop profile declares that the receiver can merge,
  and a stream that gets only one path reports it. Easy to break: hop ids and
  ports are stable across manifest edits (`README.md`, "Hop status and fan-out");
  the second path needs ids of its own without renumbering the first.
- **Automatic relay insertion is only tested in the planner.** The bench has one
  NAT'd site, so `nat-relay` pins node 1 with `via`
  (`bench/manifests/nat-relay.yaml`). NAT and caller/listener setup are the SRT
  problems vendors document most
  ([Haivision](https://www.haivision.com/blog/all/basics-getting-real-time-video-through-firewall/),
  [Vizrt](https://docs.vizrt.com/viz-now-launchpad/1.2/Sending_and_Receiving_SRT_Video_Feeds.html)),
  and a search of orchestration products found none that derives roles from
  declared reachability. Done: a second NAT'd site on the bench, and a manifest
  between the two sites with no `via` that reaches `flowing` through a relay the
  controller chose.
- **Stream status reaches an application only by polling.** The webhook carries
  `node.registered`, `node.online` and `node.offline` and nothing about streams
  (`crates/core/src/webhook.rs`). The application that decides what to route
  learns that a stream went `flowing` or `degraded` by polling `/status` or
  `GET /streams`. Done: stream condition changes go out on the webhook with the
  stream name, generation and reason code. Easy to break: the webhook is
  fire-and-forget with a bounded queue (`crates/controller/src/webhook.rs`); a
  slow receiver must not hold up reconciliation, and condition reason codes are
  stable API values.
- **WHIP and WHEP for outside peers.** WHIP and WHEP exist only as link
  transports the planner picks for browser nodes; a manifest has no `whip:` or
  `whep:` variant for an encoder or player open-weave does not manage. WHIP is
  [RFC 9725](https://www.rfc-editor.org/rfc/rfc9725.html) (March 2025), OBS has
  sent it since version 30, and Haivision added it to the Makito X4 in 2025
  (vendor). WHEP is still a draft (`draft-ietf-wish-whep-04`, June 2026).
  Broadcast use is mostly low-latency return and browser monitoring beside SRT
  contribution
  ([Dolby case study, 2024](https://optiview.dolby.com/resources/customer-stories/new-remote-production-hub-in-uk-revolutionizes-remote-broadcasting/)).
  Done: a manifest can take a source from an outside WHIP sender and deliver a
  destination to an outside WHEP player, and `GET /streams/{name}/endpoints`
  returns the URLs. Easy to break: Strom's `whip_input` accepts H264 only and
  one session per endpoint (Bugs above); an outside sender with other codecs is a
  format mismatch to report.
- **RIST.** The planner knows SRT, WHIP and WHEP (`TRANSPORT_PREFERENCE` in
  `crates/controller/src/path.rs`). RIST is the other standard contribution
  protocol (VSF TR-06). AWS MediaConnect offers it beside SRT, and Spalk takes
  commentary ingest over SRT, Zixi or RIST
  ([AWS, 2023](https://aws.amazon.com/blogs/media/remote-sports-commentary-made-easy-with-spalk-and-aws/)).
  The evidence is thinner than for SRT: none of the broadcaster cases found named
  RIST as their link. Done: hop profiles can declare RIST, the planner resolves
  which end connects as it does for SRT, and one adapter builds it on the bench.
  Unchecked: whether Strom builds RIST flows. Easy to break: where RIST goes in
  `TRANSPORT_PREFERENCE` decides whether existing streams change transport.
- **Nothing runs at distribution scale.** Everything is verified on three Strom
  nodes. Distribution runs to hundreds of receivers: PBS to more than 330
  stations, BBC World Service to hundreds of partners
  ([Zixi, 2025, vendor](https://zixi.com/news/encompass-and-zixi-partner-to-transform-bbc-world-service-to-ip-distribution/)).
  Nothing measures plan time, desired-state size or heartbeat load at that size.
  Done: a test with a few hundred registered nodes and one stream fanning out to
  all of them, with plan and reconcile times recorded. Easy to break: planning
  allocates against the full candidate stream set (Scope guards). Open question:
  whether the planner should spread a fan-out over transit nodes when a sender
  reaches `max_egresses`.

## Not scheduled

Listed so they are not picked up by accident.

- **Capture inputs beyond a browser's own camera.** A `device` endpoint covers a
  node's own capture or display device, which today only the browser node
  advertises. `NodeCapabilities` still has no way to describe an NDI, Decklink,
  or SDI input on a Strom node, and nothing plans against the endpoints adapters
  report through `EndpointDescriptor.metadata`.
- **WebRTC between Strom nodes, and mixed-transport fan-out.** No WebRTC hop is
  planned between two Strom nodes. A Strom sender fanning out to both a browser
  and another node (`srt` and `whep` egresses on one hop) is rejected by the
  adapter rather than transcoded.
- **Protocol plugins.** Awareness of each standard protocol is built into the
  planner. A plugin interface that adds a protocol without a planner change is
  wanted later, not now.
- **Port coordination with a co-tenant.** Another orchestrator writing flows to
  the same Strom allocates ports from its own scheme. Nothing detects a clash.
- **Format conversion.** Mismatches are reported and nothing is placed to fix
  them. Doing that needs nodes to advertise the transforms they can perform, and
  a cost model so the planner does not insert a transcode to rescue a mistyped
  manifest.

## Scope guards

- Do not add comments beyond what a change genuinely needs, and delete any the
  change makes wrong.
- Do not implement anything under "Not scheduled" as a side effect of an item
  above.
- Keep `cargo test --workspace` and `cargo clippy --workspace --all-targets`
  clean; both pass as of this file being written.
- Regenerate `contracts/` when an API route or wire type changes. Contract
  drift is a test failure, not a documentation follow-up.
- Keep stream planning side-effect free and allocate against the full candidate
  stream set. A one-stream preview can otherwise promise ports apply will not use.
- Preserve stream generations on semantic no-op applies and never reuse opaque
  revisions after delete and recreate. Generation describes the current spec;
  revision is mutation identity and must prevent ABA during conditional writes.
- A stream-set write is one transaction. It may prune only streams carrying the
  same owner, and a semantic no-op must preserve the set ETag and member
  generations. Existing unmanaged or differently owned names remain conflicts;
  do not turn apply into implicit adoption.
- Keep `generation` ahead of `observed_generation` until reconciliation has
  processed the new spec. Condition reason codes are stable API values, and a
  condition's transition time changes only when its status changes.
