# Backlog

A new item should state the evidence, what done looks like, and any constraint
that is easy to break while fixing it.

## Open

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
  the macOS host, through the `docker-host` alias and `browser-cam-host`, put
  H264 640x480 plus AAC on the SRT output with the flow `Playing`
  (`bench/README.md`, "Feeding open-live"). Easy to break: `bench/justfile` prints
  `browser-cam`'s status instead of waiting on it, and `bench/README.md` records
  the cycling as observed, so both change with the fix.
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
  deregistration route and the controller has no node TTL, so the node stays in
  `GET /v1/nodes` and `/v1/status` as `offline`. Each browser page start without
  `--node` picks a fresh id, and one bench run left three stale `browser-…`
  nodes beside `browser-bench` (`7 node(s)` in `/v1/status`). Done: a node that
  has not heartbeated for some interval leaves the listing, or a node can
  deregister itself. Easy to break: dropping an entry replans every stream
  placed on it. `pick_relay` skips `Offline` nodes and a pinned relay that goes
  offline is reported `degraded` rather than swapped out, and both behaviours
  read the entry that would disappear.
- **A restarted page waits out Strom's inactivity timeout.** `whip_input` is
  created with `max_sessions: 1` (`crates/strom/src/spec.rs`) and the page
  cannot release the session it left behind: Strom's CORS exposes only
  `mcp-session-id`, so the `Location` header of the WHIP `201` is unreadable in
  a browser and there is nothing to `DELETE`. Observed: Strom answers `503` for
  10–20 s until its inactivity monitor frees the slot, then the new session
  flows. Done: a page restart reconnects without that window. Easy to break:
  `max_sessions: 1` is what makes one page own one endpoint, and what a second
  connection to the same endpoint does with a higher limit is untested.
- **A hop with WebRTC on both sides is refused.** The planner can produce
  `whip → whep` (two browser nodes bridged through a Strom) and Strom can build
  `whip_input → whep_output`, but the adapter reads media progress from a hop's
  SRT byte counters and such a hop has no SRT side, so the path would report
  `degraded` for ever. `flow_spec_from_hop` returns
  `MappingError::WebRtcOnBothSides` instead (`crates/strom/src/spec.rs`). Done:
  browser to browser through a Strom places and reports its real condition. That
  needs a per-session signal Strom does not expose: `webrtc-stats` walks the
  flow pipeline and WHIP/WHEP sessions run in pipelines of their own. Easy to
  break: accepting the shape without a signal makes a working path read
  `degraded`, which is worse than refusing it.
- **The northbound payload has no version signal.** `StreamEndpoints.ingress`
  and each entry in `outputs` became `Option<EndpointAddr>`
  (`crates/controller/src/path.rs`) — an operator-contract payload change with
  nothing marking it: `API_V1` is unchanged and `PROTOCOL_VERSION` is checked
  only at `POST /v1/nodes/register` (`crates/controller/src/main.rs`), which is
  southbound. The `srt_only_endpoints_json_shape_is_unchanged` test
  (`crates/controller/src/path.rs`) shows an SRT-only stream still serializes
  `ingress` and each output as a plain object, so SRT-only clients see no
  change. A client reading every output did break: open-live's `weave` provider
  read `.node` off a `null` entry and threw, so it listed no weave source at all
  while any device destination existed, until its fork was fixed. The gap is
  that the northbound contract has no version knob at all, and the README's
  rule — `API_V1` moves when the routes change, `PROTOCOL_VERSION` moves when
  the payloads behind them do — does not say what covers a northbound payload
  shape change. Done: a client can tell this shape apart from the one before it,
  whether by a version on the payload, a bump to `API_V1`, or the README rule
  extended to name what covers it. Easy to break: the only manifests exercising
  a `device` end today are the bench's `browser-cam`, `browser-cam-host` and
  `browser-return`; a fix checked only against SRT-only manifests would not
  catch a regression here.

## Not scheduled

Listed so they are not picked up by accident.

- **Capture inputs beyond a browser's own camera.** A `device` endpoint covers a
  node's own capture or display device, which today only the browser node
  advertises. `NodeCapabilities` still has no way to describe an NDI, Decklink,
  or SDI input on a Strom node, and nothing plans against the endpoints adapters
  report through `EndpointDescriptor.metadata`.
- **Transports other than SRT, WHIP and WHEP.** The planner selects among those
  three from node capabilities. WHIP and WHEP exist only as link transports it
  chooses for browser nodes: there are no `whip:`/`whep:` manifest variants for
  external peers, no WebRTC between Strom nodes, no TLS, and no per-node tokens.
  A Strom sender fanning out to both a browser and another node (`srt` and
  `whep` egresses on one hop) is rejected by the adapter rather than transcoded.
- **Controller HA.** One controller owns all state. No leader election, no
  failover.
- **Port coordination with a co-tenant.** Another orchestrator writing flows to
  the same Strom allocates ports from its own scheme. Nothing detects a clash.
- **Format conversion.** Mismatches are reported and nothing is placed to fix
  them. Doing that needs nodes to advertise the transforms they can perform and a
  cost model, per `README.md`.

## Scope guards

- Do not add comments beyond what a change genuinely needs, and delete any the
  change makes wrong.
- Do not implement anything under "Not scheduled" as a side effect of an item
  above.
- Keep `cargo test --workspace` and `cargo clippy --workspace --all-targets`
  clean; both pass as of this file being written.
