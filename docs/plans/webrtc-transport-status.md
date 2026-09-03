# WebRTC transport as built

`webrtc-transport.md` is the plan. This records where the result departs from
it, the decisions taken along the way, and the facts about Strom and browsers
those rest on. Known gaps are in `BACKLOG.md`.

## Departures from the plan

- **Signalling bases are declared by the node, not assembled by the
  controller.** The plan gave `DataPlaneAddr` a `webrtc_base_url` and had the
  planner build `<base>/whip/<hop id>`. Instead an address carries
  `signalling: { whip, whep }`, full base URLs, and `SocketSpec::signalling`
  joins `/<hop id>` onto one of them and assumes nothing else about the path.
  The socket carries that endpoint id, so an adapter reads it instead of
  parsing the URL back apart. The Strom adapter builds them from
  `strom.signalling_base` plus its own two route constants, so Strom's URL
  layout stays in the adapter. `PlacementError::NoWebRtcBase` became
  `NoSignalling { node, transport }`.
- **One socket variant per transport.** The plan added `url: Option<String>` to
  `SocketSpec`, which left `SrtParams` on a WHIP socket and `port` optional
  where it is required. `SocketSpec` is an enum instead: `Srt(SrtSocket)`,
  `Whip(SignallingSocket)`, `Whep(SignallingSocket)`, `Device(DeviceKind)`.
- **A device is a terminal, not a transport.** The plan put `Device` in
  `Transport` beside the link transports. `Transport` has three values and a
  node declares `devices: [capture, display]` beside `transports`, so a device
  socket's role reads `capture` or `display` instead of reusing
  `listen`/`connect`.
- **`whip → whep` is refused, not built.** The plan expected it to fall out of
  the two gateway shapes. The adapter cannot report media progress for a hop
  with no SRT side, so `flow_spec_from_hop` returns
  `MappingError::WebRtcOnBothSides`.
- **A WebRTC socket can report `Stalled`.** The plan's fallback stopped at
  `Flowing` and `Connected`. `webrtc_condition` passes the hop's SRT-side stall
  verdict through and reports `Stalled` first, as `socket_condition` does. An
  SRT *egress* still never reports it, since `LinkCondition::Stalled` is
  documented about an ingress, so on a `whip → srt` hop the WHIP ingress can
  read `Stalled` beside an SRT egress reading `Connected`.
- **`EndpointAddr` gained no `transport` field.** Only SRT ends appear in
  `GET /v1/streams/{name}/endpoints`, so it would always read `srt`. A device
  end is a `null` entry, which keeps `outputs[i]` equal to destination `i`.
- **The page's protocol version is checked outside Rust.** The plan suggested a
  core test reading the JS constant. `nodes/browser/check.mjs` compares the two
  and `just browser-check` runs it, so `crates/core` does not `include_str!`
  the page.

## Decisions

- Core model: `#[allow(clippy::large_enum_variant)]` on `StreamTransport`
  rather than boxing `SrtEndpoint`; a bare name in `transports` offers both
  roles, so `transports: [srt]` configs read unchanged, and a node declaring
  no transports at all reads as SRT in both roles.
- `PROTOCOL_VERSION` is `2`. Node capabilities changed shape, so an adapter
  built against `1` is refused at registration rather than served hops whose
  sockets it cannot read. `API_V1` stays `/v1`: the routes did not change.
- A stored node registration this build cannot read is dropped with a `warn`
  naming the node id, and the boot continues. Every node implementation
  re-registers when a heartbeat is answered `404`, so the cost is one heartbeat
  interval. A stored stream definition still fails the boot — it is the only
  copy of what an operator asked for. Before this, one row written by an
  earlier build left the controller crash-looping on `hydrating nodes` with no
  indication which row or what to do about it.
- Planner: WHIP is hosted downstream only, because its connecting end pushes
  media, and WHEP upstream only, because its connecting end pulls.
  `NoCommonTransport` and `NoRelayAvailable` are separate variants so the
  message quoted in `bench/manifests/README.md` stays true. A stream that does
  not place carries a `reason`.
- Strom adapter: status by inference, not `webrtc-stats`; a paused flow reads
  as no session; two egresses on one hop asking for different transports are an
  error, not a transcode; drift comparison is order-insensitive.
- Southbound: CORS from `tower-http 0.6`; exact-origin mode echoes the
  configured origin, which is tower-http's semantics.
- Browser page: `check.mjs` launches `channel: "chromium"`, the full build,
  because the headless shell never answers `getUserMedia`; `media=video` and
  `node=<id>` are URL options; a hop whose session keeps failing reads
  `pending` for five attempts before `failed`; the `<video>` is `muted` so
  autoplay is allowed; teardown waits for Strom's inactivity timeout because
  the page cannot read `Location`.
- Bench: the page's node id is pinned so the manifests keep pointing at it
  across restarts; the container serves the page to itself and opens it as
  `127.0.0.1`, for the secure context below; `WEAVE_SOUTHBOUND_CORS_ORIGIN=*`,
  a development value like the bench tokens.

## Strom facts

| Subject | What was found |
|---|---|
| `endpoint_id` characters | Hyphenated ids (`weave-…`) are accepted and listed, so hop ids are used directly |
| WHIP/WHEP paths | `/whip/{id}` and `/whep/{id}` at the root; the `/api/whep/{id}` in the block description is stale (an SPA fallback answers it). The adapter's route constants use the root paths |
| `srt-stats` for blocks | Keys are `<block id>:srtsink` and `<block id>:srtsrc`; sinks carry `bytes_sent` |
| Session exposure | Nothing per session. `webrtc-stats` walks the flow pipeline and WHIP/WHEP sessions run in pipelines of their own, so a WebRTC socket's condition is inferred from the hop's SRT side and `gst_state` |
| Fan-out between blocks | A `tee` plus a `queue` per branch is accepted and started; this is what WHEP fan-out uses |
| `whip_input` video codecs | `["H264"]` and nothing else (`backend/src/blocks/builtin/whip.rs`) |
| CORS | Exposes only `mcp-session-id`, so a page cannot read the WHIP/WHEP `Location` and has nothing to `DELETE` |

## Browser facts

- `navigator.mediaDevices` is undefined on a plain-`http` origin that is not
  localhost, so the page needs a secure context even inside a container.
- A `getUserMedia` for a device the host never answers for hangs instead of
  rejecting, and blocks every later request in the same page. Seen on macOS for
  the fake microphone, in Chrome for Testing and in installed Chrome alike;
  `--video-only` exists for that case.
