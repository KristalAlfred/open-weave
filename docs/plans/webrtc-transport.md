# Plan: the browser as a weave node (WebRTC transport)

Brief for an autonomous overnight run in this repo. Output is a **stack of
pull requests** on `KristalAlfred/open-weave`, one per section below, each
based on the branch before it. Do not merge anything. The reader reviews the
stack in the morning.

## Goal

A web page becomes a weave node. It registers with southbound, advertises a
camera and a screen, and realises hops the controller gives it: send the
camera over WHIP to a Strom node, or play a WHEP stream from a Strom node.
Manifests name the browser node like any other node. The controller picks
WebRTC for links that touch it and puts the hosting side on Strom, whose
adapter maps the resulting gateway hops to block flows.

```yaml
name: alice-cam
source:
  device:
    node: browser-a1b2          # the node's own camera
destinations:
  - srt:
      node: strom-node-2        # open-live dials this receiver's output
```

```yaml
name: alice-return
source:
  srt:
    node: strom-node-2          # open-live sends its programme here
destinations:
  - device:
      node: browser-a1b2        # the node's own screen
```

What is deliberately **not** built: `whip:`/`whep:` manifest variants for
external peers, WebRTC between Strom nodes, TLS, per-node tokens, weave
pushing WHIP into open-live. WHIP and WHEP exist only as link transports the
planner chooses; operators never write them.

## How it works, end to end

1. The page loads, generates `browser-<8 hex>` (sessionStorage), and
   `POST /v1/nodes/register` with `protocol_version`, transports
   `whip [connect]`, `whep [connect]`, `device [listen, connect]`, and a
   `data_plane.default` marked `outbound_only`. It heartbeats every 5 s and
   polls `GET /v1/nodes/{id}/desired` every 2 s.
2. An operator applies `alice-cam`. The planner sees a link from a node that
   can only `connect` over WHIP to a node that can `listen` over WHIP and is
   dialable, so the link is WHIP with Strom hosting. The sender hop on the
   browser is `device → whip connect <url>`; the receiver hop on Strom is
   `whip listen <url> → srt listen <consumer port>`.
3. The Strom adapter maps that receiver hop to
   `whip_input → videoenc → mpegtssrt_output` and starts it. The browser sees
   its hop, calls `getUserMedia`, and does WHIP against the URL.
4. Both report hop status. The path rolls up to `flowing`; the dashboard shows
   the browser node and the link.
5. `alice-return` is the mirror: Strom hosts WHEP (`mpegtssrt_input →
   whep_output`), the browser's receiver hop is `whep connect → device` and it
   attaches the tracks to a `<video>`.

Strom blocks used, all present on the bench Strom (`GET /api/blocks`):
`builtin.whip_input` (hosts `/whip/{endpoint_id}` per open-live's code, verify),
`builtin.whep_output` (hosts `/api/whep/{endpoint_id}` per its description),
`builtin.videoenc`, `builtin.mpegtssrt_output`, `builtin.mpegtssrt_input`.

## Working rules for the run

- **Keep SRT-only behaviour identical.** Every existing test passes unchanged
  in every PR. A node advertising only `srt` is never handed a non-SRT socket,
  so `PROTOCOL_VERSION` does not bump.
- **Decide, record, continue.** When something is ambiguous, pick the option
  that is smallest and reversible, write the decision and the alternative in
  the PR description under "Decisions", and move on. Do not stop to ask.
- **Verify against the real Strom** on the running bench (`localhost:28080`,
  `bench/README.md`) before designing around an assumption. When a check
  contradicts this plan, follow the evidence and record it.
- **Finish the stack.** A PR whose verification could not be completed still
  ships, with "Not verified" stated plainly at the top of its description.
  An honest partial stack beats a polished half.
- Comments in code: only where the code cannot say it. No narration.
- Commits: one-line subjects, no attribution footers. Commit freely on the
  stack branches.
- `cargo fmt --all`, `cargo clippy --workspace --all-targets`, and
  `cargo test --workspace` clean before every push.

## Stack mechanics

Branches `webrtc/1-core`, `webrtc/2-planner`, … each created from the one
before. Create each PR with `gh pr create --base <previous branch>` (PR 1 is
based on `main`). Title `WebRTC n/6: <what>`. Body sections: **Summary**,
**Decisions**, **Verified** (commands and observed output), **Not verified**.
Start the body with the stack list, marking the current PR:

```
Stack: 1/6 core · 2/6 planner · 3/6 strom adapter · 4/6 southbound · 5/6 browser node · 6/6 bench+docs
```

When a lower PR changes after upper branches exist, rebase the upper branches
onto it (`git rebase --onto`) and `git push --force-with-lease`. Do not
squash across PRs. When all six exist, edit every body's stack line to link
the PR numbers, and write `docs/plans/webrtc-transport-status.md` with the
morning summary: what each PR does, what was verified, what was not, and
what to look at first.

## PR 1 — core model (`crates/core`)

- `Transport` gains `Whip`, `Whep`, `Device`.
- `SocketSpec` gains `url: Option<String>` (WHIP/WHEP signalling URL;
  `host`/`port` stay `None` for those). `Device` sockets carry no address.
- `SocketRole` is reused: `Listen` hosts, `Connect` dials. A `Device` socket
  is `Listen` on a source (camera produces) and `Connect` on a destination
  (screen consumes), only so the role field is never meaningless.
- `TransportDescriptor { name, roles: Vec<SocketRole> }` with `roles`
  defaulting to both, so `transports: [srt]` configs read unchanged.
- `DataPlaneAddr` gains `webrtc_base_url: Option<String>` (full URL such as
  `http://172.26.0.10:8080`). Keep the bare-string shorthand working.
- `StreamTransport` gains `Device(NodeEndpoint)`; `NodeEndpoint { node,
  network }`, `deny_unknown_fields`. Northbound validates it like an SRT
  node endpoint (node required; no `via`, `remote`, `latency`, `format`).
- `NodeCapabilities::port_range` is already optional; nothing claims a port
  on a node without one (PR 2 enforces).
- Tests: serde round-trips for the new manifest variant and socket shapes,
  backward-compat of `transports: [srt]`, `DataPlaneAddr` shorthand.

## PR 2 — planner (`crates/controller/src/path.rs`)

- `plan_link` selects a transport instead of assuming SRT. Candidates are
  transports where the upstream node has one role and the downstream node
  the complementary one, and the node that would `Listen` is dialable.
  Preference: `srt`, then `whip`, then `whep`. Upstream `connect` + downstream
  `listen` over WHIP is the browser-to-Strom case; upstream `listen` +
  downstream `connect` over WHEP is Strom-to-browser. No candidate →
  `PlacementError::NoCommonTransport { upstream, downstream }`, and
  `splice_relays` treats it like an undialable link (a relay must offer a
  compatible transport too; relays are Strom nodes, so this holds).
- WebRTC listen sockets get `url = <webrtc_base_url>/whip/<downstream hop id>`
  or `<base>/api/whep/<downstream hop id>`, resolved from the hosting node's
  alias; missing `webrtc_base_url` → `PlacementError::NoWebRtcBase { node }`.
  No port is claimed.
- `source_socket`: a `device` source yields a `Device`/`Listen` ingress and
  requires the node to advertise `device`. The receiver's terminal egress
  for a `device` destination is `Device`/`Connect` (no consumer port).
- `claim_port` on a node without `port_range` → a clear `PlacementError`.
- `stream_endpoints`: `EndpointAddr` gains `transport`; `device` ends are
  omitted from `ingress`/`outputs` (there is nothing to dial), and the
  response says so with `ingress: null` where applicable. Keep the SRT shape
  byte-identical for SRT-only streams.
- Dashboard (`ui.html`) shows the transport on each link; small.
- Tests: device sender → Strom picks WHIP hosted on Strom with the expected
  URL; Strom → device receiver picks WHEP; two browsers with no relay →
  pending with a reason; browser + Strom relay works; every existing SRT test
  unchanged; endpoints output for the two demo manifests.

## PR 3 — Strom adapter (`crates/strom`, `crates/adapter-strom`)

First, check on the bench Strom and record the results in the PR body:

1. Allowed characters in `endpoint_id` (create a `whip_input` with a
   `weave-…` id; open-live strips non-alphanumerics "that Strom may reject").
   If `-` is rejected, derive the id by stripping and keep a reversible map
   in the flow name, which stays the hop id.
2. The WHIP and WHEP URL paths (`/whip/…` vs `/api/whip/…`, `/api/whep/…`).
   PR 2's URL templates must match; adjust them there if needed.
3. What `srt-stats` returns for a flow whose SRT socket lives inside a block
   (element ids will not start with `srtsrc`/`srtsink`).
4. How WHIP/WHEP sessions are exposed (connected clients, bytes) for status.

Then:

- `FlowSpec` gains `blocks` (`id`, `block_definition_id`, `name`,
  `properties`, `position`). Strom flows already carry the field.
- Mapping by (ingress, egress) transports:
  - `srt → srt`: unchanged element flow.
  - `whip → srt`: `whip_input(endpoint_id)` → `videoenc(codec=h264)` →
    `mpegtssrt_output(srt_uri, latency)`; audio `whip_input:audio_out →
    mpegtssrt_output:audio_in_0`. Fan-out tees after the encoder.
  - `srt → whep`: `mpegtssrt_input(srt_uri, latency, decode=true)` →
    `whep_output(endpoint_id)` video and audio.
  - `whip → whep`: `whip_input → whep_output` (falls out of the two above).
- Drift: `flow_drifted` also compares block `endpoint_id`s and SRT URIs in
  block properties.
- Status: extend `srt-stats` parsing to find SRT elements inside blocks (from
  check 3). For the WebRTC socket of a gateway hop, use what check 4 found.
  If Strom exposes nothing usable, infer it from the hop's other side: bytes
  advancing on the SRT side mean the WebRTC side is `Flowing`; a running flow
  with no bytes is `Connected`; record this fallback in the PR.
- Node config: `transports` entries may be `srt` or `{ name, roles }`;
  `data_plane.<alias>.webrtc_base_url`. Registration passes both through.
- Tests: snapshot-style mapping tests for the two gateway flows; drift tests
  for endpoint ids; status tests for the inferred conditions.

## PR 4 — southbound: CORS and page token (`crates/southbound`)

- CORS layer (tower-http) on the `/v1` routes, enabled only when
  `WEAVE_SOUTHBOUND_CORS_ORIGIN` is set (exact origin or `*` for dev).
  Preflight must allow `Authorization` and `Content-Type`.
- `NodeDescriptor.endpoint` for a browser is a `browser://<id>` placeholder;
  make sure nothing in controller or southbound treats it as dialable (the
  controller makes no outbound calls today; add a test that says so for this
  shape).
- Document in `README.md` Authentication: the page presents
  `WEAVE_SOUTHBOUND_TOKEN`; per-node tokens remain a follow-up.

## PR 5 — the browser node (`nodes/browser/`, plain HTML+JS, no build step)

- `index.html` + `node.js`. Configuration from the URL fragment:
  `#southbound=http://host:8081&token=…` (fragment so it never hits a log).
- Registration payload mirrors `weave-adapter-strom`'s `registration()`:
  `protocol_version` from a constant the page must keep in step with
  `weave_core::PROTOCOL_VERSION` (add a test in core that reads the JS
  constant, or generate it; pick the smaller). Capabilities: transports
  `whip [connect]`, `whep [connect]`, `device [listen, connect]`;
  `data_plane.default = { host: "browser", reachability: outbound_only }`;
  no `port_range`; `relay: false`.
- Loop: heartbeat 5 s with `hop_status`; poll `/desired` 2 s; reconcile by
  hop id: start new hops, stop removed ones, leave unchanged ones alone.
- `device → whip connect`: `getUserMedia({video, audio})`,
  `RTCPeerConnection` with `sendonly` transceivers, WHIP: `POST` SDP offer as
  `application/sdp`, read the answer and the `Location` header, `DELETE` it on
  teardown. `whep connect → device`: WHEP `POST`, `recvonly`, attach the
  remote stream to a `<video autoplay playsinline>` per hop.
- Status from `getStats()` every poll: no connection → `Connecting`;
  connected with frozen bytes → `Connected`; bytes advancing → `Flowing`;
  advanced before and frozen for 3 polls → `Stalled`. Fill `LinkStats`
  (`connections`, rates in Mbps, RTCP `packetsLost`).
- UI: node id, registration state, a manifest snippet with the node id filled
  in for copy, one row per hop with its condition, the video elements.
- Verification without a human: a Playwright script (`nodes/browser/check.mjs`)
  that launches Chromium with `--use-fake-device-for-media-stream
  --use-fake-ui-for-media-stream`, opens the page against the bench, and
  waits for the node to appear in `GET /v1/nodes`. Media verification is in
  PR 6, where the browser runs inside the bench network.

## PR 6 — bench and docs

- `bench/docker-compose.yml`: a `browser` service (profile, off by default)
  running `mcr.microsoft.com/playwright` on `net_core` with the fake-webcam
  flags, serving `nodes/browser/` from a tiny static server in the same
  container, and pointing the page at southbound. This is how a headless
  browser reaches Strom's container ICE candidates on macOS; a host browser
  cannot. Say so in `bench/README.md`.
- Node 1 config: `transports: [srt, {name: whip, roles: [listen]},
  {name: whep, roles: [listen]}]`, `webrtc_base_url: http://172.26.0.10:8080`.
- Manifests `browser-cam` and `browser-return` templated on the node id, and
  recipes `just browser-up`, `just browser-stream <node-id>`,
  `just browser-down`. Add rows to the observed-behaviour table only for what
  was actually observed.
- `README.md`: a "Transports" section (SRT between nodes; WHIP/WHEP chosen
  by the planner for browser links; `device` endpoints). `BACKLOG.md`:
  narrow "Transports other than SRT" and "Capture inputs" to what remains.
- Run the full round trip on the bench: `browser-cam` to `flowing` with the
  ffmpeg consumer on the SRT output; `browser-return` fed by the bench
  producer, browser reports `Flowing` on its WHEP ingress. If media does not
  flow, record exactly where the conditions stop (which hop, which socket) —
  that is the morning's first debugging target.

## Morning summary (`docs/plans/webrtc-transport-status.md`)

One page: PR list with links; per PR what was verified and how; the Strom
check results from PR 3; every "Decisions" entry collected; what did not
work and the best current hypothesis; suggested review order.
