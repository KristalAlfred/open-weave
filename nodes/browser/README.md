# Browser node

A web page that is an open-weave node. Plain HTML and JavaScript, no build step.

Open `index.html` from any static server with the southbound address and the
page's node token in the URL fragment, so neither reaches a server log:

```
http://host:8000/#southbound=http://southbound:8081&token=<node token>
```

`weave node-token <id>` prints the token for node `<id>` (see "Authentication"
in the root README).

The page needs a secure context for the camera: serve it over `https`, or over
`http` from `localhost` / `127.0.0.1`; on any other `http` origin the sender hop
fails with "no camera access". Add `&media=video` to send the camera without a
microphone. The `camera-to-whip` profile declares the tracks the page sends
(`tracks: [video]`, or `[audio, video]` by default), so the WHIP end builds a
flow for those tracks only. A capture device the OS never answers for (macOS without microphone
permission for the browser) leaves `getUserMedia` pending, and a pending request
blocks every later one in the page, so the choice is made up front rather than
by falling back.

The page's node id is the one its token names, so a page that restarts keeps its
name. `&node=<id>` pins the id instead, and must then name the token's id. With
neither a node token nor a pin, the page generates a `browser-<8 hex>` id per tab,
which only a southbound with authentication disabled accepts. A pinned id must be
1–63 lowercase ASCII letters, digits, or interior hyphens. An invalid pin, a pin
naming another node than the token, and a registration southbound refuses with
`400`, `403` or `409` are shown as rejected and are not retried. The page
registers a valid id with `camera-to-whip` and `whep-to-display` hop profiles,
plus one dial-only attachment to the `internet` network. `&network=<id>` selects another network.
It heartbeats every 5 s and polls desired hops every 2 s. It realises two hop
shapes and nothing else:

- `device → whip connect`: the camera and microphone, sent over WHIP to the URL
  the controller planned.
- `whep connect → device`: a WHEP stream played in a `<video>` on the page.

Each desired egress carries a branch id. Heartbeats report condition and stats
per branch, so one failed destination cannot be hidden by another. The browser
node declares `max_egresses: 1` on both profiles, so the controller rejects
capture fan-out during placement. The page also refuses a mismatched profile id
or shape instead of guessing a constructor from sockets.

The page shows manifests naming its node id; copy one, change the Strom node,
and apply it. Southbound must allow the page's origin
(`WEAVE_SOUTHBOUND_CORS_ORIGIN`, see the root README).

`check.mjs` drives the page with Playwright and a fake camera. It waits until
southbound accepts a registration from that page, then for the node to appear in
`GET /nodes`; an entry an earlier run left there does not count:

```sh
pnpm install
node check.mjs --southbound http://127.0.0.1:8081 --token "$(weave node-token browser-1)" --serve 8000
```

`--serve` hosts this directory on loopback only, so the page is opened from the
machine running the script. Against a southbound behind TLS with a private CA,
the script needs `NODE_EXTRA_CA_CERTS` for its own polling and Chromium needs the
CA in its trust store; the bench does both (`bench/README.md`, "TLS"). `--stay` keeps the browser running afterwards, which
is how the bench hosts a node; `--headed` shows the window; `--video-only`
(or `WEAVE_BROWSER_MEDIA=video`) passes `media=video`; `--node ID` passes
`node=ID`. The script launches Playwright's full Chromium (`channel:
"chromium"`), because the headless shell never answers `getUserMedia` for the
fake devices. `--executable PATH` (or `WEAVE_BROWSER_EXECUTABLE`) launches that
Chromium binary instead; the bench image points it at Debian's `chromium`, which
encodes H264 on arm64 where Playwright's build does not.

The page carries its own copies of `weave_core::PROTOCOL_VERSION` and
`weave_core::DEVICE_TRANSPORT`, so
`check.mjs` compares them with the Rust
source before it launches anything and refuses to run when they disagree. It
also checks that hop status retains the ingress and every identified egress.
`just browser-check` (`check.mjs --check-only`) runs those checks alone, needing
only `node`: no browser, no southbound, no `pnpm install`; `just test` runs it
too. Away from the repo — the bench mounts this directory alone — the Rust
source is out of reach and the driver says so and carries on.
