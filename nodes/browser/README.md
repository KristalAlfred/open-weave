# Browser node

A web page that is an open-weave node. Plain HTML and JavaScript, no build step.

Open `index.html` from any static server with the southbound address and token
in the URL fragment, so neither reaches a server log:

```
http://host:8000/#southbound=http://southbound:8081&token=<WEAVE_SOUTHBOUND_TOKEN>
```

The page needs a secure context for the camera: serve it over `https`, or over
`http` from `localhost` / `127.0.0.1`; on any other `http` origin the sender hop
fails with "no camera access". Add `&media=video` to send the camera without a
microphone. A capture device the OS never answers for (macOS without microphone
permission for the browser) leaves `getUserMedia` pending, and a pending request
blocks every later one in the page, so the choice is made up front rather than
by falling back.

The page generates a `browser-<8 hex>` node id per tab (`&node=<id>` pins one
instead, so a page that restarts keeps its name), registers it with
transports `whip [connect]` and `whep [connect]`, devices `capture, display`,
and an `outbound_only` data plane, then heartbeats every 5 s and polls its
desired hops every 2 s. It realises two hop shapes and nothing else:

- `device → whip connect`: the camera and microphone, sent over WHIP to the URL
  the controller planned.
- `whep connect → device`: a WHEP stream played in a `<video>` on the page.

The page shows manifests naming its node id; copy one, change the Strom node,
and apply it. Southbound must allow the page's origin
(`WEAVE_SOUTHBOUND_CORS_ORIGIN`, see the root README).

`check.mjs` drives the page with Playwright and a fake camera and waits for the
node to appear in `GET /v1/nodes`:

```sh
pnpm install
node check.mjs --southbound http://127.0.0.1:8081 --token "$WEAVE_SOUTHBOUND_TOKEN" --serve 8000
```

`--serve` hosts this directory on loopback only, so the page is opened from the
machine running the script. `--stay` keeps the browser running afterwards, which
is how the bench hosts a node; `--headed` shows the window; `--video-only`
passes `media=video`; `--node ID` passes `node=ID`. The script launches
Playwright's full Chromium (`channel: "chromium"`), because the headless shell
never answers `getUserMedia` for the fake devices.

The page carries its own copies of `weave_core::PROTOCOL_VERSION` and
`weave_core::DEVICE_TRANSPORT`, so `check.mjs` compares them with the Rust
source before it launches anything and refuses to run when they disagree.
`just browser-check` (`check.mjs --check-only`) runs that comparison alone,
needing only `node`: no browser, no southbound, no `pnpm install`; `just test`
runs it too. Away from the repo — the bench mounts this directory alone — the
Rust source is out of reach and the driver says so and carries on.
