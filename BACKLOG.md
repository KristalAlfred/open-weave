# Backlog

Ordered work items. Each states the evidence, what done looks like, and any
constraint that is easy to break while fixing it. Items are independent unless
stated otherwise; take them in order when there is no reason not to.

## 1. Northbound is missing two routes the README documents

`README.md:60` lists the operator contract as `/v1/streams`,
`/v1/streams/{name}`, `/v1/streams/{name}/endpoints`, and `/v1/status`, served
by both northbound and the controller. `crates/northbound/src/main.rs:78-88`
serves only `GET/POST /streams`, `DELETE /streams/{name}`, and `/health`.

Endpoints (`crates/controller/src/main.rs:330`) and status
(`crates/controller/src/main.rs:345`) exist on the controller alone, and
`README.md` says the controller port must not be publicly exposed. Any operator
client that needs a stream's resolved address therefore has nowhere to call.

Open question to settle first: northbound's stream routes sit behind the
northbound bearer, while `/status` on the controller is unauthenticated because
the dashboard polls it. Northbound serves no dashboard. Putting both new routes
behind the bearer is the consistent choice for that surface; it diverges from
the controller, so `README.md` needs to say which surface authenticates what.

Done when: both routes proxy to the controller the same way the existing
northbound routes do, the auth decision is applied and written into the README
table, and the proxy paths have tests.

## 2. Planning ignores node status, so a dead relay is never replaced

`pick_relay` (`crates/controller/src/path.rs:374`) filters on
`capabilities.relay`, on the node not being either endpoint, and on the default
alias being dialable. `NodeStatus` does not appear anywhere in `path.rs`.
`reconcile` (`crates/controller/src/main.rs:704`) checks `Offline` only after
planning, to label the stream `Degraded`.

So when the lowest-id relay goes offline the planner keeps selecting it, and the
stream stays down even when another eligible relay is registered and healthy.
This is a bug, not a limitation of the model: the status is already on
`NodeDescriptor` and already reaches `derive_path`.

Two things to preserve. The comment at `path.rs:371` says sorting by id keeps the
choice stable so a stream does not migrate between equally eligible relays —
that property should still hold among online nodes. And the filter belongs only
in relay selection: source and destination nodes are named in the manifest and
cannot be substituted, so an offline endpoint stays a `Degraded` report.

Done when: an offline node is excluded from relay selection, a test covers
failover to a second relay, and a test covers the existing stability property.

## 3. The Strom client sends no credentials

`crates/strom/src/client.rs` builds requests with no `Authorization` header; the
crate contains no token handling at all. A Strom instance behind a bearer token
or an OSC service token cannot be driven by `weave-adapter-strom`.

Done when: the client can present a bearer token, the adapter takes it from
config and environment the way `node.southbound_token` already works
(`crates/core/src/lib.rs:535`), and it is absent by default so an unauthenticated
local Strom keeps working.

## 4. The CLI cannot delete a stream

`Command` (`crates/cli/src/main.rs:39`) offers `apply`, `get streams`, and
`nodes`. Northbound has `DELETE /streams/{name}`. Removing a stream currently
needs a hand-rolled HTTP call.

Done when: `weave delete stream <name>` calls the existing route and reports
what happened.

## 5. The Quickstart references a recipe that does not exist

`README.md:296` lists `just run-node`. The justfile has no such recipe; the
crate it ran was removed in f904e37.

Done when: the line is gone, and the rest of the Quickstart has been run once to
confirm it works as written.

## 6. `VideoFormat` cannot express chroma subsampling

`crates/core/src/media.rs:39` carries codec, width, height, and framerate.
Whether a feed is 4:2:0 or 4:2:2 decides whether a receiving node decodes on the
GPU or falls back to CPU, which makes it the field most worth checking before a
feed is placed, and the model cannot say it.

Done when: `VideoFormat` and `VideoConstraint` carry chroma subsampling, the
mismatch report names it like any other field, and the manifest examples show it.

## Not scheduled

Listed so they are not picked up by accident.

- **Capture inputs.** `NodeCapabilities` has no way to describe an NDI, Decklink,
  or SDI input. Adapters can already report arbitrary endpoints through
  `EndpointDescriptor.metadata`, but nothing plans against it. Designing that is
  its own piece of work.
- **Transports other than SRT.** `Transport` has one variant and the planner
  assumes SRT socket roles throughout.
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
- Items 1 and 3 change contracts. Update `README.md` in the same change rather
  than leaving the documentation to a follow-up.
