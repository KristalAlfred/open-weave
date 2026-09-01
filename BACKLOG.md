# Backlog

Ordered work items. Each states the evidence, what done looks like, and any
constraint that is easy to break while fixing it. Items are independent unless
stated otherwise; take them in order when there is no reason not to.

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
- Item 3 changes a contract. Update `README.md` in the same change rather
  than leaving the documentation to a follow-up.
