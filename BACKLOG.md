# Backlog

No open items. A new item should state the evidence, what done looks like,
and any constraint that is easy to break while fixing it.

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
