# Backlog

open-weave is an open-source layer 7 router for live media, offered as a free
building block. It decides which node dials which, over which standard protocol
and through which relays, and tells each node's adapter. What to route, when and
for whom belongs to an application that drives open-weave through northbound:
scheduling, bookings, feed entitlements. Media nodes and the data plane belong to
the runtimes behind the adapters: encoding, transcoding, bonding, merging
redundant copies. An item that needs open-weave to do either does not go here.
That a commercial product already does something is not a reason to leave it out.

Work items live in `backlog/`, one file each. `just board` lists them by status. This file holds the rules for working them, what is not
scheduled, and the scope guards every item keeps.

## Items

An item is `backlog/OW-<n>-<slug>.md`. Ids are never reused, and the file name
stays when the title changes. The frontmatter holds what changes as work moves:

```yaml
id: OW-2
title: "SRT hops carry no encryption"
type: feature        # bug | feature | verification
status: todo         # see Status
depends_on: []       # ids that must be done before this one starts
assignee:            # who is working on it; empty when nobody is
```

The body has these sections, in this order:

- `## Evidence`: what shows the gap, with file paths or sources. Links in items
  filed on 2026-09-25 come from web research on broadcaster use; vendor sources
  are named as such.
- `## Done when`: checkboxes. The item is done when every box is ticked.
- `## Easy to break`: constraints a fix can violate. Optional.
- `## Unchecked`: open questions nobody has verified. Optional.
- `## Log`: dated lines, newest last. Add lines; do not edit old ones.

Status lives in the frontmatter only. Nothing else in the repo repeats it.

## Status

| Status | Meaning |
|---|---|
| `todo` | Not started. Ready once every `depends_on` item is `done`. |
| `in-progress` | Has an `assignee`. |
| `blocked` | Cannot move. The last Log line says on what. |
| `done` | Every Done-when box is ticked and the work is committed to `main`. |
| `dropped` | Will not be done. The last Log line says why. |

## Working an item

Work is committed straight to `main`; there are no branches or pull requests.

1. Take a `todo` item whose dependencies are all `done`.
2. Set `status: in-progress` and `assignee`, and add a Log line.
3. Tick a Done-when box only once it has been checked, and add a Log line saying
   how: the command, test or bench run, and what it showed. Say whether a claim
   rests on unit tests or on `bench/`.
4. Before each commit, `just fmt-check`, `just lint` and `just test` pass, docs
   the change made false are fixed, and `contracts/` is regenerated if a route
   or wire type changed.
5. The commit that ticks the last box also sets `status: done`. `git log` on the
   item file finds the commits that worked it.

A gap found along the way becomes a new item with the next free id and
`status: todo`; it does not widen the item being worked. A Done-when box that
turns out to be wrong is changed with a Log line saying why.

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
- Do not implement anything under "Not scheduled" as a side effect of a
  backlog item.
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
