---
id: OW-16
title: "Browser-to-browser media has no supported Strom profile"
type: feature
status: in-progress
depends_on: []
assignee: claude-webrtc
---

## Evidence

Strom can build `whip_input → whep_output`, but the adapter reads media progress
from SRT byte counters and this shape has no SRT side. Strom therefore does not
advertise a `whip → whep` hop profile, and planning fails before desired state
is sent.

## Done when

- [ ] The adapter has a per-session media signal for a `whip → whep` flow.
- [ ] Strom advertises the `whip → whep` profile and a browser-to-browser stream
      reaches `flowing`.

## Easy to break

- Advertising the profile without that signal makes a working path read
  `degraded`.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-webrtc, box 1 first: WHIP/WHEP conditions from `webrtc-stats`.
