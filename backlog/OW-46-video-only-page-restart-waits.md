---
id: OW-46
title: "A restarted video-only page waits out Strom's inactivity timeout"
type: bug
status: blocked
depends_on: []
assignee:
---

## Evidence

Split from OW-15. On the bench with Strom 0.6.10, a page that sends audio and
video reconnects after a restart without a `503`: Strom displaces the dead
session once it has been 2–3 s without media. A page that sends video alone does
not get that. Strom's displacement (Eyevinn/strom#753, in 0.6.9 and later) only
takes over the slot of a session that delivered audio, so the new session is
refused with `503` ("all 1 slots occupied by live sessions") until the
inactivity monitor frees the slot about 10 s after the old session stopped.
Measured with `just bench browser-up video` and `docker restart -t 0 ow-browser`;
the numbers are in OW-15's Log.

The page cannot release the old session itself: Strom's CORS layer exposes only
`mcp-session-id`, so the `Location` of the WHIP `201` is unreadable in a browser
and there is nothing to `DELETE`. Upstream PR Eyevinn/strom#811 exposes it; it
was open on 2026-09-15. A displacement rule that also counts video would fix it
too. Both are changes to Strom.

## Done when

- [ ] A restarted page that sends video alone reconnects without a `503`.

## Unchecked

- Whether the page's retry interval (`RETRY_MS = 5000` in
  `nodes/browser/node.js`) adds to the wait: a refused POST costs 5 s before the
  next one.

## Log

- 2026-09-25: filed by claude-webrtc from OW-15. Blocked on Strom: PR #811, or
  displacement of sessions that deliver video only.
