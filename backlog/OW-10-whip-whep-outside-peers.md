---
id: OW-10
title: "WHIP and WHEP for outside peers"
type: feature
status: todo
priority: 3
depends_on: []
assignee:
branch:
pr:
---

## Evidence

WHIP and WHEP exist only as link transports the planner picks for browser
nodes. A manifest has no `whip:` or `whep:` variant for an encoder or player
open-weave does not manage.

- WHIP is [RFC 9725](https://www.rfc-editor.org/rfc/rfc9725.html) (March 2025).
  OBS has sent it since version 30, and Haivision added it to the Makito X4 in
  2025 (vendor).
- WHEP is still a draft (`draft-ietf-wish-whep-04`, June 2026).
- Broadcast use is mostly low-latency return and browser monitoring beside SRT
  contribution
  ([Dolby case study, 2024](https://optiview.dolby.com/resources/customer-stories/new-remote-production-hub-in-uk-revolutionizes-remote-broadcasting/)).

## Done when

- [ ] A manifest can take a source from an outside WHIP sender.
- [ ] A manifest can deliver a destination to an outside WHEP player.
- [ ] `GET /streams/{name}/endpoints` returns the URLs.

## Easy to break

- Strom's `whip_input` accepts H264 only and one session per endpoint (OW-13,
  OW-15). An outside sender with other codecs is a format mismatch to report.

## Log

- 2026-09-25: filed from broadcaster research.
