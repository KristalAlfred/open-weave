---
id: OW-35
title: "check.mjs reports a page registered when an earlier run registered it"
type: bug
status: done
depends_on: []
assignee: claude-auth
---

## Evidence

`nodes/browser/check.mjs` decides the page registered once its node id appears
in `GET /nodes`. A node stays listed after its page is gone, as `offline` until
`WEAVE_NODE_FORGET_SECS` passes. The bench page's id is fixed (it comes from its
token), so a second run finds the first run's entry. On the bench on
2026-09-25, a run whose Chromium did not trust the TLS proxy's CA logged
`net::ERR_CERT_AUTHORITY_INVALID`, sent no request the proxy logged, and still
printed `browser-bench registered with https://10.97.25.24:8443`.

## Done when

- [x] `check.mjs` reports a registration only when this run's page made it.

## Log

- 2026-09-25: filed from the OW-4 bench run.
- 2026-09-25: started by claude-auth.
- 2026-09-25: `check.mjs` now waits for a 2xx answer to its own page's
  `POST /nodes/register` (Playwright `waitForResponse`) before polling
  `GET /nodes`. No wire change. check.mjs has no test harness, so it was checked
  in the local bench browser image (which then held Debian's chromium, linked
  into Playwright's browser path) with `--network none` against a stand-in
  southbound whose `GET /nodes` always lists the page's id: with register
  answered `401`, the old check.mjs printed "registered" and exited 0, the new
  one reports that southbound accepted no registration and exits 1; with `202`
  the new one reports registered and exits 0. Not run on `bench/`.
