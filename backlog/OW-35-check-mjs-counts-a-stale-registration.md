---
id: OW-35
title: "check.mjs reports a page registered when an earlier run registered it"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] `check.mjs` reports a registration only when this run's page made it.

## Log

- 2026-09-25: filed from the OW-4 bench run.
