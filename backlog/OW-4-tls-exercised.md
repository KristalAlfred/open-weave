---
id: OW-4
title: "TLS has never been exercised"
type: verification
status: todo
depends_on: [OW-17]
assignee:
---

## Evidence

`README.md` says to terminate TLS at a reverse proxy, and the HTTP clients are
built with `rustls-tls`, but nothing in `bench/` or CI puts a surface behind
TLS. Nodes outside one network send bearer tokens and desired hops, which name
peer addresses, across it.

## Done when

- [ ] The bench runs northbound and southbound behind TLS.
- [ ] The adapter, the CLI and a browser page work against them.

## Unchecked

- Whether the services should serve TLS themselves. That is a separate decision.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: depends on OW-17, since the bench does not start until its subnets move.
