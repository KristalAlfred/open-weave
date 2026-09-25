---
id: OW-4
title: "TLS has never been exercised"
type: verification
status: done
depends_on: [OW-17]
assignee: claude-auth
---

## Evidence

`README.md` says to terminate TLS at a reverse proxy, and the HTTP clients are
built with `rustls-tls`, but nothing in `bench/` or CI puts a surface behind
TLS. Nodes outside one network send bearer tokens and desired hops, which name
peer addresses, across it.

## Done when

- [x] The bench runs northbound and southbound behind TLS.
- [x] The adapter, the CLI and a browser page work against them.

## Unchecked

- Whether the services should serve TLS themselves. That is a separate decision.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: depends on OW-17, since the bench does not start until its subnets move.
- 2026-09-25: started by claude-auth.
- 2026-09-25: `bench/`: nginx (`tls` service, 10.97.25.24) terminates TLS for
  northbound (29443) and southbound (29444); plaintext ports stay. The one-shot
  `tls-certs` service generates a private CA and server certificate into
  `bench/tls/` with netshoot's openssl. reqwest gains `rustls-tls-native-roots`
  beside the webpki roots, so the CLI and the adapter trust `SSL_CERT_FILE`.
- 2026-09-25: checked on `bench/`. `just bench up`: all three adapters logged
  `southbound_url=https://10.97.25.24:8443` and registered, and the proxy log
  shows their register, heartbeat and desired requests. `just bench stream-up
  basic` reached `flowing` with `weave` applying through
  `https://localhost:29443` (`POST /streams 202` in the proxy log). `just bench
  tls-check`: curl and `weave` verify against the bench CA, curl without it
  exits 60, `weave` without `SSL_CERT_FILE` fails with `UnknownIssuer`; inside
  adapter-1 the same holds with and without `SSL_CERT_FILE`. The in-bench page
  (Chromium trusting the CA through its NSS database, filled by certutil at
  start) registered through `https://10.97.25.24:8443` and `browser-stream` got
  `browser-return` flowing (`browser-cam` stays `pending`, OW-13). The same page
  without the NSS entry logged `net::ERR_CERT_AUTHORITY_INVALID` and no request
  of it reached the proxy. Verification is never switched off. Filed OW-35
  (check.mjs counted that run as registered).
- 2026-09-25: rebased onto node 4 and controller-2, which took 10.97.25.24; the
  proxy moved to 10.97.25.25, and `tls-certs.sh` now also regenerates when the
  server certificate does not name that address. Rerun on `bench/`: the old
  certificate was replaced on `up`; adapter-1 to adapter-4 registered through
  `https://10.97.25.25:8443`; `tls-check` and `auth-check` passed as before;
  `stream-up basic` reached `flowing` and `stream-rm basic` deleted it through
  https northbound; the in-bench page registered through the proxy and
  `browser-return` reached `flowing`.
- 2026-09-25: `config/adapter-2-rist.yaml`, added on main after the rerun,
  points at the proxy like `adapter-2.yaml`; it has not been run behind TLS.
