---
id: OW-68
title: "A controller URL without a scheme is accepted and fails every write sent to it"
type: bug
status: done
depends_on: [OW-9]
assignee: claude-ha
---

## Evidence

`Controllers::new` (`crates/core/src/upstream.rs`) kept each entry of
`WEAVE_CONTROLLER_URL` as a string. An entry such as `controller-a:8082`, with
no scheme, was accepted at start, and every request to it failed in reqwest
before connecting. Found in a review of the OW-9 code.

## Done when

- [x] A test shows what a proxy does with an entry that is not an `http` or
      `https` URL.
- [x] It refuses to start and names the entry.

## Log

- 2026-09-26: filed from a review of OW-9, and fixed by claude-ha. A probe test
  on the code before the fix listed `controller-a:8082` first and a stub
  controller second. `Controllers::new` accepted the list. A `POST` got
  `502 controller_unreachable` from a reqwest builder error, and the stub saw
  no request. A `GET` reached the stub, but only since OW-67 made a failed
  `GET` move on. Now every entry must parse with an `http` or `https` scheme
  and a host, or `Controllers::new` returns `InvalidUrl` naming it and
  northbound and southbound refuse to start.
  `every_entry_must_be_an_http_url_with_a_host` covers a schemeless entry in
  either position, another scheme, no host and no URL. Unit tests only.
