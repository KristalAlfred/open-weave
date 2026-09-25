---
id: OW-62
title: "README's list of what PROTOCOL_VERSION 5 adds misses accepts and vp8"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`README.md` ("Protocol version negotiation") lists what version 5 adds to the
southbound contract. `git log -S` on `contracts/openapi/southbound.json` since
the bump to 5 (9ec4901) finds a hop profile's `accepts` and the `vp8` video
codec value, both added in 068ec79, which the list leaves out. The list names
merge (5638b2f), `rist` (ebd86a4) and `tracks` (7b2ce9d).

## Done when

- [x] The list names every field and value version 5 added to the southbound
      contract.

## Log

- 2026-09-26: filed from a read-only review of core, the proxies and the CLI;
  started by claude-security.
- 2026-09-26: `git log -S` on `contracts/openapi/southbound.json` for each
  commit after 9ec4901 that touched it: 5638b2f (`merge`, `merge_ingress`),
  068ec79 (profile `accepts`, `vp8`), ebd86a4 (`rist`), 7b2ce9d (`tracks` on a
  capture device and a desired hop), and 8604c70, 5eb8544 and 6be0d1e, which add
  the `not_leader` and `hop_id_conflict` error codes and the passphrase
  `pattern`, none of them a field an adapter reads. The list now names
  `accepts` and `vp8` and says the desired hop carries `tracks`. It is a
  bulleted list now, one line per addition.
