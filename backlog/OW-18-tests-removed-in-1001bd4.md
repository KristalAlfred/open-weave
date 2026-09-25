---
id: OW-18
title: "Tests removed with the stable-destinations change were not replaced"
type: verification
status: in-progress
depends_on: []
assignee: claude-tests
---

## Evidence

Commit `1001bd4` ("add stable destinations and topology-aware hop profiles")
removed most of the unit tests in five files, and nothing replaced them.
`#[test]` and `#[tokio::test]` counts before and after:

| File | Before | After |
|---|---|---|
| `crates/controller/src/main.rs` | 43 | 0 |
| `crates/controller/src/path.rs` | 65 | 7 |
| `crates/core/src/lib.rs` | 47 | 4 |
| `crates/core/src/validation.rs` | 9 | 0 |
| `crates/adapter-strom/src/main.rs` | 5 | 0 |

Among the removed controller tests: offline marking, `node.offline` emitted
once, condition transition times changing only with status, replanning off an
offline relay, and serving desired hops for an offline node. `path.rs` lost the
tests that put a relay between two dial-only NAT'd nodes, which `README.md`'s
Status section and the header of `bench/manifests/nat-relay.yaml` still cite.

## Done when

- [ ] Each removed test has been compared against current behaviour, and a
      test for every behaviour that still exists is back.
- [ ] Behaviour a removed test covered that no longer exists is listed in this
      item's Log.

## Easy to break

- Old tests encode old contracts. A test that fails against the current code
  is evidence of a regression or of a contract change; decide which before
  rewriting its assertions.

## Log

- 2026-09-25: filed from research on OW-1, OW-7, OW-8 and OW-6.
- 2026-09-25: started by claude-tests.
