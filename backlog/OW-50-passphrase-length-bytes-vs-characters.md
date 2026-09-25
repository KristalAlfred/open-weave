---
id: OW-50
title: "Passphrase validation counts bytes and the schema counts characters"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`validate_passphrase` in `crates/core/src/validation.rs` takes 10 to 80 bytes
(`Passphrase::MIN_LEN` and `MAX_LEN` in `crates/core/src/lib.rs`, libsrt's
limits), while the `#[schemars(length(...))]` on the `passphrase` fields makes
the generated JSON Schema's `minLength` and `maxLength` count characters. A
non-ASCII passphrase of at most 80 characters but more than 80 bytes passes the
schema and is refused by validation, and one of 10 bytes but fewer than 10
characters passes validation and fails the schema.

## Done when

- [x] Every passphrase the schema accepts, validation accepts, and the reverse.
- [x] `contracts/` is regenerated.

## Log

- 2026-09-25: filed from claude-transport's report; started by
  claude-security.
- 2026-09-25: a passphrase is now printable ASCII, space to `~`, in both places:
  validation refuses any other byte with the existing `invalid_characters` code
  (it refused only control characters before), and `Passphrase::PATTERN`,
  `^[ -~]*$`, is the schema's `pattern` on both `passphrase` fields. Each allowed
  character is one byte, so `minLength` and `maxLength` count what libsrt
  counts. Chosen over describing bytes in the schema, since a schema cannot
  check a byte length. A manifest with a non-ASCII passphrase that validated
  before is now refused. Unit test
  `a_passphrase_is_printable_ascii_so_its_characters_are_its_bytes` in
  `validation.rs` (40 `é`, 80 bytes, gets `invalid_characters`; tab and DEL
  too; space and `~` pass); `passphrase_length_follows_libsrt` still passes.
  `just contracts` added the pattern to the stream, stream-set, stream-plan and
  desired-hop schemas. `README.md` states the rule. Unit tests only.
