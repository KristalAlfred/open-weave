---
id: OW-27
title: "`just bench stream-rm` sends no If-Match and reports every stream as not found"
type: bug
status: done
depends_on: []
assignee: claude-tests
---

## Evidence

`bench/justfile` `stream-rm` runs `curl -sf ... -X DELETE {{nb}}/streams/{{name}}`
and prints `not found: {{name}}` whenever curl fails. `DELETE /streams/{name}`
requires the current `If-Match` (`README.md`, and `crates/controller/src/main.rs`
returns `428 precondition_required` without it). On the bench on 2026-09-25,
`just bench stream-down basic` printed `not found: basic` while `basic` was
still listed by `GET /streams`; a DELETE by hand returned
`428 {"code":"precondition_required","message":"If-Match is required"}`.
`weave delete stream <name>` (`crates/cli/src/main.rs`, `delete_stream`) looks
up the ETag first and deleted the same stream.

`stream-down`, `browser-down` and `host-cam-down` go through `stream-rm`, so
none of them removes a stream.

## Done when

- [x] `just bench stream-rm <name>` deletes an existing stream and says so.
- [x] It reports a stream that does not exist as not found, and any other
      failure with the status northbound returned.

## Log

- 2026-09-25: filed by claude-bench while verifying OW-17 on the bench.
- 2026-09-25: started by claude-tests.
- 2026-09-25: `stream-rm` now reads the stream with curl, sends its `ETag` as
  `If-Match` on the DELETE, and branches on the status. Checked on `bench/`,
  built from `main` at `d25ee9a` plus this change. `just bench stream basic`
  then `stream-rm basic` printed `deleted basic`, and `GET /streams` was
  empty. A second `stream-rm basic` printed `not found: basic` and exited 0.
  With `WEAVE_NORTHBOUND_TOKEN=wrong-token` it printed `cannot read basic:
  northbound returned 401` with the error body and exited 1, and `basic` was
  still listed. On a stream owned by a stream set it printed `cannot delete
  rm-owned: northbound returned 409` with `stream_owned` and exited 1.
  `stream-down basic` printed `deleted basic` and left no streams. Both boxes
  ticked on that run.
