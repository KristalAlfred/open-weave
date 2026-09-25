---
id: OW-51
title: "A write from an earlier term passes the lease fence after the lease is taken again"
type: bug
status: done
depends_on: [OW-9]
assignee: claude-ha
---

## Evidence

`PgStore` held one epoch for the whole process, and `fence`
(`crates/controller/src/store.rs`) checked each write against it. A controller
that lost the lease and took it again under a new epoch fenced a write from the
earlier term with the new epoch, so the write passed. The earlier term's
in-memory state was already dropped, and the new term had hydrated before the
write, so the write reached Postgres and the old term's reply while the leading
controller's memory still showed the state before it. Found in a review of the
OW-9 code.

## Done when

- [x] A test shows whether a request from an earlier term writes after the same
      controller takes the lease again.
- [x] If it does, it gets `503 not_leader` and writes nothing.

## Log

- 2026-09-25: filed from a review of OW-9, and fixed by claude-ha.
  `a_write_from_an_earlier_term_is_refused_after_the_lease_is_taken_again`
  (ignored, Postgres) keeps the router of a controller's first term, takes the
  lease from it through the lease row, waits until the controller takes it
  again, and sends `DELETE /streams/x` through the first term's router. Before
  the fix that answered `204`, and the stream was gone from Postgres while the
  new term still served it. Now each term writes through its own handle
  (`PgStore::for_term`), whose fence needs the process to still hold the lease
  under that term's epoch, and Postgres to agree. The delete answers
  `503 not_leader` and the stream stays. `pg_an_earlier_term_writes_nothing_once_the_lease_is_taken_again`
  covers the store alone. Run against `postgres:16` in docker.
