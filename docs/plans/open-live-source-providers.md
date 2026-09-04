# Plan: source providers in open-live (fork)

Self-contained brief for an agent working on a fork of
`github.com/Eyevinn/open-live`. Local checkout of upstream: `~/git/open-live`.

## Goal

open-live should be able to list sources that another system produces,
without knowing that system. A **source provider** is a small module that
returns source candidates; open-live materialises them as ordinary, read-only
sources so the rest of the code (assignment, activation, the studio UI) needs
no change. The first provider is for open-weave.

open-live consumes SRT by dialling: a source's `address` is copied into a
Strom `builtin.mpegtssrt_input` block's `srt_uri` at activation
(`src/lib/flow-generator.ts`, the `else` branch of the source switch). open-weave
publishes exactly that address for each stream destination
(`GET /v1/streams/{name}/endpoints` → `outputs[].url`, e.g. `srt://172.27.0.10:20003`).

## Setup

1. `gh repo fork Eyevinn/open-live --clone=false` under the user's account
   (none exists yet), add it as a remote of `~/git/open-live`, branch
   `source-providers` from `main`.
2. `pnpm install`. Node 23+, pnpm 10.33+. CI runs `pnpm run typecheck`,
   `pnpm run build`, `pnpm test` (vitest, mocks CouchDB and Strom, no
   services needed). Keep all three green.
3. Match the existing style: Fastify plugins per route file, zod schemas,
   CouchDB via `nano` with the `withTypeGuard` proxy in `src/db/index.ts`,
   ESM imports with `.js` suffixes. Comment density in this repo is high;
   do not add more than the change needs.

## Design

### Provider interface — `src/providers/types.ts`

```ts
export interface ProviderSource {
  externalId: string;            // stable within the provider
  name: string;
  streamType: 'srt' | 'efp' | 'whip';
  address: string;               // validated with srtUrl() for srt/efp
  latency?: number;
  status?: 'active' | 'inactive';
}

export interface SourceProvider {
  readonly id: string;           // e.g. 'weave'; becomes part of the source id
  list(): Promise<ProviderSource[]>;
}
```

Poll-only. No watch/subscribe API in this iteration — a provider learns about a
source by listing again.

Since `docs/plans/controller-webhooks.md` landed, weave's controller pushes node
lifecycle events (`node.registered`, `node.online`, `node.offline`) to one
configured receiver. That is a different seam: it tells an outside service a
*node* appeared, so the service can declare a stream for it over northbound. The
`weave` provider still discovers that stream's SRT output by polling.

### Registry and sync — `src/providers/registry.ts`

- `SOURCE_PROVIDERS` (comma-separated ids, default empty) selects providers;
  `SOURCE_PROVIDER_POLL_MS` (default `5000`) sets the interval. Unknown id →
  fail at startup with a clear error, like `requireEnv` does.
- Every tick, per provider: `list()`, then reconcile against CouchDB:
  - source id is `src-ext-<provider>-<slug(externalId)>`; slug keeps
    `[a-z0-9-]`, lowercased, and the whole id stays under the 128-char cap
    that `SourceAssignmentInput.sourceId` enforces.
  - create when missing; update when `name`, `address`, `streamType`,
    `latency`, or `status` changed (compare against the decrypted stored
    address); leave untouched otherwise, so CouchDB is not written every tick.
  - a provider-owned doc whose `externalId` is no longer listed → set
    `status: 'inactive'`, keep the doc. Deleting would silently break a
    production assignment (activation skips missing sources with a warning).
  - a provider that throws → log once per state change, keep the last docs
    as they are, do not mark them inactive on a single failure.
- Write through `getSourcesDb()` with `encryptAddressPassphrase` on the
  address, same as the POST route. Retry once on a 409 `_rev` conflict, the
  pattern `productions.ts` uses.
- Start from `src/main.ts` after `connectDb()`, not inside `buildServer()`, so
  tests that build the server never start a poller. Stop on shutdown.

### Data model — `src/db/types.ts`

```ts
export interface SourceDoc {
  // ...existing fields
  /** Set when a source provider owns this document. */
  provider?: { id: string; externalId: string; syncedAt: string };
}
```

`toApi` in `src/routes/sources.ts` exposes `provider` and `readOnly: true`
when present. Add both to `docs/openapi.yaml` `Source`.

### Route guards — `src/routes/sources.ts`

`PATCH` and `DELETE` on a doc with `provider` set return `409` with
`{ error: 'Source is managed by provider <id>' }`. `POST` never sets
`provider` (the field is not in `SourceInput`). `GET` is unchanged: provider
sources are listed with everyone else, which is what makes the studio UI
work without changes.

### Weave provider — `src/providers/weave.ts`

- Env: `WEAVE_NORTHBOUND_URL` (e.g. `http://localhost:29080`),
  `WEAVE_NORTHBOUND_TOKEN`. Both required when `weave` is enabled.
- `list()`:
  1. `GET {url}/v1/streams` with `Authorization: Bearer <token>` → array of
     stream definitions `{ name, enabled, source, destinations[] }`. Skip
     `enabled: false`.
  2. Per stream, `GET {url}/v1/streams/{name}/endpoints`. `200` →
     `{ ingress: {node,host,port,url}, outputs: [{node,host,port,url}] }`.
     `503` means known but unplaced, `404` unknown: skip both quietly.
  3. Per output with a non-empty `node` (an empty `node` is a remote
     destination weave dials out to, not something open-live can pull):
     `externalId = "<stream>/<index>"`, `name = <stream>` when there is one
     output, `"<stream> (<node>)"` otherwise, `streamType: 'srt'`,
     `address = "<url>?mode=caller"`, `status: 'active'`. Leave `latency`
     unset: open-live's default is 125 ms and SRT negotiates the larger of
     both ends (weave's consumer socket uses 200 ms).
- Use global `fetch` (Node 23) with a 5 s `AbortSignal.timeout`. Never log
  the token; `src/lib/log-redact.ts` may already cover Authorization headers.
- A `401` from northbound is a configuration error: log at `error` once and
  keep polling; do not crash the server.

### Config — `src/config.ts`

Add `sourceProviders: string[]` and `sourceProviderPollMs: number`. Read the
weave variables inside the weave provider so unrelated deployments never see
them. Document all four in the README env table.

## Tests (vitest, `src/__tests__/`)

- `providers-registry.test.ts`: with a fake provider and mocked
  `getSourcesDb` (`find`, `get`, `insert`), one tick creates docs with the
  expected ids and `provider` field; an unchanged second tick writes nothing;
  a changed address updates; a vanished source becomes `inactive`; a
  throwing provider leaves docs untouched.
- `providers-weave.test.ts`: mock `fetch`; maps a placed stream to one source
  per node output with `?mode=caller`, skips remote outputs, skips `503`/`404`
  streams and disabled streams, sends the bearer header.
- `sources.test.ts` (new or extended): `PATCH`/`DELETE` on a provider-owned
  doc → `409`; `GET` includes `readOnly: true` and `provider`.
- Existing tests must keep passing untouched; `activation.test.ts` mocks
  `getSourcesDb` with only `get`, so the sync must not run in `buildServer`.

## Manual check against the running weave bench

The bench on this machine exposes northbound on `localhost:29080` with token
`bench-northbound-token` (see `~/git/open-weave/bench/README.md`). CouchDB:
`docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=admin couchdb:3`.
No Strom is needed to see sources appear.

```sh
cd ~/git/open-weave/bench && just stream basic
cd ~/git/open-live && COUCHDB_URL=http://admin:admin@localhost:5984 \
  SOURCE_PROVIDERS=weave WEAVE_NORTHBOUND_URL=http://localhost:29080 \
  WEAVE_NORTHBOUND_TOKEN=bench-northbound-token pnpm dev
curl -s localhost:3000/api/v1/sources | jq
```

Expected: one source named `basic`, `streamType: srt`, address
`srt://172.27.0.10:<port>?mode=caller` with the passphrase mask untouched,
`readOnly: true`. `just stream-rm basic` in the bench → the source turns
`inactive` within a poll interval.

## Docs

- README: env table rows, a short "Source providers" section (what a provider
  is, how to enable `weave`, that provider sources are read-only).
- `docs/openapi.yaml`: `provider` and `readOnly` on `Source`; `409` on
  `PATCH`/`DELETE` sources.

## Out of scope

- An outputs provider (telling open-live where to send its programme). Same
  seam, separate task.
- Studio UI changes. The UI may still render edit controls on read-only
  sources; the API refuses the write. Note it as a follow-up.
- Writing anything back to open-weave, or health beyond `active`/`inactive`.
- Any change to activation or the flow generator.

## Commit and hand-off

Small commits with one-line subjects, no AI attribution. Leave the branch
pushed to the fork with a summary of what was verified (test output, the
manual check above) and anything that could not be verified.
