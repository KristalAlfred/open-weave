# Plan: node lifecycle webhooks from the controller

## Goal

Let an outside service learn that a node registered, went offline, or came back,
without polling. The motivating case is a broadcast-joining service that hosts
the guest page, authenticates the guest, and — on being told the page registered
— declares a stream for that guest's seat over northbound. open-live's `weave`
provider then materialises the stream's SRT output as a source on its own poll,
so nothing downstream of northbound needs changing.

Today there is nothing to subscribe to.
`docs/plans/open-live-source-providers.md` records the state of it: *"Poll-only.
No watch/subscribe API in this iteration."*

## Why the controller

`crates/southbound/src/main.rs` is a thin proxy — `register_node` at :152 and
`node_heartbeat` at :156 both just call `proxy(..)` through to the controller.
All node state lives in the controller: `AppState` (`controller/src/main.rs:67`)
holds `nodes`, `last_seen`, `node_ttl`, `streams`, `desired` and `view`. So the
controller is the only place that can observe a lifecycle change; southbound
would have to infer it from traffic it forwards.

## Design

### Config — `crates/controller/src/main.rs`, `struct Args`

Static configuration, matching the existing `#[arg(long, env = ...)]` style:

```rust
/// Absolute URL that receives lifecycle events. Webhooks are off when unset.
#[arg(long, env = "WEAVE_WEBHOOK_URL")]
webhook_url: Option<String>,
/// Presented to the receiver as `Authorization: Bearer <token>`.
#[arg(long, env = "WEAVE_WEBHOOK_TOKEN")]
webhook_token: Option<String>,
/// Event types to deliver. Defaults to all of them.
#[arg(long, env = "WEAVE_WEBHOOK_EVENTS", value_delimiter = ',')]
webhook_events: Vec<String>,
```

One receiver, configured at start. A subscription CRUD API on northbound is the
obvious next step but is not needed to build the demo service, and it brings
persistence and auth questions with it — see Out of scope.

Bearer rather than an HMAC body signature, because it needs no new dependency
and matches how every other surface in weave authenticates
(`crates/core/src/auth.rs`: `Token`, `NORTHBOUND_TOKEN_VAR`,
`SOUTHBOUND_TOKEN_VAR`). The trade-off is that the receiver cannot distinguish a
genuine event from anyone who has learned the token, and the token travels on
every delivery. If the receiver ends up outside the trust boundary, switch to
`X-Weave-Signature: sha256=<hex>` over the raw body plus `X-Weave-Timestamp` to
bound replay, verified with `constant_time_eq` from `core/src/auth.rs:80`. That
adds `hmac` and `sha2` to the workspace.

### Event model — `crates/core/src/webhook.rs` (new)

In core so the CLI, a test receiver, and any Rust consumer share the shape:

```rust
pub struct Event {
    pub event_id: String,      // stable across retries of one delivery
    pub event_type: EventType, // node.registered | node.offline | node.online
    pub occurred_at: String,   // RFC 3339
    pub node: NodeSummary,
}

pub struct NodeSummary {
    pub id: String,
    pub status: NodeStatus,
    pub endpoint: String,
    pub capabilities: Capabilities,
}
```

`capabilities` carries `devices` and `transports`, which is what a consumer needs
to decide the node is a capture device worth declaring a stream for. The
registration body holds no secrets (`nodes/browser/node.js:59` sends id, endpoint, status,
capabilities, endpoints, hop_status), so echoing the node summary is safe.

`event_id` from a monotonic counter plus the node id avoids a `uuid` dependency;
take `uuid` instead if a globally unique id reads better to consumers.

### Emitter — `crates/controller/src/webhook.rs` (new)

- `Emitter::new(config) -> Option<Emitter>`; `None` when `webhook_url` is unset,
  so every call site is a no-op by default.
- `emit(&self, event: Event)` pushes onto a bounded `tokio::sync::mpsc` channel
  and returns immediately. **A registration must never fail or block because a
  receiver is slow or down.** On a full queue, drop the event, log once per
  transition into the dropping state, and count it — the same
  log-on-state-change discipline `open-live`'s provider registry uses.
- One background task drains the channel, so deliveries are strictly ordered.
  Per-node ordering is what matters (`node.registered` before `node.offline`) and
  a single worker gives it for free.
- Retry on connect error and 5xx with bounded exponential backoff, a small
  attempt cap, then give up and log. Do not retry 4xx — that is the receiver
  rejecting the event, not a transient fault.
- `reqwest` is already a workspace dependency (`Cargo.toml:29`, with `json` and
  `rustls-tls`); the controller does not yet pull it in, so add
  `reqwest = { workspace = true }` to `crates/controller/Cargo.toml`.

Delivery is at-least-once. That is fine for the motivating consumer, whose
action — declare the stream for this seat — is already idempotent, because
northbound `apply` upserts (`controller/src/store.rs:25 upsert_stream`).

### Hook points — `crates/controller/src/main.rs`

| Event | Where | Condition |
|---|---|---|
| `node.registered` | `register_node`, at the existing `tracing::info!(.., "node registered")` (:646) | every accepted registration, after the store write succeeds |
| `node.online` | `node_heartbeat` (:655), where `registration.node.status` is assigned | previous status was `Offline` and the new one is not |
| `node.offline` | `mark_offline` (:323), called from `reconcile_tick` (:293) | a node crosses `node_ttl` without a heartbeat |

`mark_offline` currently takes plain maps and is covered by synchronous unit
tests. Keep it pure: have it return the ids it transitioned, and let
`reconcile_tick` do the emitting. That keeps the existing tests unchanged and
the emitter out of the reconcile hot path.

A re-registration of a node already known and online emits `node.registered`
again, not `node.online`. A page reload does exactly this, and the consumer's
handler is idempotent, so that is the simpler contract.

## Tests

In `crates/controller`, which already has `tower` and `http-body-util` as
dev-dependencies for in-process HTTP:

- `Emitter::new` returns `None` with no URL configured, and every hook point is a
  no-op in that state.
- Event built from a `NodeRegistration` carries the node id, status and
  capabilities; no other registration fields leak in.
- Bearer header is set when a token is configured and absent when it is not.
- A receiver returning 500 is retried up to the cap and then abandoned; a
  receiver returning 400 is not retried.
- `register_node` still answers `202 Accepted` while the receiver is refusing
  connections — the regression that matters most.
- Queue overflow drops rather than blocks, and increments the drop count.
- `mark_offline` returns the transitioned ids, and returns none on a second call
  with no further elapsed time (no repeated `node.offline` for a node that is
  already offline).
- Event-type allowlist filters deliveries.

## Manual check against the bench

1. Add a sink. A recipe in `bench/justfile` that runs a few lines of Python
   printing method, headers and body is enough; no new image.
2. Set `WEAVE_WEBHOOK_URL` (and a token) on the `controller` service in
   `bench/docker-compose.yml`. From inside that container the host sink is
   `http://host.docker.internal:<port>`, as the weave provider's northbound URL
   already is on open-live's side.
3. `just bench up`, then `just page 8000 guest-1` and open the printed URL.
   Expect one `node.registered` for `guest-1`.
4. Reload the tab — expect a second `node.registered` for the same id, since the
   seat is pinned.
5. Close the tab and wait `WEAVE_NODE_TTL_SECS` (default 15) — expect
   `node.offline`. Reopen — expect `node.registered`.
6. Stop the sink and register again; confirm the page still registers and the
   controller logs the delivery failure without failing the request.

## Docs

- `README.md` and `bench/README.md`: the two env vars, the event list, and the
  payload shape.
- `docs/plans/open-live-source-providers.md:53`: the "no watch/subscribe API"
  note now has a qualifier — the controller pushes node lifecycle events, while
  open-live's source discovery stays poll-based.

## Out of scope

- Subscription CRUD on northbound; multiple receivers.
- Delivery durability across a controller restart — the queue is in memory.
- Stream-level events (`stream.status_changed` from `outcome.streams` in
  `reconcile_tick`). Worth adding once a consumer needs "this guest is live", but
  the reconcile tick logs a status for every stream every interval, so it needs
  change detection first or it will emit continuously.
- SSE or WebSocket streaming as an alternative transport.

## Notes for the consuming service

Webhooks alone are not a complete picture, so the demo service still needs a
reconcile path:

- **Bootstrap on start.** Events that fired before the service came up are gone.
  Call southbound `GET /v1/nodes` on boot and treat every capture-capable node as
  if it had just registered.
- **Treat events as hints, not truth.** A dropped or abandoned delivery is
  recoverable by the same reconcile. This is the property that makes an
  in-memory queue an acceptable v1.
- **Key everything on the seat.** The seat is the node id the service pins into
  the page URL with `#node=<seat>`, and it should also be the stream name, since
  open-live derives its source doc id from the stream name
  (`open-live/src/providers/registry.ts:36`). One seat, one stream,
  one source doc, one mixer input — stable across a guest rejoining.
