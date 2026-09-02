//! Persistence for the two pieces of durable controller state: operator stream
//! definitions and node registrations. Everything else (observed hop status,
//! computed desired hops, endpoints) is derived in memory and never stored.

use std::time::Duration;

use async_trait::async_trait;
use weave_core::{NodeRegistration, StreamDefinition, protocol_compatible};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("connecting to database")]
    Connect(#[source] sqlx::Error),
    #[error("database query failed")]
    Query(#[source] sqlx::Error),
    #[error("decoding stored json")]
    Decode(#[source] sqlx::Error),
}

/// Durable state access. Streams and node registrations are written through on
/// mutation and hydrated into memory on boot; nothing else is persisted.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load_streams(&self) -> Result<Vec<StreamDefinition>, StoreError>;
    async fn upsert_stream(&self, stream: &StreamDefinition) -> Result<(), StoreError>;
    async fn delete_stream(&self, name: &str) -> Result<(), StoreError>;
    async fn load_nodes(&self) -> Result<Vec<NodeRegistration>, StoreError>;
    async fn upsert_node(&self, registration: &NodeRegistration) -> Result<(), StoreError>;
}

/// Decode stored registrations, dropping any a running node will send again.
///
/// A registration is a cache of what a node reported: every node implementation
/// re-registers when a heartbeat is answered `404`, so a row this build cannot
/// read, or one hydrating a protocol version this build no longer serves,
/// costs one heartbeat interval. A stream definition has no such second copy,
/// which is why [`PgStore::load_streams`] still fails the boot.
fn decode_registrations(rows: Vec<(String, serde_json::Value)>) -> Vec<NodeRegistration> {
    rows.into_iter()
        .filter_map(
            |(id, value)| match serde_json::from_value::<NodeRegistration>(value) {
                Ok(registration) if !protocol_compatible(registration.protocol_version) => {
                    tracing::warn!(
                        node_id = %id,
                        protocol_version = registration.protocol_version,
                        "dropping a stored registration speaking a protocol version this build no longer serves; the node re-registers on its next heartbeat"
                    );
                    None
                }
                Ok(registration) => Some(registration),
                Err(error) => {
                    tracing::warn!(
                        node_id = %id,
                        %error,
                        "dropping a stored registration this build cannot read; the node re-registers on its next heartbeat"
                    );
                    None
                }
            },
        )
        .collect()
}

/// Postgres-backed store. Two JSONB tables, created idempotently on connect.
pub struct PgStore {
    pool: sqlx::PgPool,
}

impl PgStore {
    /// Connect (retrying briefly so boot survives Postgres not being ready yet)
    /// and ensure the schema exists.
    ///
    /// # Errors
    /// Returns [`StoreError`] if the connection never succeeds within the retry
    /// window or the schema DDL fails.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = Self::connect_with_retry(url, 30, Duration::from_secs(1)).await?;
        let store = Self { pool };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn connect_with_retry(
        url: &str,
        attempts: u32,
        backoff: Duration,
    ) -> Result<sqlx::PgPool, StoreError> {
        let options = sqlx::postgres::PgPoolOptions::new().max_connections(5);
        let mut last = None;
        for attempt in 1..=attempts {
            match options.clone().connect(url).await {
                Ok(pool) => return Ok(pool),
                Err(error) => {
                    tracing::warn!(attempt, "postgres not ready, retrying");
                    last = Some(error);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        Err(StoreError::Connect(last.expect("attempts >= 1")))
    }

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS streams (name TEXT PRIMARY KEY, definition JSONB NOT NULL)",
        )
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, registration JSONB NOT NULL)",
        )
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        Ok(())
    }
}

#[async_trait]
impl StateStore for PgStore {
    async fn load_streams(&self) -> Result<Vec<StreamDefinition>, StoreError> {
        use sqlx::Row;
        let rows = sqlx::query("SELECT definition FROM streams")
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::Query)?;
        rows.into_iter()
            .map(|row| {
                row.try_get::<sqlx::types::Json<StreamDefinition>, _>("definition")
                    .map(|json| json.0)
                    .map_err(StoreError::Decode)
            })
            .collect()
    }

    async fn upsert_stream(&self, stream: &StreamDefinition) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO streams (name, definition) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET definition = EXCLUDED.definition",
        )
        .bind(&stream.name)
        .bind(sqlx::types::Json(stream))
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        Ok(())
    }

    async fn delete_stream(&self, name: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM streams WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(StoreError::Query)?;
        Ok(())
    }

    async fn load_nodes(&self) -> Result<Vec<NodeRegistration>, StoreError> {
        use sqlx::Row;
        let rows = sqlx::query("SELECT id, registration FROM nodes")
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::Query)?;
        let rows = rows
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("id").map_err(StoreError::Decode)?,
                    row.try_get::<sqlx::types::Json<serde_json::Value>, _>("registration")
                        .map_err(StoreError::Decode)?
                        .0,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(decode_registrations(rows))
    }

    async fn upsert_node(&self, registration: &NodeRegistration) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO nodes (id, registration) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET registration = EXCLUDED.registration",
        )
        .bind(&registration.node.id)
        .bind(sqlx::types::Json(registration))
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        Ok(())
    }
}

/// In-memory store for tests. Records mutation counts so tests can assert which
/// paths write through (register/stream upsert) and which do not (heartbeat).
#[derive(Default)]
pub struct MemStore {
    inner: std::sync::Mutex<MemInner>,
}

#[derive(Default)]
struct MemInner {
    streams: std::collections::BTreeMap<String, StreamDefinition>,
    nodes: std::collections::BTreeMap<String, NodeRegistration>,
    #[cfg(test)]
    upsert_stream_calls: usize,
    #[cfg(test)]
    delete_stream_calls: usize,
    #[cfg(test)]
    upsert_node_calls: usize,
}

impl MemStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub fn upsert_node_calls(&self) -> usize {
        self.lock().upsert_node_calls
    }

    #[cfg(test)]
    pub fn upsert_stream_calls(&self) -> usize {
        self.lock().upsert_stream_calls
    }

    #[cfg(test)]
    pub fn delete_stream_calls(&self) -> usize {
        self.lock().delete_stream_calls
    }
}

#[async_trait]
impl StateStore for MemStore {
    async fn load_streams(&self) -> Result<Vec<StreamDefinition>, StoreError> {
        Ok(self.lock().streams.values().cloned().collect())
    }

    async fn upsert_stream(&self, stream: &StreamDefinition) -> Result<(), StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.upsert_stream_calls += 1;
        }
        inner.streams.insert(stream.name.clone(), stream.clone());
        Ok(())
    }

    async fn delete_stream(&self, name: &str) -> Result<(), StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.delete_stream_calls += 1;
        }
        inner.streams.remove(name);
        Ok(())
    }

    async fn load_nodes(&self) -> Result<Vec<NodeRegistration>, StoreError> {
        Ok(self.lock().nodes.values().cloned().collect())
    }

    async fn upsert_node(&self, registration: &NodeRegistration) -> Result<(), StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.upsert_node_calls += 1;
        }
        inner
            .nodes
            .insert(registration.node.id.clone(), registration.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{NodeCapabilities, NodeDescriptor, NodeStatus, SrtEndpoint, StreamTransport};

    fn stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: None,
            })],
        }
    }

    fn registration(id: &str) -> NodeRegistration {
        NodeRegistration {
            protocol_version: weave_core::PROTOCOL_VERSION,
            node: NodeDescriptor {
                id: id.to_string(),
                endpoint: "http://10.0.0.1:8080".to_string(),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities::default(),
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    /// Rows written before node capabilities changed shape, taken from a bench
    /// Postgres: node 1 carries the retired `webrtc_base_url`, the page carries
    /// `device` as a transport. Both were fatal at boot.
    #[test]
    fn stored_registrations_this_build_cannot_read_are_dropped() {
        let old_strom = serde_json::json!({
            "protocol_version": 1,
            "node": {
                "id": "strom-node-1",
                "endpoint": "http://172.26.0.11:8091",
                "status": "ready",
                "capabilities": {
                    "data_plane": {"default": {
                        "host": "172.26.0.10",
                        "reachability": "dialable",
                        "webrtc_base_url": "http://172.26.0.10:8080"
                    }}
                }
            }
        });
        let old_page = serde_json::json!({
            "protocol_version": 1,
            "node": {
                "id": "browser-8dc516f4",
                "endpoint": "browser://browser-8dc516f4",
                "status": "ready",
                "capabilities": {
                    "transports": [{"name": "device", "roles": ["listen", "connect"]}]
                }
            }
        });
        let current = serde_json::to_value(registration("strom-node-2")).unwrap();

        let loaded = decode_registrations(vec![
            ("strom-node-1".to_string(), old_strom),
            ("browser-8dc516f4".to_string(), old_page),
            ("strom-node-2".to_string(), current),
        ]);

        assert_eq!(
            loaded
                .iter()
                .map(|r| r.node.id.as_str())
                .collect::<Vec<_>>(),
            ["strom-node-2"],
            "the readable row survives and the boot continues"
        );
    }

    /// A row shaped exactly as a v1 adapter wrote it: it deserializes cleanly,
    /// so only the protocol-version check drops it. Left hydrated, its
    /// heartbeats are answered `202` instead of `404` and it never re-registers.
    #[test]
    fn stored_registrations_from_a_retired_protocol_version_are_dropped() {
        let v1_but_readable = serde_json::json!({
            "protocol_version": 1,
            "node": {
                "id": "strom-node-1",
                "endpoint": "http://172.26.0.11:8091",
                "status": "ready",
                "capabilities": {
                    "transports": [{"name": "srt"}]
                }
            }
        });
        let current = serde_json::to_value(registration("strom-node-2")).unwrap();

        let loaded = decode_registrations(vec![
            ("strom-node-1".to_string(), v1_but_readable),
            ("strom-node-2".to_string(), current),
        ]);

        assert_eq!(
            loaded
                .iter()
                .map(|r| r.node.id.as_str())
                .collect::<Vec<_>>(),
            ["strom-node-2"],
            "the v1 row is dropped even though this build can still parse its shape"
        );
    }

    #[tokio::test]
    async fn memstore_streams_round_trip_and_delete() {
        let store = MemStore::new();
        store.upsert_stream(&stream("basic")).await.unwrap();
        store.upsert_stream(&stream("other")).await.unwrap();

        let loaded = store.load_streams().await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.iter().any(|s| s.name == "basic"));

        store.delete_stream("basic").await.unwrap();
        let loaded = store.load_streams().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "other");
        assert_eq!(store.upsert_stream_calls(), 2);
        assert_eq!(store.delete_stream_calls(), 1);
    }

    #[tokio::test]
    async fn memstore_nodes_round_trip() {
        let store = MemStore::new();
        store
            .upsert_node(&registration("strom-node-1"))
            .await
            .unwrap();
        let loaded = store.load_nodes().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].node.id, "strom-node-1");
        assert_eq!(store.upsert_node_calls(), 1);
    }

    /// Round-trips against a real Postgres. Ignored by default so `cargo test`
    /// stays DB-free; run with `DATABASE_URL` set and `--ignored`.
    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pgstore_round_trips_streams_and_nodes() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL set for ignored test");
        let store = PgStore::connect(&url).await.expect("connect");

        store
            .upsert_stream(&stream("basic"))
            .await
            .expect("upsert stream");
        assert!(
            store
                .load_streams()
                .await
                .expect("load streams")
                .iter()
                .any(|s| s.name == "basic")
        );

        store
            .upsert_node(&registration("strom-node-1"))
            .await
            .expect("upsert node");
        assert!(
            store
                .load_nodes()
                .await
                .expect("load nodes")
                .iter()
                .any(|n| n.node.id == "strom-node-1")
        );

        store.delete_stream("basic").await.expect("delete stream");
        assert!(
            !store
                .load_streams()
                .await
                .expect("load streams")
                .iter()
                .any(|s| s.name == "basic")
        );
    }
}
