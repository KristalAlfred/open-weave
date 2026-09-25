//! Persistence for operator stream definitions, stream-set ownership, node
//! registrations with what each node last reported, and each stream's status as
//! the last tick that changed it computed it. Desired hops and endpoints are
//! derived in memory and never stored.
//!
//! Postgres also holds the controller lease. Only the controller holding it
//! writes: every write checks the lease in its own transaction.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use weave_core::{NodeRegistration, StreamDefinition, StreamStatus, protocol_compatible};

#[derive(Debug, Clone, PartialEq)]
pub struct StoredStream {
    pub spec: StreamDefinition,
    pub generation: u64,
    pub revision: u64,
    pub owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredStreamSet {
    pub owner: String,
    pub revision: u64,
    pub streams: Vec<StoredStream>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamSetWrite {
    pub stream_set: StoredStreamSet,
    pub changed: bool,
    pub deleted: Vec<String>,
    pub actions: std::collections::BTreeMap<String, StreamSetMemberAction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSetMemberAction {
    Created,
    Updated,
    Unchanged,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("connecting to database")]
    Connect(#[source] sqlx::Error),
    #[error("database query failed")]
    Query(#[source] sqlx::Error),
    #[error("decoding stored json")]
    Decode(#[source] sqlx::Error),
    #[error("stored {field} must be positive, got {value}")]
    InvalidCounter { field: &'static str, value: i64 },
    #[error("{field} exceeds the database counter range: {value}")]
    CounterTooLarge { field: &'static str, value: u64 },
    #[error("stream write precondition failed")]
    PreconditionFailed,
    #[error("stream {name:?} is owned by another workflow")]
    OwnershipConflict { name: String },
    #[error("stream set contains duplicate stream {name:?}")]
    DuplicateStreamName { name: String },
    #[error("this controller does not hold the controller lease")]
    NotLeader,
}

fn positive_counter(value: i64, field: &'static str) -> Result<u64, StoreError> {
    if value <= 0 {
        return Err(StoreError::InvalidCounter { field, value });
    }
    Ok(value as u64)
}

fn database_counter(value: u64, field: &'static str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::CounterTooLarge { field, value })
}

/// Durable state access. Everything is written through on change and hydrated
/// into memory on boot.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load_streams(&self) -> Result<Vec<StoredStream>, StoreError>;
    async fn load_stream_sets(&self) -> Result<Vec<StoredStreamSet>, StoreError>;
    async fn create_stream(&self, stream: &StreamDefinition) -> Result<StoredStream, StoreError>;
    async fn update_stream(
        &self,
        stream: &StreamDefinition,
        expected_revision: u64,
    ) -> Result<StoredStream, StoreError>;
    async fn delete_stream(&self, name: &str, expected_revision: u64) -> Result<(), StoreError>;
    async fn create_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
    ) -> Result<StreamSetWrite, StoreError>;
    async fn update_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
        expected_revision: u64,
    ) -> Result<StreamSetWrite, StoreError>;
    async fn load_nodes(&self) -> Result<Vec<NodeRegistration>, StoreError>;
    async fn upsert_node(&self, registration: &NodeRegistration) -> Result<(), StoreError>;
    async fn delete_node(&self, id: &str) -> Result<(), StoreError>;
    /// Statuses of streams that still exist.
    async fn load_stream_statuses(&self) -> Result<Vec<StreamStatus>, StoreError>;
    /// Replace the stored status of each stream named; a stream that no longer
    /// exists is skipped. A deleted stream's status goes with it.
    async fn save_stream_statuses(&self, statuses: &[StreamStatus]) -> Result<(), StoreError>;
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

/// One controller's hold on the lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTerm {
    /// One more than the previous holder's. Writes carry it as their fence.
    pub epoch: u64,
    /// When the lease was taken, in microseconds since the Unix epoch on the
    /// Postgres clock, which every controller shares.
    pub started_micros: u64,
}

/// Postgres-backed store with its schema created idempotently on connect.
pub struct PgStore {
    pool: sqlx::PgPool,
    /// The lease epoch this store holds, or 0 for none.
    epoch: AtomicU64,
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
        let store = Self {
            pool,
            epoch: AtomicU64::new(0),
        };
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
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        sqlx::query("SELECT pg_advisory_xact_lock(764704167169839731)")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS streams (name TEXT PRIMARY KEY, definition JSONB NOT NULL)",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query("CREATE SEQUENCE IF NOT EXISTS stream_revision_seq")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query("ALTER TABLE streams ADD COLUMN IF NOT EXISTS generation BIGINT")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query("ALTER TABLE streams ADD COLUMN IF NOT EXISTS revision BIGINT")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query("UPDATE streams SET generation = 1 WHERE generation IS NULL")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query(
            "UPDATE streams SET revision = nextval('stream_revision_seq') WHERE revision IS NULL",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query("ALTER TABLE streams ALTER COLUMN generation SET NOT NULL")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query("ALTER TABLE streams ALTER COLUMN revision SET NOT NULL")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query("CREATE SEQUENCE IF NOT EXISTS stream_set_revision_seq")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS stream_sets (
               owner TEXT PRIMARY KEY,
               revision BIGINT NOT NULL
             )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query(
            "ALTER TABLE streams ADD COLUMN IF NOT EXISTS owner TEXT
             REFERENCES stream_sets(owner) ON DELETE RESTRICT",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS streams_owner_idx ON streams(owner)")
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, registration JSONB NOT NULL)",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS stream_status (
               name TEXT PRIMARY KEY REFERENCES streams(name) ON DELETE CASCADE,
               status JSONB NOT NULL
             )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS controller_lease (
               id BOOLEAN PRIMARY KEY CHECK (id),
               holder TEXT NOT NULL,
               epoch BIGINT NOT NULL,
               expires_at TIMESTAMPTZ NOT NULL
             )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(())
    }

    /// Take the lease for `ttl` if nobody holds it or the holder let it
    /// expire, by the Postgres clock. `None` while another controller holds it.
    pub async fn acquire_lease(
        &self,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseTerm>, StoreError> {
        use sqlx::Row;

        let Some(row) = sqlx::query(
            "INSERT INTO controller_lease (id, holder, epoch, expires_at)
             VALUES (TRUE, $1, 1, now() + make_interval(secs => $2))
             ON CONFLICT (id) DO UPDATE SET
               holder = EXCLUDED.holder,
               epoch = controller_lease.epoch + 1,
               expires_at = EXCLUDED.expires_at
             WHERE controller_lease.expires_at <= now()
             RETURNING epoch, (extract(epoch FROM now()) * 1000000)::BIGINT AS started_micros",
        )
        .bind(holder)
        .bind(ttl.as_secs_f64())
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::Query)?
        else {
            return Ok(None);
        };
        let term = LeaseTerm {
            epoch: positive_counter(
                row.try_get::<i64, _>("epoch").map_err(StoreError::Decode)?,
                "lease epoch",
            )?,
            started_micros: positive_counter(
                row.try_get::<i64, _>("started_micros")
                    .map_err(StoreError::Decode)?,
                "lease start",
            )?,
        };
        self.epoch.store(term.epoch, Ordering::SeqCst);
        Ok(Some(term))
    }

    /// Extend the held lease by `ttl` from now. `false`, and the store holds no
    /// lease any more, once it has expired or another controller has taken it.
    pub async fn renew_lease(&self, ttl: Duration) -> Result<bool, StoreError> {
        let epoch = self.held_epoch()?;
        let renewed = sqlx::query(
            "UPDATE controller_lease SET expires_at = now() + make_interval(secs => $2)
             WHERE id AND epoch = $1 AND expires_at > now()",
        )
        .bind(epoch)
        .bind(ttl.as_secs_f64())
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?
        .rows_affected()
            == 1;
        if !renewed {
            self.drop_lease();
        }
        Ok(renewed)
    }

    /// Expire the held lease now, so a standby can take it on its next try.
    pub async fn release_lease(&self) -> Result<(), StoreError> {
        let epoch = self.epoch.swap(0, Ordering::SeqCst);
        if epoch == 0 {
            return Ok(());
        }
        sqlx::query(
            "UPDATE controller_lease SET expires_at = now()
             WHERE id AND epoch = $1 AND expires_at > now()",
        )
        .bind(database_counter(epoch, "lease epoch")?)
        .execute(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        Ok(())
    }

    /// Stop writing under the held lease without touching the row, which then
    /// expires on its own.
    pub fn drop_lease(&self) {
        self.epoch.store(0, Ordering::SeqCst);
    }

    fn held_epoch(&self) -> Result<i64, StoreError> {
        match self.epoch.load(Ordering::SeqCst) {
            0 => Err(StoreError::NotLeader),
            epoch => database_counter(epoch, "lease epoch"),
        }
    }

    /// Fail the transaction unless this store holds the current, unexpired
    /// lease. The row stays share-locked until the transaction ends, so a
    /// takeover waits for a write already past this check to commit, and the
    /// new holder then loads it.
    async fn fence(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), StoreError> {
        sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM controller_lease
             WHERE id AND epoch = $1 AND expires_at > now()
             FOR SHARE",
        )
        .bind(self.held_epoch()?)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(StoreError::Query)?
        .map(|_| ())
        .ok_or(StoreError::NotLeader)
    }

    async fn write_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
        expected_revision: Option<u64>,
    ) -> Result<StreamSetWrite, StoreError> {
        use sqlx::Row;

        let desired = streams_by_name(streams)?;
        let desired_names: Vec<String> = desired.keys().cloned().collect();
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        let (mut set_revision, created) = match expected_revision {
            None => {
                let revision = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO stream_sets (owner, revision)
                     VALUES ($1, nextval('stream_set_revision_seq'))
                     ON CONFLICT (owner) DO NOTHING
                     RETURNING revision",
                )
                .bind(owner)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(StoreError::Query)?
                .ok_or(StoreError::PreconditionFailed)?;
                (positive_counter(revision, "stream set revision")?, true)
            }
            Some(expected) => {
                let expected = database_counter(expected, "stream set revision")?;
                let revision = sqlx::query_scalar::<_, i64>(
                    "SELECT revision FROM stream_sets WHERE owner = $1 FOR UPDATE",
                )
                .bind(owner)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(StoreError::Query)?
                .filter(|revision| *revision == expected)
                .ok_or(StoreError::PreconditionFailed)?;
                (positive_counter(revision, "stream set revision")?, false)
            }
        };

        let rows = sqlx::query(
            "SELECT definition, generation, revision, owner
             FROM streams
             WHERE owner = $1 OR name = ANY($2)
             ORDER BY name
             FOR UPDATE",
        )
        .bind(owner)
        .bind(&desired_names)
        .fetch_all(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        let mut existing = std::collections::BTreeMap::new();
        for row in rows {
            let spec = row
                .try_get::<sqlx::types::Json<StreamDefinition>, _>("definition")
                .map_err(StoreError::Decode)?
                .0;
            let stored = StoredStream {
                generation: positive_counter(
                    row.try_get::<i64, _>("generation")
                        .map_err(StoreError::Decode)?,
                    "generation",
                )?,
                revision: positive_counter(
                    row.try_get::<i64, _>("revision")
                        .map_err(StoreError::Decode)?,
                    "revision",
                )?,
                owner: row
                    .try_get::<Option<String>, _>("owner")
                    .map_err(StoreError::Decode)?,
                spec,
            };
            existing.insert(stored.spec.name.clone(), stored);
        }

        for name in desired.keys() {
            if existing
                .get(name)
                .is_some_and(|stream| stream.owner.as_deref() != Some(owner))
            {
                return Err(StoreError::OwnershipConflict { name: name.clone() });
            }
        }

        let mut result: std::collections::BTreeMap<String, StoredStream> = existing
            .values()
            .filter(|stream| stream.owner.as_deref() == Some(owner))
            .map(|stream| (stream.spec.name.clone(), stream.clone()))
            .collect();
        let mut changed = created;
        let mut actions = std::collections::BTreeMap::new();
        for (name, spec) in &desired {
            match result.get(name) {
                Some(current) if current.spec == *spec => {
                    actions.insert(name.clone(), StreamSetMemberAction::Unchanged);
                }
                Some(_) => {
                    let row = sqlx::query(
                        "UPDATE streams SET
                           definition = $2,
                           generation = generation + 1,
                           revision = nextval('stream_revision_seq')
                         WHERE name = $1 AND owner = $3
                         RETURNING generation, revision, owner",
                    )
                    .bind(name)
                    .bind(sqlx::types::Json(spec))
                    .bind(owner)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(StoreError::Query)?;
                    result.insert(name.clone(), stored_stream_from_row(spec, &row)?);
                    actions.insert(name.clone(), StreamSetMemberAction::Updated);
                    changed = true;
                }
                None => {
                    let row = sqlx::query(
                        "INSERT INTO streams (name, definition, generation, revision, owner)
                         VALUES ($1, $2, 1, nextval('stream_revision_seq'), $3)
                         ON CONFLICT (name) DO NOTHING
                         RETURNING generation, revision, owner",
                    )
                    .bind(name)
                    .bind(sqlx::types::Json(spec))
                    .bind(owner)
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(StoreError::Query)?
                    .ok_or_else(|| StoreError::OwnershipConflict { name: name.clone() })?;
                    result.insert(name.clone(), stored_stream_from_row(spec, &row)?);
                    actions.insert(name.clone(), StreamSetMemberAction::Created);
                    changed = true;
                }
            }
        }

        let deleted: Vec<String> = if prune {
            result
                .keys()
                .filter(|name| !desired.contains_key(*name))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        for name in &deleted {
            sqlx::query("DELETE FROM streams WHERE name = $1 AND owner = $2")
                .bind(name)
                .bind(owner)
                .execute(&mut *transaction)
                .await
                .map_err(StoreError::Query)?;
            result.remove(name);
            changed = true;
        }
        for name in result.keys() {
            actions
                .entry(name.clone())
                .or_insert(StreamSetMemberAction::Unchanged);
        }

        if changed && !created {
            let revision = sqlx::query_scalar::<_, i64>(
                "UPDATE stream_sets SET revision = nextval('stream_set_revision_seq')
                 WHERE owner = $1
                 RETURNING revision",
            )
            .bind(owner)
            .fetch_one(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
            set_revision = positive_counter(revision, "stream set revision")?;
        }
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(StreamSetWrite {
            stream_set: StoredStreamSet {
                owner: owner.to_string(),
                revision: set_revision,
                streams: result.into_values().collect(),
            },
            changed,
            deleted,
            actions,
        })
    }
}

fn streams_by_name(
    streams: &[StreamDefinition],
) -> Result<std::collections::BTreeMap<String, StreamDefinition>, StoreError> {
    let mut by_name = std::collections::BTreeMap::new();
    for stream in streams {
        if by_name
            .insert(stream.name.clone(), stream.clone())
            .is_some()
        {
            return Err(StoreError::DuplicateStreamName {
                name: stream.name.clone(),
            });
        }
    }
    Ok(by_name)
}

#[async_trait]
impl StateStore for PgStore {
    async fn load_streams(&self) -> Result<Vec<StoredStream>, StoreError> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT definition, generation, revision, owner FROM streams ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        rows.into_iter()
            .map(|row| {
                Ok(StoredStream {
                    spec: row
                        .try_get::<sqlx::types::Json<StreamDefinition>, _>("definition")
                        .map_err(StoreError::Decode)?
                        .0,
                    generation: positive_counter(
                        row.try_get::<i64, _>("generation")
                            .map_err(StoreError::Decode)?,
                        "generation",
                    )?,
                    revision: positive_counter(
                        row.try_get::<i64, _>("revision")
                            .map_err(StoreError::Decode)?,
                        "revision",
                    )?,
                    owner: row
                        .try_get::<Option<String>, _>("owner")
                        .map_err(StoreError::Decode)?,
                })
            })
            .collect()
    }

    async fn load_stream_sets(&self) -> Result<Vec<StoredStreamSet>, StoreError> {
        use sqlx::Row;

        let rows = sqlx::query(
            "SELECT
               stream_sets.owner AS set_owner,
               stream_sets.revision AS set_revision,
               streams.definition,
               streams.generation,
               streams.revision AS stream_revision
             FROM stream_sets
             LEFT JOIN streams ON streams.owner = stream_sets.owner
             ORDER BY stream_sets.owner, streams.name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::Query)?;
        let mut sets = std::collections::BTreeMap::new();
        for row in rows {
            let owner = row
                .try_get::<String, _>("set_owner")
                .map_err(StoreError::Decode)?;
            let revision = positive_counter(
                row.try_get::<i64, _>("set_revision")
                    .map_err(StoreError::Decode)?,
                "stream set revision",
            )?;
            let stream = row
                .try_get::<Option<sqlx::types::Json<StreamDefinition>>, _>("definition")
                .map_err(StoreError::Decode)?
                .map(|spec| {
                    Ok(StoredStream {
                        spec: spec.0,
                        generation: positive_counter(
                            row.try_get::<i64, _>("generation")
                                .map_err(StoreError::Decode)?,
                            "generation",
                        )?,
                        revision: positive_counter(
                            row.try_get::<i64, _>("stream_revision")
                                .map_err(StoreError::Decode)?,
                            "revision",
                        )?,
                        owner: Some(owner.clone()),
                    })
                })
                .transpose()?;
            let set = sets
                .entry(owner.clone())
                .or_insert_with(|| StoredStreamSet {
                    owner,
                    revision,
                    streams: Vec::new(),
                });
            if let Some(stream) = stream {
                set.streams.push(stream);
            }
        }
        Ok(sets.into_values().collect())
    }

    async fn create_stream(&self, stream: &StreamDefinition) -> Result<StoredStream, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        let row = sqlx::query(
            "INSERT INTO streams (name, definition, generation, revision, owner)
             VALUES ($1, $2, 1, nextval('stream_revision_seq'), NULL)
             ON CONFLICT (name) DO NOTHING
             RETURNING generation, revision, owner",
        )
        .bind(&stream.name)
        .bind(sqlx::types::Json(stream))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        if let Some(row) = row {
            let stored = stored_stream_from_row(stream, &row)?;
            transaction.commit().await.map_err(StoreError::Query)?;
            return Ok(stored);
        }
        let owner =
            sqlx::query_scalar::<_, Option<String>>("SELECT owner FROM streams WHERE name = $1")
                .bind(&stream.name)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(StoreError::Query)?;
        match owner.flatten() {
            Some(_) => Err(StoreError::OwnershipConflict {
                name: stream.name.clone(),
            }),
            None => Err(StoreError::PreconditionFailed),
        }
    }

    async fn update_stream(
        &self,
        stream: &StreamDefinition,
        expected_revision: u64,
    ) -> Result<StoredStream, StoreError> {
        let expected_revision = database_counter(expected_revision, "revision")?;
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        let owner = sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner FROM streams WHERE name = $1 FOR UPDATE",
        )
        .bind(&stream.name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        if owner.is_some_and(|owner| owner.is_some()) {
            return Err(StoreError::OwnershipConflict {
                name: stream.name.clone(),
            });
        }
        let row = sqlx::query(
            "UPDATE streams SET
               definition = $2,
               generation = CASE WHEN definition = $2 THEN generation ELSE generation + 1 END,
               revision = CASE
                 WHEN definition = $2 THEN revision
                 ELSE nextval('stream_revision_seq')
               END
             WHERE name = $1 AND revision = $3 AND owner IS NULL
             RETURNING generation, revision, owner",
        )
        .bind(&stream.name)
        .bind(sqlx::types::Json(stream))
        .bind(expected_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(StoreError::Query)?
        .ok_or(StoreError::PreconditionFailed)?;
        let stored = stored_stream_from_row(stream, &row)?;
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(stored)
    }

    async fn delete_stream(&self, name: &str, expected_revision: u64) -> Result<(), StoreError> {
        let expected_revision = database_counter(expected_revision, "revision")?;
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        let owner = sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner FROM streams WHERE name = $1 FOR UPDATE",
        )
        .bind(name)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        if owner.is_some_and(|owner| owner.is_some()) {
            return Err(StoreError::OwnershipConflict {
                name: name.to_string(),
            });
        }
        let result =
            sqlx::query("DELETE FROM streams WHERE name = $1 AND revision = $2 AND owner IS NULL")
                .bind(name)
                .bind(expected_revision)
                .execute(&mut *transaction)
                .await
                .map_err(StoreError::Query)?;
        if result.rows_affected() == 0 {
            return Err(StoreError::PreconditionFailed);
        }
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(())
    }

    async fn create_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
    ) -> Result<StreamSetWrite, StoreError> {
        self.write_stream_set(owner, streams, prune, None).await
    }

    async fn update_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
        expected_revision: u64,
    ) -> Result<StreamSetWrite, StoreError> {
        self.write_stream_set(owner, streams, prune, Some(expected_revision))
            .await
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
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        sqlx::query(
            "INSERT INTO nodes (id, registration) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET registration = EXCLUDED.registration",
        )
        .bind(&registration.node.id)
        .bind(sqlx::types::Json(registration))
        .execute(&mut *transaction)
        .await
        .map_err(StoreError::Query)?;
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(())
    }

    async fn delete_node(&self, id: &str) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        sqlx::query("DELETE FROM nodes WHERE id = $1")
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(())
    }

    async fn load_stream_statuses(&self) -> Result<Vec<StreamStatus>, StoreError> {
        let rows: Vec<(String, sqlx::types::Json<serde_json::Value>)> =
            sqlx::query_as("SELECT name, status FROM stream_status ORDER BY name")
                .fetch_all(&self.pool)
                .await
                .map_err(StoreError::Query)?;
        Ok(rows
            .into_iter()
            .filter_map(|(name, status)| {
                serde_json::from_value(status.0)
                    .inspect_err(|error| {
                        tracing::warn!(
                            stream = %name,
                            %error,
                            "dropping a stored stream status this build cannot read; the next tick computes it afresh"
                        );
                    })
                    .ok()
            })
            .collect())
    }

    async fn save_stream_statuses(&self, statuses: &[StreamStatus]) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await.map_err(StoreError::Query)?;
        self.fence(&mut transaction).await?;
        for status in statuses {
            sqlx::query(
                "INSERT INTO stream_status (name, status)
                 SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM streams WHERE name = $1)
                 ON CONFLICT (name) DO UPDATE SET status = EXCLUDED.status",
            )
            .bind(&status.name)
            .bind(sqlx::types::Json(status))
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::Query)?;
        }
        transaction.commit().await.map_err(StoreError::Query)?;
        Ok(())
    }
}

fn stored_stream_from_row(
    stream: &StreamDefinition,
    row: &sqlx::postgres::PgRow,
) -> Result<StoredStream, StoreError> {
    use sqlx::Row;

    Ok(StoredStream {
        spec: stream.clone(),
        generation: positive_counter(
            row.try_get::<i64, _>("generation")
                .map_err(StoreError::Decode)?,
            "generation",
        )?,
        revision: positive_counter(
            row.try_get::<i64, _>("revision")
                .map_err(StoreError::Decode)?,
            "revision",
        )?,
        owner: row
            .try_get::<Option<String>, _>("owner")
            .map_err(StoreError::Decode)?,
    })
}

/// In-memory store for tests. Records mutation counts so tests can assert which
/// paths write through (register/stream upsert) and which do not (heartbeat).
#[derive(Default)]
pub struct MemStore {
    inner: std::sync::Mutex<MemInner>,
}

#[derive(Default)]
struct MemInner {
    streams: std::collections::BTreeMap<String, StoredStream>,
    next_revision: u64,
    stream_sets: std::collections::BTreeMap<String, u64>,
    next_stream_set_revision: u64,
    nodes: std::collections::BTreeMap<String, NodeRegistration>,
    stream_statuses: std::collections::BTreeMap<String, StreamStatus>,
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

    fn write_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
        expected_revision: Option<u64>,
    ) -> Result<StreamSetWrite, StoreError> {
        let desired = streams_by_name(streams)?;
        let mut inner = self.lock();
        let created = match (inner.stream_sets.get(owner), expected_revision) {
            (None, None) => true,
            (Some(current), Some(expected)) if *current == expected => false,
            _ => return Err(StoreError::PreconditionFailed),
        };
        for name in desired.keys() {
            if inner
                .streams
                .get(name)
                .is_some_and(|stream| stream.owner.as_deref() != Some(owner))
            {
                return Err(StoreError::OwnershipConflict { name: name.clone() });
            }
        }

        let deleted: Vec<String> = if prune {
            inner
                .streams
                .iter()
                .filter(|(name, stream)| {
                    stream.owner.as_deref() == Some(owner) && !desired.contains_key(*name)
                })
                .map(|(name, _)| name.clone())
                .collect()
        } else {
            Vec::new()
        };
        let specs_changed = desired.iter().any(|(name, spec)| {
            inner
                .streams
                .get(name)
                .is_none_or(|current| current.spec != *spec)
        });
        let changed = created || specs_changed || !deleted.is_empty();

        if created {
            inner.next_stream_set_revision += 1;
            let revision = inner.next_stream_set_revision;
            inner.stream_sets.insert(owner.to_string(), revision);
        }
        let mut actions = std::collections::BTreeMap::new();
        for (name, spec) in desired {
            match inner.streams.get(&name) {
                Some(current) if current.spec == spec => {
                    actions.insert(name, StreamSetMemberAction::Unchanged);
                }
                Some(current) => {
                    let generation = current.generation + 1;
                    inner.next_revision += 1;
                    let revision = inner.next_revision;
                    inner.streams.insert(
                        name.clone(),
                        StoredStream {
                            spec,
                            generation,
                            revision,
                            owner: Some(owner.to_string()),
                        },
                    );
                    actions.insert(name, StreamSetMemberAction::Updated);
                }
                None => {
                    inner.next_revision += 1;
                    let revision = inner.next_revision;
                    inner.streams.insert(
                        name.clone(),
                        StoredStream {
                            spec,
                            generation: 1,
                            revision,
                            owner: Some(owner.to_string()),
                        },
                    );
                    actions.insert(name, StreamSetMemberAction::Created);
                }
            }
        }
        for name in &deleted {
            inner.streams.remove(name);
            inner.stream_statuses.remove(name);
        }
        for (name, stream) in &inner.streams {
            if stream.owner.as_deref() == Some(owner) {
                actions
                    .entry(name.clone())
                    .or_insert(StreamSetMemberAction::Unchanged);
            }
        }
        if changed && !created {
            inner.next_stream_set_revision += 1;
            let revision = inner.next_stream_set_revision;
            inner.stream_sets.insert(owner.to_string(), revision);
        }
        let revision = inner.stream_sets[owner];
        let streams = inner
            .streams
            .values()
            .filter(|stream| stream.owner.as_deref() == Some(owner))
            .cloned()
            .collect();
        Ok(StreamSetWrite {
            stream_set: StoredStreamSet {
                owner: owner.to_string(),
                revision,
                streams,
            },
            changed,
            deleted,
            actions,
        })
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
    async fn load_streams(&self) -> Result<Vec<StoredStream>, StoreError> {
        Ok(self.lock().streams.values().cloned().collect())
    }

    async fn load_stream_sets(&self) -> Result<Vec<StoredStreamSet>, StoreError> {
        let inner = self.lock();
        Ok(inner
            .stream_sets
            .iter()
            .map(|(owner, revision)| StoredStreamSet {
                owner: owner.clone(),
                revision: *revision,
                streams: inner
                    .streams
                    .values()
                    .filter(|stream| stream.owner.as_deref() == Some(owner))
                    .cloned()
                    .collect(),
            })
            .collect())
    }

    async fn create_stream(&self, stream: &StreamDefinition) -> Result<StoredStream, StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.upsert_stream_calls += 1;
        }
        if let Some(current) = inner.streams.get(&stream.name) {
            return if current.owner.is_some() {
                Err(StoreError::OwnershipConflict {
                    name: stream.name.clone(),
                })
            } else {
                Err(StoreError::PreconditionFailed)
            };
        }
        inner.next_revision += 1;
        let stored = StoredStream {
            spec: stream.clone(),
            generation: 1,
            revision: inner.next_revision,
            owner: None,
        };
        inner.streams.insert(stream.name.clone(), stored.clone());
        Ok(stored)
    }

    async fn update_stream(
        &self,
        stream: &StreamDefinition,
        expected_revision: u64,
    ) -> Result<StoredStream, StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.upsert_stream_calls += 1;
        }
        let current = inner
            .streams
            .get(&stream.name)
            .cloned()
            .ok_or(StoreError::PreconditionFailed)?;
        if current.owner.is_some() {
            return Err(StoreError::OwnershipConflict {
                name: stream.name.clone(),
            });
        }
        if current.revision != expected_revision {
            return Err(StoreError::PreconditionFailed);
        }
        if current.spec == *stream {
            return Ok(current);
        }
        inner.next_revision += 1;
        let stored = StoredStream {
            spec: stream.clone(),
            generation: current.generation + 1,
            revision: inner.next_revision,
            owner: None,
        };
        inner.streams.insert(stream.name.clone(), stored.clone());
        Ok(stored)
    }

    async fn delete_stream(&self, name: &str, expected_revision: u64) -> Result<(), StoreError> {
        let mut inner = self.lock();
        #[cfg(test)]
        {
            inner.delete_stream_calls += 1;
        }
        let current = inner
            .streams
            .get(name)
            .ok_or(StoreError::PreconditionFailed)?;
        if current.owner.is_some() {
            return Err(StoreError::OwnershipConflict {
                name: name.to_string(),
            });
        }
        if current.revision != expected_revision {
            return Err(StoreError::PreconditionFailed);
        }
        inner.streams.remove(name);
        inner.stream_statuses.remove(name);
        Ok(())
    }

    async fn create_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
    ) -> Result<StreamSetWrite, StoreError> {
        self.write_stream_set(owner, streams, prune, None)
    }

    async fn update_stream_set(
        &self,
        owner: &str,
        streams: &[StreamDefinition],
        prune: bool,
        expected_revision: u64,
    ) -> Result<StreamSetWrite, StoreError> {
        self.write_stream_set(owner, streams, prune, Some(expected_revision))
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

    async fn delete_node(&self, id: &str) -> Result<(), StoreError> {
        self.lock().nodes.remove(id);
        Ok(())
    }

    async fn load_stream_statuses(&self) -> Result<Vec<StreamStatus>, StoreError> {
        Ok(self.lock().stream_statuses.values().cloned().collect())
    }

    async fn save_stream_statuses(&self, statuses: &[StreamStatus]) -> Result<(), StoreError> {
        let mut inner = self.lock();
        for status in statuses {
            if inner.streams.contains_key(&status.name) {
                inner
                    .stream_statuses
                    .insert(status.name.clone(), status.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use weave_core::{
        NodeCapabilities, NodeDescriptor, NodeStatus, NodeTopology, SrtEndpoint, StreamDestination,
        StreamTransport,
    };

    fn stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: None,
                passphrase: None,
            }),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("strom-node-2".to_string()),
                    remote: None,
                    via: Vec::new(),
                    format: None,
                    accepts: None,
                    network: None,
                    latency: None,
                    passphrase: None,
                }),
            }],
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
                topology: NodeTopology::default(),
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
        let created = store.create_stream(&stream("basic")).await.unwrap();
        store.create_stream(&stream("other")).await.unwrap();

        let loaded = store.load_streams().await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.iter().any(|s| s.spec.name == "basic"));
        assert_eq!(created.generation, 1);
        assert!(created.revision > 0);
        assert_eq!(created.owner, None);

        store
            .delete_stream("basic", created.revision)
            .await
            .unwrap();
        let loaded = store.load_streams().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].spec.name, "other");
        assert_eq!(store.upsert_stream_calls(), 2);
        assert_eq!(store.delete_stream_calls(), 1);
    }

    #[tokio::test]
    async fn memstore_tracks_semantic_generations_and_unique_revisions() {
        let store = MemStore::new();
        let original = stream("basic");
        let created = store.create_stream(&original).await.unwrap();
        let unchanged = store
            .update_stream(&original, created.revision)
            .await
            .unwrap();
        assert_eq!(unchanged.generation, created.generation);
        assert_eq!(unchanged.revision, created.revision);
        assert_eq!(unchanged.owner, None);

        let mut updated = original.clone();
        updated.enabled = false;
        let changed = store
            .update_stream(&updated, unchanged.revision)
            .await
            .unwrap();
        assert_eq!(changed.generation, created.generation + 1);
        assert!(changed.revision > created.revision);
        assert!(matches!(
            store.update_stream(&original, created.revision).await,
            Err(StoreError::PreconditionFailed)
        ));
        assert!(matches!(
            store.delete_stream("basic", created.revision).await,
            Err(StoreError::PreconditionFailed)
        ));

        store
            .delete_stream("basic", changed.revision)
            .await
            .unwrap();
        let recreated = store.create_stream(&original).await.unwrap();
        assert_eq!(recreated.generation, 1);
        assert!(recreated.revision > changed.revision);
    }

    #[tokio::test]
    async fn memstore_stream_sets_round_trip_in_stable_order() {
        let store = MemStore::new();
        let created = store
            .create_stream_set("production", &[stream("zulu"), stream("alpha")], true)
            .await
            .unwrap();

        assert!(created.changed);
        assert!(created.deleted.is_empty());
        assert_eq!(
            created.actions.values().copied().collect::<Vec<_>>(),
            [
                StreamSetMemberAction::Created,
                StreamSetMemberAction::Created
            ]
        );
        assert_eq!(created.stream_set.owner, "production");
        assert_eq!(created.stream_set.revision, 1);
        assert_eq!(
            created
                .stream_set
                .streams
                .iter()
                .map(|stream| stream.spec.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "zulu"]
        );
        assert!(
            created
                .stream_set
                .streams
                .iter()
                .all(|stream| stream.owner.as_deref() == Some("production"))
        );
        assert_eq!(
            store.load_stream_sets().await.unwrap(),
            [created.stream_set]
        );
    }

    #[tokio::test]
    async fn memstore_stream_set_updates_are_atomic_and_semantic() {
        let store = MemStore::new();
        let created = store
            .create_stream_set(
                "production",
                &[stream("changed"), stream("removed"), stream("same")],
                true,
            )
            .await
            .unwrap();
        let before = created
            .stream_set
            .streams
            .iter()
            .map(|stream| (stream.spec.name.clone(), stream.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut changed = stream("changed");
        changed.enabled = false;

        let updated = store
            .update_stream_set(
                "production",
                &[stream("same"), changed, stream("new")],
                true,
                created.stream_set.revision,
            )
            .await
            .unwrap();

        assert!(updated.changed);
        assert_eq!(updated.deleted, ["removed"]);
        assert_eq!(
            updated.actions,
            std::collections::BTreeMap::from([
                ("changed".to_string(), StreamSetMemberAction::Updated),
                ("new".to_string(), StreamSetMemberAction::Created),
                ("same".to_string(), StreamSetMemberAction::Unchanged),
            ])
        );
        assert!(updated.stream_set.revision > created.stream_set.revision);
        let after = updated
            .stream_set
            .streams
            .iter()
            .map(|stream| (stream.spec.name.as_str(), stream))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(after["same"].generation, before["same"].generation);
        assert_eq!(after["same"].revision, before["same"].revision);
        assert_eq!(
            after["changed"].generation,
            before["changed"].generation + 1
        );
        assert!(after["changed"].revision > before["changed"].revision);
        assert_eq!(after["new"].generation, 1);
    }

    #[tokio::test]
    async fn memstore_stream_set_noop_preserves_every_revision() {
        let store = MemStore::new();
        let created = store
            .create_stream_set("production", &[stream("basic"), stream("retained")], false)
            .await
            .unwrap();
        let replayed = store
            .update_stream_set(
                "production",
                &[stream("basic")],
                false,
                created.stream_set.revision,
            )
            .await
            .unwrap();

        assert!(!replayed.changed);
        assert!(replayed.deleted.is_empty());
        assert_eq!(
            replayed.actions,
            std::collections::BTreeMap::from([
                ("basic".to_string(), StreamSetMemberAction::Unchanged),
                ("retained".to_string(), StreamSetMemberAction::Unchanged),
            ])
        );
        assert_eq!(replayed.stream_set, created.stream_set);
    }

    #[tokio::test]
    async fn memstore_stream_set_prune_stays_inside_owner_boundary() {
        let store = MemStore::new();
        let production = store
            .create_stream_set("production", &[stream("keep"), stream("remove")], true)
            .await
            .unwrap();
        store
            .create_stream_set("staging", &[stream("staging")], true)
            .await
            .unwrap();
        store.create_stream(&stream("unowned")).await.unwrap();

        let pruned = store
            .update_stream_set(
                "production",
                &[stream("keep")],
                true,
                production.stream_set.revision,
            )
            .await
            .unwrap();

        assert_eq!(pruned.deleted, ["remove"]);
        let loaded = store.load_streams().await.unwrap();
        assert_eq!(
            loaded
                .iter()
                .map(|stream| stream.spec.name.as_str())
                .collect::<Vec<_>>(),
            ["keep", "staging", "unowned"]
        );
    }

    #[tokio::test]
    async fn memstore_stream_set_conflicts_change_nothing() {
        let store = MemStore::new();
        store.create_stream(&stream("unowned")).await.unwrap();
        let existing = store
            .create_stream_set("staging", &[stream("owned")], true)
            .await
            .unwrap();
        let before_streams = store.load_streams().await.unwrap();
        let before_sets = store.load_stream_sets().await.unwrap();

        for name in ["unowned", "owned"] {
            assert!(matches!(
                store
                    .create_stream_set("production", &[stream("new"), stream(name)], true)
                    .await,
                Err(StoreError::OwnershipConflict { name: conflict }) if conflict == name
            ));
            assert_eq!(store.load_streams().await.unwrap(), before_streams);
            assert_eq!(store.load_stream_sets().await.unwrap(), before_sets);
        }
        assert!(matches!(
            store
                .update_stream_set(
                    "staging",
                    &[stream("replacement")],
                    true,
                    existing.stream_set.revision + 1,
                )
                .await,
            Err(StoreError::PreconditionFailed)
        ));
        assert_eq!(store.load_streams().await.unwrap(), before_streams);
    }

    #[tokio::test]
    async fn memstore_rejects_duplicate_set_members_without_creating_owner() {
        let store = MemStore::new();
        assert!(matches!(
            store
                .create_stream_set("production", &[stream("basic"), stream("basic")], true)
                .await,
            Err(StoreError::DuplicateStreamName { name }) if name == "basic"
        ));
        assert!(store.load_streams().await.unwrap().is_empty());
        assert!(store.load_stream_sets().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn memstore_retains_empty_sets_and_never_reuses_stream_revisions() {
        let store = MemStore::new();
        let created = store
            .create_stream_set("production", &[stream("basic")], true)
            .await
            .unwrap();
        let original = created.stream_set.streams[0].clone();
        let emptied = store
            .update_stream_set("production", &[], true, created.stream_set.revision)
            .await
            .unwrap();
        assert!(emptied.stream_set.streams.is_empty());
        assert_eq!(store.load_stream_sets().await.unwrap().len(), 1);

        let recreated = store
            .update_stream_set(
                "production",
                &[stream("basic")],
                true,
                emptied.stream_set.revision,
            )
            .await
            .unwrap();
        assert_eq!(recreated.stream_set.streams[0].generation, 1);
        assert!(recreated.stream_set.streams[0].revision > original.revision);
    }

    #[tokio::test]
    async fn memstore_single_writes_refuse_owned_streams() {
        let store = MemStore::new();
        let created = store
            .create_stream_set("production", &[stream("basic")], true)
            .await
            .unwrap();
        let owned = &created.stream_set.streams[0];

        assert!(matches!(
            store.create_stream(&stream("basic")).await,
            Err(StoreError::OwnershipConflict { name }) if name == "basic"
        ));
        assert!(matches!(
            store.update_stream(&stream("basic"), owned.revision).await,
            Err(StoreError::OwnershipConflict { name }) if name == "basic"
        ));
        assert!(matches!(
            store.delete_stream("basic", owned.revision).await,
            Err(StoreError::OwnershipConflict { name }) if name == "basic"
        ));
        assert_eq!(
            store.load_streams().await.unwrap().as_slice(),
            std::slice::from_ref(owned)
        );
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

        store.delete_node("strom-node-1").await.unwrap();
        assert!(store.load_nodes().await.unwrap().is_empty());
    }

    /// A new, empty database on the server `DATABASE_URL` names, so ignored
    /// tests can run in parallel without sharing the lease row.
    pub(crate) async fn fresh_database() -> String {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL set for ignored test");
        let mut suffix = [0u8; 8];
        getrandom::fill(&mut suffix).unwrap();
        let name = format!(
            "weave_test_{}",
            suffix
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let admin = sqlx::PgPool::connect(&url).await.expect("connect");
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .expect("create test database");
        admin.close().await;
        let (server, _) = url.rsplit_once('/').expect("DATABASE_URL names a database");
        format!("{server}/{name}")
    }

    pub(crate) async fn leading_pg_store(url: &str) -> PgStore {
        let store = PgStore::connect(url).await.expect("connect");
        store
            .acquire_lease("test", Duration::from_secs(60))
            .await
            .expect("acquire")
            .expect("nobody else holds the lease");
        store
    }

    /// Round-trips against a real Postgres. Ignored by default so `cargo test`
    /// stays DB-free; run with `DATABASE_URL` set and `--ignored`.
    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pgstore_round_trips_streams_and_nodes() {
        let store = leading_pg_store(&fresh_database().await).await;

        let stored = store
            .create_stream(&stream("basic"))
            .await
            .expect("upsert stream");
        assert!(
            store
                .load_streams()
                .await
                .expect("load streams")
                .iter()
                .any(|s| s.spec.name == "basic")
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
        store
            .delete_node("strom-node-1")
            .await
            .expect("delete node");
        assert!(
            !store
                .load_nodes()
                .await
                .expect("load nodes")
                .iter()
                .any(|n| n.node.id == "strom-node-1")
        );

        store
            .delete_stream("basic", stored.revision)
            .await
            .expect("delete stream");
        assert!(
            !store
                .load_streams()
                .await
                .expect("load streams")
                .iter()
                .any(|s| s.spec.name == "basic")
        );
    }
    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pg_lease_has_one_holder_until_it_expires() {
        let url = fresh_database().await;
        let first = PgStore::connect(&url).await.unwrap();
        let second = PgStore::connect(&url).await.unwrap();
        let ttl = Duration::from_secs(1);

        let taken = first.acquire_lease("first", ttl).await.unwrap().unwrap();
        assert_eq!(second.acquire_lease("second", ttl).await.unwrap(), None);
        assert!(first.renew_lease(ttl).await.unwrap());
        assert_eq!(second.acquire_lease("second", ttl).await.unwrap(), None);

        tokio::time::sleep(Duration::from_millis(1200)).await;
        let takeover = second.acquire_lease("second", ttl).await.unwrap().unwrap();
        assert_eq!(takeover.epoch, taken.epoch + 1);
        assert!(takeover.started_micros > taken.started_micros);
        assert!(
            !first.renew_lease(ttl).await.unwrap(),
            "an expired lease is not renewed once another store took it"
        );
        assert!(matches!(
            first.renew_lease(ttl).await,
            Err(StoreError::NotLeader)
        ));
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pg_a_released_lease_is_taken_at_once() {
        let url = fresh_database().await;
        let first = PgStore::connect(&url).await.unwrap();
        let second = PgStore::connect(&url).await.unwrap();
        let ttl = Duration::from_secs(60);

        first.acquire_lease("first", ttl).await.unwrap().unwrap();
        first.release_lease().await.unwrap();
        assert!(second.acquire_lease("second", ttl).await.unwrap().is_some());
        assert!(matches!(
            first.create_stream(&stream("basic")).await,
            Err(StoreError::NotLeader)
        ));
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pg_every_write_is_fenced_by_the_lease() {
        let url = fresh_database().await;
        let old = PgStore::connect(&url).await.unwrap();
        let new = PgStore::connect(&url).await.unwrap();
        let ttl = Duration::from_secs(1);

        old.acquire_lease("old", ttl).await.unwrap().unwrap();
        let basic = old.create_stream(&stream("basic")).await.unwrap();
        let set = old
            .create_stream_set("production", &[stream("owned")], true)
            .await
            .unwrap();
        old.upsert_node(&registration("strom-node-1"))
            .await
            .unwrap();
        assert!(
            matches!(
                new.create_stream(&stream("other")).await,
                Err(StoreError::NotLeader)
            ),
            "a store that never took the lease writes nothing"
        );

        tokio::time::sleep(Duration::from_millis(1200)).await;
        new.acquire_lease("new", ttl).await.unwrap().unwrap();
        let mut changed = stream("basic");
        changed.enabled = false;
        let refused = [
            old.create_stream(&stream("other")).await.err(),
            old.update_stream(&changed, basic.revision).await.err(),
            old.delete_stream("basic", basic.revision).await.err(),
            old.create_stream_set("staging", &[stream("staged")], true)
                .await
                .err(),
            old.update_stream_set("production", &[], true, set.stream_set.revision)
                .await
                .err(),
            old.upsert_node(&registration("strom-node-2")).await.err(),
            old.delete_node("strom-node-1").await.err(),
        ];
        for (index, error) in refused.into_iter().enumerate() {
            assert!(
                matches!(error, Some(StoreError::NotLeader)),
                "write {index}: {error:?}"
            );
        }

        assert_eq!(
            new.load_streams()
                .await
                .unwrap()
                .iter()
                .map(|stream| (stream.spec.name.as_str(), stream.revision))
                .collect::<Vec<_>>(),
            [
                ("basic", basic.revision),
                ("owned", set.stream_set.streams[0].revision)
            ]
        );
        assert_eq!(new.load_nodes().await.unwrap().len(), 1);
        new.update_stream(&changed, basic.revision).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pg_a_takeover_waits_for_a_write_past_its_fence() {
        let url = fresh_database().await;
        let old = PgStore::connect(&url).await.unwrap();
        let new = std::sync::Arc::new(PgStore::connect(&url).await.unwrap());
        let ttl = Duration::from_secs(1);
        old.acquire_lease("old", ttl).await.unwrap().unwrap();

        let mut transaction = old.pool.begin().await.unwrap();
        old.fence(&mut transaction).await.unwrap();
        sqlx::query("INSERT INTO nodes (id, registration) VALUES ('late', '{}')")
            .execute(&mut *transaction)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let takeover = tokio::spawn({
            let new = new.clone();
            async move { new.acquire_lease("new", ttl).await }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !takeover.is_finished(),
            "the takeover waits on the write's share lock"
        );

        transaction.commit().await.unwrap();
        assert!(takeover.await.unwrap().unwrap().is_some());
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM nodes")
            .fetch_all(&new.pool)
            .await
            .unwrap();
        assert_eq!(
            ids,
            ["late"],
            "the new holder reads the write it waited for"
        );
    }
    fn status_of(name: &str) -> StreamStatus {
        StreamStatus {
            name: name.to_string(),
            generation: 1,
            observed_generation: Some(1),
            status: weave_core::PathStatus::Flowing,
            nodes: Vec::new(),
            conditions: Vec::new(),
            ingress: None,
            destinations: Vec::new(),
        }
    }

    #[tokio::test]
    async fn memstore_stream_statuses_go_with_their_stream() {
        let store = MemStore::new();
        let basic = store.create_stream(&stream("basic")).await.unwrap();
        store
            .save_stream_statuses(&[status_of("basic"), status_of("gone")])
            .await
            .unwrap();
        assert_eq!(
            store.load_stream_statuses().await.unwrap(),
            [status_of("basic")]
        );
        store.delete_stream("basic", basic.revision).await.unwrap();
        assert!(store.load_stream_statuses().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn pg_stream_statuses_go_with_their_stream_and_are_fenced() {
        let url = fresh_database().await;
        let store = leading_pg_store(&url).await;
        let basic = store.create_stream(&stream("basic")).await.unwrap();
        store
            .create_stream_set("production", &[stream("owned")], true)
            .await
            .unwrap();
        store
            .save_stream_statuses(&[status_of("basic"), status_of("owned"), status_of("gone")])
            .await
            .unwrap();
        let mut flowing = status_of("basic");
        flowing.status = weave_core::PathStatus::Degraded;
        store
            .save_stream_statuses(std::slice::from_ref(&flowing))
            .await
            .unwrap();
        assert_eq!(
            store.load_stream_statuses().await.unwrap(),
            [flowing, status_of("owned")]
        );

        store.delete_stream("basic", basic.revision).await.unwrap();
        let set = store.load_stream_sets().await.unwrap();
        store
            .update_stream_set("production", &[], true, set[0].revision)
            .await
            .unwrap();
        assert!(store.load_stream_statuses().await.unwrap().is_empty());

        store.release_lease().await.unwrap();
        assert!(matches!(
            store.save_stream_statuses(&[status_of("basic")]).await,
            Err(StoreError::NotLeader)
        ));
    }
}
