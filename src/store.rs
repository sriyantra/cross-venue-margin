//! The engine's event log in Postgres.
//!
//! The table is append-only, and the database enforces that with triggers.
//! One task writes to it, in the order the engine emitted the events, so
//! reading it back in `seq` order gives `Engine::restore` the same history.

use std::time::{Duration, Instant};

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::Json;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::engine::Event;
use crate::metrics::Metrics;

/// Most events written in one transaction.
const BATCH: usize = 256;
const RETRY_START: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("{0} does not fit a database integer")]
    OutOfRange(u64),
}

#[derive(Debug, Clone)]
pub struct EventStore {
    pool: PgPool,
}

impl EventStore {
    /// Connects and brings the schema up to date.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    /// Appends events in order, all of them or none.
    pub async fn append(&self, events: &[Event]) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        for event in events {
            sqlx::query(
                "INSERT INTO events (account_id, kind, at_ms, payload) VALUES ($1, $2, $3, $4)",
            )
            .bind(db_int(event.account().0)?)
            .bind(event.kind())
            .bind(db_int(event.at())?)
            .bind(Json(event))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Every event, in the order it was emitted.
    pub async fn load(&self) -> Result<Vec<Event>, StoreError> {
        let rows: Vec<(Json<Event>,)> = sqlx::query_as("SELECT payload FROM events ORDER BY seq")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(Json(event),)| event).collect())
    }
}

fn db_int(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::OutOfRange(value))
}

/// Writes every event from the engine to the log until the channel closes.
///
/// A failed write is retried with the same events, so nothing is skipped
/// and nothing is reordered. While it retries, the channel fills and the
/// engine waits. Once `shutdown` is cancelled a failed write is no longer
/// retried, and the events it held are reported as lost.
pub async fn run(
    store: EventStore,
    mut events: mpsc::Receiver<Event>,
    metrics: Metrics,
    shutdown: CancellationToken,
) {
    let mut batch = Vec::with_capacity(BATCH);
    while events.recv_many(&mut batch, BATCH).await > 0 {
        let mut delay = RETRY_START;
        loop {
            let started = Instant::now();
            match store.append(&batch).await {
                Ok(()) => {
                    metrics.log_written(started.elapsed());
                    debug!(count = batch.len(), "events written");
                    break;
                }
                Err(error) if shutdown.is_cancelled() => {
                    for event in &batch {
                        let event = serde_json::to_string(event).unwrap_or_default();
                        error!(%error, event, "event lost at shutdown");
                    }
                    break;
                }
                Err(error) => {
                    metrics.log_write_failed();
                    warn!(%error, count = batch.len(), retry_in = ?delay, "event write failed");
                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(RETRY_MAX);
                }
            }
        }
        batch.clear();
    }
    info!("event log stopped");
}
