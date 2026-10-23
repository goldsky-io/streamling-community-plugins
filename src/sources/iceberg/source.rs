//! `iceberg_source` — SourcePlugin implementation. See the module docs in
//! `mod.rs` for configuration and semantics.

use crate::sources::iceberg::config::{
    ENV_PREFIX, IcebergSourceConfig, PLUGIN_NAME, StartPosition, parse_config,
};
use crate::sources::iceberg::reader::{Phase, Position, ReadSchema, TableReader};
use crate::sources::iceberg::snapshots::{expired, snapshots_since};
use crate::utils::iceberg::iceberg_error;
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginLabel, PluginStateBackend,
    SourcePlugin,
};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info, warn};

/// Pause before returning an empty batch once caught up, so the host's
/// polling does not busy-loop.
const BATCH_WAIT: Duration = Duration::from_millis(100);

/// Attempts for the catalog calls in `initialize`.
const INIT_ATTEMPTS: u32 = 3;
const INIT_RETRY_BASE: Duration = Duration::from_millis(500);

/// Bound on the constructor's table-schema lookup.
const SCHEMA_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// First pause after a read error; doubles per consecutive error up to the
/// poll interval (at least this much).
const ERROR_BACKOFF_MIN: Duration = Duration::from_secs(1);
/// Granularity at which a backoff sleep notices shutdown.
const SHUTDOWN_CHECK: Duration = Duration::from_millis(100);

struct ReadState {
    reader: TableReader,
    last_refresh: Instant,
    /// Pause after the last consecutive read error; `None` after a success.
    error_backoff: Option<Duration>,
}

/// The pause after a read error, given the previous one.
fn next_backoff(previous: Option<Duration>, cap: Duration) -> Duration {
    let cap = cap.max(ERROR_BACKOFF_MIN);
    previous
        .map_or(ERROR_BACKOFF_MIN, |d| d.saturating_mul(2))
        .min(cap)
}

struct RunningState {
    catalog: RestCatalog,
    read: Mutex<ReadState>,
    /// Position after every batch the host has delivered downstream — what a
    /// checkpoint marker may record. The reader's cursor also covers the
    /// batch most recently returned; it becomes visible here only at the
    /// start of the next generate_batch call — the host delivers a batch
    /// before asking for another, whereas a checkpoint marker can be
    /// processed while a batch is still queued for delivery; recording the
    /// cursor early would let such a marker checkpoint past an undelivered
    /// batch (data loss on restart instead of duplicates).
    delivered: Mutex<Option<Position>>,
    /// Position snapshots per checkpoint epoch, persisted on finalize.
    pending_epochs: Mutex<BTreeMap<u64, Position>>,
}

pub struct IcebergSource {
    config: Arc<IcebergSourceConfig>,
    schema: ReadSchema,
    state: Arc<PluginStateBackend<Position>>,
    metrics: PluginMetricsRecorder,
    inner: OnceCell<RunningState>,
    running: AtomicBool,
}

impl IcebergSource {
    pub fn new(
        _rt: PluginAsyncRuntimeObj,
        state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(options, PLUGIN_NAME, ENV_PREFIX);
        let config = parse_config(&opts).map_err(configuration_error)?;
        // The host requires the output schema before initialize(), so the
        // table schema is loaded here (blocking, scratch thread + timeout).
        let schema = load_schema_blocking(&config)
            .and_then(ReadSchema::new)
            .map_err(configuration_error)?;

        Ok(Self {
            config: Arc::new(config),
            schema,
            state: state_backend_factory.create(),
            metrics: metrics_recorder,
            inner: OnceCell::new(),
            running: AtomicBool::new(true),
        })
    }

    fn inner(&self) -> Result<&RunningState, PluginError> {
        self.inner
            .get()
            .ok_or_else(|| PluginError::Internal(format!("{PLUGIN_NAME} is not initialized")))
    }

    fn empty_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(self.schema.output())
    }

    /// Runs a catalog call with bounded retries, giving up early on shutdown.
    async fn with_retries<T, F, Fut>(&self, what: &str, mut op: F) -> Result<T, PluginError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, PluginError>>,
    {
        let mut attempt = 1;
        loop {
            match op().await {
                Ok(value) => return Ok(value),
                Err(e) if attempt < INIT_ATTEMPTS && self.is_running() => {
                    let delay = INIT_RETRY_BASE * 2u32.pow(attempt - 1);
                    warn!(attempt, error = %e, ?delay, "{PLUGIN_NAME}: {what} failed; retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn record_lag(&self, reader: &TableReader) {
        if let Some(committed_ms) = reader.last_processed_ms() {
            let lag = now_ms().saturating_sub(committed_ms).max(0);
            self.metrics
                .record_gauge("iceberg_source.snapshot_lag_ms", lag as u64);
        }
    }

    /// Reloads the table (new snapshots, refreshed storage credentials).
    /// Returns whether it succeeded; failures are retried a poll later.
    async fn refresh(&self, inner: &RunningState, read: &mut ReadState) -> bool {
        read.last_refresh = Instant::now();
        let loaded = tokio::time::timeout(
            SCHEMA_DISCOVERY_TIMEOUT,
            load_table(&inner.catalog, &self.config.table),
        )
        .await
        .unwrap_or_else(|_| {
            Err(PluginError::Execution(format!(
                "{PLUGIN_NAME}: loading table {} timed out after {SCHEMA_DISCOVERY_TIMEOUT:?}",
                self.config.table
            )))
        });
        match loaded {
            Ok(table) => {
                read.reader.set_table(table);
                true
            }
            Err(e) => {
                warn!(error = %e, "{PLUGIN_NAME}: table refresh failed; retrying next poll");
                self.metrics
                    .record_count("iceberg_source.refresh_errors", 1);
                false
            }
        }
    }

    /// Sleeps for `delay`, returning early on shutdown.
    async fn sleep_while_running(&self, delay: Duration) {
        let deadline = tokio::time::Instant::now() + delay;
        while self.is_running() {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep((deadline - now).min(SHUTDOWN_CHECK)).await;
        }
    }
}

async fn load_table(catalog: &RestCatalog, ident: &TableIdent) -> Result<Table, PluginError> {
    catalog
        .load_table(ident)
        .await
        .map_err(|e| iceberg_error(&format!("load table {ident}"), e))
}

/// Loads the table's current schema, blocking the caller. Runs on a scratch
/// thread with its own runtime so it is safe from plain and tokio threads.
fn load_schema_blocking(
    config: &IcebergSourceConfig,
) -> Result<iceberg::spec::SchemaRef, PluginError> {
    let catalog = config.catalog.clone();
    let ident = config.table.clone();
    std::thread::spawn(move || -> Result<iceberg::spec::SchemaRef, PluginError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PluginError::Internal(format!("{PLUGIN_NAME}: schema discovery runtime: {e}"))
            })?;
        rt.block_on(async move {
            tokio::time::timeout(SCHEMA_DISCOVERY_TIMEOUT, async {
                let catalog = catalog.connect().await?;
                let table = load_table(&catalog, &ident).await?;
                Ok(table.metadata().current_schema().clone())
            })
            .await
            .map_err(|_| {
                PluginError::Execution(format!(
                    "{PLUGIN_NAME}: loading the schema of {ident} timed out after \
                     {SCHEMA_DISCOVERY_TIMEOUT:?}"
                ))
            })?
        })
    })
    .join()
    .map_err(|_| PluginError::Internal(format!("{PLUGIN_NAME}: schema discovery panicked")))?
}

/// Where a source without persisted state starts.
fn initial_position(metadata: &TableMetadata, start: StartPosition) -> Option<Position> {
    let current = metadata.current_snapshot_id()?;
    Some(match start {
        StartPosition::Earliest => Position::start(current, Phase::Scan),
        StartPosition::Latest => Position::completed(current, Phase::Incremental),
    })
}

/// Fails fast when a persisted position can no longer be resumed.
fn validate_resume(metadata: &TableMetadata, position: &Position) -> Result<(), PluginError> {
    if position.complete {
        snapshots_since(metadata, Some(position.snapshot_id)).map(drop)
    } else if metadata.snapshot_by_id(position.snapshot_id).is_none() {
        Err(expired(position.snapshot_id))
    } else {
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[async_trait]
impl SupportsGracefulShutdown for IcebergSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        // REST catalog calls are stateless HTTP; nothing remote to release.
        self.running.store(false, Ordering::SeqCst);
        info!("{PLUGIN_NAME} terminated");
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for IcebergSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.inner
            .get_or_try_init(|| async {
                let catalog = self
                    .with_retries("connect to the catalog", || self.config.catalog.connect())
                    .await?;
                let table = self
                    .with_retries("load the table", || {
                        load_table(&catalog, &self.config.table)
                    })
                    .await?;

                let persisted = self.state.get().await.map_err(PluginError::State)?;
                if let Some(position) = &persisted {
                    validate_resume(table.metadata(), position)?;
                }
                let resumed = persisted.is_some();
                let cursor = persisted
                    .or_else(|| initial_position(table.metadata(), self.config.start_position));

                info!(
                    table = %self.config.table,
                    resumed,
                    position = ?cursor,
                    start_position = ?self.config.start_position,
                    poll_interval = ?self.config.poll_interval,
                    batch_size = self.config.batch_size,
                    "{PLUGIN_NAME} initialized successfully"
                );
                let reader = TableReader::new(
                    table,
                    self.schema.clone(),
                    self.config.batch_size,
                    self.config.on_non_append_snapshot,
                    cursor.clone(),
                    self.metrics.clone(),
                );
                Ok(RunningState {
                    catalog,
                    read: Mutex::new(ReadState {
                        reader,
                        last_refresh: Instant::now(),
                        error_backoff: None,
                    }),
                    delivered: Mutex::new(cursor),
                    pending_epochs: Mutex::new(BTreeMap::new()),
                })
            })
            .await?;
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.output())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        vec![PluginLabel::new("table", self.config.table.to_string())]
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let inner = self.inner()?;
        if !self.is_running() {
            return Ok(self.empty_batch());
        }
        let mut read = inner.read.lock().await;

        // The host only asks for a new batch after delivering the previous
        // one, so the cursor past it may become checkpoint-visible now.
        *inner.delivered.lock().await = read.reader.cursor().cloned();

        let mut refreshed = false;
        // Due refreshes also happen mid-read: vended storage credentials
        // expire, and a long backlog or a read error must not outlive them.
        // After a read error this retries the reload once per poll interval.
        if read.last_refresh.elapsed() >= self.config.poll_interval {
            self.refresh(inner, &mut read).await;
            refreshed = true;
        }
        loop {
            let next = match read.reader.next_batch().await {
                Ok(next) => next,
                Err(e) => {
                    // The host only logs the error and asks again at once:
                    // back off so a lasting error does not hot-loop.
                    let backoff = next_backoff(read.error_backoff, self.config.poll_interval);
                    read.error_backoff = Some(backoff);
                    drop(read);
                    self.metrics.record_count("iceberg_source.read_errors", 1);
                    warn!(error = %e, ?backoff, "{PLUGIN_NAME}: read failed; retrying after backoff");
                    self.sleep_while_running(backoff).await;
                    return Err(e);
                }
            };
            read.error_backoff = None;
            if let Some(batch) = next {
                self.metrics
                    .record_count("iceberg_source.rows_emitted", batch.num_rows() as u64);
                self.record_lag(&read.reader);
                debug!(rows = batch.num_rows(), "{PLUGIN_NAME} generated batch");
                return Ok(batch);
            }
            // Caught up and nothing undelivered: the cursor (possibly moved
            // past skipped snapshots) is checkpointable as is.
            *inner.delivered.lock().await = read.reader.cursor().cloned();
            if refreshed
                || read.last_refresh.elapsed() < self.config.poll_interval
                || !self.refresh(inner, &mut read).await
            {
                break;
            }
            refreshed = true;
        }
        self.record_lag(&read.reader);
        drop(read);
        tokio::time::sleep(BATCH_WAIT).await;
        Ok(self.empty_batch())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let inner = self.inner()?;
        if let Some(position) = inner.delivered.lock().await.clone() {
            inner.pending_epochs.lock().await.insert(epoch.0, position);
        }
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let inner = self.inner()?;
        let snapshot = {
            let mut pending = inner.pending_epochs.lock().await;
            let snapshot = pending.remove(&epoch.0);
            // Positions are monotonic; snapshots for older epochs are subsumed.
            *pending = pending.split_off(&epoch.0);
            snapshot
        };
        let Some(position) = snapshot else {
            return Ok(());
        };
        debug!(
            ?epoch,
            ?position,
            "{PLUGIN_NAME} persisting checkpoint position"
        );
        self.state.put(position).await.map_err(PluginError::State)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::iceberg::snapshots::tests::metadata;
    use crate::utils::test_support;
    use iceberg::spec::Operation;
    use streamling_plugin::r#async::DirectTokioProxy;

    #[test]
    fn constructor_rejects_invalid_options_before_any_io() {
        let err = IcebergSource::new(
            DirectTokioProxy::new().into_async_runtime_obj(),
            test_support::state_backend_factory("test_iceberg_source"),
            test_support::metrics_recorder(),
            HashMap::from([("catalog_uri".to_string(), "http://127.0.0.1:1".to_string())]),
        )
        .err()
        .expect("must fail without table");
        assert!(format!("{err:?}").contains("table"), "got {err:?}");
    }

    #[test]
    fn initial_position_follows_start_position() {
        let md = metadata(&[
            (1, None, Operation::Append),
            (2, Some(1), Operation::Append),
        ]);
        assert_eq!(
            initial_position(&md, StartPosition::Earliest),
            Some(Position::start(2, Phase::Scan))
        );
        assert_eq!(
            initial_position(&md, StartPosition::Latest),
            Some(Position::completed(2, Phase::Incremental))
        );
        assert_eq!(
            initial_position(&metadata(&[]), StartPosition::Earliest),
            None
        );
    }

    #[test]
    fn resume_from_expired_snapshot_fails() {
        let md = metadata(&[(2, None, Operation::Append)]);
        assert!(validate_resume(&md, &Position::start(2, Phase::Scan)).is_ok());
        assert!(validate_resume(&md, &Position::completed(2, Phase::Incremental)).is_ok());
        for position in [
            Position::start(1, Phase::Scan),
            Position::completed(1, Phase::Incremental),
        ] {
            let err = validate_resume(&md, &position).unwrap_err().to_string();
            assert!(err.contains("no longer exists"), "{err}");
        }
    }

    #[test]
    fn error_backoff_grows_to_poll_interval() {
        let cap = Duration::from_secs(5);
        let mut backoff = None;
        let delays: Vec<_> = (0..5)
            .map(|_| {
                let d = next_backoff(backoff, cap);
                backoff = Some(d);
                d.as_secs()
            })
            .collect();
        assert_eq!(delays, [1, 2, 4, 5, 5]);
        // A poll interval below the minimum still backs off by the minimum.
        assert_eq!(
            next_backoff(None, Duration::from_millis(10)),
            ERROR_BACKOFF_MIN
        );
        assert_eq!(
            next_backoff(Some(ERROR_BACKOFF_MIN), Duration::from_millis(10)),
            ERROR_BACKOFF_MIN
        );
    }
}
