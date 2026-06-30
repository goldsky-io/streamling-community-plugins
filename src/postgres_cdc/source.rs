//! `postgres_cdc_source` — SourcePlugin over an embedded supabase/etl
//! pipeline.
//!
//! ## Output
//! The output schema is the replicated table's own columns (typed, all
//! nullable) plus `_gs_op` — no CDC envelope. The table's columns are
//! discovered from Postgres at plugin construction; one source instance
//! replicates exactly one table.
//!
//! ## Configuration
//! See `config.rs` module docs for the full option list. Minimal:
//!
//! ```yaml
//! sources:
//!   my_cdc:
//!     type: postgres_cdc_source
//!     host: db.example.com
//!     database: app
//!     username: replicator
//!     password: ...
//!     publication_name: my_pub
//!     table: public.users
//!     slot_name: my_slot   # replication-slot group key; sources sharing it share one slot
//! ```
//!
//! ## Delivery semantics
//! At-least-once. etl acks (and therefore the slot's confirmed_flush_lsn)
//! are deferred until `process_checkpoint_finalizer`, so a crash replays
//! events that were not yet durably checkpointed downstream — sinks upsert
//! by primary key, so replays are idempotent.
//!
//! ## Throughput coupling
//! etl allows one in-flight destination batch, so deferred acks bound
//! throughput to ~2 etl batches per checkpoint epoch. Tune
//! `batch_max_bytes`/`batch_max_fill_ms` upward for high-volume tables.

use crate::postgres_cdc::arrow::{CdcRow, rows_to_record_batch};
use crate::postgres_cdc::bridge::Unit;
use crate::postgres_cdc::config::{ParsedConfig, SourceSettings, parse_options};
use crate::postgres_cdc::discovery::{build_output_schema, discover_columns_blocking};
use crate::postgres_cdc::ledger::{AckLedger, SourceAckHandle};
use crate::postgres_cdc::shared::{self, SharedPipeline, Subscription};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once, OnceLock};
use streamling_plugin::PluginInitializationError;
use streamling_plugin::api::{
    CheckpointEpoch, PluginError, PluginStateBackendFactory, SourcePlugin, SupportsGracefulShutdown,
};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use tokio::sync::Mutex;
use tracing::info;

/// rustls requires a process-level default crypto provider; etl's TLS stack
/// expects one to be installed.
static INIT_CRYPTO: Once = Once::new();

fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

struct RecvState {
    rx: tokio::sync::mpsc::Receiver<Unit>,
    /// Unit partially emitted by a previous generate_batch call.
    carry: Option<Unit>,
}

struct RunningState {
    recv: Mutex<RecvState>,
    ledger: Mutex<AckLedger<SourceAckHandle>>,
    shared: Arc<SharedPipeline>,
    source_id: u64,
}

pub struct PostgresCdcSource {
    schema: SchemaRef,
    settings: SourceSettings,
    /// Set in new() from the registry; consumed into RunningState by initialize().
    pending: Mutex<Option<(ParsedConfig, Subscription)>>,
    state: OnceLock<RunningState>,
    /// Governs the pre-initialize() window only; once initialized, is_running()
    /// delegates to the shared pipeline's health.
    running: Arc<AtomicBool>,
}

impl PostgresCdcSource {
    pub fn new(
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        _metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let parsed = parse_options(&options)
            .map_err(|e| PluginInitializationError::Configuration(e.into()))?;
        // The host requires the output schema at construction, so column
        // discovery happens here (blocking, scratch thread + 10s timeout).
        let columns = discover_columns_blocking(
            &parsed.pipeline.pg_connection,
            &parsed.settings.table_schema,
            &parsed.settings.table_name,
        )
        .map_err(|e| PluginInitializationError::Configuration(e.into()))?;
        let schema = Arc::new(
            build_output_schema(&columns)
                .map_err(|e| PluginInitializationError::Configuration(e.into()))?,
        );
        let data_columns: Vec<String> = schema.fields()[1..]
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let table_label = format!(
            "{}.{}",
            parsed.settings.table_schema, parsed.settings.table_name
        );
        let subscription = shared::register(
            &parsed.settings.slot_name,
            parsed.pipeline.clone(),
            parsed.group_identity(),
            table_label,
            data_columns,
            parsed.settings.max_buffered_units,
        )
        .map_err(|e| PluginInitializationError::Configuration(e.into()))?;

        Ok(Self {
            schema,
            settings: parsed.settings.clone(),
            pending: Mutex::new(Some((parsed, subscription))),
            state: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn state(&self) -> Result<&RunningState, PluginError> {
        self.state
            .get()
            .ok_or_else(|| PluginError::Internal("postgres_cdc_source not initialized".into()))
    }
}

#[async_trait]
impl SupportsGracefulShutdown for PostgresCdcSource {
    fn is_running(&self) -> bool {
        match self.state.get() {
            Some(state) => state.shared.is_running(),
            None => self.running.load(Ordering::Acquire),
        }
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        if let Some(state) = self.state.get() {
            // Close the unit channel so any in-flight `write_events` send in
            // etl fails promptly instead of waiting for plugin drop.
            state.recv.lock().await.rx.close();
            state.shared.deregister(state.source_id);
        }
        self.running.store(false, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for PostgresCdcSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        let (_cfg, subscription) = self.pending.lock().await.take().ok_or_else(|| {
            // `pending` is also empty when a previous initialize consumed
            // the config and then failed partway; a retry cannot succeed.
            PluginError::Internal(
                "source already initialized (or a previous initialize failed)".into(),
            )
        })?;
        install_crypto_provider();

        subscription
            .shared
            .ensure_started()
            .await
            .map_err(PluginError::Internal)?;

        let Subscription {
            source_id,
            rx,
            shared,
        } = subscription;

        self.state
            .set(RunningState {
                recv: Mutex::new(RecvState { rx, carry: None }),
                ledger: Mutex::new(AckLedger::new()),
                shared,
                source_id,
            })
            .map_err(|_| PluginError::Internal("source already initialized".into()))?;
        info!("postgres_cdc_source: initialized");
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let state = self.state()?;
        let batch_size = self.settings.batch_size;
        let mut rows: Vec<CdcRow> = Vec::new();
        let mut completed: Vec<SourceAckHandle> = Vec::new();

        {
            let mut recv = state.recv.lock().await;

            // Move up to `limit` rows out of `unit`; returns the ack when the
            // unit is exhausted, otherwise re-carries it.
            fn take_from(unit: &mut Unit, rows: &mut Vec<CdcRow>, batch_size: usize) -> bool {
                let n = (batch_size - rows.len()).min(unit.rows.len());
                rows.extend(unit.rows.drain(..n));
                unit.rows.is_empty()
            }

            if let Some(mut unit) = recv.carry.take() {
                if take_from(&mut unit, &mut rows, batch_size) {
                    completed.push(unit.ack);
                } else {
                    recv.carry = Some(unit);
                }
            }

            if rows.is_empty() && recv.carry.is_none() {
                // Wait briefly for the first unit; empty batch on timeout or
                // channel close (pipeline gone; is_running flips separately).
                let wait = std::time::Duration::from_millis(self.settings.batch_interval_ms);
                if let Ok(Some(mut unit)) = tokio::time::timeout(wait, recv.rx.recv()).await {
                    if take_from(&mut unit, &mut rows, batch_size) {
                        completed.push(unit.ack);
                    } else {
                        recv.carry = Some(unit);
                    }
                }
            }

            while rows.len() < batch_size && recv.carry.is_none() {
                match recv.rx.try_recv() {
                    Ok(mut unit) => {
                        if take_from(&mut unit, &mut rows, batch_size) {
                            completed.push(unit.ack);
                        } else {
                            recv.carry = Some(unit);
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        if rows.is_empty() {
            // Row-less units (e.g. Begin/Commit-only event batches) delivered
            // nothing; arm them at the current position so the next covering
            // checkpoint releases them.
            let mut ledger = state.ledger.lock().await;
            for ack in completed {
                ledger.unit_completed(ack);
            }
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Build the batch BEFORE touching the ledger: rows acked in the
        // ledger are considered delivered, so a failed Arrow build after
        // ledger updates would let a later finalize advance etl's
        // confirmed_flush_lsn past rows that never reached downstream.
        let row_count = rows.len() as u64;
        let schema = self.schema.clone();
        let built = tokio::task::spawn_blocking(move || rows_to_record_batch(schema, rows))
            .await
            .map_err(|e| PluginError::Internal(format!("blocking task panicked: {e}")))
            .and_then(|r| r.map_err(PluginError::ArrowError));

        match built {
            Ok(batch) => {
                // Order matters: deliveries advance the position BEFORE units
                // arm at it, so a unit whose last row is in this batch is
                // released only by a checkpoint that covers this batch.
                let mut ledger = state.ledger.lock().await;
                ledger.rows_delivered(row_count);
                for ack in completed {
                    ledger.unit_completed(ack);
                }
                Ok(batch)
            }
            Err(e) => {
                // The drained rows are lost; they must never be acked. A
                // partially drained carry would ack later even though its
                // drained rows were in this failed batch — clear it. Dropping
                // `completed` (and the carry's ack) fails etl's in-flight
                // flush, stopping the pipeline; etl replays from the last
                // confirmed LSN on restart.
                state.recv.lock().await.carry = None;
                drop(completed);
                Err(e)
            }
        }
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.state()?.ledger.lock().await.marker(epoch.0);
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let released = self.state()?.ledger.lock().await.finalize(epoch.0);
        for (shared, source_id) in released {
            shared.release(source_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::external_types::crossbeam_channel;
    use streamling_plugin::PluginStateBackendConfig;
    use streamling_plugin::r#async::DirectTokioProxy;

    fn test_state_backend() -> PluginStateBackendFactory {
        PluginStateBackendFactory::new(PluginStateBackendConfig::new(
            "test_app".to_string(),
            "test_postgres_cdc_source".to_string(),
            r#"{"backend_type": "InMemory"}"#.to_string(),
        ))
    }

    fn test_metrics() -> PluginMetricsRecorder {
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        PluginMetricsRecorder::new(sender)
    }

    #[test]
    fn constructor_rejects_missing_required_options() {
        let err = PostgresCdcSource::new(
            DirectTokioProxy::new().into_async_runtime_obj(),
            test_state_backend(),
            test_metrics(),
            HashMap::new(),
        )
        .err()
        .expect("must fail without required options");
        assert!(format!("{err:?}").contains("missing required option"));
        assert!(format!("{err:?}").contains("slot_name"));
    }

    /// With valid options the constructor proceeds to schema discovery,
    /// which needs a reachable Postgres — the e2e test covers the success
    /// path. Here: an unreachable host must fail with a discovery error,
    /// not a config error.
    #[test]
    fn constructor_with_unreachable_host_fails_at_discovery() {
        let options: HashMap<String, String> = [
            ("host", "127.0.0.1"),
            ("port", "1"), // nothing listens on port 1
            ("database", "app"),
            ("username", "u"),
            ("publication_name", "p"),
            ("table", "public.users"),
            ("slot_name", "t"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let err = PostgresCdcSource::new(
            DirectTokioProxy::new().into_async_runtime_obj(),
            test_state_backend(),
            test_metrics(),
            options,
        )
        .err()
        .expect("must fail discovery against unreachable host");
        assert!(format!("{err:?}").contains("schema discovery"));
    }
}
