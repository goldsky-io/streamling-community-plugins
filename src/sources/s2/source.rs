//! `s2_source` — SourcePlugin implementation. See the module docs in
//! `mod.rs` for configuration and semantics.

use crate::sources::s2::config::{S2SourceConfig, parse_config};
use crate::sources::s2::convert::{RecordConverter, SourceRecord};
use crate::sources::s2::reader::StreamReaders;
use crate::utils::plugin_options::PluginOptions;
use crate::utils::s2::s2_client;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginLabel, PluginStateBackend,
    SourcePlugin,
};
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// Bounded buffer of S2 read batches shared by all stream readers; when full,
/// readers stop pulling from S2 (backpressure).
const MAX_BUFFERED_BATCHES: usize = 16;

/// Max wait in generate_batch for the first record before returning an empty
/// batch.
const BATCH_WAIT: Duration = Duration::from_millis(100);

struct RecvState {
    rx: tokio::sync::mpsc::Receiver<Vec<SourceRecord>>,
    /// Records buffered ahead of the next generated batch. Records leave the
    /// carry only after a batch containing them has been built, so an errored
    /// or cancelled generate_batch retries the exact same records.
    carry: VecDeque<SourceRecord>,
    /// Positions of the batch most recently returned to the host. They become
    /// visible to checkpoint markers only at the start of the next
    /// generate_batch call — the host delivers a batch downstream before
    /// asking for another, whereas a checkpoint marker can be processed while
    /// a batch is still queued for delivery; advancing positions early would
    /// let such a marker checkpoint past an undelivered batch (data loss on
    /// restart instead of duplicates).
    pending_positions: BTreeMap<String, u64>,
}

impl RecvState {
    /// Tops up the carry from the channel until it can fill a batch or the
    /// channel is drained; if the carry is empty, waits up to `first_wait`
    /// for the next read batch.
    async fn fill(&mut self, batch_size: usize, first_wait: Duration) {
        while self.carry.len() < batch_size {
            match self.rx.try_recv() {
                Ok(records) => self.carry.extend(records),
                Err(_) => break,
            }
        }
        if self.carry.is_empty()
            && let Ok(Some(records)) = tokio::time::timeout(first_wait, self.rx.recv()).await
        {
            self.carry.extend(records);
        }
    }
}

struct RunningState {
    readers: Arc<StreamReaders>,
    recv: Mutex<RecvState>,
    /// Position snapshots per checkpoint epoch, persisted on finalize.
    pending_epochs: Mutex<BTreeMap<u64, BTreeMap<String, u64>>>,
    refresh_task: Option<JoinHandle<()>>,
}

pub struct S2Source {
    config: Arc<S2SourceConfig>,
    access_token: String,
    converter: RecordConverter,
    state: Arc<PluginStateBackend<u64>>,
    inner: OnceCell<RunningState>,
    running: AtomicBool,
}

impl S2Source {
    pub fn new(
        _rt: PluginAsyncRuntimeObj,
        state_backend_factory: PluginStateBackendFactory,
        _metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let configuration_error =
            |e: PluginError| PluginInitializationError::Configuration(e.to_string().into());

        let opts = PluginOptions::new(options, "s2_source", "STREAMLING__PLUGIN__S2_SOURCE");
        let config = parse_config(&opts).map_err(configuration_error)?;
        let access_token = opts.get_secret("access_token").ok_or_else(|| {
            PluginInitializationError::Configuration(
                "s2_source: access_token is not specified".into(),
            )
        })?;
        let converter = RecordConverter::new(&config.output).map_err(configuration_error)?;

        Ok(Self {
            config: Arc::new(config),
            access_token,
            converter,
            state: state_backend_factory.create(),
            inner: OnceCell::new(),
            running: AtomicBool::new(true),
        })
    }

    fn inner(&self) -> Result<&RunningState, PluginError> {
        self.inner
            .get()
            .ok_or_else(|| PluginError::Internal("s2_source is not initialized".to_string()))
    }
}

#[async_trait]
impl SupportsGracefulShutdown for S2Source {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(inner) = self.inner.get() {
            if let Some(task) = &inner.refresh_task {
                task.abort();
            }
            inner.readers.shutdown().await;
            inner.recv.lock().await.rx.close();
        }
        info!("s2_source terminated");
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for S2Source {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.inner
            .get_or_try_init(|| async {
                // No retry override: read sessions self-heal via the reopen
                // loop in reader.rs, while initialize-time calls should fail
                // fast instead of hanging.
                let s2 = s2_client(
                    self.access_token.clone(),
                    Duration::from_millis(self.config.request_timeout_ms),
                    self.config.endpoints.clone(),
                    None,
                )?;

                let (tx, rx) = tokio::sync::mpsc::channel(MAX_BUFFERED_BATCHES);
                let readers = Arc::new(StreamReaders::new(
                    s2.basin(self.config.basin.clone()),
                    self.config.clone(),
                    self.state.clone(),
                    tx,
                ));
                let refresh_task = readers.start().await?;

                info!(
                    basin = %self.config.basin,
                    exact_streams = self.config.streams.len(),
                    stream_prefix = ?self.config.stream_prefix,
                    start_position = ?self.config.start_position,
                    batch_size = self.config.batch_size,
                    "s2_source initialized successfully"
                );
                Ok(RunningState {
                    readers,
                    recv: Mutex::new(RecvState {
                        rx,
                        carry: VecDeque::new(),
                        pending_positions: BTreeMap::new(),
                    }),
                    pending_epochs: Mutex::new(BTreeMap::new()),
                    refresh_task,
                })
            })
            .await?;
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.converter.schema())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        vec![PluginLabel::new("basin", self.config.basin.to_string())]
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let inner = self.inner()?;
        if !self.is_running() {
            return Ok(RecordBatch::new_empty(self.converter.schema()));
        }

        let mut recv = inner.recv.lock().await;

        // The host only asks for a new batch after delivering the previous
        // one, so its positions may become checkpoint-visible now.
        let delivered = std::mem::take(&mut recv.pending_positions);
        if !delivered.is_empty() {
            inner.readers.record_emitted(&delivered).await;
        }

        recv.fill(self.config.batch_size, BATCH_WAIT).await;
        let take = recv.carry.len().min(self.config.batch_size);
        if take == 0 {
            return Ok(RecordBatch::new_empty(self.converter.schema()));
        }

        // On error the carry is left intact, so the next call retries the
        // exact same records (the source stalls on a poison record rather
        // than losing data — see `on_malformed`). No awaits from here to
        // return: once records leave the carry the batch is guaranteed to be
        // returned.
        let rows = &recv.carry.make_contiguous()[..take];
        let batch = self.converter.convert(rows)?;

        // Rows are in per-stream order, so reverse iteration sees each
        // stream's max sequence number first.
        let mut positions = BTreeMap::new();
        for row in rows.iter().rev() {
            positions
                .entry(row.stream.to_string())
                .or_insert_with(|| row.seq_num.saturating_add(1));
        }
        debug!(
            rows = batch.num_rows(),
            streams = positions.len(),
            "s2_source generated batch"
        );
        recv.carry.drain(..take);
        recv.pending_positions = positions;
        Ok(batch)
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let inner = self.inner()?;
        let snapshot = inner.readers.snapshot_positions().await;
        inner.pending_epochs.lock().await.insert(epoch.0, snapshot);
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
        let Some(mut snapshot) = snapshot else {
            return Ok(());
        };
        // Skip streams pruned since the marker (deleted in S2): persisting
        // them would resurrect state that reader pruning already removed.
        let current = inner.readers.snapshot_positions().await;
        snapshot.retain(|stream, _| current.contains_key(stream));

        futures::future::try_join_all(
            snapshot
                .iter()
                .map(|(stream, next_seq_num)| self.state.put_kv(stream, *next_seq_num)),
        )
        .await
        .map_err(PluginError::State)?;
        debug!(
            ?epoch,
            streams = snapshot.len(),
            "s2_source persisted checkpoint positions"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::external_types::crossbeam_channel;
    use bytes::Bytes;
    use streamling_plugin::PluginStateBackendConfig;
    use streamling_plugin::r#async::DirectTokioProxy;

    fn test_state_backend() -> PluginStateBackendFactory {
        PluginStateBackendFactory::new(PluginStateBackendConfig::new(
            "test_app".to_string(),
            "test_s2_source".to_string(),
            r#"{"backend_type": "InMemory"}"#.to_string(),
        ))
    }

    fn test_metrics() -> PluginMetricsRecorder {
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        PluginMetricsRecorder::new(sender)
    }

    fn new_source(options: &[(&str, &str)]) -> Result<S2Source, PluginInitializationError> {
        S2Source::new(
            DirectTokioProxy::new().into_async_runtime_obj(),
            test_state_backend(),
            test_metrics(),
            options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn constructor_rejects_missing_required_options() {
        let err = new_source(&[]).err().expect("must fail without options");
        assert!(format!("{err:?}").contains("basin"));

        let err = new_source(&[("basin", "my-basin"), ("streams", "events")])
            .err()
            .expect("must fail without access_token");
        assert!(format!("{err:?}").contains("access_token"));
    }

    #[test]
    fn constructor_builds_output_schema() {
        let source = new_source(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("access_token", "secret"),
            ("schema", "id:int64,value:string?"),
            ("include_metadata", "true"),
        ])
        .expect("valid options");
        let schema = source.output_schema().expect("schema");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "_gs_op",
                "id",
                "value",
                "_s2_stream",
                "_s2_seq_num",
                "_s2_timestamp"
            ]
        );
    }

    fn record(stream: &str, seq_num: u64) -> SourceRecord {
        SourceRecord {
            stream: Arc::from(stream),
            seq_num,
            timestamp: 0,
            headers: Vec::new(),
            body: Bytes::from_static(b"{}"),
        }
    }

    fn recv_state(rx: tokio::sync::mpsc::Receiver<Vec<SourceRecord>>) -> RecvState {
        RecvState {
            rx,
            carry: VecDeque::new(),
            pending_positions: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn fill_tops_up_the_carry_and_preserves_order() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut recv = recv_state(rx);
        tx.send((0..3).map(|i| record("a", i)).collect())
            .await
            .unwrap();
        tx.send((0..3).map(|i| record("b", i)).collect())
            .await
            .unwrap();

        recv.fill(4, Duration::from_millis(10)).await;
        assert!(recv.carry.len() >= 4);

        recv.fill(10, Duration::from_millis(10)).await;
        assert_eq!(recv.carry.len(), 6);
        let seq_nums: Vec<(String, u64)> = recv
            .carry
            .iter()
            .map(|r| (r.stream.to_string(), r.seq_num))
            .collect();
        assert_eq!(seq_nums[2], ("a".to_string(), 2));
        assert_eq!(seq_nums[3], ("b".to_string(), 0));
    }

    #[tokio::test]
    async fn fill_returns_empty_after_timeout() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Vec<SourceRecord>>(1);
        let mut recv = recv_state(rx);
        recv.fill(4, Duration::from_millis(5)).await;
        assert!(recv.carry.is_empty());
    }

    #[tokio::test]
    async fn fill_waits_for_first_records_when_carry_is_empty() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut recv = recv_state(rx);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let _ = tx.send(vec![record("a", 0)]).await;
        });
        recv.fill(4, Duration::from_millis(200)).await;
        assert_eq!(recv.carry.len(), 1);
    }
}
