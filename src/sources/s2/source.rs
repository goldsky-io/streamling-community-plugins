//! `s2_source` — SourcePlugin implementation. See the module docs in
//! `mod.rs` for configuration and semantics.

use crate::sources::s2::config::{S2SourceConfig, parse_config};
use crate::sources::s2::convert::{RecordConverter, SourceRecord};
use crate::sources::s2::reader::StreamReaders;
use crate::utils::plugin_options::PluginOptions;
use crate::utils::s2::s2_endpoints;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use s2_sdk::S2;
use s2_sdk::types::S2Config;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginLabel, PluginStateBackend,
    SourcePlugin,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info};

struct RecvState {
    rx: tokio::sync::mpsc::Receiver<Vec<SourceRecord>>,
    /// Records buffered ahead of the next generated batch.
    carry: VecDeque<SourceRecord>,
}

impl RecvState {
    /// Moves up to `batch_size` records into `rows`: buffered records first,
    /// then whatever the readers already pushed; if that yields nothing,
    /// waits up to `first_wait` for the next read batch.
    async fn fill(
        &mut self,
        rows: &mut Vec<SourceRecord>,
        batch_size: usize,
        first_wait: Duration,
    ) {
        self.drain_carry(rows, batch_size);
        while rows.len() < batch_size {
            match self.rx.try_recv() {
                Ok(records) => {
                    self.carry.extend(records);
                    self.drain_carry(rows, batch_size);
                }
                Err(_) => break,
            }
        }
        if rows.is_empty()
            && let Ok(Some(records)) = tokio::time::timeout(first_wait, self.rx.recv()).await
        {
            self.carry.extend(records);
            self.drain_carry(rows, batch_size);
        }
    }

    fn drain_carry(&mut self, rows: &mut Vec<SourceRecord>, batch_size: usize) {
        let take = (batch_size - rows.len()).min(self.carry.len());
        rows.extend(self.carry.drain(..take));
    }

    /// Returns rows to the front of the buffer, preserving order, so a
    /// failed batch is retried with the same records.
    fn put_back(&mut self, rows: Vec<SourceRecord>) {
        for row in rows.into_iter().rev() {
            self.carry.push_front(row);
        }
    }
}

struct RunningState {
    readers: Arc<StreamReaders>,
    recv: Mutex<RecvState>,
    /// Position snapshots per checkpoint epoch, persisted on finalize.
    pending: Mutex<BTreeMap<u64, BTreeMap<String, u64>>>,
    refresh_task: Option<JoinHandle<()>>,
}

pub struct S2Source {
    config: Arc<S2SourceConfig>,
    access_token: String,
    converter: Arc<RecordConverter>,
    schema: SchemaRef,
    state: Arc<PluginStateBackend<u64>>,
    init: Mutex<()>,
    inner: OnceLock<RunningState>,
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
        let converter =
            Arc::new(RecordConverter::new(&config.output).map_err(configuration_error)?);

        Ok(Self {
            schema: converter.schema(),
            config: Arc::new(config),
            access_token,
            converter,
            state: state_backend_factory.create(),
            init: Mutex::new(()),
            inner: OnceLock::new(),
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
        let _guard = self.init.lock().await;
        if self.inner.get().is_some() {
            return Ok(());
        }

        // s2-sdk talks HTTP/2 over rustls; install the aws-lc-rs CryptoProvider
        // process-wide if nothing else has (install_default is idempotent).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut s2_config = S2Config::new(self.access_token.clone())
            .with_request_timeout(Duration::from_millis(self.config.request_timeout_ms));
        if let Some(endpoint) = &self.config.endpoint {
            s2_config = s2_config.with_endpoints(s2_endpoints(endpoint)?);
        }
        let s2 = S2::new(s2_config)
            .map_err(|e| PluginError::Internal(format!("failed to construct S2 client: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel(self.config.max_buffered_batches);
        let readers = Arc::new(StreamReaders::new(
            s2.basin(self.config.basin.clone()),
            self.config.clone(),
            self.state.clone(),
            tx,
        ));
        let refresh_task = readers.start().await?;

        self.inner
            .set(RunningState {
                readers,
                recv: Mutex::new(RecvState {
                    rx,
                    carry: VecDeque::new(),
                }),
                pending: Mutex::new(BTreeMap::new()),
                refresh_task,
            })
            .map_err(|_| PluginError::Internal("s2_source already initialized".to_string()))?;

        info!(
            basin = %self.config.basin,
            exact_streams = self.config.streams.len(),
            stream_prefix = ?self.config.stream_prefix,
            start_position = ?self.config.start_position,
            batch_size = self.config.batch_size,
            "s2_source initialized successfully"
        );
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        vec![PluginLabel::new("basin", self.config.basin.to_string())]
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let inner = self.inner()?;
        if !self.is_running() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let mut rows = Vec::new();
        inner
            .recv
            .lock()
            .await
            .fill(
                &mut rows,
                self.config.batch_size,
                Duration::from_millis(self.config.batch_interval_ms),
            )
            .await;
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let rows = Arc::new(rows);
        let converter = self.converter.clone();
        let convert_rows = rows.clone();
        let built = tokio::task::spawn_blocking(move || converter.convert(&convert_rows))
            .await
            .map_err(|e| PluginError::Internal(format!("s2_source: conversion task panicked: {e}")))
            .and_then(|result| result);

        match built {
            Ok(batch) => {
                // Rows are in per-stream order, so the last row per stream
                // carries its max sequence number.
                let mut updates = BTreeMap::new();
                for row in rows.iter() {
                    updates.insert(row.stream.to_string(), row.seq_num.saturating_add(1));
                }
                inner.readers.record_emitted(&updates).await;
                debug!(
                    rows = batch.num_rows(),
                    streams = updates.len(),
                    "s2_source generated batch"
                );
                Ok(batch)
            }
            Err(e) => {
                // Positions were not advanced; put the rows back so the next
                // generate_batch retries the exact same records (the source
                // stalls on a poison record rather than losing data — see
                // `on_malformed`).
                if let Ok(rows) = Arc::try_unwrap(rows) {
                    inner.recv.lock().await.put_back(rows);
                }
                Err(e)
            }
        }
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let inner = self.inner()?;
        let snapshot = inner.readers.snapshot_positions().await;
        inner.pending.lock().await.insert(epoch.0, snapshot);
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let inner = self.inner()?;
        let snapshot = {
            let mut pending = inner.pending.lock().await;
            let snapshot = pending.remove(&epoch.0);
            // Positions are monotonic; snapshots for older epochs are subsumed.
            *pending = pending.split_off(&epoch.0);
            snapshot
        };
        if let Some(snapshot) = snapshot {
            for (stream, next_seq_num) in snapshot {
                self.state
                    .put_kv(&stream, next_seq_num)
                    .await
                    .map_err(PluginError::State)?;
                debug!(
                    ?epoch,
                    stream = %stream,
                    next_seq_num,
                    "s2_source persisted checkpoint position"
                );
            }
        }
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

        let err = new_source(&[("basin", "my-basin"), ("stream", "events")])
            .err()
            .expect("must fail without access_token");
        assert!(format!("{err:?}").contains("access_token"));
    }

    #[test]
    fn constructor_builds_output_schema() {
        let source = new_source(&[
            ("basin", "my-basin"),
            ("stream", "events"),
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

    #[tokio::test]
    async fn fill_respects_batch_size_and_buffers_the_rest() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut recv = RecvState {
            rx,
            carry: VecDeque::new(),
        };
        tx.send((0..3).map(|i| record("a", i)).collect())
            .await
            .unwrap();
        tx.send((0..3).map(|i| record("b", i)).collect())
            .await
            .unwrap();

        let mut rows = Vec::new();
        recv.fill(&mut rows, 4, Duration::from_millis(10)).await;
        assert_eq!(rows.len(), 4);
        assert_eq!(recv.carry.len(), 2);

        let mut rest = Vec::new();
        recv.fill(&mut rest, 4, Duration::from_millis(10)).await;
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].stream.as_ref(), "b");
        assert_eq!(rest[0].seq_num, 1);
    }

    #[tokio::test]
    async fn fill_returns_empty_after_timeout() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Vec<SourceRecord>>(1);
        let mut recv = RecvState {
            rx,
            carry: VecDeque::new(),
        };
        let mut rows = Vec::new();
        recv.fill(&mut rows, 4, Duration::from_millis(5)).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn put_back_preserves_order_for_retry() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut recv = RecvState {
            rx,
            carry: VecDeque::new(),
        };
        tx.send(vec![record("a", 0), record("a", 1)]).await.unwrap();

        let mut rows = Vec::new();
        recv.fill(&mut rows, 2, Duration::from_millis(10)).await;
        assert_eq!(rows.len(), 2);
        recv.put_back(rows);

        let mut retried = Vec::new();
        recv.fill(&mut retried, 2, Duration::from_millis(10)).await;
        let seq_nums: Vec<u64> = retried.iter().map(|r| r.seq_num).collect();
        assert_eq!(seq_nums, vec![0, 1]);
    }
}
