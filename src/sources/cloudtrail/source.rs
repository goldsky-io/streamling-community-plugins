//! `cloudtrail_source` — SourcePlugin implementation. See the module docs in
//! `mod.rs` for configuration and semantics.

use crate::sources::cloudtrail::config::{CloudTrailSourceConfig, parse_config};
use crate::sources::cloudtrail::convert::{CloudTrailRecord, RecordConverter};
use crate::sources::cloudtrail::poller::{LOOKUP_RETENTION_MS, Poller, now_ms, resolve_start};
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_cloudtrail::Client;
use aws_sdk_cloudtrail::error::DisplayErrorContext;
use aws_sdk_cloudtrail::types::LookupAttribute;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
use tracing::{debug, info, warn};

/// Bounded buffer of record batches from the poller; when full, the poller
/// stops fetching from CloudTrail (backpressure).
const MAX_BUFFERED_BATCHES: usize = 16;

/// Max wait in generate_batch for the first record before returning an empty
/// batch.
const BATCH_WAIT: Duration = Duration::from_millis(100);

struct RecvState {
    rx: tokio::sync::mpsc::Receiver<Vec<CloudTrailRecord>>,
    /// Records buffered ahead of the next generated batch. Records leave the
    /// carry only after a batch containing them has been built, so an errored
    /// or cancelled generate_batch retries the exact same records.
    carry: VecDeque<CloudTrailRecord>,
    /// Max event time of the batch most recently returned to the host. It
    /// becomes visible to checkpoint markers only at the start of the next
    /// generate_batch call — the host delivers a batch downstream before
    /// asking for another, whereas a checkpoint marker can be processed while
    /// a batch is still queued for delivery; advancing the watermark early
    /// would let such a marker checkpoint past an undelivered batch (data
    /// loss on restart instead of duplicates).
    pending_watermark: Option<i64>,
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
    poller_task: JoinHandle<()>,
    recv: Mutex<RecvState>,
    /// Max event time across delivered batches — the checkpointable
    /// low-watermark (batches arrive in ascending event-time order within a
    /// cycle; `fetch_max` absorbs late-arrival batches from lookback).
    delivered_watermark: AtomicI64,
    /// Watermark snapshots per checkpoint epoch, persisted on finalize.
    pending_epochs: Mutex<BTreeMap<u64, i64>>,
}

pub struct CloudTrailSource {
    opts: PluginOptions,
    config: Arc<CloudTrailSourceConfig>,
    converter: RecordConverter,
    state: Arc<PluginStateBackend<i64>>,
    metrics: PluginMetricsRecorder,
    inner: OnceCell<RunningState>,
    running: AtomicBool,
}

impl CloudTrailSource {
    pub fn new(
        _rt: PluginAsyncRuntimeObj,
        state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(
            options,
            "cloudtrail_source",
            "STREAMLING__PLUGIN__CLOUDTRAIL_SOURCE",
        );
        let config = parse_config(&opts).map_err(configuration_error)?;

        Ok(Self {
            opts,
            config: Arc::new(config),
            converter: RecordConverter::new(),
            state: state_backend_factory.create(),
            metrics: metrics_recorder,
            inner: OnceCell::new(),
            running: AtomicBool::new(true),
        })
    }

    fn inner(&self) -> Result<&RunningState, PluginError> {
        self.inner.get().ok_or_else(|| {
            PluginError::Internal("cloudtrail_source is not initialized".to_string())
        })
    }
}

#[async_trait]
impl SupportsGracefulShutdown for CloudTrailSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(inner) = self.inner.get() {
            inner.poller_task.abort();
            inner.recv.lock().await.rx.close();
        }
        info!("cloudtrail_source terminated");
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for CloudTrailSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.inner
            .get_or_try_init(|| async {
                let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(region) = &self.config.region {
                    config_loader =
                        config_loader.region(aws_types::region::Region::new(region.clone()));
                }
                if let Some(endpoint_url) = &self.config.endpoint_url {
                    config_loader = config_loader.endpoint_url(endpoint_url);
                }
                let access_key_id = self.opts.get_secret("access_key_id");
                let secret_access_key = self.opts.get_secret("secret_access_key");
                let session_token = self.opts.get_secret("session_token");
                if let (Some(access_key_id), Some(secret_access_key)) =
                    (access_key_id, secret_access_key)
                {
                    let creds = Credentials::new(
                        access_key_id,
                        secret_access_key,
                        session_token,
                        None,
                        "CloudTrailSourcePlugin",
                    );
                    config_loader = config_loader.credentials_provider(creds);
                }
                let sdk_config = config_loader.load().await;
                let client = Client::new(&sdk_config);

                // Probe so bad credentials/region fail initialization instead
                // of WARN-looping forever in the background poller.
                client
                    .lookup_events()
                    .max_results(1)
                    .send()
                    .await
                    .map_err(|e| {
                        PluginError::Internal(format!(
                            "cloudtrail_source: LookupEvents probe failed: {}",
                            DisplayErrorContext(&e)
                        ))
                    })?;

                let checkpoint = self.state.get().await.map_err(PluginError::State)?;
                let now = now_ms();
                if let Some(watermark) = checkpoint
                    && now - watermark > LOOKUP_RETENTION_MS
                {
                    warn!(
                        checkpoint_ms = watermark,
                        "cloudtrail_source: checkpoint is older than the 90-day \
                         LookupEvents retention; events between the checkpoint and \
                         the retention horizon are gone"
                    );
                }
                let (watermark, floor) = resolve_start(checkpoint, self.config.start_time, now);

                let lookup_attribute = match &self.config.lookup_attribute {
                    None => None,
                    Some((key, value)) => Some(
                        LookupAttribute::builder()
                            .attribute_key(key.as_str().into())
                            .attribute_value(value)
                            .build()
                            .map_err(|e| {
                                PluginError::Internal(format!(
                                    "cloudtrail_source: invalid lookup attribute: {e}"
                                ))
                            })?,
                    ),
                };

                let (tx, rx) = tokio::sync::mpsc::channel(MAX_BUFFERED_BATCHES);
                let poller_task = tokio::spawn(
                    Poller {
                        client,
                        config: self.config.clone(),
                        lookup_attribute,
                        tx,
                        metrics: self.metrics.clone(),
                        watermark_ms: watermark,
                        floor_ms: floor,
                    }
                    .run(),
                );

                info!(
                    region = ?self.config.region,
                    start_time = ?self.config.start_time,
                    checkpoint_ms = ?checkpoint,
                    watermark_ms = watermark,
                    "cloudtrail_source initialized successfully"
                );
                Ok(RunningState {
                    poller_task,
                    recv: Mutex::new(RecvState {
                        rx,
                        carry: VecDeque::new(),
                        pending_watermark: None,
                    }),
                    delivered_watermark: AtomicI64::new(watermark),
                    pending_epochs: Mutex::new(BTreeMap::new()),
                })
            })
            .await?;
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.converter.schema())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        self.config
            .region
            .iter()
            .map(|region| PluginLabel::new("region", region.clone()))
            .collect()
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let inner = self.inner()?;
        if !self.is_running() {
            return Ok(RecordBatch::new_empty(self.converter.schema()));
        }

        let mut recv = inner.recv.lock().await;

        // The host only asks for a new batch after delivering the previous
        // one, so its watermark may become checkpoint-visible now.
        if let Some(delivered) = recv.pending_watermark.take() {
            inner
                .delivered_watermark
                .fetch_max(delivered, Ordering::SeqCst);
        }

        recv.fill(self.config.batch_size, BATCH_WAIT).await;
        let take = recv.carry.len().min(self.config.batch_size);
        if take == 0 {
            self.metrics
                .record_gauge("cloudtrail_source.carry_records", 0);
            return Ok(RecordBatch::new_empty(self.converter.schema()));
        }

        // On error the carry is left intact, so the next call retries the
        // exact same records. No awaits from here to return: once records
        // leave the carry the batch is guaranteed to be returned.
        let (batch, max_event_time) = {
            let rows = &recv.carry.make_contiguous()[..take];
            let batch = self.converter.convert(rows)?;
            let max_event_time = rows.iter().map(|row| row.event_time_ms).max();
            (batch, max_event_time)
        };
        debug!(rows = batch.num_rows(), "cloudtrail_source generated batch");
        recv.carry.drain(..take);
        recv.pending_watermark = max_event_time;
        self.metrics
            .record_count("cloudtrail_source.records_emitted", batch.num_rows() as u64);
        self.metrics
            .record_gauge("cloudtrail_source.carry_records", recv.carry.len() as u64);
        Ok(batch)
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let inner = self.inner()?;
        let watermark = inner.delivered_watermark.load(Ordering::SeqCst);
        inner.pending_epochs.lock().await.insert(epoch.0, watermark);
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
            // The watermark is monotonic; snapshots for older epochs are
            // subsumed.
            *pending = pending.split_off(&epoch.0);
            snapshot
        };
        let Some(watermark) = snapshot else {
            return Ok(());
        };
        self.state
            .put(watermark)
            .await
            .map_err(PluginError::State)?;
        debug!(
            ?epoch,
            watermark, "cloudtrail_source persisted checkpoint watermark"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_support;
    use streamling_plugin::r#async::DirectTokioProxy;

    fn new_source(options: &[(&str, &str)]) -> Result<CloudTrailSource, PluginInitializationError> {
        CloudTrailSource::new(
            DirectTokioProxy::new().into_async_runtime_obj(),
            test_support::state_backend_factory("test_cloudtrail_source"),
            test_support::metrics_recorder(),
            options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn constructor_accepts_zero_config() {
        let source = new_source(&[]).expect("zero config is valid");
        assert!(source.labels().is_empty());

        let source = new_source(&[("region", "us-east-1")]).expect("valid options");
        assert_eq!(source.labels().len(), 1);
    }

    #[test]
    fn constructor_rejects_invalid_options() {
        let err = new_source(&[("start_time", "bogus")])
            .err()
            .expect("must fail on invalid start_time");
        assert!(format!("{err:?}").contains("start_time"));

        let err = new_source(&[("access_key_id", "AKIA123")])
            .err()
            .expect("must fail on a lone access key");
        assert!(format!("{err:?}").contains("together"));
    }

    #[test]
    fn constructor_builds_output_schema() {
        let source = new_source(&[]).expect("valid options");
        let schema = source.output_schema().expect("schema");
        assert_eq!(schema.fields().len(), 33);
        assert_eq!(schema.field(0).name(), "_gs_op");
        assert_eq!(schema.field(1).name(), "event_id");
        assert_eq!(schema.field(2).name(), "event_time");
    }

    fn record(event_id: &str, event_time_ms: i64) -> CloudTrailRecord {
        CloudTrailRecord {
            event_id: event_id.to_string(),
            event_time_ms,
            ..CloudTrailRecord::default()
        }
    }

    fn recv_state(rx: tokio::sync::mpsc::Receiver<Vec<CloudTrailRecord>>) -> RecvState {
        RecvState {
            rx,
            carry: VecDeque::new(),
            pending_watermark: None,
        }
    }

    #[tokio::test]
    async fn fill_tops_up_the_carry_and_preserves_order() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut recv = recv_state(rx);
        tx.send((0..3).map(|i| record("a", i)).collect())
            .await
            .unwrap();
        tx.send((3..6).map(|i| record("b", i)).collect())
            .await
            .unwrap();

        recv.fill(4, Duration::from_millis(10)).await;
        assert!(recv.carry.len() >= 4);

        recv.fill(10, Duration::from_millis(10)).await;
        assert_eq!(recv.carry.len(), 6);
        let times: Vec<i64> = recv.carry.iter().map(|r| r.event_time_ms).collect();
        assert_eq!(times, vec![0, 1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn fill_returns_empty_after_timeout() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Vec<CloudTrailRecord>>(1);
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
