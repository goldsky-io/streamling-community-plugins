//! `evm_logs_source` — SourcePlugin implementation. See the module docs in
//! `mod.rs` for configuration and semantics.

use crate::sources::evm_logs::config::{EvmLogsConfig, StartBlock, parse_config};
use crate::sources::evm_logs::rpc::{RpcClient, RpcLog, parse_quantity};
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use arrow::array::{
    ArrayRef, BooleanBuilder, LargeStringBuilder, ListBuilder, RecordBatch, StringArray,
    StringBuilder, UInt64Builder,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use streamling_plugin::api::{
    PluginStateBackendFactory, STREAMLING_COLUMN_NAME_OP, SupportsGracefulShutdown,
};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginStateBackend, SourcePlugin,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Longest a caught-up `generate_batch` blocks: control messages (checkpoint
/// markers, terminate) are only handled between calls.
const IDLE_WAIT: Duration = Duration::from_millis(200);

/// Same names and types as the matching `edge.evm.logs` columns, so a pipeline
/// can swap this source for the dataset. The receipt and transaction columns
/// that dataset adds need per-transaction calls and are not produced.
pub fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("data", DataType::LargeUtf8, false),
        Field::new("topics", DataType::List(Arc::new(topic_field())), false),
        Field::new("removed", DataType::Boolean, false),
        Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
    ]))
}

fn topic_field() -> Field {
    Field::new("item", DataType::Utf8, true)
}

/// Builds a batch in RPC order (block, then log index). `timestamps` covers
/// every block whose log lacks `blockTimestamp`.
pub fn logs_to_batch(
    logs: &[RpcLog],
    timestamps: &HashMap<u64, u64>,
) -> Result<RecordBatch, PluginError> {
    let mut id = StringBuilder::new();
    let mut block_number = UInt64Builder::new();
    let mut block_hash = StringBuilder::new();
    let mut block_timestamp = UInt64Builder::new();
    let mut transaction_hash = StringBuilder::new();
    let mut transaction_index = UInt64Builder::new();
    let mut log_index = UInt64Builder::new();
    let mut address = StringBuilder::new();
    let mut data = LargeStringBuilder::new();
    let mut topics = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(topic_field()));
    let mut removed = BooleanBuilder::new();

    for log in logs {
        let number = parse_quantity(&log.block_number)?;
        let index = parse_quantity(&log.log_index)?;
        let timestamp = match &log.block_timestamp {
            Some(hex) => parse_quantity(hex)?,
            None => *timestamps.get(&number).ok_or_else(|| {
                PluginError::Internal(format!("evm_logs_source: no timestamp for block {number}"))
            })?,
        };
        id.append_value(format!("{}_{index}", log.block_hash));
        block_number.append_value(number);
        block_hash.append_value(&log.block_hash);
        block_timestamp.append_value(timestamp);
        transaction_hash.append_value(&log.transaction_hash);
        transaction_index.append_value(parse_quantity(&log.transaction_index)?);
        log_index.append_value(index);
        address.append_value(&log.address);
        data.append_value(&log.data);
        for topic in &log.topics {
            topics.values().append_value(topic);
        }
        topics.append(true);
        removed.append_value(log.removed);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(block_number.finish()),
        Arc::new(block_hash.finish()),
        Arc::new(block_timestamp.finish()),
        Arc::new(transaction_hash.finish()),
        Arc::new(transaction_index.finish()),
        Arc::new(log_index.finish()),
        Arc::new(address.finish()),
        Arc::new(data.finish()),
        Arc::new(topics.finish()),
        Arc::new(removed.finish()),
        Arc::new(StringArray::from(vec!["i"; logs.len()])),
    ];
    RecordBatch::try_new(output_schema(), columns).map_err(PluginError::ArrowError)
}

struct Cursor {
    /// First block not yet read.
    next_block: u64,
    /// `next_block` after the batch most recently returned. It becomes
    /// checkpoint-visible at the start of the next `generate_batch`: the host
    /// delivers a batch before asking for another, while a checkpoint marker
    /// may be processed while a batch is still queued for delivery.
    pending_next: Option<u64>,
    head: u64,
    head_checked: Instant,
    /// Shrinks when the RPC rejects a range (result or range limits).
    span: u64,
}

pub struct EvmLogsSource {
    config: Arc<EvmLogsConfig>,
    rpc: RpcClient,
    schema: SchemaRef,
    state: Arc<PluginStateBackend<u64>>,
    metrics: PluginMetricsRecorder,
    cursor: Mutex<Option<Cursor>>,
    /// First block not yet delivered downstream: the checkpointable position.
    delivered_next: AtomicU64,
    pending_epochs: Mutex<BTreeMap<u64, u64>>,
    running: AtomicBool,
}

impl EvmLogsSource {
    pub fn new(
        _rt: PluginAsyncRuntimeObj,
        state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(
            options,
            "evm_logs_source",
            "STREAMLING__PLUGIN__EVM_LOGS_SOURCE",
        );
        let config = parse_config(&opts).map_err(configuration_error)?;
        let rpc = RpcClient::new(config.rpc_url.clone()).map_err(configuration_error)?;
        Ok(Self {
            config: Arc::new(config),
            rpc,
            schema: output_schema(),
            state: state_backend_factory.create(),
            metrics: metrics_recorder,
            cursor: Mutex::new(None),
            delivered_next: AtomicU64::new(0),
            pending_epochs: Mutex::new(BTreeMap::new()),
            running: AtomicBool::new(true),
        })
    }

    fn empty(&self) -> RecordBatch {
        RecordBatch::new_empty(self.schema.clone())
    }

    /// Reads `[from, to]`, halving the range while the RPC rejects it. Returns
    /// the block actually read up to and whether the range had to be narrowed.
    async fn fetch(&self, from: u64, to: u64) -> Result<(u64, bool, Vec<RpcLog>), PluginError> {
        let mut end = to;
        loop {
            self.metrics.record_count("evm_logs_source.rpc_requests", 1);
            match self
                .rpc
                .get_logs(from, end, &self.config.addresses, &self.config.topic0)
                .await
            {
                Ok(logs) => return Ok((end, end < to, logs)),
                Err(e) if end > from => {
                    let narrowed = from + (end - from) / 2;
                    warn!(from, to = end, narrowed, error = %e, "evm_logs_source: eth_getLogs failed, narrowing range");
                    end = narrowed;
                }
                Err(e) => {
                    // The host retries a failed batch at once; don't hammer the RPC.
                    tokio::time::sleep(self.config.poll_interval).await;
                    return Err(e);
                }
            }
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for EvmLogsSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        info!("evm_logs_source terminated");
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for EvmLogsSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        let mut cursor = self.cursor.lock().await;
        if cursor.is_some() {
            return Ok(());
        }
        // Probe so a bad URL fails initialization, not the first batch.
        let head = self.rpc.block_number().await?;
        let checkpoint = self.state.get().await.map_err(PluginError::State)?;
        let next_block = checkpoint.unwrap_or(match self.config.start_block {
            StartBlock::At(block) => block,
            StartBlock::Latest => head.saturating_sub(self.config.confirmations),
        });
        self.delivered_next.store(next_block, Ordering::SeqCst);
        info!(
            rpc = self.rpc.redacted_url(),
            head,
            next_block,
            checkpoint = ?checkpoint,
            end_block = ?self.config.end_block,
            "evm_logs_source initialized"
        );
        *cursor = Some(Cursor {
            next_block,
            pending_next: None,
            head,
            head_checked: Instant::now(),
            span: self.config.blocks_per_request,
        });
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        if !self.is_running() {
            return Ok(self.empty());
        }
        let mut guard = self.cursor.lock().await;
        let cursor = guard
            .as_mut()
            .ok_or_else(|| PluginError::Internal("evm_logs_source is not initialized".into()))?;

        if let Some(delivered) = cursor.pending_next.take() {
            self.delivered_next.fetch_max(delivered, Ordering::SeqCst);
        }

        if let Some(end) = self.config.end_block
            && cursor.next_block > end
        {
            info!(end_block = end, "evm_logs_source reached end_block");
            self.running.store(false, Ordering::SeqCst);
            return Ok(self.empty());
        }

        let confirmations = self.config.confirmations;
        if cursor.next_block + confirmations > cursor.head
            && cursor.head_checked.elapsed() >= self.config.poll_interval
        {
            cursor.head = self.rpc.block_number().await?;
            cursor.head_checked = Instant::now();
            self.metrics
                .record_gauge("evm_logs_source.head_block", cursor.head);
        }
        let Some(safe_head) = cursor.head.checked_sub(confirmations) else {
            tokio::time::sleep(IDLE_WAIT).await;
            return Ok(self.empty());
        };
        if cursor.next_block > safe_head {
            tokio::time::sleep(IDLE_WAIT.min(self.config.poll_interval)).await;
            return Ok(self.empty());
        }

        let from = cursor.next_block;
        let mut to = (from + cursor.span - 1).min(safe_head);
        if let Some(end) = self.config.end_block {
            to = to.min(end);
        }
        let (to, narrowed, logs) = self.fetch(from, to).await?;
        if narrowed {
            cursor.span = to - from + 1;
        }

        let missing: BTreeSet<u64> = logs
            .iter()
            .filter(|log| log.block_timestamp.is_none())
            .map(|log| parse_quantity(&log.block_number))
            .collect::<Result<_, _>>()?;
        let timestamps = self.rpc.block_timestamps(missing).await?;
        let batch = logs_to_batch(&logs, &timestamps)?;

        cursor.next_block = to + 1;
        cursor.pending_next = Some(to + 1);
        self.metrics
            .record_count("evm_logs_source.logs_emitted", batch.num_rows() as u64);
        self.metrics
            .record_gauge("evm_logs_source.next_block", cursor.next_block);
        debug!(
            from,
            to,
            rows = batch.num_rows(),
            "evm_logs_source generated batch"
        );
        Ok(batch)
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let next = self.delivered_next.load(Ordering::SeqCst);
        self.pending_epochs.lock().await.insert(epoch.0, next);
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let snapshot = {
            let mut pending = self.pending_epochs.lock().await;
            let snapshot = pending.remove(&epoch.0);
            // Positions are monotonic; older epochs are subsumed.
            *pending = pending.split_off(&epoch.0);
            snapshot
        };
        if let Some(next) = snapshot {
            self.state.put(next).await.map_err(PluginError::State)?;
            debug!(?epoch, next, "evm_logs_source persisted checkpoint");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::UInt64Type;

    fn log(block: &str, index: &str, timestamp: Option<&str>, topics: &[&str]) -> RpcLog {
        RpcLog {
            address: "0x7c5bf43b851c1dff1a4fee8db225b87f2c223639".into(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            data: "0x".into(),
            block_number: block.into(),
            block_hash: format!("0xhash{block}"),
            transaction_hash: "0xtx".into(),
            transaction_index: "0x0".into(),
            log_index: index.into(),
            removed: false,
            block_timestamp: timestamp.map(String::from),
        }
    }

    #[test]
    fn batch_matches_schema_and_keys_rows_by_block_hash_and_log_index() {
        let logs = [
            log("0x10", "0x0", Some("0x64"), &["0xa", "0xb"]),
            log("0x11", "0x3", None, &[]),
        ];
        let batch = logs_to_batch(&logs, &HashMap::from([(0x11, 200)])).unwrap();
        assert_eq!(batch.schema(), output_schema());

        let ids = batch.column(0).as_string::<i32>();
        assert_eq!(ids.value(0), "0xhash0x10_0");
        assert_eq!(ids.value(1), "0xhash0x11_3");

        let timestamps = batch.column(3).as_primitive::<UInt64Type>();
        assert_eq!((timestamps.value(0), timestamps.value(1)), (100, 200));

        let topics = batch.column(9).as_list::<i32>();
        assert_eq!(topics.value(0).len(), 2);
        assert_eq!(topics.value(1).len(), 0);
        assert!(!topics.is_null(1));
    }

    #[test]
    fn missing_timestamp_is_an_error() {
        let err = logs_to_batch(&[log("0x10", "0x0", None, &[])], &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("block 16"), "{err}");
    }
}
