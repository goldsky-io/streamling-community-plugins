//! S2 (s2.dev) sink — appends each Arrow row as a JSON record to an S2 stream.
//!
//! ## Configuration
//!
//! Required:
//! - access_token (secret) — supply via env STREAMLING__PLUGIN__S2_SINK__ACCESS_TOKEN
//!   when possible; YAML inlining is supported but logs WARN.
//! - basin — S2 basin name. Must already exist.
//! - stream — S2 stream name within the basin.
//!
//! Optional:
//! - create_stream (default true) — call basin.ensure_stream at init so the
//!   stream is created if missing (idempotent). Disable if the access token
//!   only has append scope.
//! - request_timeout_ms (default 5000) — per-request HTTP timeout passed to
//!   S2Config::with_request_timeout.
//!
//! Each option can be overridden by the matching STREAMLING__PLUGIN__S2_SINK__<KEY>
//! env var; the env var wins when both are set.
//!
//! ## Delivery
//!
//! Each process_batch packs the incoming RecordBatch's JSON rows into
//! AppendRecordBatch chunks (≤1000 records / ≤1 MiB, per s2-sdk's
//! RECORD_BATCH_MAX) and calls stream.append sequentially. If a later
//! chunk fails after earlier ones succeed, process_batch returns an
//! error and the entire batch is replayed — duplicates from the
//! already-acked chunks are standard at-least-once semantics.
//!
//! Retries are delegated to the SDK via S2Config::with_retry: max_attempts
//! = u32::MAX with capped exponential backoff (250ms → 15s) so transient
//! errors retry until the pipeline shuts down. AppendRetryPolicy::NoSideEffects
//! avoids duplicate appends at the cost of occasionally surfacing a Server
//! error when the SDK cannot prove the request didn't reach the broker; the
//! streamling supervisor then restarts from the last checkpoint.
//!
//! Checkpoint hooks are no-ops because stream.append returns once durable.

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use s2_sdk::{
    S2, S2Stream,
    types::{
        AppendInput, AppendRecord, AppendRecordBatch, AppendRetryPolicy, BasinName,
        EnsureStreamInput, RetryConfig, S2Config, StreamName,
    },
};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};
use tracing::{debug, error, info};

use crate::utils::plugin_options::PluginOptions;
use crate::utils::record_batch_json;

/// Per-record overhead applied by the S2 metered-bytes formula
/// (`8 + 2*len(headers) + sum(name+value) + len(body)`). We never set
/// headers, so the overhead is the constant `8`.
const APPEND_RECORD_OVERHEAD: usize = 8;

/// Maximum records per `AppendRecordBatch`, mirroring `s2_common::caps::RECORD_BATCH_MAX.count`.
/// The s2-sdk doesn't re-export the constant, so we duplicate it here.
const APPEND_RECORD_BATCH_MAX_COUNT: usize = 1000;

/// Maximum metered bytes per `AppendRecordBatch`, mirroring
/// `s2_common::caps::RECORD_BATCH_MAX.bytes` (1 MiB).
const APPEND_RECORD_BATCH_MAX_BYTES: usize = 1024 * 1024;

pub struct S2Sink {
    opts: PluginOptions,
    _schema: SchemaRef,
    stream: OnceLock<S2Stream>,
    stream_id: OnceLock<String>,
    running: Arc<AtomicBool>,
}

impl S2Sink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        _metric_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        S2Sink {
            opts: PluginOptions::new(options, "s2_sink", "STREAMLING__PLUGIN__S2_SINK"),
            _schema: schema,
            stream: OnceLock::new(),
            stream_id: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for S2Sink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for S2Sink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.stream.get().is_some() {
            return Ok(());
        }

        // s2-sdk talks HTTP/2 over rustls; install the aws-lc-rs CryptoProvider
        // process-wide if nothing else has. `install_default` is idempotent:
        // returns Err if already installed.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let access_token = self.opts.get_secret("access_token").ok_or_else(|| {
            let err = "s2_sink: access_token is not specified".to_string();
            error!(error = %err, "S2 sink initialization failed");
            PluginError::Internal(err)
        })?;

        let basin = self.opts.get("basin")?;
        let stream = self.opts.get("stream")?;

        let create_stream: bool =
            self.opts
                .get_or("create_stream", "true")
                .parse()
                .map_err(|e| {
                    PluginError::Internal(format!("create_stream is not a valid bool: {}", e))
                })?;

        let request_timeout_ms: u64 = self
            .opts
            .get_or("request_timeout_ms", "5000")
            .parse()
            .map_err(|e| {
                PluginError::Internal(format!("request_timeout_ms is not a valid u64: {}", e))
            })?;

        let basin_name: BasinName = basin
            .parse()
            .map_err(|e| PluginError::Internal(format!("invalid basin name '{}': {}", basin, e)))?;
        let stream_name: StreamName = stream.parse().map_err(|e| {
            PluginError::Internal(format!("invalid stream name '{}': {}", stream, e))
        })?;

        let cfg = S2Config::new(access_token)
            .with_request_timeout(Duration::from_millis(request_timeout_ms))
            .with_retry(
                RetryConfig::new()
                    .with_max_attempts(NonZeroU32::new(u32::MAX).expect("u32::MAX is nonzero"))
                    .with_min_base_delay(Duration::from_millis(250))
                    .with_max_base_delay(Duration::from_secs(15))
                    .with_append_retry_policy(AppendRetryPolicy::NoSideEffects),
            );

        let s2 = S2::new(cfg)
            .map_err(|e| PluginError::Internal(format!("failed to construct S2 client: {}", e)))?;
        let basin_handle = s2.basin(basin_name.clone());

        if create_stream {
            basin_handle
                .ensure_stream(EnsureStreamInput::new(stream_name.clone()))
                .await
                .map_err(|e| {
                    PluginError::Internal(format!(
                        "failed to ensure S2 stream '{}/{}': {}",
                        basin_name, stream_name, e
                    ))
                })?;
        }

        let s2_stream: S2Stream = basin_handle.stream(stream_name.clone());
        let stream_id = format!("{}/{}", basin_name, stream_name);

        let _ = self.stream.set(s2_stream);
        let _ = self.stream_id.set(stream_id.clone());

        info!(
            stream_id = %stream_id,
            create_stream,
            request_timeout_ms,
            "S2 sink initialized successfully"
        );
        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if !self.is_running() {
            return Err(PluginError::Internal(
                "S2 sink is not running, cannot process batch".to_string(),
            ));
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        let stream = self
            .stream
            .get()
            .ok_or_else(|| PluginError::Internal("S2 stream is not initialized".to_string()))?;
        let stream_id = self
            .stream_id
            .get()
            .map(String::as_str)
            .unwrap_or("<uninit>");

        append_record_batch(stream, &batch)
            .await
            .map_err(|e| match e {
                PluginError::Internal(msg) => {
                    PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
                }
                other => other,
            })
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let stream_id = self
            .stream_id
            .get()
            .map(String::as_str)
            .unwrap_or("<uninit>");
        info!(stream_id = %stream_id, ?epoch, "S2 sink received checkpoint marker");
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

/// Pack pre-encoded JSON rows into `AppendRecordBatch` chunks that satisfy
/// the s2-sdk's `RECORD_BATCH_MAX` limit (≤1000 records and ≤1 MiB in
/// metered bytes). `AppendRecordBatch` does not expose a public `push` /
/// `new`; we accumulate `Vec<AppendRecord>` chunks and convert via
/// `try_from_iter`, tracking the metered-bytes budget ourselves so the
/// final conversion can't reject the batch.
pub(crate) fn pack_into_append_record_batches(
    json_rows: Vec<Vec<u8>>,
) -> Result<Vec<AppendRecordBatch>, PluginError> {
    let mut out: Vec<AppendRecordBatch> = Vec::new();
    let mut current: Vec<AppendRecord> = Vec::new();
    let mut current_bytes: usize = 0;

    for row in json_rows {
        let row_len = row.len();
        let record_bytes = APPEND_RECORD_OVERHEAD + row_len;
        let record = AppendRecord::new(row).map_err(|e| {
            PluginError::Internal(format!(
                "failed to build S2 AppendRecord (row {} bytes): {}",
                row_len, e
            ))
        })?;

        let would_overflow = !current.is_empty()
            && (current.len() + 1 > APPEND_RECORD_BATCH_MAX_COUNT
                || current_bytes + record_bytes > APPEND_RECORD_BATCH_MAX_BYTES);

        if would_overflow {
            let batch =
                AppendRecordBatch::try_from_iter(std::mem::take(&mut current)).map_err(|e| {
                    PluginError::Internal(format!("failed to build S2 AppendRecordBatch: {}", e))
                })?;
            out.push(batch);
            current_bytes = 0;
        }

        current_bytes += record_bytes;
        current.push(record);
    }

    if !current.is_empty() {
        let batch = AppendRecordBatch::try_from_iter(current).map_err(|e| {
            PluginError::Internal(format!("failed to build S2 AppendRecordBatch: {}", e))
        })?;
        out.push(batch);
    }

    Ok(out)
}

/// Convert a `RecordBatch` into newline-delimited JSON rows, pack them into
/// `AppendRecordBatch` chunks, and append each chunk to the given S2 stream.
/// Extracted from [`S2Sink`] so the logic can be exercised in unit tests.
pub(crate) async fn append_record_batch(
    stream: &S2Stream,
    batch: &RecordBatch,
) -> Result<(), PluginError> {
    let json_rows = record_batch_json::record_batch_to_line_delimited_json(batch)
        .map_err(|e| PluginError::Internal(format!("failed to convert batch to JSON: {}", e)))?;

    if json_rows.is_empty() {
        return Ok(());
    }

    let total = json_rows.len();
    let append_batches = pack_into_append_record_batches(json_rows)?;
    let chunks = append_batches.len();

    for b in append_batches {
        stream
            .append(AppendInput::new(b))
            .await
            .map_err(|e| PluginError::Internal(format!("failed to append to S2: {}", e)))?;
    }

    debug!(rows = total, chunks, "Appended to S2");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_empty_rows_produces_no_batches() {
        let batches = pack_into_append_record_batches(Vec::new()).expect("pack empty");
        assert!(
            batches.is_empty(),
            "expected no batches, got {}",
            batches.len()
        );
    }

    #[test]
    fn test_pack_many_small_rows_fans_out_when_over_record_count_cap() {
        let rows: Vec<Vec<u8>> = (0..1500).map(|_| b"x".to_vec()).collect();
        let batches = pack_into_append_record_batches(rows).expect("pack 1500 rows");
        assert_eq!(
            batches.len(),
            2,
            "expected 2 batches, got {}",
            batches.len()
        );
        assert_eq!(
            batches[0].len(),
            1000,
            "first batch should be at the 1000-record cap"
        );
        assert_eq!(
            batches[1].len(),
            500,
            "second batch should hold the remainder"
        );
    }

    #[test]
    fn test_pack_oversized_single_record_in_own_batch() {
        // 900 KiB row: well below the 1 MiB single-record cap, but two of them
        // would exceed the 1 MiB batch byte cap, so each must land in its own batch.
        let big = vec![b'y'; 900 * 1024];
        let rows = vec![big.clone(), big];
        let batches = pack_into_append_record_batches(rows).expect("pack oversized");
        assert_eq!(
            batches.len(),
            2,
            "two 900 KiB rows must split across two batches"
        );
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 1);
    }
}
