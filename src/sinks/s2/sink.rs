//! S2 (s2.dev) sink - appends each Arrow row as a JSON record to an S2 stream.
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
//! - ensure_stream (default true) — call basin.ensure_stream at init so the
//!   stream is created if missing (idempotent). Disable if the access token
//!   only has append scope.
//! - endpoint — optional S2-compatible endpoint, useful for s2-lite.
//! - request_timeout_ms (default 5000) — per-request HTTP timeout passed to
//!   S2Config::with_request_timeout.
//! - linger_ms (default 5) - how long the SDK Producer waits for more records
//!   before flushing a partial batch.
//!
//! Each option can be overridden by the matching STREAMLING__PLUGIN__S2_SINK__<KEY>
//! env var; the env var wins when both are set.
//!
//! ## Delivery
//!
//! Each process_batch converts the incoming RecordBatch's rows into JSON
//! AppendRecords and submits them to the s2-sdk Producer. The Producer batches
//! records internally and uses an append session for high-throughput appends.
//! process_batch returns once records have been accepted by the Producer; the
//! checkpoint marker is the durability barrier.
//!
//! A checkpoint marker awaits all outstanding Producer record tickets before
//! returning, so the dispatcher only acknowledges the checkpoint after S2 has
//! durably appended every record submitted before the marker. Termination drains
//! pending tickets and then closes the Producer.

use arrow::array::{Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use s2_sdk::{
    S2,
    batching::BatchingConfig,
    producer::{Producer, ProducerConfig, RecordSubmitTicket},
    types::{
        AccountEndpoint, AppendRecord, AppendRetryPolicy, BasinEndpoint, BasinName,
        EnsureStreamInput, Header, RetryConfig, S2Config, S2Endpoints, StreamName,
    },
};
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::{STREAMLING_COLUMN_NAME_OP, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::utils::plugin_options::PluginOptions;
use crate::utils::record_batch_json;

const S2_HEADER_OPERATION: &str = "dbz.op";

struct ProducerState {
    producer: Producer,
    pending: Vec<RecordSubmitTicket>,
}

impl ProducerState {
    fn new(producer: Producer) -> Self {
        Self {
            producer,
            pending: Vec::new(),
        }
    }
}

pub struct S2Sink {
    opts: PluginOptions,
    _schema: SchemaRef,
    producer: Mutex<Option<ProducerState>>,
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
            producer: Mutex::new(None),
            stream_id: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn stream_id_for_logs(&self) -> String {
        self.stream_id
            .get()
            .cloned()
            .unwrap_or_else(|| "<uninit>".to_string())
    }

    async fn submit_records(
        &self,
        records: Vec<AppendRecord>,
    ) -> Result<(usize, usize), PluginError> {
        let stream_id = self.stream_id_for_logs();
        let mut producer_guard = self.producer.lock().await;
        let state = producer_guard
            .as_mut()
            .ok_or_else(|| PluginError::Internal("S2 producer is not initialized".to_string()))?;

        let acknowledged_records = drain_ready_record_tickets(&stream_id, &mut state.pending)?;

        for record in records {
            let ticket = state.producer.submit(record).await.map_err(|e| {
                PluginError::Internal(format!("failed to submit record to S2 Producer: {}", e))
            })?;
            state.pending.push(ticket);
        }

        Ok((state.pending.len(), acknowledged_records))
    }

    async fn flush_pending_records(&self) -> Result<usize, PluginError> {
        let stream_id = self.stream_id_for_logs();
        let tickets = {
            let mut producer_guard = self.producer.lock().await;
            let state = producer_guard.as_mut().ok_or_else(|| {
                PluginError::Internal("S2 producer is not initialized".to_string())
            })?;
            std::mem::take(&mut state.pending)
        };

        await_record_tickets(&stream_id, tickets).await
    }
}

#[async_trait]
impl SupportsGracefulShutdown for S2Sink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);

        let stream_id = self.stream_id_for_logs();
        let Some(state) = self.producer.lock().await.take() else {
            return Ok(());
        };

        let flush_result = await_record_tickets(&stream_id, state.pending).await;
        let close_result = state.producer.close().await.map_err(|e| {
            PluginError::Internal(format!(
                "stream '{}': failed to close S2 Producer: {}",
                stream_id, e
            ))
        });

        let flushed_records = flush_result?;
        close_result?;

        info!(
            stream_id = %stream_id,
            flushed_records,
            "S2 sink terminated after closing Producer"
        );
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for S2Sink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.producer.lock().await.is_some() {
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

        let ensure_stream: bool =
            self.opts
                .get_or("ensure_stream", "true")
                .parse()
                .map_err(|e| {
                    PluginError::Internal(format!("ensure_stream is not a valid bool: {}", e))
                })?;

        let request_timeout_ms: u64 = self
            .opts
            .get_or("request_timeout_ms", "5000")
            .parse()
            .map_err(|e| {
                PluginError::Internal(format!("request_timeout_ms is not a valid u64: {}", e))
            })?;
        let endpoint = self.opts.get_or("endpoint", "");
        let linger_ms: u64 =
            self.opts.get_or("linger_ms", "5").parse().map_err(|e| {
                PluginError::Internal(format!("linger_ms is not a valid u64: {}", e))
            })?;

        let batching = BatchingConfig::new().with_linger(Duration::from_millis(linger_ms));
        let producer_config = ProducerConfig::new().with_batching(batching);

        let basin_name: BasinName = basin
            .parse()
            .map_err(|e| PluginError::Internal(format!("invalid basin name '{}': {}", basin, e)))?;
        let stream_name: StreamName = stream.parse().map_err(|e| {
            PluginError::Internal(format!("invalid stream name '{}': {}", stream, e))
        })?;

        let mut cfg = S2Config::new(access_token)
            .with_request_timeout(Duration::from_millis(request_timeout_ms))
            .with_retry(
                RetryConfig::new()
                    .with_max_attempts(NonZeroU32::new(u32::MAX).expect("u32::MAX is nonzero"))
                    .with_min_base_delay(Duration::from_millis(250))
                    .with_max_base_delay(Duration::from_secs(15))
                    .with_append_retry_policy(AppendRetryPolicy::All),
            );
        if !endpoint.is_empty() {
            let endpoints = S2Endpoints::new(
                AccountEndpoint::new(&endpoint).map_err(|e| {
                    PluginError::Internal(format!("invalid S2 account endpoint: {}", e))
                })?,
                BasinEndpoint::new(&endpoint).map_err(|e| {
                    PluginError::Internal(format!("invalid S2 basin endpoint: {}", e))
                })?,
            )
            .map_err(|e| PluginError::Internal(format!("invalid S2 endpoints: {}", e)))?;
            cfg = cfg.with_endpoints(endpoints);
        }

        let s2 = S2::new(cfg)
            .map_err(|e| PluginError::Internal(format!("failed to construct S2 client: {}", e)))?;
        let basin_handle = s2.basin(basin_name.clone());

        if ensure_stream {
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

        let s2_stream = basin_handle.stream(stream_name.clone());
        let producer = s2_stream.producer(producer_config);
        let stream_id = format!("{}/{}", basin_name, stream_name);

        let _ = self.stream_id.set(stream_id.clone());
        let mut producer_guard = self.producer.lock().await;
        if producer_guard.is_some() {
            return Ok(());
        }
        *producer_guard = Some(ProducerState::new(producer));

        info!(
            stream_id = %stream_id,
            ensure_stream,
            request_timeout_ms,
            linger_ms,
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

        let stream_id = self.stream_id_for_logs();
        let total = batch.num_rows();
        let records = append_records_from_batch(&batch).map_err(|e| match e {
            PluginError::Internal(msg) => {
                PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
            }
            other => other,
        })?;
        let (pending_records, acknowledged_records) =
            self.submit_records(records).await.map_err(|e| match e {
                PluginError::Internal(msg) => {
                    PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
                }
                other => other,
            })?;

        debug!(
            stream_id = %stream_id,
            rows = total,
            acknowledged_records,
            pending_records,
            "Submitted records to S2 Producer"
        );
        Ok(())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let stream_id = self.stream_id_for_logs();
        let flushed_records = self.flush_pending_records().await?;
        info!(
            stream_id = %stream_id,
            ?epoch,
            flushed_records,
            "S2 sink flushed pending records for checkpoint marker"
        );
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

pub(crate) fn append_records_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<AppendRecord>, PluginError> {
    let operation_headers = operation_headers_from_batch(batch)?;
    let payload_batch = strip_streamling_op_column(batch)?;
    let json_rows = record_batch_json::record_batch_to_line_delimited_json(&payload_batch)
        .map_err(|e| PluginError::Internal(format!("failed to convert batch to JSON: {}", e)))?;

    append_records_from_json_rows(json_rows, operation_headers)
}

fn strip_streamling_op_column(batch: &RecordBatch) -> Result<RecordBatch, PluginError> {
    let Some(op_idx) = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == STREAMLING_COLUMN_NAME_OP)
    else {
        return Ok(batch.clone());
    };

    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != op_idx)
        .map(|(_, field)| field.as_ref().clone())
        .collect();
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != op_idx)
        .map(|(_, column)| column.clone())
        .collect();
    let schema = Arc::new(Schema::new(fields));

    if schema.fields().is_empty() {
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
    } else {
        RecordBatch::try_new(schema, columns)
    }
    .map_err(|e| {
        PluginError::Internal(format!(
            "failed to strip {} column from S2 payload: {}",
            STREAMLING_COLUMN_NAME_OP, e
        ))
    })
}

fn operation_headers_from_batch(batch: &RecordBatch) -> Result<Option<Vec<Header>>, PluginError> {
    let Some(op_column) = batch.column_by_name(STREAMLING_COLUMN_NAME_OP) else {
        return Ok(None);
    };

    let op_column = op_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PluginError::Internal(format!(
                "{} column must be Utf8 to write S2 operation headers",
                STREAMLING_COLUMN_NAME_OP
            ))
        })?;

    let mut headers = Vec::with_capacity(op_column.len());
    for row_idx in 0..op_column.len() {
        if op_column.is_null(row_idx) {
            return Err(PluginError::Internal(format!(
                "{} column is null at row {}",
                STREAMLING_COLUMN_NAME_OP, row_idx
            )));
        }
        headers.push(Header::new(
            S2_HEADER_OPERATION,
            dbz_op_from_streamling_op(op_column.value(row_idx))?,
        ));
    }

    Ok(Some(headers))
}

fn dbz_op_from_streamling_op(op: &str) -> Result<&'static str, PluginError> {
    match op.to_ascii_lowercase().as_str() {
        "i" => Ok("c"),
        "u" => Ok("u"),
        "d" => Ok("d"),
        _ => Err(PluginError::Internal(format!(
            "invalid {} value for S2 operation header: {}",
            STREAMLING_COLUMN_NAME_OP, op
        ))),
    }
}

fn append_records_from_json_rows(
    json_rows: Vec<Vec<u8>>,
    operation_headers: Option<Vec<Header>>,
) -> Result<Vec<AppendRecord>, PluginError> {
    if let Some(headers) = &operation_headers
        && headers.len() != json_rows.len()
    {
        return Err(PluginError::Internal(format!(
            "S2 operation header count {} does not match JSON row count {}",
            headers.len(),
            json_rows.len()
        )));
    }

    json_rows
        .into_iter()
        .enumerate()
        .map(|(idx, row)| {
            let row_len = row.len();
            let record = AppendRecord::new(row).map_err(|e| {
                PluginError::Internal(format!(
                    "failed to build S2 AppendRecord (row {} bytes): {}",
                    row_len, e
                ))
            })?;

            if let Some(headers) = &operation_headers {
                record
                    .with_headers([headers[idx].clone()])
                    .map_err(|e| PluginError::Internal(format!("failed to add S2 headers: {}", e)))
            } else {
                Ok(record)
            }
        })
        .collect()
}

fn drain_ready_record_tickets(
    stream_id: &str,
    tickets: &mut Vec<RecordSubmitTicket>,
) -> Result<usize, PluginError> {
    let waker = futures::task::noop_waker_ref();
    let mut cx = Context::from_waker(waker);
    let mut acknowledged = 0;
    let mut last_seq_num = None;
    let mut idx = 0;

    while idx < tickets.len() {
        match Future::poll(Pin::new(&mut tickets[idx]), &mut cx) {
            Poll::Ready(Ok(ack)) => {
                acknowledged += 1;
                last_seq_num = Some(ack.seq_num);
                tickets.swap_remove(idx);
            }
            Poll::Ready(Err(e)) => {
                return Err(PluginError::Internal(format!(
                    "failed to append pending S2 Producer record: {}",
                    e
                )));
            }
            Poll::Pending => {
                idx += 1;
            }
        }
    }

    if acknowledged > 0 {
        debug!(
            stream_id = %stream_id,
            acknowledged_records = acknowledged,
            pending_records = tickets.len(),
            ?last_seq_num,
            "Drained acknowledged S2 Producer tickets"
        );
    }

    Ok(acknowledged)
}

async fn await_record_tickets(
    stream_id: &str,
    tickets: Vec<RecordSubmitTicket>,
) -> Result<usize, PluginError> {
    let total = tickets.len();
    let mut last_seq_num = None;

    for ticket in tickets {
        let ack = ticket.await.map_err(|e| {
            PluginError::Internal(format!(
                "stream '{}': failed to append pending S2 Producer record: {}",
                stream_id, e
            ))
        })?;
        last_seq_num = Some(ack.seq_num);
    }

    debug!(
        stream_id = %stream_id,
        records = total,
        ?last_seq_num,
        "S2 Producer records acknowledged"
    );
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    #[test]
    fn test_empty_rows_produce_no_append_records() {
        let records = append_records_from_json_rows(Vec::new(), None).expect("convert empty");
        assert!(records.is_empty());
    }

    #[test]
    fn test_json_rows_are_converted_to_append_records_in_order() {
        let rows = vec![br#"{"id":1}"#.to_vec(), br#"{"id":2}"#.to_vec()];
        let records = append_records_from_json_rows(rows, None).expect("convert rows");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].body(), br#"{"id":1}"#);
        assert_eq!(records[1].body(), br#"{"id":2}"#);
        assert!(records[0].headers().is_empty());
        assert!(records[1].headers().is_empty());
    }

    #[test]
    fn test_streamling_op_is_written_as_s2_header() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["i", "u", "d"])),
            ],
        )
        .expect("valid batch");

        let records = append_records_from_batch(&batch).expect("convert records");

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].body(), br#"{"id":1}"#);
        assert_eq!(records[1].body(), br#"{"id":2}"#);
        assert_eq!(records[2].body(), br#"{"id":3}"#);

        let ops: Vec<&[u8]> = records
            .iter()
            .map(|record| {
                let header = record
                    .headers()
                    .iter()
                    .find(|header| header.name.as_ref() == S2_HEADER_OPERATION.as_bytes())
                    .expect("operation header");
                header.value.as_ref()
            })
            .collect();

        assert_eq!(ops, vec![b"c".as_slice(), b"u".as_slice(), b"d".as_slice()]);
    }

    #[test]
    fn test_oversized_json_row_returns_error() {
        let rows = vec![vec![b'y'; 1024 * 1024]];
        let err = append_records_from_json_rows(rows, None).expect_err("oversized row should fail");

        assert!(
            err.to_string().contains("failed to build S2 AppendRecord"),
            "unexpected error: {err}"
        );
    }
}
