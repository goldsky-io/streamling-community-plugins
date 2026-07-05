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
//! - timestamp_column — name of a column carrying event time; its value is set
//!   as the S2 record timestamp (epoch ms). Accepts Arrow Timestamp columns of
//!   any unit, or Int64/UInt64 epoch milliseconds. The column stays in the
//!   JSON body. Without it, S2 assigns arrival time.
//! - drop_op_column (default false) — strip streamling's `_gs_op` row-kind
//!   column from the JSON body. By default it is kept, so CDC updates and
//!   deletes land as records tagged `"_gs_op": "u"` / `"d"` and the stream is
//!   a faithful change log.
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
//!
//! Delivery is at-least-once: appends are retried even when the outcome of a
//! previous attempt is unknown (AppendRetryPolicy::All), and a pipeline
//! restart replays from the last finalized checkpoint — either can duplicate
//! records on the stream.

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use s2_sdk::{
    S2,
    batching::BatchingConfig,
    producer::{Producer, ProducerConfig, RecordSubmitTicket},
    types::{
        AccountEndpoint, AppendRecord, AppendRetryPolicy, BasinEndpoint, BasinName,
        EnsureStreamInput, RetryConfig, S2Config, S2Endpoints, StreamName,
    },
};
use std::collections::{HashMap, VecDeque};
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

#[derive(Debug, Clone, Default)]
struct WriteOptions {
    /// Column carrying event time to use as the S2 record timestamp.
    timestamp_column: Option<String>,
    /// Strip `_gs_op` from the JSON body.
    drop_op_column: bool,
}

struct ProducerState {
    producer: Producer,
    pending: VecDeque<RecordSubmitTicket>,
}

impl ProducerState {
    fn new(producer: Producer) -> Self {
        Self {
            producer,
            pending: VecDeque::new(),
        }
    }
}

pub struct S2Sink {
    opts: PluginOptions,
    _schema: SchemaRef,
    producer: Mutex<Option<ProducerState>>,
    stream_id: OnceLock<String>,
    write_options: OnceLock<WriteOptions>,
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
            write_options: OnceLock::new(),
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
            state.pending.push_back(ticket);
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

        let timestamp_column = self.opts.get_or("timestamp_column", "");
        let drop_op_column: bool = self
            .opts
            .get_or("drop_op_column", "false")
            .parse()
            .map_err(|e| {
                PluginError::Internal(format!("drop_op_column is not a valid bool: {}", e))
            })?;
        let _ = self.write_options.set(WriteOptions {
            timestamp_column: (!timestamp_column.is_empty()).then_some(timestamp_column),
            drop_op_column,
        });

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
        let write_options = self.write_options.get().cloned().unwrap_or_default();
        let timestamps = write_options
            .timestamp_column
            .as_deref()
            .map(|column| timestamps_ms_from_column(&batch, column))
            .transpose()?;
        let batch = if write_options.drop_op_column {
            without_op_column(&batch)?
        } else {
            batch
        };
        let json_rows =
            record_batch_json::record_batch_to_line_delimited_json(&batch).map_err(|e| {
                PluginError::Internal(format!(
                    "stream '{}': failed to convert batch to JSON: {}",
                    stream_id, e
                ))
            })?;
        let total = json_rows.len();
        let records = append_records_from_json_rows(json_rows, timestamps.as_deref()).map_err(
            |e| match e {
                PluginError::Internal(msg) => {
                    PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
                }
                other => other,
            },
        )?;
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

pub(crate) fn append_records_from_json_rows(
    json_rows: Vec<Vec<u8>>,
    timestamps: Option<&[u64]>,
) -> Result<Vec<AppendRecord>, PluginError> {
    if let Some(timestamps) = timestamps
        && timestamps.len() != json_rows.len()
    {
        return Err(PluginError::Internal(format!(
            "timestamp count {} does not match row count {}",
            timestamps.len(),
            json_rows.len()
        )));
    }
    json_rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let row_len = row.len();
            let record = AppendRecord::new(row).map_err(|e| {
                PluginError::Internal(format!(
                    "failed to build S2 AppendRecord (row {} bytes): {}",
                    row_len, e
                ))
            })?;
            Ok(match timestamps {
                Some(timestamps) => record.with_timestamp(timestamps[index]),
                None => record,
            })
        })
        .collect()
}

/// Extracts per-row epoch-millisecond timestamps from `column`. Accepts Arrow
/// Timestamp columns of any unit, or Int64/UInt64 epoch milliseconds; nulls
/// and pre-epoch values are errors.
pub(crate) fn timestamps_ms_from_column(
    batch: &RecordBatch,
    column: &str,
) -> Result<Vec<u64>, PluginError> {
    use arrow::array::{
        Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, TimeUnit};

    let index = batch.schema().index_of(column).map_err(|_| {
        PluginError::Internal(format!("timestamp_column '{column}' not found in batch"))
    })?;
    let array = batch.column(index);

    fn collect_i64<'a>(
        column: &str,
        values: impl Iterator<Item = Option<i64>> + 'a,
        to_ms: impl Fn(i64) -> Option<i64>,
    ) -> Result<Vec<u64>, PluginError> {
        values
            .enumerate()
            .map(|(row, value)| {
                let value = value.ok_or_else(|| {
                    PluginError::Internal(format!(
                        "timestamp_column '{column}' is null at row {row}"
                    ))
                })?;
                to_ms(value)
                    .filter(|ms| *ms >= 0)
                    .map(|ms| ms as u64)
                    .ok_or_else(|| {
                        PluginError::Internal(format!(
                            "timestamp_column '{column}' value {value} at row {row} is not a \
                             valid epoch-millisecond timestamp"
                        ))
                    })
            })
            .collect()
    }

    macro_rules! downcast {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("data type matches downcast")
                .iter()
        };
    }

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            collect_i64(column, downcast!(TimestampSecondArray), |v| {
                v.checked_mul(1000)
            })
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            collect_i64(column, downcast!(TimestampMillisecondArray), Some)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            collect_i64(column, downcast!(TimestampMicrosecondArray), |v| {
                Some(v / 1000)
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            collect_i64(column, downcast!(TimestampNanosecondArray), |v| {
                Some(v / 1_000_000)
            })
        }
        DataType::Int64 => collect_i64(column, downcast!(Int64Array), Some),
        DataType::UInt64 => collect_i64(
            column,
            downcast!(UInt64Array).map(|v| v.map(|v| v as i64)),
            Some,
        ),
        other => Err(PluginError::Internal(format!(
            "timestamp_column '{column}' has unsupported type {other}; expected a Timestamp \
             column or Int64/UInt64 epoch milliseconds"
        ))),
    }
}

/// Projects out streamling's `_gs_op` column; no-op when absent.
pub(crate) fn without_op_column(batch: &RecordBatch) -> Result<RecordBatch, PluginError> {
    let Ok(op_index) = batch.schema().index_of(STREAMLING_COLUMN_NAME_OP) else {
        return Ok(batch.clone());
    };
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|index| *index != op_index)
        .collect();
    batch.project(&keep).map_err(PluginError::ArrowError)
}

fn drain_ready_record_tickets(
    stream_id: &str,
    tickets: &mut VecDeque<RecordSubmitTicket>,
) -> Result<usize, PluginError> {
    let waker = futures::task::noop_waker_ref();
    let mut cx = Context::from_waker(waker);
    let mut acknowledged = 0;
    let mut last_seq_num = None;

    while let Some(ticket) = tickets.front_mut() {
        match Future::poll(Pin::new(ticket), &mut cx) {
            Poll::Ready(Ok(ack)) => {
                acknowledged += 1;
                last_seq_num = Some(ack.seq_num);
                tickets.pop_front();
            }
            Poll::Ready(Err(e)) => {
                // Pop the completed ticket: leaving it queued would wedge the
                // drain, and its oneshot panics if polled again.
                tickets.pop_front();
                return Err(PluginError::Internal(format!(
                    "failed to append pending S2 Producer record: {}",
                    e
                )));
            }
            Poll::Pending => {
                break;
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
    tickets: VecDeque<RecordSubmitTicket>,
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
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema};

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
        assert_eq!(records[0].timestamp(), None);
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

    #[test]
    fn test_timestamps_are_applied_to_append_records() {
        let rows = vec![br#"{"id":1}"#.to_vec(), br#"{"id":2}"#.to_vec()];
        let records =
            append_records_from_json_rows(rows, Some(&[1_000, 2_000])).expect("convert rows");
        assert_eq!(records[0].timestamp(), Some(1_000));
        assert_eq!(records[1].timestamp(), Some(2_000));

        let err = append_records_from_json_rows(vec![br#"{"id":1}"#.to_vec()], Some(&[1, 2]))
            .expect_err("mismatched timestamp count should fail");
        assert!(err.to_string().contains("does not match"), "got {err}");
    }

    fn batch_with_columns(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect();
        let arrays = columns.into_iter().map(|(_, array)| array).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("valid batch")
    }

    #[test]
    fn test_timestamps_from_timestamp_and_integer_columns() {
        let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            1_700_000_000_123_456_i64,
        ]));
        let batch = batch_with_columns(vec![("at", micros)]);
        assert_eq!(
            timestamps_ms_from_column(&batch, "at").expect("micros convert"),
            vec![1_700_000_000_123]
        );

        let millis: ArrayRef = Arc::new(Int64Array::from(vec![1_700_000_000_000_i64]));
        let batch = batch_with_columns(vec![("at", millis)]);
        assert_eq!(
            timestamps_ms_from_column(&batch, "at").expect("int64 convert"),
            vec![1_700_000_000_000]
        );
    }

    #[test]
    fn test_timestamp_column_errors() {
        let batch = batch_with_columns(vec![(
            "at",
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        )]);
        let err = timestamps_ms_from_column(&batch, "at").expect_err("null should fail");
        assert!(err.to_string().contains("null at row 1"), "got {err}");

        let err = timestamps_ms_from_column(&batch, "missing").expect_err("missing column");
        assert!(err.to_string().contains("not found"), "got {err}");

        let batch = batch_with_columns(vec![(
            "at",
            Arc::new(StringArray::from(vec!["2026-01-01"])) as ArrayRef,
        )]);
        let err = timestamps_ms_from_column(&batch, "at").expect_err("string type unsupported");
        assert!(err.to_string().contains("unsupported type"), "got {err}");
    }

    #[test]
    fn test_without_op_column_projects_it_out() {
        let batch = batch_with_columns(vec![
            (
                STREAMLING_COLUMN_NAME_OP,
                Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
            ),
            ("id", Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef),
        ]);
        let projected = without_op_column(&batch).expect("project");
        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), "id");

        let no_op = batch_with_columns(vec![(
            "id",
            Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef,
        )]);
        let unchanged = without_op_column(&no_op).expect("no-op project");
        assert_eq!(unchanged.num_columns(), 1);
    }

    #[test]
    fn test_timestamp_second_overflow_is_an_error() {
        // Timestamp(Second) columns multiply by 1000; guard against overflow.
        let seconds: ArrayRef = Arc::new(arrow::array::TimestampSecondArray::from(vec![i64::MAX]));
        let batch = batch_with_columns(vec![("at", seconds)]);
        let err = timestamps_ms_from_column(&batch, "at").expect_err("overflow should fail");
        assert!(err.to_string().contains("not a valid"), "got {err}");
    }
}
