//! S2 (s2.dev) sink - appends each Arrow row as a JSON record to an S2 stream.
//!
//! ## Configuration
//!
//! Required:
//! - access_token (secret) — supply via env STREAMLING__PLUGIN__S2_SINK__ACCESS_TOKEN
//!   when possible; YAML inlining is supported but logs WARN.
//! - basin — S2 basin name. Must already exist.
//! - Exactly one of:
//!   - stream — a fixed S2 stream name within the basin;
//!   - stream_template — a per-row stream name with `{column}` placeholders,
//!     e.g. `events/{tenant}` — records fan out across streams by the row's
//!     column values, composing with the s2_source's `stream_prefix`
//!     discovery on the read side. Placeholder columns must be non-null and
//!     stringify to a valid stream name that contains no '/' (values must
//!     not escape the template's namespace); a row that violates this fails
//!     the batch — and, since the host fails the pipeline on batch errors,
//!     upstream data must uphold it. Per-stream record order follows row
//!     order. Producers are created (and streams ensured) lazily per distinct
//!     resolved name; producers idle for five minutes are closed at
//!     checkpoint markers, so high-cardinality routing stays bounded.
//!
//! Optional:
//! - ensure_stream (default true) — create target streams if missing
//!   (idempotent): at init for a fixed `stream`, on first use per resolved
//!   `stream_template` name. Disable if the access token only has append
//!   scope, or when the basin has `create_stream_on_append` enabled — the
//!   natural pairing for `stream_template`, where the first append creates
//!   each stream server-side with no extra RPCs.
//! - endpoint — optional S2-compatible endpoint, useful for s2-lite.
//! - request_timeout_ms (default 5000) — per-request HTTP timeout passed to
//!   S2Config::with_request_timeout.
//! - linger_ms (default 5) - how long the SDK Producer waits for more records
//!   before flushing a partial batch.
//!
//! Each option can be overridden by the matching STREAMLING__PLUGIN__S2_SINK__<KEY>
//! env var; the env var wins when both are set.
//!
//! ## Metrics
//!
//! - `s2_sink.records_submitted` (count) — records handed to the Producer.
//! - `s2_sink.records_acknowledged` (count) — records durably appended.
//! - `s2_sink.pending_records` (gauge) — submitted-but-unacknowledged records.
//! - `s2_sink.checkpoint_flush_latency` (latency) — time spent draining
//!   pending acks at a checkpoint marker.
//! - `s2_sink.open_producers` (gauge) — producers currently open (template
//!   targets only; idle ones are closed at checkpoint markers).
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
//! Every record carries a Debezium-style `dbz.op` header with the row kind
//! from streamling's `_gs_op` column (i→c, u→u, d→d — the same encoding as
//! streamling's Kafka sink), so CDC updates and deletes are distinguishable
//! out-of-band by consumers. The `_gs_op` column itself is stripped from the
//! JSON body: the header is the canonical channel, and internal plumbing
//! stays out of the payload.
//!
//! Delivery is at-least-once: appends are retried even when the outcome of a
//! previous attempt is unknown (AppendRetryPolicy::All), and a pipeline
//! restart replays from the last finalized checkpoint — either can duplicate
//! records on the stream.

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::util::display::{ArrayFormatter, FormatOptions, array_value_to_string};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use s2_sdk::{
    batching::BatchingConfig,
    producer::{Producer, ProducerConfig, RecordSubmitTicket},
    types::{AppendRecord, AppendRetryPolicy, EnsureStreamInput, Header, RetryConfig, StreamName},
};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::{STREAMLING_COLUMN_NAME_OP, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SinkPlugin};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info, warn};

use crate::sinks::s2::config::{S2SinkConfig, Segment, StreamTarget, parse_config};
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use crate::utils::record_batch_json;
use crate::utils::s2::{DBZ_OP_HEADER, dbz_op_from_row_kind, s2_client};

/// Idle template producers are closed at checkpoint markers after this long
/// without a submit, so high-cardinality routing does not grow the producer
/// map (and its append sessions) without bound.
const PRODUCER_IDLE_TTL: Duration = Duration::from_secs(300);

struct ProducerState {
    producer: Producer,
    pending: VecDeque<RecordSubmitTicket>,
    last_used: std::time::Instant,
}

impl ProducerState {
    fn new(producer: Producer) -> Self {
        Self {
            producer,
            pending: VecDeque::new(),
            last_used: std::time::Instant::now(),
        }
    }
}

/// One producer per target stream, created lazily as template streams are
/// first resolved. A fixed `stream` is pre-populated at initialization.
struct SinkState {
    /// Control-plane handle with the SDK's default (finite) retries:
    /// ensure_stream runs on the record-submit path for template streams, and
    /// unbounded retries there would wedge the sink (and with it checkpoint
    /// and terminate handling) instead of surfacing an error.
    ensure_basin: s2_sdk::S2Basin,
    /// Data-plane handle with unbounded append retries — appends must not be
    /// dropped; the checkpoint barrier bounds the exposure.
    producer_basin: s2_sdk::S2Basin,
    producer_config: ProducerConfig,
    ensure_stream: bool,
    producers: Mutex<HashMap<StreamName, ProducerState>>,
}

impl SinkState {
    /// Removes and returns producers that have no pending tickets and have
    /// not been submitted to for `PRODUCER_IDLE_TTL`.
    async fn take_idle_producers(&self) -> Vec<(StreamName, ProducerState)> {
        self.producers
            .lock()
            .await
            .extract_if(|_, p| p.pending.is_empty() && p.last_used.elapsed() >= PRODUCER_IDLE_TTL)
            .collect()
    }

    async fn ensure_producer<'a>(
        &self,
        producers: &'a mut HashMap<StreamName, ProducerState>,
        stream: &StreamName,
    ) -> Result<&'a mut ProducerState, PluginError> {
        if !producers.contains_key(stream) {
            if self.ensure_stream {
                self.ensure_basin
                    .ensure_stream(EnsureStreamInput::new(stream.clone()))
                    .await
                    .map_err(|e| {
                        PluginError::Internal(format!("failed to ensure S2 stream '{stream}': {e}"))
                    })?;
            }
            let producer = self
                .producer_basin
                .stream(stream.clone())
                .producer(self.producer_config.clone());
            producers.insert(stream.clone(), ProducerState::new(producer));
            info!(stream = %stream, "S2 sink opened producer for stream");
        }
        Ok(producers.get_mut(stream).expect("producer just ensured"))
    }
}

pub struct S2Sink {
    config: S2SinkConfig,
    access_token: String,
    /// Log/error label: `basin/stream` or `basin/<template>`.
    stream_id: String,
    state: OnceCell<SinkState>,
    metrics: PluginMetricsRecorder,
    running: Arc<AtomicBool>,
}

impl S2Sink {
    pub fn new(
        _schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metric_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(options, "s2_sink", "STREAMLING__PLUGIN__S2_SINK");
        let config = parse_config(&opts).map_err(configuration_error)?;
        let access_token = opts.get_secret("access_token").ok_or_else(|| {
            PluginInitializationError::Configuration(
                "s2_sink: access_token is not specified".into(),
            )
        })?;
        let stream_id = match &config.target {
            StreamTarget::Fixed(stream) => format!("{}/{stream}", config.basin),
            StreamTarget::Template(_) => format!(
                "{}/{}",
                config.basin,
                opts.get_or("stream_template", "<template>")
            ),
        };

        Ok(S2Sink {
            config,
            access_token,
            stream_id,
            state: OnceCell::new(),
            metrics: metric_recorder,
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn state(&self) -> Result<&SinkState, PluginError> {
        self.state
            .get()
            .ok_or_else(|| PluginError::Internal("S2 sink is not initialized".to_string()))
    }

    fn with_stream_context(&self, e: PluginError) -> PluginError {
        match e {
            PluginError::Internal(msg) => {
                PluginError::Internal(format!("stream '{}': {}", self.stream_id, msg))
            }
            other => other,
        }
    }

    /// Resolves each record's target stream and groups them into consecutive
    /// same-stream runs (preserving per-stream order); a fixed target is one
    /// run.
    fn route(
        &self,
        records: Vec<AppendRecord>,
        batch: &RecordBatch,
    ) -> Result<Vec<(StreamName, Vec<AppendRecord>)>, PluginError> {
        match &self.config.target {
            StreamTarget::Fixed(stream) => Ok(vec![(stream.clone(), records)]),
            StreamTarget::Template(segments) => {
                let streams = resolve_streams(segments, batch)?;
                // Routing determines where data lands: zipping a mismatched
                // stream list would silently truncate and misroute records.
                if streams.len() != records.len() {
                    return Err(PluginError::Internal(format!(
                        "resolved stream count {} does not match record count {}",
                        streams.len(),
                        records.len()
                    )));
                }
                Ok(group_by_stream(records, streams))
            }
        }
    }

    /// Submits each run of records to its stream's producer.
    async fn submit_records(
        &self,
        runs: Vec<(StreamName, Vec<AppendRecord>)>,
    ) -> Result<(usize, usize), PluginError> {
        let state = self.state()?;
        let mut producers = state.producers.lock().await;

        let mut acknowledged_records = 0;
        for (stream, producer_state) in producers.iter_mut() {
            acknowledged_records +=
                drain_ready_record_tickets(stream.as_ref(), &mut producer_state.pending)?;
        }

        for (stream, run) in runs {
            let producer_state = state.ensure_producer(&mut producers, &stream).await?;
            producer_state.last_used = std::time::Instant::now();
            for record in run {
                let ticket = producer_state.producer.submit(record).await.map_err(|e| {
                    PluginError::Internal(format!(
                        "failed to submit record to S2 Producer for stream '{}': {}",
                        stream, e
                    ))
                })?;
                producer_state.pending.push_back(ticket);
            }
        }

        let pending_records = producers.values().map(|p| p.pending.len()).sum::<usize>();
        Ok((pending_records, acknowledged_records))
    }

    /// Takes every producer's pending tickets and awaits them all,
    /// concurrently across streams.
    async fn flush_pending_records(&self) -> Result<usize, PluginError> {
        let pending: Vec<(StreamName, VecDeque<RecordSubmitTicket>)> = {
            let mut producers = self.state()?.producers.lock().await;
            producers
                .iter_mut()
                .map(|(stream, producer_state)| {
                    (stream.clone(), std::mem::take(&mut producer_state.pending))
                })
                .collect()
        };

        // join_all, not try_join_all: the tickets were already taken from the
        // pending queues, and cancelling a sibling stream's flush on the first
        // error would drop its tickets un-awaited — the next flush, seeing an
        // empty queue, would acknowledge a checkpoint whose records were never
        // confirmed durable (the same invariant await_record_tickets upholds
        // within one stream).
        let results =
            futures::future::join_all(pending.into_iter().map(|(stream, tickets)| async move {
                await_record_tickets(stream.as_ref(), tickets).await
            }))
            .await;
        sum_or_first_error(results)
    }

    async fn process_batch_inner(&self, batch: RecordBatch) -> Result<(), PluginError> {
        let ops = dbz_ops_from_batch(&batch)?;
        let payload = without_op_column(&batch)?;
        let json_rows = record_batch_json::record_batch_to_line_delimited_json(&payload)
            .map_err(|e| PluginError::Internal(format!("failed to convert batch to JSON: {e}")))?;
        let total = json_rows.len();
        let records = append_records_from_json_rows(json_rows, &ops)?;
        let runs = self.route(records, &batch)?;
        let (pending_records, acknowledged_records) = self.submit_records(runs).await?;

        self.metrics
            .record_count("s2_sink.records_submitted", total as u64);
        self.metrics
            .record_count("s2_sink.records_acknowledged", acknowledged_records as u64);
        self.metrics
            .record_gauge("s2_sink.pending_records", pending_records as u64);

        debug!(
            stream_id = %self.stream_id,
            rows = total,
            acknowledged_records,
            pending_records,
            "Submitted records to S2 Producer"
        );
        Ok(())
    }

    async fn process_checkpoint_marker_inner(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let flush_started_at = std::time::Instant::now();
        let flushed_records = self.flush_pending_records().await?;
        self.metrics.record_latency(
            "s2_sink.checkpoint_flush_latency",
            flush_started_at.elapsed(),
        );
        self.metrics
            .record_count("s2_sink.records_acknowledged", flushed_records as u64);
        self.metrics.record_gauge("s2_sink.pending_records", 0);

        // The flush above emptied every pending queue, so this is a safe
        // point to close template producers that have gone idle. The fixed
        // target keeps its single producer for the sink's lifetime.
        if matches!(self.config.target, StreamTarget::Template(_)) {
            let state = self.state()?;
            for (stream, producer_state) in state.take_idle_producers().await {
                if let Err(e) = producer_state.producer.close().await {
                    warn!(stream = %stream, error = %e, "failed to close idle S2 Producer");
                }
                info!(stream = %stream, "S2 sink closed idle producer");
            }
            self.metrics.record_gauge(
                "s2_sink.open_producers",
                state.producers.lock().await.len() as u64,
            );
        }

        info!(
            stream_id = %self.stream_id,
            ?epoch,
            flushed_records,
            "S2 sink flushed pending records for checkpoint marker"
        );
        Ok(())
    }
}

/// Sums successful counts, deferring the first error until every result has
/// been consumed — partial failures must not short-circuit siblings.
fn sum_or_first_error(
    results: impl IntoIterator<Item = Result<usize, PluginError>>,
) -> Result<usize, PluginError> {
    let mut total = 0;
    let mut first_error = None;
    for result in results {
        match result {
            Ok(count) => total += count,
            Err(e) => first_error = first_error.or(Some(e)),
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(total),
    }
}

/// Groups row-ordered (record, stream) pairs into runs of consecutive
/// same-stream records, preserving per-stream order.
fn group_by_stream(
    records: Vec<AppendRecord>,
    streams: Vec<StreamName>,
) -> Vec<(StreamName, Vec<AppendRecord>)> {
    let mut runs: Vec<(StreamName, Vec<AppendRecord>)> = Vec::new();
    for (record, stream) in records.into_iter().zip(streams) {
        match runs.last_mut() {
            Some((last, run)) if *last == stream => run.push(record),
            _ => runs.push((stream, vec![record])),
        }
    }
    runs
}

#[async_trait]
impl SupportsGracefulShutdown for S2Sink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);

        let Some(state) = self.state.get() else {
            return Ok(());
        };
        let producers: Vec<(StreamName, ProducerState)> =
            state.producers.lock().await.drain().collect();

        let results = futures::future::join_all(producers.into_iter().map(
            |(stream, producer_state)| async move {
                let flushed = await_record_tickets(stream.as_ref(), producer_state.pending).await;
                let closed = producer_state.producer.close().await.map_err(|e| {
                    PluginError::Internal(format!(
                        "stream '{}': failed to close S2 Producer: {}",
                        stream, e
                    ))
                });
                (flushed, closed)
            },
        ))
        .await;

        let flushed_records = sum_or_first_error(
            results
                .into_iter()
                .flat_map(|(flushed, closed)| [flushed, closed.map(|()| 0)]),
        )?;

        info!(
            stream_id = %self.stream_id,
            flushed_records,
            "S2 sink terminated after closing Producers"
        );
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for S2Sink {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.state
            .get_or_try_init(|| async {
                // Appends retry without bound — a record handed to the sink
                // must not be dropped, and the checkpoint barrier bounds the
                // exposure. Control-plane calls (ensure_stream) keep the
                // SDK's finite default so failures surface instead of
                // wedging the submit path.
                let data_client = s2_client(
                    self.access_token.clone(),
                    self.config.request_timeout,
                    self.config.endpoints.clone(),
                    Some(
                        RetryConfig::new()
                            .with_max_attempts(
                                NonZeroU32::new(u32::MAX).expect("u32::MAX is nonzero"),
                            )
                            .with_min_base_delay(Duration::from_millis(250))
                            .with_max_base_delay(Duration::from_secs(15))
                            .with_append_retry_policy(AppendRetryPolicy::All),
                    ),
                )?;
                let control_client = s2_client(
                    self.access_token.clone(),
                    self.config.request_timeout,
                    self.config.endpoints.clone(),
                    None,
                )?;

                let batching = BatchingConfig::new().with_linger(self.config.linger);
                let state = SinkState {
                    ensure_basin: control_client.basin(self.config.basin.clone()),
                    producer_basin: data_client.basin(self.config.basin.clone()),
                    producer_config: ProducerConfig::new().with_batching(batching),
                    ensure_stream: self.config.ensure_stream,
                    producers: Mutex::new(HashMap::new()),
                };
                // A fixed stream is ensured and its producer opened up front,
                // so configuration problems fail initialization rather than
                // the first batch. Template streams are only known per row;
                // they are ensured and opened lazily on first use.
                if let StreamTarget::Fixed(stream) = &self.config.target {
                    let mut producers = state.producers.lock().await;
                    state
                        .ensure_producer(&mut producers, stream)
                        .await
                        .map_err(|e| self.with_stream_context(e))?;
                }

                info!(
                    stream_id = %self.stream_id,
                    ensure_stream = self.config.ensure_stream,
                    request_timeout = ?self.config.request_timeout,
                    linger = ?self.config.linger,
                    "S2 sink initialized successfully"
                );
                Ok(state)
            })
            .await?;
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
        self.process_batch_inner(batch)
            .await
            .map_err(|e| self.with_stream_context(e))
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.process_checkpoint_marker_inner(epoch)
            .await
            .map_err(|e| self.with_stream_context(e))
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

/// Maps the batch's `_gs_op` row kinds to Debezium ops (i→c, u→u, d→d).
/// The column is required — streamling delivers it with every sink batch,
/// and silently omitting the header would degrade CDC updates/deletes to
/// inserts on the read side.
pub(crate) fn dbz_ops_from_batch(batch: &RecordBatch) -> Result<Vec<&'static str>, PluginError> {
    fn to_op(value: &str) -> Result<&'static str, PluginError> {
        dbz_op_from_row_kind(value).ok_or_else(|| {
            PluginError::Internal(format!(
                "invalid '{}' row kind '{}' (expected i/u/d)",
                STREAMLING_COLUMN_NAME_OP, value
            ))
        })
    }

    let column = batch
        .column_by_name(STREAMLING_COLUMN_NAME_OP)
        .ok_or_else(|| {
            PluginError::Internal(format!(
                "batch is missing the '{}' column",
                STREAMLING_COLUMN_NAME_OP
            ))
        })?;
    if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
        strings.iter().map(|op| to_op(op.unwrap_or(""))).collect()
    } else {
        // Other string encodings (e.g. dictionary) — render per row.
        (0..column.len())
            .map(|row| {
                if column.is_null(row) {
                    return to_op("");
                }
                let value = array_value_to_string(column, row).map_err(|e| {
                    PluginError::Internal(format!(
                        "failed to read '{}' at row {}: {}",
                        STREAMLING_COLUMN_NAME_OP, row, e
                    ))
                })?;
                to_op(&value)
            })
            .collect()
    }
}

/// Projects out the `_gs_op` column: the row kind travels as a record
/// header, not payload.
pub(crate) fn without_op_column(batch: &RecordBatch) -> Result<RecordBatch, PluginError> {
    let Ok(op_index) = batch.schema().index_of(STREAMLING_COLUMN_NAME_OP) else {
        return Ok(batch.clone());
    };
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|index| *index != op_index)
        .collect();
    batch.project(&keep).map_err(PluginError::ArrowError)
}

/// Resolves the target stream name for each row of the batch. Placeholder
/// columns must exist, be non-null, and yield a valid stream name.
pub(crate) fn resolve_streams(
    segments: &[Segment],
    batch: &RecordBatch,
) -> Result<Vec<StreamName>, PluginError> {
    // One formatter per placeholder column for the whole batch — building
    // one per row (as `array_value_to_string` would) is measurable on the
    // per-record path.
    let format_options = FormatOptions::default();
    let columns: HashMap<&str, (&ArrayRef, ArrayFormatter)> = segments
        .iter()
        .filter_map(|segment| match segment {
            Segment::Column(name) => Some(name.as_str()),
            Segment::Literal(_) => None,
        })
        .map(|name| {
            let column = batch.column_by_name(name).ok_or_else(|| {
                PluginError::Internal(format!(
                    "s2_sink: stream_template column '{name}' not found in batch"
                ))
            })?;
            let formatter =
                ArrayFormatter::try_new(column.as_ref(), &format_options).map_err(|e| {
                    PluginError::Internal(format!(
                        "s2_sink: cannot render stream_template column '{name}': {e}"
                    ))
                })?;
            Ok((name, (column, formatter)))
        })
        .collect::<Result<_, PluginError>>()?;

    // Template columns are typically low-cardinality; memoize the rendered
    // name → parsed StreamName so repeated rows skip validation.
    let mut parsed: HashMap<String, StreamName> = HashMap::new();
    let mut resolved = Vec::with_capacity(batch.num_rows());
    let mut name = String::new();
    for row in 0..batch.num_rows() {
        name.clear();
        for segment in segments {
            match segment {
                Segment::Literal(literal) => name.push_str(literal),
                Segment::Column(column) => {
                    let (array, formatter) = &columns[column.as_str()];
                    if array.is_null(row) {
                        return Err(PluginError::Internal(format!(
                            "s2_sink: stream_template column '{column}' is null at row {row}"
                        )));
                    }
                    let start = name.len();
                    formatter.value(row).write(&mut name).map_err(|e| {
                        PluginError::Internal(format!(
                            "s2_sink: failed to render stream_template column \
                             '{column}' at row {row}: {e}"
                        ))
                    })?;
                    // '/' delimits the S2 stream namespace; a value containing
                    // it would escape the template's intended prefix (e.g.
                    // tenant "a/b" under "events/{tenant}" landing in
                    // "events/a/b", visible to a reader of "events/a").
                    if name[start..].contains('/') {
                        return Err(PluginError::Internal(format!(
                            "s2_sink: stream_template column '{column}' value '{}' at \
                             row {row} contains '/', which would escape the stream namespace",
                            &name[start..]
                        )));
                    }
                }
            }
        }
        if let Some(stream) = parsed.get(&name) {
            resolved.push(stream.clone());
            continue;
        }
        let stream: StreamName = name.parse().map_err(|e| {
            PluginError::Internal(format!(
                "s2_sink: stream_template resolved to invalid stream name '{name}': {e}"
            ))
        })?;
        parsed.insert(name.clone(), stream.clone());
        resolved.push(stream);
    }
    Ok(resolved)
}

pub(crate) fn append_records_from_json_rows(
    json_rows: Vec<bytes::Bytes>,
    ops: &[&'static str],
) -> Result<Vec<AppendRecord>, PluginError> {
    if ops.len() != json_rows.len() {
        return Err(PluginError::Internal(format!(
            "op count {} does not match row count {}",
            ops.len(),
            json_rows.len()
        )));
    }
    json_rows
        .into_iter()
        .zip(ops)
        .map(|(row, op)| {
            let row_len = row.len();
            AppendRecord::new(row)
                .map_err(|e| {
                    PluginError::Internal(format!(
                        "failed to build S2 AppendRecord (row {} bytes): {}",
                        row_len, e
                    ))
                })?
                .with_headers([Header::new(DBZ_OP_HEADER, *op)])
                .map_err(|e| {
                    PluginError::Internal(format!("failed to set S2 record header: {}", e))
                })
        })
        .collect()
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
                    "stream '{}': failed to append pending S2 Producer record: {}",
                    stream_id, e
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
    let mut first_error = None;

    // Await every ticket even after a failure: returning early would drop
    // the remaining tickets un-awaited, and the next flush — seeing an empty
    // pending queue — would acknowledge a checkpoint whose records were
    // never confirmed durable.
    for ticket in tickets {
        match ticket.await {
            Ok(ack) => last_seq_num = Some(ack.seq_num),
            Err(e) => {
                first_error = first_error.or_else(|| {
                    Some(PluginError::Internal(format!(
                        "stream '{}': failed to append pending S2 Producer record: {}",
                        stream_id, e
                    )))
                });
            }
        }
    }
    if let Some(e) = first_error {
        return Err(e);
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
    use crate::sinks::s2::config::parse_stream_template;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow_schema::{Field, Schema};

    #[test]
    fn test_empty_rows_produce_no_append_records() {
        let records = append_records_from_json_rows(Vec::new(), &[]).expect("convert empty");
        assert!(records.is_empty());
    }

    #[test]
    fn test_json_rows_are_converted_to_append_records_in_order() {
        let rows = vec![
            bytes::Bytes::from_static(br#"{"id":1}"#),
            bytes::Bytes::from_static(br#"{"id":2}"#),
        ];
        let records = append_records_from_json_rows(rows, &["c", "c"]).expect("convert rows");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].body(), br#"{"id":1}"#);
        assert_eq!(records[1].body(), br#"{"id":2}"#);
    }

    #[test]
    fn test_oversized_json_row_returns_error() {
        let rows = vec![bytes::Bytes::from(vec![b'y'; 1024 * 1024])];
        let err =
            append_records_from_json_rows(rows, &["c"]).expect_err("oversized row should fail");

        assert!(
            err.to_string().contains("failed to build S2 AppendRecord"),
            "unexpected error: {err}"
        );
    }

    fn op_batch(ops: Vec<&str>) -> RecordBatch {
        let rows = ops.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    STREAMLING_COLUMN_NAME_OP,
                    arrow_schema::DataType::Utf8,
                    false,
                ),
                Field::new("id", arrow_schema::DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(ops)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ],
        )
        .expect("valid batch")
    }

    fn routing_batch() -> RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("acme"), Some("globex"), None])),
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn test_gs_op_maps_to_dbz_ops() {
        let ops = dbz_ops_from_batch(&op_batch(vec!["i", "u", "d"])).expect("map ops");
        assert_eq!(ops, vec!["c", "u", "d"]);

        let err = dbz_ops_from_batch(&op_batch(vec!["x"])).expect_err("invalid op should fail");
        assert!(err.to_string().contains("row kind"), "got {err}");
    }

    #[test]
    fn test_dictionary_encoded_op_column_is_supported() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let ops: DictionaryArray<Int32Type> = vec!["i", "d", "i"].into_iter().collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                STREAMLING_COLUMN_NAME_OP,
                ops.data_type().clone(),
                false,
            )])),
            vec![Arc::new(ops) as ArrayRef],
        )
        .expect("valid batch");
        assert_eq!(
            dbz_ops_from_batch(&batch).expect("map ops"),
            vec!["c", "d", "c"]
        );
    }

    #[test]
    fn test_batch_without_op_column_is_an_error() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("valid batch");
        let err = dbz_ops_from_batch(&batch).expect_err("missing op column should fail");
        assert!(err.to_string().contains("missing"), "got {err}");
    }

    #[test]
    fn test_op_column_is_stripped_from_payload() {
        let payload = without_op_column(&op_batch(vec!["i"])).expect("project");
        assert_eq!(payload.num_columns(), 1);
        assert_eq!(payload.schema().field(0).name(), "id");
    }

    #[test]
    fn test_ops_become_dbz_op_headers() {
        let rows = vec![
            bytes::Bytes::from_static(br#"{"id":1}"#),
            bytes::Bytes::from_static(br#"{"id":2}"#),
        ];
        let records = append_records_from_json_rows(rows, &["c", "d"]).expect("convert rows");

        let headers = records[1].headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name.as_ref(), DBZ_OP_HEADER.as_bytes());
        assert_eq!(headers[0].value.as_ref(), b"d");

        let err = append_records_from_json_rows(
            vec![bytes::Bytes::from_static(br#"{"id":1}"#)],
            &["c", "d"],
        )
        .expect_err("mismatched op count should fail");
        assert!(err.to_string().contains("does not match"), "got {err}");
    }

    #[test]
    fn test_resolves_stream_names_per_row() {
        let segments = parse_stream_template("events/{tenant}/{id}").expect("valid template");
        let streams =
            resolve_streams(&segments, &routing_batch().slice(0, 2)).expect("resolvable rows");
        let names: Vec<String> = streams.iter().map(ToString::to_string).collect();
        assert_eq!(names, vec!["events/acme/1", "events/globex/2"]);
    }

    #[tokio::test]
    async fn test_idle_producers_are_evicted_and_active_ones_kept() {
        use s2_sdk::S2;
        use s2_sdk::types::{BasinName, S2Config};
        let s2 = S2::new(S2Config::new("token")).expect("offline client");
        let basin: BasinName = "test-basin".parse().expect("valid basin");
        let state = SinkState {
            ensure_basin: s2.basin(basin.clone()),
            producer_basin: s2.basin(basin),
            producer_config: ProducerConfig::new(),
            ensure_stream: false,
            producers: Mutex::new(HashMap::new()),
        };

        let idle: StreamName = "idle".parse().expect("valid stream");
        let active: StreamName = "active".parse().expect("valid stream");
        {
            let mut producers = state.producers.lock().await;
            state
                .ensure_producer(&mut producers, &idle)
                .await
                .expect("open idle producer");
            state
                .ensure_producer(&mut producers, &active)
                .await
                .expect("open active producer");
            let backdated = std::time::Instant::now()
                .checked_sub(PRODUCER_IDLE_TTL)
                .expect("backdate");
            producers.get_mut(&idle).expect("idle exists").last_used = backdated;
        }

        let evicted = state.take_idle_producers().await;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].0.as_ref(), "idle");
        let producers = state.producers.lock().await;
        assert!(producers.contains_key(&active));
        assert!(!producers.contains_key(&idle));
    }

    #[test]
    fn test_placeholder_values_must_not_escape_the_namespace() {
        use arrow::array::StringArray;
        use arrow_schema::{DataType, Schema};
        let segments = parse_stream_template("events/{tenant}").expect("valid template");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "tenant",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["acme/evil"]))],
        )
        .expect("valid batch");
        let err = resolve_streams(&segments, &batch).expect_err("'/' in value should fail");
        assert!(err.to_string().contains("escape"), "got {err}");
    }

    #[test]
    fn test_constructor_rejects_bad_config() {
        use crate::utils::test_support;
        use streamling_plugin::r#async::DirectTokioProxy;

        let new_sink = |pairs: &[(&str, &str)]| {
            S2Sink::new(
                Arc::new(Schema::new(vec![Field::new(
                    "id",
                    arrow_schema::DataType::Int64,
                    false,
                )])),
                DirectTokioProxy::new().into_async_runtime_obj(),
                test_support::state_backend_factory("test_s2_sink"),
                test_support::metrics_recorder(),
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        };

        let ok = &[
            ("basin", "my-basin"),
            ("stream", "events"),
            ("access_token", "token"),
        ];
        assert!(new_sink(ok).is_ok());
        for bad in [
            vec![("stream", "events"), ("access_token", "token")], // no basin
            vec![("basin", "my-basin"), ("access_token", "token")], // no target
            vec![("basin", "my-basin"), ("stream", "events")],     // no access_token
            vec![
                ("basin", "my-basin"),
                ("stream_template", "e/{ten"), // unclosed
                ("access_token", "token"),
            ],
            vec![
                ("basin", "my-basin"),
                ("stream", "a"),
                ("stream_template", "b/{c}"),
                ("access_token", "token"),
            ],
        ] {
            let err = new_sink(&bad).err().expect("bad config must fail");
            assert!(matches!(
                err,
                PluginInitializationError::Configuration(_)
                    | PluginInitializationError::Execution(_)
            ));
        }
    }

    #[test]
    fn test_resolve_errors_are_specific() {
        let segments = parse_stream_template("events/{tenant}").expect("valid template");

        let err =
            resolve_streams(&segments, &routing_batch()).expect_err("null tenant should fail");
        assert!(err.to_string().contains("null at row 2"), "got {err}");

        let missing = parse_stream_template("events/{nope}").expect("valid template");
        let err = resolve_streams(&missing, &routing_batch()).expect_err("missing column");
        assert!(err.to_string().contains("'nope' not found"), "got {err}");

        // "." is not a valid S2 stream name.
        use arrow::array::StringArray;
        use arrow_schema::{DataType, Field, Schema};
        let dot_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "tenant",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["."]))],
        )
        .expect("valid batch");
        let err = resolve_streams(&segments[1..], &dot_batch).expect_err("invalid name");
        assert!(err.to_string().contains("invalid stream name"), "got {err}");
    }
}
