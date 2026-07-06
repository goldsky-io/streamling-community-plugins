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
//!     stringify to a valid stream name; per-stream record order follows row
//!     order. Producers are created (and streams ensured) lazily per distinct
//!     resolved name and kept for the sink's lifetime, so the resolved set
//!     should be bounded.
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
//! out-of-band by consumers.

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::util::display::array_value_to_string;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use s2_sdk::{
    S2,
    batching::BatchingConfig,
    producer::{Producer, ProducerConfig, RecordSubmitTicket},
    types::{
        AppendRecord, AppendRetryPolicy, BasinName, EnsureStreamInput, Header, RetryConfig,
        S2Config, StreamName,
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

/// Where records go: one fixed stream, or a per-row stream name resolved
/// from a template with `{column}` placeholders.
#[derive(Debug, Clone)]
pub(crate) enum StreamTarget {
    Fixed(StreamName),
    Template(Vec<Segment>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Segment {
    Literal(String),
    Column(String),
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

/// One producer per target stream, created lazily as template streams are
/// first resolved. A fixed `stream` is pre-populated at initialization.
struct SinkState {
    basin: s2_sdk::S2Basin,
    producer_config: ProducerConfig,
    ensure_stream: bool,
    producers: HashMap<String, ProducerState>,
}

impl SinkState {
    async fn producer_for(
        &mut self,
        stream: &StreamName,
    ) -> Result<&mut ProducerState, PluginError> {
        let key = stream.to_string();
        if !self.producers.contains_key(&key) {
            if self.ensure_stream {
                self.basin
                    .ensure_stream(EnsureStreamInput::new(stream.clone()))
                    .await
                    .map_err(|e| {
                        PluginError::Internal(format!(
                            "failed to ensure S2 stream '{}': {}",
                            key, e
                        ))
                    })?;
            }
            let producer = self
                .basin
                .stream(stream.clone())
                .producer(self.producer_config.clone());
            self.producers
                .insert(key.clone(), ProducerState::new(producer));
            info!(stream = %key, "S2 sink opened producer for stream");
        }
        Ok(self
            .producers
            .get_mut(&key)
            .expect("producer inserted above"))
    }
}

pub struct S2Sink {
    opts: PluginOptions,
    _schema: SchemaRef,
    state: Mutex<Option<SinkState>>,
    target: OnceLock<StreamTarget>,
    stream_id: OnceLock<String>,
    metrics: PluginMetricsRecorder,
    running: Arc<AtomicBool>,
}

impl S2Sink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metric_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        S2Sink {
            opts: PluginOptions::new(options, "s2_sink", "STREAMLING__PLUGIN__S2_SINK"),
            _schema: schema,
            state: Mutex::new(None),
            target: OnceLock::new(),
            stream_id: OnceLock::new(),
            metrics: metric_recorder,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn stream_id_for_logs(&self) -> String {
        self.stream_id
            .get()
            .cloned()
            .unwrap_or_else(|| "<uninit>".to_string())
    }

    /// Submits records to their target producers; `streams[i]` names record
    /// i's stream (all records go to the fixed stream when None).
    async fn submit_records(
        &self,
        records: Vec<AppendRecord>,
        streams: Option<Vec<StreamName>>,
    ) -> Result<(usize, usize), PluginError> {
        let mut state_guard = self.state.lock().await;
        let state = state_guard
            .as_mut()
            .ok_or_else(|| PluginError::Internal("S2 sink is not initialized".to_string()))?;

        let mut acknowledged_records = 0;
        for (stream, producer_state) in state.producers.iter_mut() {
            acknowledged_records +=
                drain_ready_record_tickets(stream, &mut producer_state.pending)?;
        }

        let fixed = match (&streams, self.target.get()) {
            (None, Some(StreamTarget::Fixed(name))) => Some(name.clone()),
            (None, _) => {
                return Err(PluginError::Internal(
                    "S2 sink target is not initialized".to_string(),
                ));
            }
            (Some(_), _) => None,
        };
        for (index, record) in records.into_iter().enumerate() {
            let stream = match (&fixed, &streams) {
                (Some(name), _) => name,
                (None, Some(streams)) => &streams[index],
                (None, None) => unreachable!("fixed target resolved above"),
            };
            let producer_state = state.producer_for(stream).await?;
            let ticket = producer_state.producer.submit(record).await.map_err(|e| {
                PluginError::Internal(format!(
                    "failed to submit record to S2 Producer for stream '{}': {}",
                    stream, e
                ))
            })?;
            producer_state.pending.push_back(ticket);
        }

        let pending_records = state
            .producers
            .values()
            .map(|p| p.pending.len())
            .sum::<usize>();
        Ok((pending_records, acknowledged_records))
    }

    /// Takes every producer's pending tickets and awaits them all.
    async fn flush_pending_records(&self) -> Result<usize, PluginError> {
        let pending: Vec<(String, VecDeque<RecordSubmitTicket>)> = {
            let mut state_guard = self.state.lock().await;
            let state = state_guard
                .as_mut()
                .ok_or_else(|| PluginError::Internal("S2 sink is not initialized".to_string()))?;
            state
                .producers
                .iter_mut()
                .map(|(stream, producer_state)| {
                    (stream.clone(), std::mem::take(&mut producer_state.pending))
                })
                .collect()
        };

        let mut flushed = 0;
        for (stream, tickets) in pending {
            flushed += await_record_tickets(&stream, tickets).await?;
        }
        Ok(flushed)
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
        let Some(state) = self.state.lock().await.take() else {
            return Ok(());
        };

        let mut flushed_records = 0;
        let mut first_error = None;
        for (stream, producer_state) in state.producers {
            match await_record_tickets(&stream, producer_state.pending).await {
                Ok(flushed) => flushed_records += flushed,
                Err(e) => first_error = first_error.or(Some(e)),
            }
            if let Err(e) = producer_state.producer.close().await {
                first_error = first_error.or_else(|| {
                    Some(PluginError::Internal(format!(
                        "stream '{}': failed to close S2 Producer: {}",
                        stream, e
                    )))
                });
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        info!(
            stream_id = %stream_id,
            flushed_records,
            "S2 sink terminated after closing Producers"
        );
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for S2Sink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.state.lock().await.is_some() {
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
        let target = stream_target_from_options(&self.opts)?;

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
            cfg = cfg.with_endpoints(crate::utils::s2::s2_endpoints(&endpoint)?);
        }

        let s2 = S2::new(cfg)
            .map_err(|e| PluginError::Internal(format!("failed to construct S2 client: {}", e)))?;
        let basin_handle = s2.basin(basin_name.clone());

        let stream_id = match &target {
            StreamTarget::Fixed(stream_name) => format!("{}/{}", basin_name, stream_name),
            StreamTarget::Template(_) => format!(
                "{}/{}",
                basin_name,
                self.opts.get_or("stream_template", "<template>")
            ),
        };

        let mut state = SinkState {
            basin: basin_handle,
            producer_config,
            ensure_stream,
            producers: HashMap::new(),
        };
        // A fixed stream is ensured and its producer opened up front, so
        // configuration problems fail initialization rather than the first
        // batch. Template streams are only known per row; they are ensured
        // and opened lazily on first use.
        if let StreamTarget::Fixed(stream_name) = &target {
            state
                .producer_for(stream_name)
                .await
                .map_err(|e| PluginError::Internal(format!("stream_id '{}': {}", stream_id, e)))?;
        }

        let _ = self.target.set(target);
        let _ = self.stream_id.set(stream_id.clone());
        let mut state_guard = self.state.lock().await;
        if state_guard.is_some() {
            return Ok(());
        }
        *state_guard = Some(state);

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
        let ops = dbz_ops_from_batch(&batch)?;
        let streams = match self.target.get() {
            Some(StreamTarget::Template(segments)) => Some(resolve_streams(segments, &batch)?),
            _ => None,
        };
        let json_rows =
            record_batch_json::record_batch_to_line_delimited_json(&batch).map_err(|e| {
                PluginError::Internal(format!(
                    "stream '{}': failed to convert batch to JSON: {}",
                    stream_id, e
                ))
            })?;
        let total = json_rows.len();
        let records =
            append_records_from_json_rows(json_rows, ops.as_deref()).map_err(|e| match e {
                PluginError::Internal(msg) => {
                    PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
                }
                other => other,
            })?;
        let (pending_records, acknowledged_records) = self
            .submit_records(records, streams)
            .await
            .map_err(|e| match e {
            PluginError::Internal(msg) => {
                PluginError::Internal(format!("stream '{}': {}", stream_id, msg))
            }
            other => other,
        })?;

        self.metrics
            .record_count("s2_sink.records_submitted", total as u64);
        self.metrics
            .record_count("s2_sink.records_acknowledged", acknowledged_records as u64);
        self.metrics
            .record_gauge("s2_sink.pending_records", pending_records as u64);

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
        let flush_started_at = std::time::Instant::now();
        let flushed_records = self.flush_pending_records().await?;
        self.metrics.record_latency(
            "s2_sink.checkpoint_flush_latency",
            flush_started_at.elapsed(),
        );
        self.metrics
            .record_count("s2_sink.records_acknowledged", flushed_records as u64);
        self.metrics.record_gauge("s2_sink.pending_records", 0);
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

/// Header carrying the row kind on every record, Debezium-encoded — the same
/// scheme as streamling's Kafka sink (`dbz.op` message header).
pub(crate) const OP_HEADER: &str = "dbz.op";

/// Maps the batch's `_gs_op` row kinds to Debezium ops (i→c, u→u, d→d);
/// None when the batch has no `_gs_op` column.
pub(crate) fn dbz_ops_from_batch(
    batch: &RecordBatch,
) -> Result<Option<Vec<&'static str>>, PluginError> {
    let Some(column) = batch.column_by_name(STREAMLING_COLUMN_NAME_OP) else {
        return Ok(None);
    };
    let ops = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PluginError::Internal(format!(
                "column '{}' must be Utf8, got {}",
                STREAMLING_COLUMN_NAME_OP,
                column.data_type()
            ))
        })?;
    ops.iter()
        .map(|op| match op {
            Some("i") => Ok("c"),
            Some("u") => Ok("u"),
            Some("d") => Ok("d"),
            other => Err(PluginError::Internal(format!(
                "invalid '{}' row kind {:?} (expected i/u/d)",
                STREAMLING_COLUMN_NAME_OP, other
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Reads the routing target: exactly one of `stream` (fixed) or
/// `stream_template` (per-row, with `{column}` placeholders).
pub(crate) fn stream_target_from_options(
    opts: &PluginOptions,
) -> Result<StreamTarget, PluginError> {
    let stream = opts.get_or("stream", "");
    let template = opts.get_or("stream_template", "");
    match (stream.is_empty(), template.is_empty()) {
        (false, true) => Ok(StreamTarget::Fixed(stream.parse().map_err(|e| {
            PluginError::Internal(format!("invalid stream name '{}': {}", stream, e))
        })?)),
        (true, false) => Ok(StreamTarget::Template(parse_stream_template(&template)?)),
        (false, false) => Err(PluginError::Internal(
            "s2_sink: 'stream' and 'stream_template' are mutually exclusive".to_string(),
        )),
        (true, true) => Err(PluginError::Internal(
            "s2_sink: one of 'stream' or 'stream_template' is required".to_string(),
        )),
    }
}

/// Parses `events/{tenant}`-style templates into literal and column segments.
pub(crate) fn parse_stream_template(template: &str) -> Result<Vec<Segment>, PluginError> {
    let mut segments = Vec::new();
    let mut literal = String::new();
    let mut chars = template.chars();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if !literal.is_empty() {
                    segments.push(Segment::Literal(std::mem::take(&mut literal)));
                }
                let mut column = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some('{') | None => {
                            return Err(PluginError::Internal(format!(
                                "s2_sink: unclosed '{{' in stream_template '{template}'"
                            )));
                        }
                        Some(c) => column.push(c),
                    }
                }
                if column.is_empty() {
                    return Err(PluginError::Internal(format!(
                        "s2_sink: empty column placeholder in stream_template '{template}'"
                    )));
                }
                segments.push(Segment::Column(column));
            }
            '}' => {
                return Err(PluginError::Internal(format!(
                    "s2_sink: unmatched '}}' in stream_template '{template}'"
                )));
            }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }
    if segments.is_empty() {
        return Err(PluginError::Internal(
            "s2_sink: stream_template cannot be empty".to_string(),
        ));
    }
    Ok(segments)
}

/// Resolves the target stream name for each row of the batch. Placeholder
/// columns must exist, be non-null, and yield a valid stream name.
pub(crate) fn resolve_streams(
    segments: &[Segment],
    batch: &RecordBatch,
) -> Result<Vec<StreamName>, PluginError> {
    let columns: HashMap<&str, &ArrayRef> = segments
        .iter()
        .filter_map(|segment| match segment {
            Segment::Column(name) => Some(name.as_str()),
            Segment::Literal(_) => None,
        })
        .map(|name| {
            batch
                .column_by_name(name)
                .map(|column| (name, column))
                .ok_or_else(|| {
                    PluginError::Internal(format!(
                        "s2_sink: stream_template column '{name}' not found in batch"
                    ))
                })
        })
        .collect::<Result<_, _>>()?;

    (0..batch.num_rows())
        .map(|row| {
            let mut name = String::new();
            for segment in segments {
                match segment {
                    Segment::Literal(literal) => name.push_str(literal),
                    Segment::Column(column) => {
                        let array = columns[column.as_str()];
                        if array.is_null(row) {
                            return Err(PluginError::Internal(format!(
                                "s2_sink: stream_template column '{column}' is null at row {row}"
                            )));
                        }
                        name.push_str(&array_value_to_string(array, row).map_err(|e| {
                            PluginError::Internal(format!(
                                "s2_sink: failed to render stream_template column \
                                 '{column}' at row {row}: {e}"
                            ))
                        })?);
                    }
                }
            }
            name.parse().map_err(|e| {
                PluginError::Internal(format!(
                    "s2_sink: stream_template resolved to invalid stream name '{name}': {e}"
                ))
            })
        })
        .collect()
}

pub(crate) fn append_records_from_json_rows(
    json_rows: Vec<Vec<u8>>,
    ops: Option<&[&'static str]>,
) -> Result<Vec<AppendRecord>, PluginError> {
    if let Some(ops) = ops
        && ops.len() != json_rows.len()
    {
        return Err(PluginError::Internal(format!(
            "op count {} does not match row count {}",
            ops.len(),
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
            match ops {
                Some(ops) => record
                    .with_headers([Header::new(OP_HEADER, ops[index])])
                    .map_err(|e| {
                        PluginError::Internal(format!("failed to set S2 record header: {}", e))
                    }),
                None => Ok(record),
            }
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
    use arrow::array::{ArrayRef, Int64Array};
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
        assert!(records[0].headers().is_empty());
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

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            "s2_sink",
            "STREAMLING__PLUGIN__S2_SINK_ROUTING_TEST",
        )
    }

    #[test]
    fn test_stream_and_template_options_are_exclusive_and_required() {
        assert!(matches!(
            stream_target_from_options(&options(&[("stream", "events")])),
            Ok(StreamTarget::Fixed(_))
        ));
        assert!(matches!(
            stream_target_from_options(&options(&[("stream_template", "events/{tenant}")])),
            Ok(StreamTarget::Template(_))
        ));

        let err = stream_target_from_options(&options(&[
            ("stream", "events"),
            ("stream_template", "events/{tenant}"),
        ]))
        .expect_err("both set should fail");
        assert!(err.to_string().contains("mutually exclusive"), "got {err}");

        let err = stream_target_from_options(&options(&[])).expect_err("neither set should fail");
        assert!(err.to_string().contains("required"), "got {err}");
    }

    #[test]
    fn test_parses_stream_templates() {
        assert_eq!(
            parse_stream_template("events/{tenant}-{region}").expect("valid template"),
            vec![
                Segment::Literal("events/".to_string()),
                Segment::Column("tenant".to_string()),
                Segment::Literal("-".to_string()),
                Segment::Column("region".to_string()),
            ]
        );

        for invalid in ["events/{tenant", "events/{}", "events/}", "{a{b}}", ""] {
            assert!(
                parse_stream_template(invalid).is_err(),
                "'{invalid}' should be rejected"
            );
        }
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
        let ops = dbz_ops_from_batch(&op_batch(vec!["i", "u", "d"]))
            .expect("map ops")
            .expect("ops present");
        assert_eq!(ops, vec!["c", "u", "d"]);

        let err = dbz_ops_from_batch(&op_batch(vec!["x"])).expect_err("invalid op should fail");
        assert!(err.to_string().contains("row kind"), "got {err}");
    }

    #[test]
    fn test_batch_without_op_column_yields_no_ops() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("valid batch");
        assert!(dbz_ops_from_batch(&batch).expect("map ops").is_none());
    }

    #[test]
    fn test_ops_become_dbz_op_headers() {
        let rows = vec![br#"{"id":1}"#.to_vec(), br#"{"id":2}"#.to_vec()];
        let records = append_records_from_json_rows(rows, Some(&["c", "d"])).expect("convert rows");

        let headers = records[1].headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name.as_ref(), OP_HEADER.as_bytes());
        assert_eq!(headers[0].value.as_ref(), b"d");

        let err = append_records_from_json_rows(vec![br#"{"id":1}"#.to_vec()], Some(&["c", "d"]))
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
