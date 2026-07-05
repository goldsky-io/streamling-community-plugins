//! S2 (s2.dev) source - reads records from S2 streams into Arrow batches.
//!
//! ## Configuration
//!
//! Required:
//! - access_token (secret) — supply via env STREAMLING__PLUGIN__S2_SOURCE__ACCESS_TOKEN
//!   when possible; YAML inlining is supported but logs WARN.
//! - basin — S2 basin name.
//! - At least one of:
//!   - stream — a single stream name (or comma-separated list),
//!   - streams — comma-separated stream names,
//!   - stream_prefix — read every stream whose name starts with this prefix;
//!     the stream list is refreshed periodically so newly created streams are
//!     picked up automatically.
//!
//! Optional:
//! - schema — typed mode: comma-separated `name:type` output columns decoded
//!   from JSON record bodies (append `?` to a type for nullable, e.g.
//!   `id:int64,value:string?`). Without it the source runs in raw mode and
//!   emits the S2 record envelope: `stream`, `seq_num`, `timestamp`,
//!   `headers`, `body`. Both modes lead with streamling's `_gs_op` row-kind
//!   column, always "i" (S2 streams are append-only).
//! - include_metadata (typed mode; default false) — append `_s2_stream`,
//!   `_s2_seq_num`, `_s2_timestamp` columns to the configured schema.
//! - on_malformed (typed mode; default `error`) — `error` fails the batch when
//!   a record body cannot be decoded (the source retries the same records, so
//!   it stalls rather than losing data); `skip` drops undecodable records with
//!   a WARN log.
//! - start_position (default `earliest`) — where to begin reading a stream
//!   that has no checkpointed position yet: `earliest` or `latest`.
//! - batch_size (default 1000) — max records per generated Arrow batch.
//! - batch_interval_ms (default 100) — max wait for the first record in
//!   generate_batch before emitting an empty batch.
//! - max_buffered_batches (default 16) — bounded buffer of S2 read batches
//!   shared by all stream readers; when full, readers stop pulling from S2
//!   (backpressure).
//! - update_streams_interval_secs (default 60) — how often `stream_prefix`
//!   re-lists streams.
//! - ignore_command_records (default true) — filter out S2 command records
//!   (fence/trim).
//! - endpoint — optional S2-compatible endpoint, useful for s2-lite.
//! - request_timeout_ms (default 5000) — per-request HTTP timeout.
//!
//! Each option can be overridden by the matching STREAMLING__PLUGIN__S2_SOURCE__<KEY>
//! env var; the env var wins when both are set.
//!
//! ## Architecture
//!
//! One background task per stream holds a long-lived S2 read session (the SDK
//! resumes it transparently on retryable errors; the task reopens it with a
//! delay otherwise) and pushes record batches into a bounded channel.
//! `generate_batch` drains that channel, converts records to Arrow, and
//! advances an in-memory per-stream position. With `stream_prefix`, a refresh
//! task periodically lists streams and starts/stops readers to match.
//!
//! ## Delivery semantics
//!
//! At-least-once. Per-stream next-sequence-numbers are snapshotted at each
//! checkpoint marker and persisted to the plugin state backend when the
//! checkpoint is finalized. On restart the source resumes from the persisted
//! positions, so records emitted after the last finalized checkpoint are read
//! again.
//!
//! ## Example
//!
//! JSON events flowing from S2 into ClickHouse:
//!
//! ```yaml
//! sources:
//!   events:
//!     type: s2_source
//!     basin: my-basin
//!     stream_prefix: "events/"
//!     schema: "id:int64,user:string,amount:float64,at:timestamp"
//!     include_metadata: true
//!
//! transforms: {}
//!
//! sinks:
//!   clickhouse:
//!     type: clickhouse
//!     from: events
//!     table: events
//!     primary_key: id
//! ```

mod config;
mod convert;
mod reader;
mod source;

pub use source::S2Source;
