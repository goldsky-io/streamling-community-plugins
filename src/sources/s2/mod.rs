//! S2 (s2.dev) source - reads records from S2 streams into Arrow batches.
//!
//! ## Configuration
//!
//! Required:
//! - access_token (secret) — supply via env STREAMLING__PLUGIN__S2_SOURCE__ACCESS_TOKEN
//!   when possible; YAML inlining is supported but logs WARN.
//! - basin — S2 basin name.
//! - At least one of:
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
//!   column, restored from a record's Debezium-style `dbz.op` header when
//!   present (the same convention streamling's Kafka sink uses; c|r → i,
//!   u → u, d → d) and "i" otherwise — so CDC streams round-trip through S2
//!   losslessly.
//! - include_metadata (typed mode; default false) — append `_s2_stream`,
//!   `_s2_seq_num`, `_s2_timestamp` columns to the configured schema.
//! - on_malformed (typed mode; default `error`) — `error` fails the batch when
//!   a record body cannot be decoded (the source retries the same records, so
//!   it stalls rather than losing data); `skip` drops undecodable records with
//!   a WARN log.
//! - start_position (default `earliest`) — where to begin reading a stream
//!   that has no checkpointed position yet: `earliest` or `latest`. Applies to
//!   every exact or prefix-discovered stream, regardless of when it is found.
//! - batch_size (default 1000) — max records per generated Arrow batch.
//! - update_streams_interval_secs (default 60) — how often `stream_prefix`
//!   fetches the next page of up to 1,000 streams. Readers start as pages
//!   arrive; removals are applied only after a complete scan.
//! - endpoint — optional S2-compatible endpoint, useful for s2-lite.
//! - request_timeout_ms (default 5000) — per-request HTTP timeout.
//!
//! Each option can be overridden by the matching STREAMLING__PLUGIN__S2_SOURCE__<KEY>
//! env var; the env var wins when both are set.
//!
//! ## Metrics
//!
//! - `s2_source.records_read` (count) — records received from S2 read
//!   sessions.
//! - `s2_source.records_emitted` (count) — rows emitted in generated batches.
//! - `s2_source.carry_records` (gauge) — records buffered ahead of the next
//!   batch.
//! - `s2_source.open_readers` (gauge) — stream reader tasks running.
//! - `s2_source.read_session_reopens` (count) — read sessions reopened after
//!   an error the SDK did not absorb.
//!
//! ## Architecture
//!
//! One background task per stream holds a long-lived S2 read session (the SDK
//! resumes it transparently on retryable errors; the task reopens it with
//! backoff otherwise) and pushes record batches into a bounded channel.
//! `generate_batch` drains that channel, converts records to Arrow, and
//! advances an in-memory per-stream position. With `stream_prefix`, a refresh
//! task fetches one page of up to 1,000 streams per interval. It starts new
//! readers page by page and stops missing readers after a complete scan.
//!
//! ## Delivery semantics
//!
//! At-least-once. Per-stream next-sequence-numbers advance only once a batch
//! has been handed to the host (a checkpoint marker processed while a batch
//! is still queued for delivery can therefore never record positions past
//! it), are snapshotted at each checkpoint marker, and persist to the plugin
//! state backend when the checkpoint is finalized. On restart the source
//! resumes from the persisted positions, so records emitted after the last
//! finalized checkpoint are read again.
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
