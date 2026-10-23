//! Apache Iceberg source — streams the rows of an Iceberg table into Arrow
//! batches: a full scan of the current snapshot, then each newly committed
//! `append` snapshot incrementally (Flink's `TABLE_SCAN_THEN_INCREMENTAL`).
//!
//! ## Configuration
//!
//! Catalog options are shared with `iceberg_sink`; see
//! `src/utils/iceberg.rs` for details.
//!
//! | Option | Required | Default | Description |
//! |---|---|---|---|
//! | `catalog_uri` | yes | | REST catalog base URL, e.g. `http://localhost:8181` |
//! | `warehouse` | no | | Warehouse name or location passed to the catalog |
//! | `token` | no | | Static bearer token (secret; env var preferred) |
//! | `credential` | no | | OAuth2 client credentials `client_id:client_secret` (secret; env var preferred) |
//! | `s3_endpoint` / `s3_region` | no | | S3-compatible endpoint and region |
//! | `s3_access_key_id` / `s3_secret_access_key` / `s3_session_token` | no | | Static S3 credentials (secrets); omit for vended credentials or the AWS default chain |
//! | `s3_path_style_access` | no | | Path-style S3 URLs (MinIO and most S3-compatible stores) |
//! | `catalog_properties` | no | | Extra comma-separated `key=value` catalog/FileIO properties |
//! | `table` | yes | | Table to read, `namespace.table` (nested namespaces: `a.b.table`) |
//! | `start_position` | no | `earliest` | Where a source without checkpointed state begins: `earliest` scans the current snapshot, then reads new snapshots; `latest` reads only snapshots committed after the source first starts |
//! | `poll_interval_secs` | no | `60` | How often to reload table metadata (new snapshots, refreshed vended credentials); also caps the read-error backoff (Flink's monitor-interval default) |
//! | `batch_size` | no | `1000` | Max rows per generated Arrow batch |
//! | `on_non_append_snapshot` | no | `error` | `overwrite`/`delete` snapshots: `error` stalls the source at that snapshot (ERROR logs, retried with backoff — a source cannot stop the pipeline; reset the state or switch to `skip`), `skip` passes over them with an INFO log |
//!
//! Each option can be overridden by the matching
//! `STREAMLING__PLUGIN__ICEBERG_SOURCE__<KEY>` env var (e.g.
//! `STREAMLING__PLUGIN__ICEBERG_SOURCE__CREDENTIAL`); the env var wins when
//! both are set.
//!
//! ## Output
//!
//! `_gs_op` (always `i`) followed by the table's columns, converted from the
//! Iceberg schema current when the source starts (Iceberg's own Arrow
//! mapping). The schema is fixed for the run: after a schema change, restart
//! the pipeline to pick it up. Data files written under older schemas are
//! read by field id, so columns added since read as null and renamed columns
//! keep their data.
//!
//! ## Snapshot handling
//!
//! New snapshots are found by walking the current snapshot's ancestry back to
//! the last processed one, oldest first.
//!
//! | Operation | Handling |
//! |---|---|
//! | `append` | Emits the data files the snapshot added |
//! | `replace` | Skipped silently: compaction and rewrites change no rows |
//! | `overwrite` | `on_non_append_snapshot`: `error` (default) or `skip` |
//! | `delete` | `on_non_append_snapshot`: `error` (default) or `skip` |
//!
//! The initial scan (`start_position: earliest`) reads the live rows of the
//! start snapshot, delete files applied, whatever operations produced it.
//!
//! ## Errors
//!
//! A failed read is returned to the host, which logs it at ERROR and asks
//! again; the source first backs off (1s, doubling up to `poll_interval_secs`,
//! reset by the next successful read) and re-reads from its position, so
//! lasting errors (a rejected snapshot, an expired file) stall the source
//! rather than stop it. Table metadata — and with it vended storage
//! credentials — is reloaded whenever `poll_interval_secs` has elapsed,
//! including mid-backlog and while retrying errors; each reload is bounded by
//! a 30s timeout.
//!
//! ## Delivery semantics
//!
//! At-least-once. The position — snapshot, phase (scan or incremental),
//! files done and rows done in the next file, with each snapshot's files
//! read in data-file-path order — is snapshotted at each checkpoint marker
//! and persists to the plugin state backend when the checkpoint is
//! finalized. On restart the source re-plans the checkpointed snapshot and
//! skips the rows the position covers. A batch counts towards the position
//! only once the host asks for the next one (a marker can be processed while
//! a batch is still queued), so while the source is behind, a restart may
//! re-emit the last batch (up to `batch_size` rows) before the finalized
//! checkpoint as well as everything after it; once caught up, the position is
//! exact.
//!
//! ## Limitations
//!
//! - Append-only incremental reads: rows deleted or overwritten after they
//!   were emitted are not retracted (`_gs_op` is always `i`).
//! - No schema evolution mid-run (see Output).
//! - Snapshot retention must exceed the source's downtime: resuming from a
//!   snapshot that was expired, or that is no longer an ancestor of the
//!   current snapshot (table rollback), fails with an error naming the
//!   snapshot; reset the source state or raise `history.expire.*`.
//! - `start_position: latest` resolves the start snapshot when the source
//!   starts; until the first checkpoint is finalized a restart resolves it
//!   again.
//! - REST catalog only.
//!
//! ## Metrics
//!
//! - `iceberg_source.rows_emitted` (count) — rows emitted in generated
//!   batches.
//! - `iceberg_source.files_read` (count) — data files read to the end.
//! - `iceberg_source.snapshots_processed` (count) — snapshots fully read
//!   (the initial scan and each append).
//! - `iceberg_source.snapshots_skipped` (count) — `replace` snapshots, and
//!   `overwrite`/`delete` with `on_non_append_snapshot: skip`.
//! - `iceberg_source.snapshot_lag_ms` (gauge) — now minus the commit time of
//!   the last processed snapshot; grows while the table receives no commits.
//! - `iceberg_source.refresh_errors` (count) — failed metadata reloads
//!   (retried on the next poll).
//! - `iceberg_source.read_errors` (count) — failed `generate_batch` reads
//!   (retried after backoff).
//!
//! All metrics carry the `table` label.
//!
//! ## Example
//!
//! ```yaml
//! sources:
//!   events:
//!     type: iceberg_source
//!     catalog_uri: https://catalog.example.com/api/catalog
//!     warehouse: analytics
//!     table: lake.events
//!     start_position: earliest
//!     poll_interval_secs: 30
//!     # credential via STREAMLING__PLUGIN__ICEBERG_SOURCE__CREDENTIAL
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
mod reader;
mod snapshots;
mod source;

pub use source::IcebergSource;
