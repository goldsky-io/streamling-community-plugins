//! Apache Iceberg sink — appends rows to an Iceberg table through a REST
//! catalog, writing Parquet data files and committing one snapshot per
//! Streamling checkpoint.
//!
//! ## Configuration
//!
//! | Option | Required | Default | Description |
//! |---|---|---|---|
//! | `catalog_uri` … `catalog_properties` | see below | | Shared REST catalog and storage options |
//! | `table` | yes | | Target table as `namespace.table` (nested namespaces: `a.b.table`) |
//! | `auto_create` | no | `true` | Create the namespace and table if missing, with a schema derived from the sink's input schema. With `false` a missing table fails initialization |
//! | `partition_by` | no | unpartitioned | Partition spec for tables the sink creates: comma-separated Iceberg transforms over input columns — `col` / `identity(col)`, `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`, `truncate(W, col)`. Ignored for existing tables (their spec is used) |
//! | `format_version` | no | `2` | Iceberg format version (`1` or `2`) for tables the sink creates |
//! | `table_properties` | no | | Extra comma-separated `key=value` table properties for tables the sink creates, merged over the sink defaults `write.parquet.compression-codec=zstd`, `write.metadata.delete-after-commit.enabled=true`, `write.metadata.previous-versions-max=100` |
//! | `include_op_column` | no | `true` | Keep Streamling's `_gs_op` row-kind column (`i`/`u`/`d`) as a regular table column; see "Change data" |
//!
//! The shared catalog options are documented in `src/utils/iceberg.rs`:
//! `catalog_uri` (required), `warehouse`, `token` / `credential` (secrets),
//! `s3_endpoint`, `s3_region`, `s3_access_key_id` / `s3_secret_access_key`
//! / `s3_session_token` (secrets), `s3_path_style_access`, and
//! `catalog_properties`. Vended credentials are requested by default.
//!
//! Each option can be overridden by the matching
//! STREAMLING__PLUGIN__ICEBERG_SINK__<KEY> env var (e.g.
//! `STREAMLING__PLUGIN__ICEBERG_SINK__TOKEN`); the env var wins when both
//! are set. Supply secrets through env vars.
//!
//! Data files follow the table's own properties: `write.parquet.compression-codec`
//! / `write.parquet.compression-level` (zstd level 3 by default),
//! `write.parquet.row-group-size-bytes` (128 MiB), `write.parquet.page-size-bytes`,
//! `write.parquet.dict-size-bytes`, `write.target-file-size-bytes` (512 MiB
//! rolling threshold), `write.data.path`, and the commit retry settings
//! `commit.retry.num-retries` / `commit.retry.min-wait-ms` /
//! `commit.retry.max-wait-ms` / `commit.retry.total-timeout-ms`.
//!
//! ## Delivery semantics
//!
//! Batches are written to Parquet data files as they arrive (one rolling
//! file per partition for the epoch; partitioned tables fan out by the
//! partition values Iceberg computes from the spec). At each checkpoint
//! marker the files are closed and committed in one fast-append snapshot
//! whose summary carries `streamling.checkpoint-epoch` (the epoch) and
//! `streamling.writer-id` (a per-sink-instance UUID, also the data file name
//! prefix). Epochs without rows commit nothing. The marker is acknowledged
//! only after the commit succeeds, so the checkpoint marker is the only
//! durability point; a failed commit fails the marker and the epoch is
//! replayed. Commit conflicts with other writers are retried by
//! iceberg-rust on a refreshed table per `commit.retry.*`.
//!
//! `terminate` does not commit. Under Streamling's `fast` drain policy (the
//! default for plain streaming topologies) shutdown skips the terminal
//! checkpoint, so rows written since the last commit are not committed and
//! replay on restart; set `STREAMLING__DRAIN_POLICY=drain` to commit the
//! tail on graceful shutdown.
//!
//! Delivery is **at-least-once**: a crash after the commit but before the
//! checkpoint is acknowledged replays the epoch and appends its rows again.
//! Deduplicate downstream when that matters (e.g. by a key and the
//! snapshot's `streamling.checkpoint-epoch`).
//!
//! Commit cadence equals the Streamling checkpoint interval. Every commit
//! adds a snapshot, a manifest, and at least one small data file per touched
//! partition, so keep checkpoints at **one minute or longer** for Iceberg
//! sinks, and run the usual table maintenance from an engine such as Spark:
//! `rewrite_data_files` (compaction), `expire_snapshots`, and
//! `remove_orphan_files` — the latter also removes data files written for
//! an epoch whose commit never happened (crash or failed commit).
//!
//! ## Change data
//!
//! The sink only appends (iceberg-rust 0.10 cannot commit delete files).
//! By default (`include_op_column=true`) the table is an append-only
//! changelog with a `_gs_op` column, which downstream jobs apply to a
//! current-state table with `MERGE INTO target USING (latest change per key)
//! ON key ... WHEN MATCHED AND _gs_op = 'd' THEN DELETE WHEN MATCHED THEN
//! UPDATE ... WHEN NOT MATCHED AND _gs_op <> 'd' THEN INSERT ...`. With
//! `include_op_column=false` the column is dropped and every row must be an
//! insert: a batch with any `_gs_op` other than `i` fails, because appending
//! updates and deletes as plain rows would silently corrupt the table.
//!
//! ## Schema mapping
//!
//! When the sink creates a table, the Iceberg schema is derived from the
//! input schema (`_gs_op` included unless `include_op_column=false`;
//! non-nullable columns become required):
//!
//! | Arrow input | Iceberg |
//! |---|---|
//! | Boolean | boolean |
//! | Int8/16/32, UInt8/16 | int |
//! | Int64, UInt32 | long |
//! | UInt64 | decimal(20, 0) |
//! | Float16/32, Float64 | float, double |
//! | Decimal128(p, s) | decimal(p, s) |
//! | Decimal256, U256 / I256 (`streamling.u256` / `streamling.i256`) | string (exact decimal text) |
//! | Utf8, LargeUtf8, Utf8View | string |
//! | Binary, LargeBinary, BinaryView / FixedSizeBinary(n) | binary / fixed(n) |
//! | Date32, Date64 | date |
//! | Time32, Time64 | time (microseconds) |
//! | Timestamp(any unit, no tz) | timestamp (microseconds) |
//! | Timestamp(any unit, any tz) | timestamptz (microseconds, UTC) |
//! | Dictionary(_, V) | the mapping of V |
//! | List, LargeList, FixedSizeList, Struct, Map | list, struct, map (recursively) |
//!
//! Nanosecond timestamps lose sub-microsecond precision (format v1/v2 has no
//! nanosecond type). Null-typed columns and U256/I256 nested inside lists or
//! structs are rejected.
//!
//! For an existing table its current schema is authoritative: input columns
//! are matched to table columns by name, and struct fields to table struct
//! fields by name at every depth (through list elements and map values;
//! map keys and values match by position). Table columns and nested fields
//! absent from the input are written as nulls when optional and fail
//! initialization when required; input columns or nested fields absent from
//! the table fail initialization, naming their path.
//!
//! Only lossless conversions are accepted; anything else fails
//! initialization naming the column and both types:
//!
//! - identical types; integer widening (unsigned only into a wider signed
//!   type), and integers into a decimal with enough integer digits;
//! - float → float/double, double → double;
//! - decimal(p, s) → decimal(p', s) with `p' >= p` (same scale);
//!   Decimal256 → string;
//! - within the string family, within the binary family (incl. fixed →
//!   binary); dictionaries by their value type;
//! - Date32/Date64 → date; time and timestamp units into the same or a finer
//!   unit, plus nanoseconds → microseconds (truncating sub-µs digits);
//!   timestamp ↔ timestamptz keeps the stored instant (zoned inputs are
//!   normalized to UTC; tz-less values are taken as UTC);
//! - U256/I256 → string (exact decimal text), or binary / fixed(32) (the raw
//!   32 big-endian bytes).
//!
//! Values that still cannot convert (e.g. a string longer than a fixed width)
//! fail the batch rather than becoming nulls.
//!
//! ## Limitations
//!
//! - Append-only; no upserts or deletes (see "Change data").
//! - No schema evolution: add columns to the table yourself (e.g.
//!   `ALTER TABLE ... ADD COLUMN`) before deploying a pipeline that writes
//!   them; the sink reads the schema at initialization.
//! - REST catalog only; storage through OpenDAL: S3 and S3-compatible
//!   stores, and the local filesystem (`file://`).
//! - Writes the table's default partition spec; existing tables with
//!   sort orders are written unsorted.
//!
//! ## Metrics
//!
//! - `iceberg_sink.rows_written` (count) — rows written to data files.
//! - `iceberg_sink.rows_committed` (count) — rows in committed snapshots.
//! - `iceberg_sink.files_committed` (count) — data files committed.
//! - `iceberg_sink.bytes_committed` (count) — data file bytes committed.
//! - `iceberg_sink.commit_latency` (latency) — time to commit a snapshot,
//!   including conflict retries.
//!
//! ## Example
//!
//! ```yaml
//! sources:
//!   events:
//!     type: kafka
//!     topic: events.v1
//!     starting_offsets: earliest
//!     primary_key: id
//!
//! transforms: {}
//!
//! sinks:
//!   lake:
//!     type: iceberg_sink
//!     from: events
//!     catalog_uri: https://catalog.example.com/api/catalog
//!     warehouse: analytics
//!     table: raw.events
//!     partition_by: day(event_time), bucket(16, id)
//!     # credential via STREAMLING__PLUGIN__ICEBERG_SINK__CREDENTIAL
//! ```

mod config;
mod schema;
pub mod sink;

pub use sink::IcebergSink;
