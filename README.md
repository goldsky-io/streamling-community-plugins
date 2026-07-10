# Streamling Community Plugins

Community-maintained plugins for [Streamling](https://github.com/goldsky-io/streamling).

## How to Use

Build the plugins as a shared library (`cargo build --profile release-optimized --lib`) or download a pre-built release from the GitHub releases page. 
Then set the `STREAMLING__PLUGIN__PATH` environment variable to the path of the compiled `.so`/`.dylib`/`.dll` file and run `streamling` as usual.

## Available Plugins

### S3 Sink (`s3_sink`)

Writes data as Parquet files to S3-compatible storage. Supports optional Hive-style partitioning.

All YAML options can also be set via `STREAMLING__PLUGIN__S3_SINK__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Description |
|---|---|---|
| `bucket` | yes | S3 bucket name |
| `region` | yes | AWS region |
| `access_key_id` | yes | AWS access key (env var preferred) |
| `secret_access_key` | yes | AWS secret key (env var preferred) |
| `session_token` | no | STS session token (env var preferred) |
| `prefix` | no | Key prefix (trailing `/` is stripped) |
| `endpoint` | no | Custom S3-compatible endpoint URL |
| `allow_http` | no | Allow plain HTTP (auto-detected from `endpoint`) |
| `partition_columns` | no | Comma-separated column names for Hive partitioning |
| `max_concurrent_partition_uploads` | no | Max parallel partition uploads (default: 16) |

### MySQL Sink (`mysql_sink`)

Writes to MySQL with upsert/delete (CDC) support. Rows with `_gs_op = "d"` are deleted; all others are upserted.

All YAML options can also be set via `STREAMLING__PLUGIN__MYSQL_SINK__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `host` | yes | — | MySQL host |
| `port` | no | `3306` | MySQL port |
| `user` | yes | — | MySQL user |
| `password` | yes | — | MySQL password |
| `database` | yes | — | Database name |
| `table` | yes | — | Target table (auto-created if missing) |
| `primary_key` | no | — | Comma-separated PK columns for upsert/delete |
| `on_conflict` | no | `update` | `update` (upsert) or `nothing` (`INSERT IGNORE`) |
| `sslmode` | no | `disabled` | `disabled`, `preferred`, `required`, `verify_ca`, `verify_identity` |
| `batch_size` | no | `1000` | Max rows per INSERT statement |

### SQS Sink (`sqs`)

Sends each row as a JSON message to an AWS SQS queue. Handles SQS 10-message batch limits and retries partial failures.

All YAML options can also be set via `STREAMLING__PLUGIN__SQS_SINK__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Description |
|---|---|---|
| `queue_url` | yes | SQS queue URL |
| `region` | no | AWS region override |
| `endpoint_url` | no | Custom SQS endpoint (e.g. LocalStack) |
| `access_key_id` | no | AWS access key (env var preferred) |
| `secret_access_key` | no | AWS secret key (env var preferred) |
| `session_token` | no | STS session token (env var preferred) |

### S2 Sink (`s2_sink`)

Appends each row as a JSON record to a stream on [s2.dev](https://s2.dev) — a durable streaming service — via the `s2-sdk` Producer. Rows are JSON-serialized and submitted to the Producer, which batches them internally; checkpoint markers drain pending record tickets, so the dispatcher only acknowledges a checkpoint after S2 has durably appended every record submitted before it. Each record carries a Debezium-style `dbz.op` header with the row kind, like the built-in Kafka sink; the `_gs_op` column is stripped from record bodies. Delivery is at-least-once — ambiguous append retries and checkpoint replay can duplicate records.

All YAML options can also be set via `STREAMLING__PLUGIN__S2_SINK__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `access_token` | yes | — | S2 access token (env var preferred) |
| `basin` | yes | — | S2 basin name (must already exist) |
| `stream` | one of | — | Fixed S2 stream name within the basin |
| `stream_template` | one of | — | Per-row stream name with `{column}` placeholders (e.g. `events/{tenant}`); streams are created lazily as names resolve |
| `ensure_stream` | no | `true` | Create target streams if missing (idempotent). Disable if the token only has append scope, or when the basin has `create_stream_on_append` enabled (the natural pairing for `stream_template`) |
| `endpoint` | no | — | Custom S2-compatible endpoint URL (e.g. for s2-lite) |
| `request_timeout_ms` | no | `5000` | Per-request HTTP timeout (ms) |
| `linger_ms` | no | `5` | How long the Producer waits for more records before flushing a partial batch (ms) |

### S2 Source (`s2_source`)

Reads records from streams on [s2.dev](https://s2.dev) into Arrow batches, tailing each stream over a long-lived read session; with `stream_prefix`, newly created streams are picked up automatically. Emits either the raw record envelope (`stream`, `seq_num`, `timestamp`, `headers`, `body`) or, with `schema`, typed columns decoded from JSON bodies. Delivery is at-least-once with checkpointed per-stream resume — see the module docs in [`src/sources/s2/mod.rs`](src/sources/s2/mod.rs) for details.

All YAML options can also be set via `STREAMLING__PLUGIN__S2_SOURCE__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `access_token` | yes | — | S2 access token (env var preferred) |
| `basin` | yes | — | S2 basin name |
| `streams` | one of | — | Exact stream name(s), comma-separated |
| `stream_prefix` | one of | — | Read every stream whose name starts with this prefix |
| `schema` | no | — | Typed mode: `name:type` columns decoded from JSON bodies (`bool`, `int8..64`, `uint8..64`, `float32/64`, `string`, `date`, `timestamp[_s/_ms/_us/_ns]`; `?` suffix = nullable) |
| `include_metadata` | no | `false` | Typed mode: append `_s2_stream`, `_s2_seq_num`, `_s2_timestamp` columns |
| `on_malformed` | no | `error` | Typed mode: `error` fails (and retries) the batch on an undecodable body; `skip` drops it with a WARN |
| `start_position` | no | `earliest` | Where to start any stream with no checkpointed position: `earliest` or `latest`, regardless of when it is discovered |
| `batch_size` | no | `1000` | Max records per generated Arrow batch |
| `update_streams_interval_secs` | no | `60` | How often `stream_prefix` fetches the next page (up to 1,000 streams); removals apply after a complete scan |
| `endpoint` | no | — | Custom S2-compatible endpoint URL (e.g. for s2-lite) |
| `request_timeout_ms` | no | `5000` | Per-request HTTP timeout (ms) |

### Postgres CDC Source (`postgres_cdc_source`)

Streams Postgres logical-replication changes (an initial table copy followed by
continuous CDC) by embedding the [`supabase/etl`](https://github.com/supabase/etl)
pipeline. One source instance replicates exactly one table. The output is the
table's own typed columns plus a `_gs_op` column (`i` = insert/copy, `u` =
update, `d` = delete) — no CDC envelope. Sinks should upsert by primary key and
delete on `_gs_op = "d"`; delivery is at-least-once, so replays are idempotent.

Sources that share a `slot_name` share one replication slot and one etl pipeline
(coordinated fan-out and acks); give each source its own `slot_name` otherwise.

**Requirements & caveats:**

- **Postgres >= 14** and `wal_level = logical` (Postgres 13 does not work).
- The connecting role needs specific privileges; see **Permissions** below.
- **Schema evolution is not supported.** The output schema is fixed at startup
  from the table's columns; columns added later are dropped, dropped columns
  become null, and type/constraint changes are not applied. Recreate the source
  after DDL.
- `UPDATE`/`DELETE` carry a full old-row image only with
  `ALTER TABLE ... REPLICA IDENTITY FULL`; the default emits key-only images for
  deletes (and unchanged-TOAST columns are null on updates).
- **Metadata storage:** etl persists replication state (table schemas, sync
  progress, slot state) in a `PostgresStore`, and installs an `etl` schema
  (helper functions + a DDL trigger) in the source database on start. By default
  the store is the source database; set the `store_*` options to keep this
  bookkeeping elsewhere. (A future version may use Streamling's own state store.)
- **Memory backpressure can stall live changes.** etl pauses its replication
  apply stream while *system-wide* memory use exceeds
  `memory_backpressure_activate_threshold` (default 85%), resuming only once it
  drops below `memory_backpressure_resume_threshold` (default 75%). On hosts that
  sit above the resume threshold — common on local/dev machines where reported memory 
  use stays high — the stream stays paused, so live changes stop arriving after 
  the initial copy. Set `memory_backpressure_enabled: false` to turn it off for 
  local/dev, or raise the thresholds.

**Permissions**

The connecting role needs the `REPLICATION` attribute (for logical replication)
and `SELECT` on the replicated table (for the initial copy). Because
`auto_create_publication` is on by default, the role must also be able to
create/alter the publication and etl's metadata schema: grant `CREATE` on the
database, and the role must **own** the replicated table(s) — only the owner
(or a superuser) can add a table to a publication. Example least-privilege
setup:

```sql
-- A login role with the REPLICATION attribute.
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'change-me';

-- Run these in the database you replicate from:
GRANT CONNECT ON DATABASE mydb TO cdc_user;
GRANT USAGE  ON SCHEMA public TO cdc_user;
GRANT SELECT ON TABLE public.users TO cdc_user;   -- initial table copy
GRANT CREATE ON DATABASE mydb TO cdc_user;         -- publication + etl schema

-- The role must own the table to add it to a publication. Either create the
-- table as cdc_user, or transfer ownership:
ALTER TABLE public.users OWNER TO cdc_user;
```

If the role can't own the tables (or you'd rather manage the publication
yourself), create the publication with a privileged role and set
`auto_create_publication: false`.

All YAML options can also be set via `STREAMLING__PLUGIN__POSTGRES_CDC_SOURCE__<KEY>`
environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `host` | yes | — | Postgres host |
| `database` | yes | — | Database name |
| `username` | yes | — | Role with `REPLICATION` |
| `password` | no | — | Postgres password (env var preferred) |
| `port` | no | `5432` | Postgres port |
| `publication_name` | yes | — | Logical-replication publication (auto-created by default; disable with `auto_create_publication: false`) |
| `table` | yes | — | Replicated table, `schema.name` (bare names default to `public`) |
| `slot_name` | yes | — | Replication-slot group key; sources sharing it share one slot |
| `auto_create_publication` | no | `true` | Create the publication if missing and add any registered tables not yet in it (needs CREATE on the DB + table ownership) |
| `tls_enabled` | no | `false` | Require TLS |
| `trusted_root_certs` | no | — | PEM-encoded CA bundle |
| `store_host` / `store_port` / `store_database` / `store_username` / `store_password` | no | source connection | Separate metadata-store database (host/database/username required together; password optional) |
| `batch_max_fill_ms` | no | `1000` | etl batch fill window |
| `batch_max_bytes` | no | `8388608` | etl batch byte budget |
| `max_table_sync_workers` | no | `4` | Parallel initial-copy workers |
| `batch_size` | no | `1000` | Max rows per generated output batch |
| `batch_interval_ms` | no | `100` | Max wait for the first row of a batch |
| `max_buffered_units` | no | `8` | Bounded in-flight write-unit buffer |
| `memory_backpressure_enabled` | no | `true` | Pause the replication apply stream while *system* memory use exceeds the activate threshold. Disable (`false`) on hosts whose system memory stays high, where the pause otherwise stalls live changes (see caveats) |
| `memory_backpressure_activate_threshold` | no | `0.85` | System-memory ratio above which backpressure activates (ignored when disabled) |
| `memory_backpressure_resume_threshold` | no | `0.75` | System-memory ratio below which backpressure releases; must be `<` the activate threshold (ignored when disabled) |

### Quick start

```bash
just check    # verify compilation
just lint     # fmt + clippy
just test     # unit tests
just build    # debug build (.so / .dylib)
```

### Building

```bash
just build-release   # release build
```

The project compiles as a shared library that Streamling loads at runtime via the `STREAMLING__PLUGIN__PATH` environment variable.
