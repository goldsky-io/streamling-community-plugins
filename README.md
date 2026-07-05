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

Appends each row as a JSON record to a stream on [s2.dev](https://s2.dev) — a durable streaming service — via the `s2-sdk` Producer. Rows are JSON-serialized and submitted to the Producer, which batches them internally; checkpoint markers drain pending record tickets, so the dispatcher only acknowledges a checkpoint after S2 has durably appended every record submitted before it.

All YAML options can also be set via `STREAMLING__PLUGIN__S2_SINK__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `access_token` | yes | — | S2 access token (env var preferred) |
| `basin` | yes | — | S2 basin name (must already exist) |
| `stream` | yes | — | S2 stream name within the basin |
| `ensure_stream` | no | `true` | Create the stream if missing (idempotent). Disable if the token only has append scope |
| `endpoint` | no | — | Custom S2-compatible endpoint URL (e.g. for s2-lite) |
| `request_timeout_ms` | no | `5000` | Per-request HTTP timeout (ms) |
| `linger_ms` | no | `5` | How long the Producer waits for more records before flushing a partial batch (ms) |

### S2 Source (`s2_source`)

Reads records from streams on [s2.dev](https://s2.dev) into Arrow batches. Each active stream is tailed by a long-lived S2 read session; with `stream_prefix` the stream list is refreshed periodically, so newly created streams are picked up automatically.

Two output modes:

- **Raw** (no `schema`): emits the S2 record envelope — `stream`, `seq_num`, `timestamp` (ms, UTC), `headers` (JSON object, null when empty), `body` — one row per record.
- **Typed** (`schema` set): decodes JSON record bodies into the configured columns, e.g. `schema: "id:int64,value:string?,at:timestamp"` (`?` marks a column nullable). Set `include_metadata: true` to also attach `_s2_stream`, `_s2_seq_num`, `_s2_timestamp` columns.

Both modes lead with streamling's `_gs_op` row-kind column, always `"i"` — S2 streams are append-only, so every record is an insert.

Delivery is at-least-once: per-stream positions are snapshotted at each checkpoint marker and persisted when the checkpoint finalizes, so on restart the source resumes from the last finalized position.

All YAML options can also be set via `STREAMLING__PLUGIN__S2_SOURCE__<KEY>` environment variables (uppercase key). Env vars take precedence over YAML.

| YAML option | Required | Default | Description |
|---|---|---|---|
| `access_token` | yes | — | S2 access token (env var preferred) |
| `basin` | yes | — | S2 basin name |
| `stream` / `streams` | one of | — | Exact stream name(s), comma-separated |
| `stream_prefix` | one of | — | Read every stream whose name starts with this prefix |
| `schema` | no | — | Typed mode: `name:type` columns decoded from JSON bodies (`bool`, `int8..64`, `uint8..64`, `float32/64`, `string`, `date`, `timestamp[_s/_ms/_us/_ns]`; `?` suffix = nullable) |
| `include_metadata` | no | `false` | Typed mode: append `_s2_stream`, `_s2_seq_num`, `_s2_timestamp` columns |
| `on_malformed` | no | `error` | Typed mode: `error` fails (and retries) the batch on an undecodable body; `skip` drops it with a WARN |
| `start_position` | no | `earliest` | Where to start a stream with no checkpointed position: `earliest` or `latest` |
| `batch_size` | no | `1000` | Max records per generated Arrow batch |
| `batch_interval_ms` | no | `100` | Max wait for the first record before emitting an empty batch (ms) |
| `max_buffered_batches` | no | `16` | Bounded buffer of S2 read batches shared by all readers (backpressure) |
| `update_streams_interval_secs` | no | `60` | How often `stream_prefix` re-lists streams |
| `ignore_command_records` | no | `true` | Filter out S2 command records (fence/trim) |
| `endpoint` | no | — | Custom S2-compatible endpoint URL (e.g. for s2-lite) |
| `request_timeout_ms` | no | `5000` | Per-request HTTP timeout (ms) |

Example — events flowing from S2 into ClickHouse:

```yaml
sources:
  events:
    type: s2_source
    basin: my-basin
    stream_prefix: "events/"
    schema: "id:int64,user:string,amount:float64,at:timestamp"
    include_metadata: true

transforms: {}

sinks:
  clickhouse:
    type: clickhouse
    from: events
    table: events
    primary_key: id
```

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
