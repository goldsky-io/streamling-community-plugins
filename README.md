# Streamling Community Plugins

Community-maintained plugins for [Streamling](https://github.com/goldsky-io/streamling).

## Available Plugins

### S3 Sink (`s3_sink`)

Writes data as Parquet files to S3-compatible storage. Supports optional Hive-style partitioning.

| YAML option | Env var | Required | Description |
|---|---|---|---|
| `bucket` | — | yes | S3 bucket name |
| `region` | `STREAMLING__PLUGIN__S3_SINK__REGION` | yes | AWS region |
| `access_key_id` | `STREAMLING__PLUGIN__S3_SINK__ACCESS_KEY_ID` | yes | AWS access key (env var preferred) |
| `secret_access_key` | `STREAMLING__PLUGIN__S3_SINK__SECRET_ACCESS_KEY` | yes | AWS secret key (env var preferred) |
| `session_token` | `STREAMLING__PLUGIN__S3_SINK__SESSION_TOKEN` | no | STS session token |
| `prefix` | — | no | Key prefix (trailing `/` is stripped) |
| `endpoint` | — | no | Custom S3-compatible endpoint URL |
| `allow_http` | — | no | Allow plain HTTP (auto-detected from `endpoint`) |
| `partition_columns` | — | no | Comma-separated column names for Hive partitioning |
| `max_concurrent_partition_uploads` | `STREAMLING__PLUGIN__S3_SINK__MAX_CONCURRENT_PARTITION_UPLOADS` | no | Max parallel partition uploads (default: 16) |

### MySQL Sink (`mysql_sink`)

Writes to MySQL with upsert/delete (CDC) support. Rows with `_gs_op = "d"` are deleted; all others are upserted.

All YAML options can also be set via `STREAMLING__PLUGIN__MYSQL_SINK__<KEY>` environment variables (uppercase key).

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

| YAML option | Env var | Required | Description |
|---|---|---|---|
| `queue_url` | — | yes | SQS queue URL |
| `region` | `STREAMLING__PLUGIN__SQS_SINK__REGION` | no | AWS region override |
| `access_key_id` | `STREAMLING__PLUGIN__SQS_SINK__ACCESS_KEY_ID` | no | AWS access key (env var preferred) |
| `secret_access_key` | `STREAMLING__PLUGIN__SQS_SINK__SECRET_ACCESS_KEY` | no | AWS secret key (env var preferred) |
| `session_token` | `STREAMLING__PLUGIN__SQS_SINK__SESSION_TOKEN` | no | STS session token |
| `endpoint_url` | — | no | Custom SQS endpoint (e.g. LocalStack) |

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
