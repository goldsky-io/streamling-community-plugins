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
