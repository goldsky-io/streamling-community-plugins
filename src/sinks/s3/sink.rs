use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};
use tracing::{error, info, trace, warn};
use uuid::Uuid;

use super::partitioning::{PartitionConfig, partition_batch};
use crate::utils::plugin_options::PluginOptions;

const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const MAX_RETRY_ATTEMPTS: u32 = 10;

/// Returns `true` if the `object_store::Error` is transient and worth retrying.
///
/// `Generic` errors wrap HTTP-level failures (throttling, 503s, timeouts,
/// connection resets). All other variants (permissions, auth, not-found, config)
/// are permanent.
fn is_retriable_s3_error(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Generic { .. })
}

pub struct S3Sink {
    opts: PluginOptions,
    schema: SchemaRef,
    store: OnceLock<Arc<AmazonS3>>,
    bucket: OnceLock<String>,
    prefix: OnceLock<Option<String>>,
    partition_config: OnceLock<Option<PartitionConfig>>,
    running: Arc<AtomicBool>,
}

impl S3Sink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        _metric_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        S3Sink {
            opts: PluginOptions::new(options, "s3_sink", "STREAMLING__PLUGIN__S3_SINK"),
            schema,
            store: OnceLock::new(),
            bucket: OnceLock::new(),
            prefix: OnceLock::new(),
            partition_config: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Serialize a RecordBatch to Parquet bytes.
    fn serialize_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>, PluginError> {
        let estimated_size = batch.get_array_memory_size();
        let mut buffer = Vec::with_capacity(estimated_size);
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
                let err_msg = format!("Failed to create Parquet writer: {}", e);
                error!(error = %err_msg);
                PluginError::Internal(err_msg)
            })?;

        writer.write(batch).map_err(|e| {
            let err_msg = format!("Failed to write batch to Parquet: {}", e);
            error!(error = %err_msg);
            PluginError::Internal(err_msg)
        })?;

        writer.close().map_err(|e| {
            let err_msg = format!("Failed to close Parquet writer: {}", e);
            error!(error = %err_msg);
            PluginError::Internal(err_msg)
        })?;

        Ok(buffer)
    }

    /// Upload bytes to S3 at the given key, retrying transient errors with exponential backoff.
    async fn upload_to_s3(&self, key: &str, data: Vec<u8>) -> Result<(), PluginError> {
        let store = self.store.get().ok_or_else(|| {
            let err = "S3 store is not initialized".to_string();
            error!(error = %err);
            PluginError::Internal(err)
        })?;
        let bucket = self.bucket.get().ok_or_else(|| {
            let err = "Bucket is not initialized".to_string();
            error!(error = %err);
            PluginError::Internal(err)
        })?;

        let path = object_store::path::Path::from(key);
        let payload: bytes::Bytes = bytes::Bytes::from(data);

        trace!(
            bucket = bucket,
            key = key,
            size_bytes = payload.len(),
            "Uploading to S3"
        );

        let mut attempt: u32 = 0;
        let mut backoff = INITIAL_RETRY_BACKOFF;

        loop {
            attempt += 1;

            match store.put(&path, payload.clone().into()).await {
                Ok(_) => {
                    if attempt > 1 {
                        warn!(
                            bucket = bucket,
                            key = key,
                            attempts = attempt,
                            "S3 upload recovered after retries"
                        );
                    }
                    trace!(bucket = bucket, key = key, "Successfully uploaded to S3");
                    return Ok(());
                }
                Err(e) => {
                    if !is_retriable_s3_error(&e) || attempt >= MAX_RETRY_ATTEMPTS {
                        let err_msg = format!("Failed to upload to S3: {}", e);
                        error!(
                            error = %err_msg,
                            bucket = bucket,
                            key = key,
                            attempt = attempt,
                            "S3 upload failed (permanent)"
                        );
                        return Err(PluginError::Internal(err_msg));
                    }

                    warn!(
                        error = %e,
                        bucket = bucket,
                        key = key,
                        attempt = attempt,
                        max_attempts = MAX_RETRY_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        "S3 upload failed (transient), retrying"
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(
                        Duration::from_millis(backoff.as_millis() as u64 * 2),
                        MAX_RETRY_BACKOFF,
                    );
                }
            }
        }
    }

    /// Build the S3 object key for a file, optionally including a partition path segment.
    fn build_key(&self, partition_segment: Option<&str>) -> String {
        let filename = format!("batch_{}.parquet", Uuid::now_v7());
        let prefix = self.prefix.get().and_then(|o| o.as_deref());

        match (prefix, partition_segment) {
            (Some(p), Some(seg)) => format!("{}/{}/{}", p, seg, filename),
            (Some(p), None) => format!("{}/{}", p, filename),
            (None, Some(seg)) => format!("{}/{}", seg, filename),
            (None, None) => filename,
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for S3Sink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for S3Sink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.store.get().is_some() {
            info!("S3 sink already initialized");
            return Ok(());
        }

        info!("Initializing S3 sink...");

        // Parse and validate partition configuration against schema
        let pc = PartitionConfig::parse(
            self.opts.get("partition_columns").ok().as_deref(),
            &self.schema,
        )?;
        if let Some(ref config) = pc {
            info!(
                partition_columns = ?config.columns,
                "S3 sink partitioning enabled"
            );
        }
        let _ = self.partition_config.set(pc);

        // Cache the trimmed prefix once (avoids per-batch allocation in build_key)
        let prefix = self
            .opts.get("prefix")
            .ok()
            .map(|p| p.trim_end_matches('/').to_string());
        let _ = self.prefix.set(prefix);

        let access_key_id = self.opts.get_secret("access_key_id").ok_or_else(|| {
            let err = "s3_sink: required option 'access_key_id' is not specified";
            error!(error = %err, "S3 sink initialization failed");
            PluginError::Internal(err.to_string())
        })?;

        let secret_access_key =
            self.opts.get_secret("secret_access_key").ok_or_else(|| {
                let err = "s3_sink: required option 'secret_access_key' is not specified";
                error!(error = %err, "S3 sink initialization failed");
                PluginError::Internal(err.to_string())
            })?;

        let region = self.opts.get("region")?;
        let bucket = self.opts.get("bucket")?;

        info!(
            bucket = %bucket,
            region = %region,
            endpoint = self.opts.get("endpoint").ok(),
            "Configuring S3 store"
        );

        // Build S3 store with credentials
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket.clone())
            .with_region(&region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        // Optional session token (required for temporary STS credentials)
        if let Some(token) = self.opts.get_secret("session_token") {
            builder = builder.with_token(token);
        }

        // Optional endpoint URL (for S3-compatible services)
        let mut should_allow_http = false;
        if let Ok(endpoint) = self.opts.get("endpoint") {
            if endpoint.to_lowercase().starts_with("http://") {
                should_allow_http = true;
            }
            builder = builder.with_endpoint(endpoint);
        }

        // Allow HTTP if detected from endpoint URL, or if explicitly set in config
        if should_allow_http
            || self
                .opts.get_or("allow_http", "false")
                .parse::<bool>()
                .unwrap_or(false)
        {
            builder = builder.with_allow_http(true);
        }

        let store = builder.build().map_err(|e| {
            let err_msg = format!("Failed to create S3 store: {}", e);
            error!(error = %err_msg, "S3 sink initialization failed");
            PluginError::Internal(err_msg)
        })?;

        let _ = self.store.set(Arc::new(store));
        let _ = self.bucket.set(bucket.clone());

        info!("S3 sink initialized successfully");
        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if !self.is_running() {
            let err = "S3 sink is not running".to_string();
            error!(error = %err, "S3 sink is not running, cannot process batch");
            return Err(PluginError::Internal(err));
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        trace!(
            rows = batch.num_rows(),
            cols = batch.num_columns(),
            "Processing batch for S3 upload"
        );

        let partition_config = self.partition_config.get().and_then(|o| o.as_ref());

        match partition_config {
            Some(config) => {
                let partitions = partition_batch(&batch, config)?;

                trace!(
                    num_partitions = partitions.len(),
                    "Batch split into partitions"
                );

                // Serialize all partitions to Parquet upfront (CPU-bound, not async)
                let uploads: Vec<(String, Vec<u8>)> = partitions
                    .into_iter()
                    .map(|p| {
                        let key = self.build_key(Some(&p.path_segment));
                        let data = Self::serialize_to_parquet(&p.batch)?;
                        Ok((key, data))
                    })
                    .collect::<Result<Vec<_>, PluginError>>()?;

                // Upload partitions concurrently with a bounded parallelism limit.
                let max_concurrent = self
                    .opts.get_or("max_concurrent_partition_uploads", "16")
                    .parse::<usize>()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Invalid max_concurrent_partition_uploads, using default (16)");
                        16
                    });
                let results: Vec<Result<(), PluginError>> = stream::iter(uploads)
                    .map(|(key, data)| async move {
                        self.upload_to_s3(&key, data).await.inspect_err(|_| {
                            error!(key = key, "Partition upload failed");
                        })
                    })
                    .buffer_unordered(max_concurrent)
                    .collect()
                    .await;

                // Propagate first error if any upload failed
                for result in results {
                    result?;
                }
            }
            None => {
                let key = self.build_key(None);
                let data = Self::serialize_to_parquet(&batch)?;
                self.upload_to_s3(&key, data).await?;
            }
        }

        Ok(())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        info!(?epoch, "S3 sink received checkpoint marker");
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        // No-op: checkpoint acknowledgments are handled by the dispatcher
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, RecordBatchReader, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            make_test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
                Arc::new(Float64Array::from(vec![10.5, 20.0, 30.0])),
            ],
        )
        .unwrap()
    }

    /// Helper to construct an S3Sink for unit testing without real S3 credentials.
    /// Only initializes the OnceLock fields needed for build_key / serialization tests.
    fn make_test_sink(
        options: HashMap<String, String>,
        prefix: Option<String>,
        partition_config: Option<PartitionConfig>,
    ) -> S3Sink {
        let schema = make_test_schema();
        let sink = S3Sink {
            opts: PluginOptions::new(options, "s3_sink", "STREAMLING__PLUGIN__S3_SINK"),
            schema,
            store: OnceLock::new(),
            bucket: OnceLock::new(),
            prefix: OnceLock::new(),
            partition_config: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        };
        let _ = sink.prefix.set(prefix);
        let _ = sink.partition_config.set(partition_config);
        let _ = sink.bucket.set("test-bucket".to_string());
        sink
    }

    // ── Backwards-compatibility: serialize_to_parquet ──

    #[test]
    fn test_serialize_to_parquet_produces_valid_parquet() {
        let batch = make_test_batch();
        let data = S3Sink::serialize_to_parquet(&batch).unwrap();

        assert!(!data.is_empty());
        // Parquet magic bytes: PAR1
        assert_eq!(&data[..4], b"PAR1");
        assert_eq!(&data[data.len() - 4..], b"PAR1");
    }

    #[test]
    fn test_serialize_to_parquet_roundtrips_data() {
        let batch = make_test_batch();
        let data = S3Sink::serialize_to_parquet(&batch).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

        assert_eq!(batches.len(), 1);
        let roundtripped = &batches[0];
        assert_eq!(roundtripped.num_rows(), 3);
        assert_eq!(roundtripped.num_columns(), 3);
        assert_eq!(
            roundtripped.schema().fields().len(),
            batch.schema().fields().len()
        );

        let ids = roundtripped
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);

        let names = roundtripped
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));
    }

    #[test]
    fn test_serialize_to_parquet_preserves_schema_field_names() {
        let batch = make_test_batch();
        let data = S3Sink::serialize_to_parquet(&batch).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
            .unwrap()
            .build()
            .unwrap();
        let roundtripped_schema = reader.schema();
        let field_names: Vec<&str> = roundtripped_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(field_names, vec!["id", "name", "value"]);
    }

    // ── Backwards-compatibility: build_key ──

    #[test]
    fn test_build_key_no_prefix_no_partition() {
        let sink = make_test_sink(HashMap::new(), None, None);
        let key = sink.build_key(None);

        assert!(key.starts_with("batch_"));
        assert!(key.ends_with(".parquet"));
        assert!(!key.contains('/'));
    }

    #[test]
    fn test_build_key_with_prefix_no_partition() {
        let sink = make_test_sink(HashMap::new(), Some("my/prefix".to_string()), None);
        let key = sink.build_key(None);

        assert!(key.starts_with("my/prefix/batch_"));
        assert!(key.ends_with(".parquet"));
    }

    #[test]
    fn test_build_key_prefix_trailing_slash_trimmed_during_init() {
        // The original code did `prefix.trim_end_matches('/')` per-batch.
        // The new code does it once during initialize(). Verify the result
        // matches the original behavior: the prefix stored is already trimmed.
        let mut options = HashMap::new();
        options.insert("prefix".to_string(), "data/output/".to_string());

        let trimmed = options
            .get("prefix")
            .map(|p| p.trim_end_matches('/').to_string());

        let sink = make_test_sink(options, trimmed, None);
        let key = sink.build_key(None);

        // Should be "data/output/batch_*.parquet", NOT "data/output//batch_*.parquet"
        assert!(key.starts_with("data/output/batch_"));
        assert!(!key.contains("//"));
    }

    #[test]
    fn test_build_key_with_partition_and_prefix() {
        let sink = make_test_sink(HashMap::new(), Some("ethereum/transfers".to_string()), None);
        let key = sink.build_key(Some("dt=2025-03-24"));

        assert!(key.starts_with("ethereum/transfers/dt=2025-03-24/batch_"));
        assert!(key.ends_with(".parquet"));
    }

    #[test]
    fn test_build_key_with_partition_no_prefix() {
        let sink = make_test_sink(HashMap::new(), None, None);
        let key = sink.build_key(Some("dt=2025-03-24/chain_id=1"));

        assert!(key.starts_with("dt=2025-03-24/chain_id=1/batch_"));
        assert!(key.ends_with(".parquet"));
    }

    #[test]
    fn test_build_key_generates_unique_filenames() {
        let sink = make_test_sink(HashMap::new(), None, None);
        let key1 = sink.build_key(None);
        let key2 = sink.build_key(None);

        assert_ne!(key1, key2, "Each key should contain a unique UUID");
    }

    // ── Backwards-compatibility: PartitionConfig::parse with no partition_columns ──

    #[test]
    fn test_no_partition_columns_returns_none() {
        let schema = make_test_schema();
        let result = PartitionConfig::parse(None, &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_partition_columns_returns_none() {
        let schema = make_test_schema();
        let result = PartitionConfig::parse(Some(""), &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_whitespace_only_partition_columns_returns_none() {
        let schema = make_test_schema();
        let result = PartitionConfig::parse(Some("   "), &schema).unwrap();
        assert!(result.is_none());
    }

    // ── Backwards-compatibility: key path format matches original ──

    #[test]
    fn test_key_format_matches_original_no_prefix() {
        // Original: `format!("batch_{}.parquet", Uuid::now_v7())`
        // New: same via `build_key(None)` with no prefix
        let sink = make_test_sink(HashMap::new(), None, None);
        let key = sink.build_key(None);

        let re = regex::Regex::new(r"^batch_[0-9a-f-]+\.parquet$").unwrap();
        assert!(
            re.is_match(&key),
            "Key should match pattern 'batch_{{uuid}}.parquet', got: {}",
            key
        );
    }

    #[test]
    fn test_key_format_matches_original_with_prefix() {
        // Original: `format!("{}/{}", prefix.trim_end_matches('/'), filename)`
        // New: same via `build_key(None)` with prefix cached (already trimmed)
        let sink = make_test_sink(HashMap::new(), Some("ethereum/transfers".to_string()), None);
        let key = sink.build_key(None);

        let re = regex::Regex::new(r"^ethereum/transfers/batch_[0-9a-f-]+\.parquet$").unwrap();
        assert!(
            re.is_match(&key),
            "Key should match pattern 'prefix/batch_{{uuid}}.parquet', got: {}",
            key
        );
    }

    // ── Backwards-compatibility: partition config does not affect non-partitioned path ──

    #[test]
    fn test_partition_config_none_takes_non_partitioned_path() {
        // When partition_config is None, process_batch should call build_key(None)
        // and produce a single upload. Verify by checking build_key output.
        let sink = make_test_sink(HashMap::new(), Some("output".to_string()), None);

        let key = sink.build_key(None);
        assert!(key.starts_with("output/batch_"));
        assert!(
            !key.contains("="),
            "Non-partitioned key should not contain '=' (Hive partition marker)"
        );
    }

    // ── S3 error classification ──

    #[test]
    fn test_generic_s3_error_is_retriable() {
        let err = object_store::Error::Generic {
            store: "AmazonS3",
            source: "connection reset by peer".into(),
        };
        assert!(is_retriable_s3_error(&err));
    }

    #[test]
    fn test_permission_denied_is_not_retriable() {
        let err = object_store::Error::PermissionDenied {
            path: "test-key".to_string(),
            source: "access denied".into(),
        };
        assert!(!is_retriable_s3_error(&err));
    }

    #[test]
    fn test_not_found_is_not_retriable() {
        let err = object_store::Error::NotFound {
            path: "test-key".to_string(),
            source: "not found".into(),
        };
        assert!(!is_retriable_s3_error(&err));
    }

    #[test]
    fn test_unauthenticated_is_not_retriable() {
        let err = object_store::Error::Unauthenticated {
            path: "test-key".to_string(),
            source: "invalid credentials".into(),
        };
        assert!(!is_retriable_s3_error(&err));
    }

    #[test]
    fn test_not_supported_is_not_retriable() {
        let err = object_store::Error::NotSupported {
            source: "operation not supported".into(),
        };
        assert!(!is_retriable_s3_error(&err));
    }
}
