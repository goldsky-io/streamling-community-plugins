use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::Client as SqsClient;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};
use tracing::{debug, error, info, warn};

use crate::utils::record_batch_json;

const SQS_MAX_BATCH_SIZE: usize = 10;
const SQS_PARTIAL_FAILURE_MAX_RETRIES: u32 = 5;

pub struct SqsSink {
    options: HashMap<String, String>,
    _schema: SchemaRef,
    client: OnceLock<SqsClient>,
    queue_url: OnceLock<String>,
    running: std::sync::Arc<AtomicBool>,
}

impl SqsSink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        _metric_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        SqsSink {
            options,
            _schema: schema,
            client: OnceLock::new(),
            queue_url: OnceLock::new(),
            running: std::sync::Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for SqsSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for SqsSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.client.get().is_some() {
            return Ok(());
        }

        let queue_url = self
            .options
            .get("queue_url")
            .ok_or_else(|| {
                let err = "queue_url is not specified".to_string();
                error!(error = %err, "SQS sink initialization failed");
                PluginError::Internal(err)
            })?
            .clone();

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        let region = std::env::var("STREAMLING__PLUGIN__SQS_SINK__REGION")
            .ok()
            .or_else(|| self.options.get("region").cloned());
        if let Some(region) = region {
            config_loader = config_loader.region(aws_types::region::Region::new(region));
        }

        if let Some(endpoint_url) = self.options.get("endpoint_url") {
            config_loader = config_loader.endpoint_url(endpoint_url.clone());
        }

        let access_key_id = std::env::var("STREAMLING__PLUGIN__SQS_SINK__ACCESS_KEY_ID")
            .ok()
            .or_else(|| {
                if let Some(val) = self.options.get("access_key_id") {
                    warn!(
                        "access_key_id is set in plaintext YAML configuration. \
                        Consider using environment variable STREAMLING__PLUGIN__SQS_SINK__ACCESS_KEY_ID instead."
                    );
                    Some(val.clone())
                } else {
                    None
                }
            });

        let secret_access_key = std::env::var("STREAMLING__PLUGIN__SQS_SINK__SECRET_ACCESS_KEY")
            .ok()
            .or_else(|| {
                if let Some(val) = self.options.get("secret_access_key") {
                    warn!(
                        "secret_access_key is set in plaintext YAML configuration. \
                        Consider using environment variable STREAMLING__PLUGIN__SQS_SINK__SECRET_ACCESS_KEY instead."
                    );
                    Some(val.clone())
                } else {
                    None
                }
            });

        let session_token = std::env::var("STREAMLING__PLUGIN__SQS_SINK__SESSION_TOKEN")
            .ok()
            .or_else(|| {
                if let Some(val) = self.options.get("session_token") {
                    warn!(
                        "session_token is set in plaintext YAML configuration. \
                        Consider using environment variable STREAMLING__PLUGIN__SQS_SINK__SESSION_TOKEN instead."
                    );
                    Some(val.clone())
                } else {
                    None
                }
            });

        if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
            let creds = Credentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
                "SqsSinkPlugin",
            );
            config_loader = config_loader.credentials_provider(creds);
        }

        let sdk_config = config_loader.load().await;
        let client = SqsClient::new(&sdk_config);

        let _ = self.client.set(client);
        let _ = self.queue_url.set(queue_url.clone());

        info!(queue_url = %queue_url, "SQS sink initialized successfully");
        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if !self.is_running() {
            return Err(PluginError::Internal(
                "SQS sink is not running, cannot process batch".to_string(),
            ));
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        let client = self
            .client
            .get()
            .ok_or_else(|| PluginError::Internal("SQS client is not initialized".to_string()))?;
        let queue_url = self
            .queue_url
            .get()
            .ok_or_else(|| PluginError::Internal("Queue URL is not initialized".to_string()))?;

        let json_rows =
            record_batch_json::record_batch_to_line_delimited_json(&batch).map_err(|e| {
                PluginError::Internal(format!("failed to convert batch to JSON: {}", e))
            })?;

        let messages: Vec<String> = json_rows
            .into_iter()
            .map(|bytes| {
                String::from_utf8(bytes).map_err(|e| {
                    PluginError::Internal(format!("failed to convert row to UTF-8: {}", e))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        if messages.is_empty() {
            return Ok(());
        }

        let sent = Self::send_messages(client, queue_url, &messages).await?;
        debug!("Sent {} messages to SQS", sent);

        Ok(())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        info!(?epoch, "SQS sink received checkpoint marker");
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

impl SqsSink {
    async fn send_messages(
        client: &SqsClient,
        queue_url: &str,
        messages: &[String],
    ) -> Result<usize, PluginError> {
        let mut total_sent = 0;
        let mut to_send: Vec<(usize, String)> = messages
            .iter()
            .enumerate()
            .map(|(i, s)| (i, s.clone()))
            .collect();

        while !to_send.is_empty() {
            let chunk: Vec<(usize, String)> = to_send
                .drain(..std::cmp::min(SQS_MAX_BATCH_SIZE, to_send.len()))
                .collect();
            if chunk.is_empty() {
                break;
            }

            let (sent, mut failed) = Self::send_chunk_with_retry(client, queue_url, &chunk).await?;
            total_sent += sent;
            to_send.append(&mut failed);
        }

        Ok(total_sent)
    }

    async fn send_chunk_with_retry(
        client: &SqsClient,
        queue_url: &str,
        chunk: &[(usize, String)],
    ) -> Result<(usize, Vec<(usize, String)>), PluginError> {
        let chunk_len = chunk.len();
        let mut to_retry: Vec<(usize, String)> = chunk.to_vec();
        let mut backoff_ms: u64 = 100;

        for attempt in 0..=SQS_PARTIAL_FAILURE_MAX_RETRIES {
            let entries: Vec<aws_sdk_sqs::types::SendMessageBatchRequestEntry> = to_retry
                .iter()
                .enumerate()
                .map(|(i, (_idx, body))| {
                    aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                        .id(format!("msg_{}", i))
                        .message_body(body.clone())
                        .build()
                        .map_err(|e| {
                            PluginError::Internal(format!(
                                "failed to build SQS message entry: {}",
                                e
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let result = client
                .send_message_batch()
                .queue_url(queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| {
                    PluginError::Internal(format!("failed to send message batch to SQS: {}", e))
                })?;

            let failed = result.failed();
            if failed.is_empty() {
                return Ok((chunk_len, vec![]));
            }

            for f in failed.iter() {
                if f.sender_fault() {
                    let errors: Vec<&str> = failed
                        .iter()
                        .map(|e| e.message().unwrap_or("unknown error"))
                        .collect();
                    return Err(PluginError::Internal(format!(
                        "SQS batch send failed (sender fault) for {} messages: {:?}",
                        failed.len(),
                        errors
                    )));
                }
            }

            let mut failed_to_retry = Vec::new();
            let mut unrecognized_ids = Vec::new();
            for f in failed.iter() {
                let id = f.id();
                match id
                    .strip_prefix("msg_")
                    .and_then(|s| s.parse::<usize>().ok())
                    .filter(|&i| i < to_retry.len())
                {
                    Some(i) => failed_to_retry.push(to_retry[i].clone()),
                    None => {
                        unrecognized_ids.push(if id.is_empty() {
                            "(empty)".to_string()
                        } else {
                            id.to_string()
                        });
                    }
                }
            }
            if !unrecognized_ids.is_empty() {
                return Err(PluginError::Internal(format!(
                    "SQS batch send: could not map failed entry IDs back to messages (unrecognized IDs: {:?}). Possible data loss.",
                    unrecognized_ids
                )));
            }

            if attempt == SQS_PARTIAL_FAILURE_MAX_RETRIES {
                return Err(PluginError::Internal(format!(
                    "SQS batch send failed for {} messages after {} retries",
                    failed_to_retry.len(),
                    SQS_PARTIAL_FAILURE_MAX_RETRIES
                )));
            }

            warn!(
                "SQS partial batch failure: {} messages failed (attempt {}), retrying...",
                failed_to_retry.len(),
                attempt + 1
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = std::cmp::min(backoff_ms * 2, 5000);
            to_retry = failed_to_retry;
        }

        Ok((0, vec![]))
    }
}
