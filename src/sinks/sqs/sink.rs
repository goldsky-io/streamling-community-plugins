use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use aws_config::sts::AssumeRoleProvider;
use aws_config::{BehaviorVersion, ConfigLoader, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::error::DisplayErrorContext;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::r#api::PluginStateBackendFactory;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SinkPlugin};
use tracing::{debug, info, warn};

use super::config::{AssumeRoleConfig, SqsSinkConfig};
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use crate::utils::record_batch_json;

const SQS_MAX_BATCH_SIZE: usize = 10;
const SQS_PARTIAL_FAILURE_MAX_RETRIES: u32 = 5;

pub struct SqsSink {
    opts: PluginOptions,
    config: SqsSinkConfig,
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
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(options, "sqs_sink", "STREAMLING__PLUGIN__SQS_SINK");
        let config = SqsSinkConfig::from_options(&opts).map_err(configuration_error)?;
        Ok(SqsSink {
            opts,
            config,
            _schema: schema,
            client: OnceLock::new(),
            queue_url: OnceLock::new(),
            running: std::sync::Arc::new(AtomicBool::new(true)),
        })
    }
}

/// No `endpoint_url` here: an assumed role's STS client is built from this config.
async fn load_sdk_config(
    loader: ConfigLoader,
    config: &SqsSinkConfig,
    opts: &PluginOptions,
) -> Result<SdkConfig, PluginError> {
    let mut loader = loader;
    if let Some(region) = &config.region {
        loader = loader.region(aws_types::region::Region::new(region.clone()));
    }
    let secret = |key| opts.get_secret(key).filter(|value| !value.is_empty());
    if let (Some(access_key_id), Some(secret_access_key)) =
        (secret("access_key_id"), secret("secret_access_key"))
    {
        loader = loader.credentials_provider(Credentials::new(
            access_key_id,
            secret_access_key,
            secret("session_token"),
            None,
            "SqsSinkPlugin",
        ));
    }
    let base = loader.load().await;

    match &config.assume_role {
        None => Ok(base),
        Some(role) => with_assumed_role(base, role).await,
    }
}

/// Swaps only the credentials provider, keeping the config's lazy identity
/// cache so the short-lived assumed credentials are refreshed.
async fn with_assumed_role(
    base: SdkConfig,
    role: &AssumeRoleConfig,
) -> Result<SdkConfig, PluginError> {
    let mut builder = AssumeRoleProvider::builder(&role.role_arn)
        .session_name(&role.session_name)
        .configure(&base);
    if let Some(external_id) = &role.external_id {
        builder = builder.external_id(external_id);
    }
    let provider = builder.build().await;
    // Assume once up front so a broken trust relationship fails startup, not the first batch.
    provider
        .provide_credentials()
        .await
        .map_err(|e| assume_role_error(role, &e))?;
    Ok(base
        .into_builder()
        .credentials_provider(SharedCredentialsProvider::new(provider))
        .build())
}

fn assume_role_error(role: &AssumeRoleConfig, error: &CredentialsError) -> PluginError {
    let hint = if matches!(error, CredentialsError::ProviderError(_)) {
        " The role's trust policy must allow this pipeline's AWS identity and that external ID."
    } else {
        ""
    };
    PluginError::Internal(format!(
        "sqs_sink: could not assume role {} with external ID {}: {}.{hint}",
        role.role_arn,
        role.external_id.as_deref().unwrap_or("(none)"),
        DisplayErrorContext(error)
    ))
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

        let queue_url = self.opts.get("queue_url")?;

        let sdk_config = load_sdk_config(
            aws_config::defaults(BehaviorVersion::latest()),
            &self.config,
            &self.opts,
        )
        .await?;
        let mut sqs_config = aws_sdk_sqs::config::Builder::from(&sdk_config);
        if let Some(endpoint_url) = &self.config.endpoint_url {
            sqs_config = sqs_config.endpoint_url(endpoint_url);
        }
        let client = SqsClient::from_conf(sqs_config.build());

        let _ = self.client.set(client);
        let _ = self.queue_url.set(queue_url.clone());

        let role_arn = self
            .config
            .assume_role
            .as_ref()
            .map_or("-", |r| r.role_arn.as_str());
        info!(queue_url = %queue_url, role_arn = %role_arn, "SQS sink initialized successfully");
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
                String::from_utf8(bytes.into()).map_err(|e| {
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
                    PluginError::Internal(format!(
                        "failed to send message batch to SQS: {}",
                        DisplayErrorContext(&e)
                    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_async::rt::sleep::{SharedAsyncSleep, TokioSleep};
    use aws_smithy_async::time::SharedTimeSource;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use aws_types::region::Region;

    const ROLE: &str = "arn:aws:iam::123456789012:role/partner-role";
    const EXTERNAL_ID: &str = "tenant:abc";

    const ASSUME_ROLE_OK: &str = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId><SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><AssumedRoleId>AROAEXAMPLE:streamling-sqs-sink</AssumedRoleId><Arn>arn:aws:sts::123456789012:assumed-role/partner-role/streamling-sqs-sink</Arn></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>"#;

    const ASSUME_ROLE_DENIED: &str = r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>not authorized to perform: sts:AssumeRole</Message></Error><RequestId>req-1</RequestId></ErrorResponse>"#;

    fn sts_events(responses: &[(u16, &str)]) -> StaticReplayClient {
        StaticReplayClient::new(
            responses
                .iter()
                .map(|(status, body)| {
                    ReplayEvent::new(
                        http::Request::builder()
                            .uri("https://sts.us-east-1.amazonaws.com/")
                            .body(SdkBody::empty())
                            .unwrap(),
                        http::Response::builder()
                            .status(*status)
                            .body(SdkBody::from(*body))
                            .unwrap(),
                    )
                })
                .collect(),
        )
    }

    fn opts(pairs: &[(&str, &str)]) -> PluginOptions {
        let base = [
            ("region", "us-east-1"),
            ("endpoint_url", "http://localhost:9324"),
            ("access_key_id", "AKIDBASE"),
            ("secret_access_key", "base-secret"),
        ];
        let all: Vec<_> = base.iter().chain(pairs).copied().collect();
        PluginOptions::for_test("sqs_sink", "STREAMLING__PLUGIN__SQS_SINK_UNIT_TEST", &all)
    }

    /// Built by hand rather than loaded, so no env var or profile on the host
    /// (e.g. `AWS_ENDPOINT_URL`) can redirect the STS calls under test.
    fn base_config(http: &StaticReplayClient) -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "AKIDBASE",
                "base-secret",
                None,
                None,
                "test",
            )))
            .http_client(http.clone())
            .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
            .time_source(SharedTimeSource::default())
            .build()
    }

    fn role() -> AssumeRoleConfig {
        SqsSinkConfig::from_options(&opts(&[("role_arn", ROLE), ("external_id", EXTERNAL_ID)]))
            .unwrap()
            .assume_role
            .unwrap()
    }

    #[tokio::test]
    async fn assumes_the_role_at_sts_with_the_external_id() {
        let http = sts_events(&[(200, ASSUME_ROLE_OK), (200, ASSUME_ROLE_OK)]);

        let sdk = with_assumed_role(base_config(&http), &role())
            .await
            .expect("role assumed");
        let creds = sdk
            .credentials_provider()
            .expect("credentials provider")
            .provide_credentials()
            .await
            .expect("assumed credentials");
        assert_eq!(creds.access_key_id(), "ASIAASSUMED");

        let requests: Vec<_> = http.actual_requests().collect();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert!(
                request
                    .uri()
                    .starts_with("https://sts.us-east-1.amazonaws.com"),
                "STS must not use the SQS endpoint_url, got {}",
                request.uri()
            );
            let body = std::str::from_utf8(request.body().bytes().unwrap()).unwrap();
            assert!(body.contains("Action=AssumeRole"), "{body}");
            assert!(body.contains("ExternalId=tenant%3Aabc"), "{body}");
            assert!(
                body.contains("RoleSessionName=streamling-sqs-sink"),
                "{body}"
            );
            assert!(
                body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fpartner-role"),
                "{body}"
            );
        }
    }

    #[tokio::test]
    async fn denied_assume_role_fails_with_the_role_and_external_id() {
        let http = sts_events(&[(403, ASSUME_ROLE_DENIED)]);

        let err = with_assumed_role(base_config(&http), &role())
            .await
            .expect_err("denied")
            .to_string();
        assert!(err.contains(ROLE), "{err}");
        assert!(err.contains(EXTERNAL_ID), "{err}");
        assert!(err.contains("AccessDenied"), "{err}");
        assert!(err.contains("trust policy"), "{err}");
    }

    #[tokio::test]
    async fn without_a_role_no_sts_call_is_made() {
        let http = sts_events(&[]);
        let opts = opts(&[]);
        let config = SqsSinkConfig::from_options(&opts).unwrap();
        let loader = aws_config::defaults(BehaviorVersion::latest()).http_client(http.clone());
        let sdk = load_sdk_config(loader, &config, &opts)
            .await
            .expect("static credentials");
        let creds = sdk
            .credentials_provider()
            .expect("credentials provider")
            .provide_credentials()
            .await
            .expect("static credentials");
        assert_eq!(creds.access_key_id(), "AKIDBASE");
        assert_eq!(http.actual_requests().count(), 0);
    }
}
