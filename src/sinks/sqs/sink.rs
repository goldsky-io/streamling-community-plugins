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
use aws_types::region::Region;
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
/// AWS caps a single message and the summed message lengths of one
/// `SendMessageBatch` request at the same 1 MiB.
const SQS_MAX_PAYLOAD_BYTES: usize = 1_048_576;
/// Lowest value a queue's `MaximumMessageSize` attribute accepts, so the floor
/// for `max_message_bytes`.
const SQS_MIN_PAYLOAD_BYTES: usize = 1_024;

pub struct SqsSink {
    opts: PluginOptions,
    config: SqsSinkConfig,
    _schema: SchemaRef,
    client: OnceLock<SqsClient>,
    queue_url: OnceLock<String>,
    one_row_per_request: OnceLock<bool>,
    max_message_bytes: OnceLock<usize>,
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
            one_row_per_request: OnceLock::new(),
            max_message_bytes: OnceLock::new(),
            running: std::sync::Arc::new(AtomicBool::new(true)),
        })
    }
}

/// No `endpoint_url`/`region`: STS uses this config, and may be off in the queue's region.
async fn load_sdk_config(
    loader: ConfigLoader,
    config: &SqsSinkConfig,
    opts: &PluginOptions,
) -> Result<SdkConfig, PluginError> {
    let mut loader = loader;
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
        Some(role) => with_assumed_role(base, role, config.region.as_deref()).await,
    }
}

/// Swaps only the credentials provider so the lazy identity cache refreshes assumed credentials.
async fn with_assumed_role(
    base: SdkConfig,
    role: &AssumeRoleConfig,
    fallback_region: Option<&str>,
) -> Result<SdkConfig, PluginError> {
    let mut builder = AssumeRoleProvider::builder(&role.role_arn)
        .session_name(&role.session_name)
        .configure(&base);
    if let (None, Some(region)) = (base.region(), fallback_region) {
        builder = builder.region(Region::new(region.to_string()));
    }
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
        let one_row_per_request: bool = self.opts.get_parsed_or("one_row_per_request", true)?;
        let max_message_bytes: usize = self
            .opts
            .get_parsed_or("max_message_bytes", SQS_MAX_PAYLOAD_BYTES)?;
        if !(SQS_MIN_PAYLOAD_BYTES..=SQS_MAX_PAYLOAD_BYTES).contains(&max_message_bytes) {
            return Err(PluginError::Internal(format!(
                "sqs_sink: max_message_bytes is {max_message_bytes}, outside the \
                 {SQS_MIN_PAYLOAD_BYTES}..={SQS_MAX_PAYLOAD_BYTES} byte range a queue's \
                 MaximumMessageSize attribute allows"
            )));
        }

        let sdk_config = load_sdk_config(
            aws_config::defaults(BehaviorVersion::latest()),
            &self.config,
            &self.opts,
        )
        .await?;
        let mut sqs_config = aws_sdk_sqs::config::Builder::from(&sdk_config);
        if let Some(region) = &self.config.region {
            sqs_config = sqs_config.region(Region::new(region.clone()));
        }
        if let Some(endpoint_url) = &self.config.endpoint_url {
            sqs_config = sqs_config.endpoint_url(endpoint_url);
        }
        let client = SqsClient::from_conf(sqs_config.build());

        let _ = self.client.set(client);
        let _ = self.queue_url.set(queue_url.clone());
        let _ = self.one_row_per_request.set(one_row_per_request);
        let _ = self.max_message_bytes.set(max_message_bytes);

        let role_arn = self
            .config
            .assume_role
            .as_ref()
            .map_or("-", |r| r.role_arn.as_str());
        info!(
            queue_url = %queue_url,
            role_arn = %role_arn,
            one_row_per_request,
            max_message_bytes,
            "SQS sink initialized successfully"
        );
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
        let one_row_per_request = *self.one_row_per_request.get().ok_or_else(|| {
            PluginError::Internal("one_row_per_request is not initialized".to_string())
        })?;
        let max_message_bytes = *self.max_message_bytes.get().ok_or_else(|| {
            PluginError::Internal("max_message_bytes is not initialized".to_string())
        })?;

        let json_rows =
            record_batch_json::record_batch_to_line_delimited_json(&batch).map_err(|e| {
                PluginError::Internal(format!("failed to convert batch to JSON: {}", e))
            })?;

        let rows: Vec<String> = json_rows
            .into_iter()
            .map(|bytes| {
                String::from_utf8(bytes.into()).map_err(|e| {
                    PluginError::Internal(format!("failed to convert row to UTF-8: {}", e))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(());
        }

        let bodies = if one_row_per_request {
            rows
        } else {
            pack_array_bodies(&rows, max_message_bytes)?
        };

        let sent = Self::send_messages(client, queue_url, bodies, max_message_bytes).await?;
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
        bodies: Vec<String>,
        max_bytes: usize,
    ) -> Result<usize, PluginError> {
        ensure_bodies_fit(&bodies, max_bytes)?;

        let mut total_sent = 0;
        let mut to_send: Vec<(usize, String)> = bodies.into_iter().enumerate().collect();

        while !to_send.is_empty() {
            let chunk_len = next_chunk_len(&to_send, SQS_MAX_BATCH_SIZE, max_bytes);
            let chunk: Vec<(usize, String)> = to_send.drain(..chunk_len).collect();

            let (sent, mut failed) = Self::send_chunk_with_retry(client, queue_url, chunk).await?;
            total_sent += sent;
            to_send.append(&mut failed);
        }

        Ok(total_sent)
    }

    async fn send_chunk_with_retry(
        client: &SqsClient,
        queue_url: &str,
        chunk: Vec<(usize, String)>,
    ) -> Result<(usize, Vec<(usize, String)>), PluginError> {
        let chunk_len = chunk.len();
        let mut to_retry = chunk;
        let mut backoff_ms: u64 = 100;

        for attempt in 0..=SQS_PARTIAL_FAILURE_MAX_RETRIES {
            let entries: Vec<aws_sdk_sqs::types::SendMessageBatchRequestEntry> = to_retry
                .iter()
                .enumerate()
                .map(|(i, (_idx, body))| {
                    aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                        .id(format!("msg_{}", i))
                        // The SDK stores an owned body and a partial failure
                        // retries from the original, so this copy stays.
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

/// Packs per-row JSON into JSON-array message bodies, each within `max_bytes`.
///
/// Rows are accumulated greedily; a body always carries at least one row, so a
/// row that cannot fit on its own is an error rather than a silent drop.
fn pack_array_bodies(rows: &[String], max_bytes: usize) -> Result<Vec<String>, PluginError> {
    const BRACKETS: usize = 2;
    const SEPARATOR: usize = 1;

    let mut bodies = Vec::new();
    let mut start = 0;

    while start < rows.len() {
        let mut content = 0;
        let mut end = start;
        while end < rows.len() {
            let cost = rows[end].len() + if end > start { SEPARATOR } else { 0 };
            if end > start && BRACKETS + content + cost > max_bytes {
                break;
            }
            content += cost;
            end += 1;
        }

        if BRACKETS + content > max_bytes {
            return Err(PluginError::Internal(format!(
                "sqs_sink: a single row is {} bytes, which exceeds the {} byte SQS payload limit \
                 even as the only element of an array body",
                rows[start].len(),
                max_bytes
            )));
        }

        let mut body = String::with_capacity(BRACKETS + content);
        body.push('[');
        for (i, row) in rows[start..end].iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push_str(row);
        }
        body.push(']');
        bodies.push(body);

        start = end;
    }

    Ok(bodies)
}

/// Rejects a body that no `SendMessageBatch` request could carry.
///
/// SQS would reject it too; failing here names the size and the limit instead
/// of surfacing an opaque SDK error, and dropping the row would be data loss.
fn ensure_bodies_fit(bodies: &[String], max_bytes: usize) -> Result<(), PluginError> {
    for body in bodies {
        if body.len() > max_bytes {
            return Err(PluginError::Internal(format!(
                "sqs_sink: a single message is {} bytes, which exceeds the {} byte SQS message \
                 limit; shrink the rows or lower the sink's batch_size",
                body.len(),
                max_bytes
            )));
        }
    }

    Ok(())
}

/// How many leading bodies fit in one `SendMessageBatch` request, bounded by
/// both the entry-count cap and the request-wide payload budget.
///
/// The budget counts message bodies only: AWS sums message lengths, not the
/// batch-entry correlation IDs. Always at least one, so an oversized body
/// surfaces as a send error instead of spinning forever on an empty chunk.
fn next_chunk_len(bodies: &[(usize, String)], max_count: usize, max_bytes: usize) -> usize {
    let mut total = 0;
    let mut count = 0;

    for (_, body) in bodies.iter().take(max_count) {
        if count > 0 && total + body.len() > max_bytes {
            break;
        }
        total += body.len();
        count += 1;
    }

    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::sqs::config::DEFAULT_ROLE_SESSION_NAME;
    use aws_smithy_async::rt::sleep::{SharedAsyncSleep, TokioSleep};
    use aws_smithy_async::time::SharedTimeSource;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

    /// `count` rows of exactly `len` bytes each, every one valid JSON.
    fn rows(count: usize, len: usize) -> Vec<String> {
        let padding = len
            .checked_sub(r#"{"v":""}"#.len())
            .expect("rows shorter than the JSON wrapper cannot be built");
        (0..count)
            .map(|_| format!(r#"{{"v":"{}"}}"#, "x".repeat(padding)))
            .collect()
    }

    #[test]
    fn pack_array_bodies_joins_rows_that_fit_into_one_body() {
        let rows = vec![r#"{"id":1}"#.to_string(), r#"{"id":2}"#.to_string()];
        let bodies = pack_array_bodies(&rows, SQS_MAX_PAYLOAD_BYTES).unwrap();

        assert_eq!(bodies, vec![r#"[{"id":1},{"id":2}]"#.to_string()]);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&bodies[0]).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn pack_array_bodies_splits_at_the_byte_boundary() {
        // Three 10-byte rows: "[" + row + "," + row + "]" is 23 bytes, adding a
        // third would be 34, so the budget of 30 splits after the second row.
        let bodies = pack_array_bodies(&rows(3, 10), 30).unwrap();

        assert_eq!(bodies.len(), 2);
        assert!(bodies.iter().all(|b| b.len() <= 30));
        let counts: Vec<usize> = bodies
            .iter()
            .map(|b| {
                serde_json::from_str::<Vec<serde_json::Value>>(b)
                    .unwrap()
                    .len()
            })
            .collect();
        assert_eq!(counts, vec![2, 1]);
    }

    #[test]
    fn pack_array_bodies_rejects_a_row_that_cannot_fit_alone() {
        let err = pack_array_bodies(&rows(1, 40), 30).unwrap_err();
        assert!(
            err.to_string().contains("40 bytes"),
            "error should name the row size: {err}"
        );
    }

    #[test]
    fn pack_array_bodies_yields_nothing_for_no_rows() {
        assert!(
            pack_array_bodies(&[], SQS_MAX_PAYLOAD_BYTES)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn next_chunk_len_stops_at_the_entry_count_cap() {
        let bodies: Vec<(usize, String)> = rows(25, 10).into_iter().enumerate().collect();
        assert_eq!(
            next_chunk_len(&bodies, SQS_MAX_BATCH_SIZE, SQS_MAX_PAYLOAD_BYTES),
            SQS_MAX_BATCH_SIZE
        );
    }

    #[test]
    fn next_chunk_len_stops_at_the_payload_budget() {
        let bodies: Vec<(usize, String)> = rows(10, 100).into_iter().enumerate().collect();
        // Three 100-byte bodies fit in a 350-byte budget; a fourth would not.
        assert_eq!(next_chunk_len(&bodies, SQS_MAX_BATCH_SIZE, 350), 3);
    }

    #[test]
    fn a_body_at_exactly_the_budget_packs_and_ships() {
        // "[" + 13-byte row + "," + 13-byte row + "]" is exactly 29 bytes, so
        // packing and the send-side budget have to agree on that boundary.
        let bodies = pack_array_bodies(&rows(2, 13), 29).unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].len(), 29);

        ensure_bodies_fit(&bodies, 29).unwrap();
        let indexed: Vec<(usize, String)> = bodies.into_iter().enumerate().collect();
        assert_eq!(next_chunk_len(&indexed, SQS_MAX_BATCH_SIZE, 29), 1);
    }

    #[test]
    fn a_row_above_256_kib_is_still_sendable() {
        let bodies = rows(1, 300_000);

        ensure_bodies_fit(&bodies, SQS_MAX_PAYLOAD_BYTES).unwrap();
        let indexed: Vec<(usize, String)> = bodies.into_iter().enumerate().collect();
        assert_eq!(
            next_chunk_len(&indexed, SQS_MAX_BATCH_SIZE, SQS_MAX_PAYLOAD_BYTES),
            1
        );
    }

    #[test]
    fn ensure_bodies_fit_rejects_a_body_above_the_budget() {
        let err = ensure_bodies_fit(&rows(1, 40), 30).unwrap_err();
        assert!(
            err.to_string().contains("40 bytes"),
            "error should name the body size: {err}"
        );
    }

    #[test]
    fn next_chunk_len_never_returns_zero() {
        let bodies: Vec<(usize, String)> = rows(2, 1000).into_iter().enumerate().collect();
        assert_eq!(next_chunk_len(&bodies, SQS_MAX_BATCH_SIZE, 10), 1);
    }

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

    /// Built by hand so host AWS env or profiles (e.g. `AWS_ENDPOINT_URL`) can't redirect STS.
    fn base_config(http: &StaticReplayClient, region: Option<&str>) -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region.map(|r| Region::new(r.to_string())))
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
        AssumeRoleConfig {
            role_arn: ROLE.to_string(),
            external_id: Some(EXTERNAL_ID.to_string()),
            session_name: DEFAULT_ROLE_SESSION_NAME.to_string(),
        }
    }

    #[tokio::test]
    async fn assumes_the_role_at_sts_with_the_external_id() {
        let http = sts_events(&[(200, ASSUME_ROLE_OK), (200, ASSUME_ROLE_OK)]);

        let sdk = with_assumed_role(
            base_config(&http, Some("us-east-1")),
            &role(),
            Some("eu-west-1"),
        )
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

        let err = with_assumed_role(
            base_config(&http, Some("us-east-1")),
            &role(),
            Some("eu-west-1"),
        )
        .await
        .expect_err("denied")
        .to_string();
        assert!(err.contains(ROLE), "{err}");
        assert!(err.contains(EXTERNAL_ID), "{err}");
        assert!(err.contains("AccessDenied"), "{err}");
        assert!(err.contains("trust policy"), "{err}");
    }

    #[tokio::test]
    async fn sts_uses_the_host_region_and_falls_back_to_the_queue_region() {
        let host = sts_events(&[(200, ASSUME_ROLE_OK)]);
        with_assumed_role(
            base_config(&host, Some("us-west-2")),
            &role(),
            Some("ap-east-1"),
        )
        .await
        .expect("role assumed");
        let uri = host.actual_requests().next().unwrap().uri().to_string();
        assert!(
            uri.starts_with("https://sts.us-west-2.amazonaws.com"),
            "{uri}"
        );

        let no_host = sts_events(&[(200, ASSUME_ROLE_OK)]);
        with_assumed_role(base_config(&no_host, None), &role(), Some("ap-east-1"))
            .await
            .expect("role assumed");
        let uri = no_host.actual_requests().next().unwrap().uri().to_string();
        assert!(
            uri.starts_with("https://sts.ap-east-1.amazonaws.com"),
            "{uri}"
        );
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
