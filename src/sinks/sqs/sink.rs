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
use tracing::{debug, info, warn};

use crate::utils::plugin_options::PluginOptions;
use crate::utils::record_batch_json;

const SQS_MAX_BATCH_SIZE: usize = 10;
const SQS_PARTIAL_FAILURE_MAX_RETRIES: u32 = 5;
/// SQS caps a single message at 256 KiB and applies the same cap to the total
/// payload of a `SendMessageBatch` request, so both are budgeted against this.
const SQS_MAX_PAYLOAD_BYTES: usize = 262_144;
/// Batch entry IDs are `msg_{i}` with `i < SQS_MAX_BATCH_SIZE`, so five bytes
/// is the exact worst case and lets the payload budget be computed without
/// materializing the ID.
const SQS_ENTRY_ID_MAX_BYTES: usize = 5;

pub struct SqsSink {
    opts: PluginOptions,
    _schema: SchemaRef,
    client: OnceLock<SqsClient>,
    queue_url: OnceLock<String>,
    one_row_per_request: OnceLock<bool>,
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
            opts: PluginOptions::new(options, "sqs_sink", "STREAMLING__PLUGIN__SQS_SINK"),
            _schema: schema,
            client: OnceLock::new(),
            queue_url: OnceLock::new(),
            one_row_per_request: OnceLock::new(),
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

        let queue_url = self.opts.get("queue_url")?;
        let one_row_per_request: bool = self.opts.get_parsed_or("one_row_per_request", true)?;

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        if let Ok(region) = self.opts.get("region") {
            config_loader = config_loader.region(aws_types::region::Region::new(region));
        }

        if let Ok(endpoint_url) = self.opts.get("endpoint_url") {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        let access_key_id = self.opts.get_secret("access_key_id");
        let secret_access_key = self.opts.get_secret("secret_access_key");
        let session_token = self.opts.get_secret("session_token");

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
        let _ = self.one_row_per_request.set(one_row_per_request);

        info!(queue_url = %queue_url, one_row_per_request, "SQS sink initialized successfully");
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
            pack_array_bodies(&rows, SQS_MAX_PAYLOAD_BYTES)?
        };

        let sent = Self::send_messages(client, queue_url, bodies).await?;
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
    ) -> Result<usize, PluginError> {
        for body in &bodies {
            let cost = body.len() + SQS_ENTRY_ID_MAX_BYTES;
            if cost > SQS_MAX_PAYLOAD_BYTES {
                return Err(PluginError::Internal(format!(
                    "sqs_sink: a single message is {} bytes, which exceeds the {} byte SQS payload limit; \
                     shrink the rows or lower the sink's batch_size",
                    body.len(),
                    SQS_MAX_PAYLOAD_BYTES
                )));
            }
        }

        let mut total_sent = 0;
        let mut to_send: Vec<(usize, String)> = bodies.into_iter().enumerate().collect();

        while !to_send.is_empty() {
            let chunk_len = next_chunk_len(&to_send, SQS_MAX_BATCH_SIZE, SQS_MAX_PAYLOAD_BYTES);
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
                        .message_body(body.as_str())
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

/// How many leading bodies fit in one `SendMessageBatch` request, bounded by
/// both the entry-count cap and the request-wide payload budget.
///
/// Always at least one, so an oversized body surfaces as a send error instead
/// of spinning forever on an empty chunk.
fn next_chunk_len(bodies: &[(usize, String)], max_count: usize, max_bytes: usize) -> usize {
    let mut total = 0;
    let mut count = 0;

    for (_, body) in bodies.iter().take(max_count) {
        let cost = body.len() + SQS_ENTRY_ID_MAX_BYTES;
        if count > 0 && total + cost > max_bytes {
            break;
        }
        total += cost;
        count += 1;
    }

    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let bodies: Vec<(usize, String)> = rows(10, 95).into_iter().enumerate().collect();
        // Each entry costs 95 + 5 = 100 bytes, so a 350-byte budget fits three.
        assert_eq!(next_chunk_len(&bodies, SQS_MAX_BATCH_SIZE, 350), 3);
    }

    #[test]
    fn next_chunk_len_never_returns_zero() {
        let bodies: Vec<(usize, String)> = rows(2, 1000).into_iter().enumerate().collect();
        assert_eq!(next_chunk_len(&bodies, SQS_MAX_BATCH_SIZE, 10), 1);
    }
}
