//! SQS sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and write to SQS.
//! Uses ElasticMQ (or any SQS-compatible endpoint) as the SQS backend.

use serde::Serialize;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, TestContext, TestContextOptions, init_tracing};

// ============================================================================
// Test Record Types
// ============================================================================

/// Basic test record structure
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

// ============================================================================
// Scenario 1: Basic Kafka to SQS sink
// ============================================================================

/// Basic test: read records from Kafka source and write to SQS sink
#[tokio::test]
async fn test_sqs_sink_basic() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");

    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");

    // Register schema for input topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce test records
    let records_to_produce = 10;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source -> SQS sink
    // Credentials come from build_env_vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  sqs_sink:
    type: sqs
    from: kafka_source
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
"#,
        input_topic = ctx.kafka_topic,
        queue_url = sqs.queue_url,
        endpoint_url = sqs.endpoint_url,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify messages in SQS
    let messages = sqs
        .receive_all_messages(records_to_produce as usize + 10, Duration::from_secs(10))
        .await
        .expect("Failed to receive messages from SQS");

    assert_eq!(
        messages.len(),
        records_to_produce as usize,
        "Expected {} messages in SQS, got {}",
        records_to_produce,
        messages.len()
    );

    // Verify message content (each message should be a valid JSON)
    for msg in &messages {
        let parsed: serde_json::Value =
            serde_json::from_str(msg).expect("Message should be valid JSON");
        assert!(
            parsed.get("id").is_some(),
            "Message should contain 'id' field"
        );
        assert!(
            parsed.get("value").is_some(),
            "Message should contain 'value' field"
        );
        assert!(
            parsed.get("timestamp").is_some(),
            "Message should contain 'timestamp' field"
        );
    }

    // Verify specific records exist
    let ids: Vec<i64> = messages
        .iter()
        .filter_map(|msg| {
            serde_json::from_str::<serde_json::Value>(msg)
                .ok()
                .and_then(|v| v.get("id").and_then(|id| id.as_i64()))
        })
        .collect();

    assert!(ids.contains(&1), "Should contain id=1");
    assert!(
        ids.contains(&records_to_produce),
        "Should contain id={}",
        records_to_produce
    );
}

// ============================================================================
// Scenario 2: Multiple batches through SQS sink
// ============================================================================

/// Test with multiple batches of records flowing to SQS
#[tokio::test]
async fn test_sqs_sink_multiple_batches() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");

    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");

    // Register schema for input topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce more records (enough to span multiple batches)
    let records_to_produce = 50;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline with small batch size to force multiple batches
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  sqs_sink:
    type: sqs
    from: kafka_source
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
"#,
        input_topic = ctx.kafka_topic,
        queue_url = sqs.queue_url,
        endpoint_url = sqs.endpoint_url,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "5")
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify messages in SQS
    let messages = sqs
        .receive_all_messages(records_to_produce as usize + 10, Duration::from_secs(15))
        .await
        .expect("Failed to receive messages from SQS");

    assert_eq!(
        messages.len(),
        records_to_produce as usize,
        "Expected {} messages in SQS, got {}",
        records_to_produce,
        messages.len()
    );

    // Verify first and last records are present
    let ids: Vec<i64> = messages
        .iter()
        .filter_map(|msg| {
            serde_json::from_str::<serde_json::Value>(msg)
                .ok()
                .and_then(|v| v.get("id").and_then(|id| id.as_i64()))
        })
        .collect();

    assert!(ids.contains(&1), "Should contain id=1");
    assert!(
        ids.contains(&records_to_produce),
        "Should contain id={}",
        records_to_produce
    );
}

// ============================================================================
// Scenario 3: SQS sink with SQL transform
// ============================================================================

/// Test: read from Kafka, apply SQL transform, then write to SQS
#[tokio::test]
async fn test_sqs_sink_with_transform() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");

    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");

    // Register schema for input topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce test records (some with even, some with odd IDs)
    let records_to_produce = 20;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source -> SQL filter (even IDs only) -> SQS sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  filter_even:
    type: sql
    primary_key: id
    sql: "SELECT * FROM kafka_source WHERE id % 2 = 0"

sinks:
  sqs_sink:
    type: sqs
    from: filter_even
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
"#,
        input_topic = ctx.kafka_topic,
        queue_url = sqs.queue_url,
        endpoint_url = sqs.endpoint_url,
    );

    // Only even IDs should pass through the filter
    let expected_count = records_to_produce / 2;

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(expected_count as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify messages in SQS
    let messages = sqs
        .receive_all_messages(expected_count as usize + 10, Duration::from_secs(10))
        .await
        .expect("Failed to receive messages from SQS");

    assert_eq!(
        messages.len(),
        expected_count as usize,
        "Expected {} messages in SQS (even IDs only), got {}",
        expected_count,
        messages.len()
    );

    // Verify all IDs are even
    for msg in &messages {
        let parsed: serde_json::Value =
            serde_json::from_str(msg).expect("Message should be valid JSON");
        let id = parsed.get("id").and_then(|v| v.as_i64()).unwrap();
        assert!(id % 2 == 0, "Expected even ID, got {}", id);
    }
}

// ============================================================================
// Scenario 4: SQS batching with uneven record batches
// ============================================================================

/// Test that the SQS sink correctly handles records arriving in batches that
/// don't divide evenly, exercising the SQS 10-message batch chunking logic.
#[tokio::test]
async fn test_sqs_sink_uneven_record_batches() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");

    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records_to_produce = 35;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  sqs_sink:
    type: sqs
    from: kafka_source
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
"#,
        input_topic = ctx.kafka_topic,
        queue_url = sqs.queue_url,
        endpoint_url = sqs.endpoint_url,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "7")
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let messages = sqs
        .receive_all_messages(records_to_produce as usize + 10, Duration::from_secs(15))
        .await
        .expect("Failed to receive messages from SQS");

    assert_eq!(
        messages.len(),
        records_to_produce as usize,
        "Expected {} messages in SQS, got {}",
        records_to_produce,
        messages.len()
    );

    let ids: Vec<i64> = messages
        .iter()
        .filter_map(|msg| {
            serde_json::from_str::<serde_json::Value>(msg)
                .ok()
                .and_then(|v| v.get("id").and_then(|id| id.as_i64()))
        })
        .collect();
    assert!(ids.contains(&1), "Should contain id=1");
    assert!(
        ids.contains(&records_to_produce),
        "Should contain id={}",
        records_to_produce
    );
}

// ============================================================================
// Scenario 5: Uint256 type serialization
// ============================================================================

/// Record with decimal string for uint256 conversion
#[derive(Debug, Clone, Serialize)]
struct U256TestRecord {
    id: i64,
    value: String, // decimal string, e.g. "12345678901234567890"
}

const U256_TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "U256TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

/// Test that uint256 columns are correctly serialized to JSON (as decimal strings)
#[tokio::test]
async fn test_sqs_sink_uint256() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");

    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");

    ctx.kafka
        .register_schema(U256_TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<U256TestRecord> = vec![
        U256TestRecord {
            id: 1,
            value: "12345678901234567890".to_string(),
        },
        U256TestRecord {
            id: 2,
            value: "0".to_string(),
        },
        U256TestRecord {
            id: 3,
            value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                .to_string(), // 2^256 - 1
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // SQL transform: to_u256(value) creates a uint256 column
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  with_u256:
    type: sql
    primary_key: id
    sql: "SELECT id, to_u256(value) as amount FROM kafka_source"

sinks:
  sqs_sink:
    type: sqs
    from: with_u256
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
"#,
        input_topic = ctx.kafka_topic,
        queue_url = sqs.queue_url,
        endpoint_url = sqs.endpoint_url,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let messages = sqs
        .receive_all_messages(records.len() + 10, Duration::from_secs(10))
        .await
        .expect("Failed to receive messages from SQS");

    assert_eq!(
        messages.len(),
        records.len(),
        "Expected {} messages in SQS, got {}",
        records.len(),
        messages.len()
    );

    // Verify uint256 is serialized as decimal string in JSON
    let expected_by_id: std::collections::HashMap<i64, &str> = [
        (1, "12345678901234567890"),
        (2, "0"),
        (
            3,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        ),
    ]
    .into_iter()
    .collect();

    for msg in &messages {
        let parsed: serde_json::Value =
            serde_json::from_str(msg).expect("Message should be valid JSON");
        let id = parsed
            .get("id")
            .and_then(|v| v.as_i64())
            .expect("Message should contain 'id' field");
        let amount = parsed
            .get("amount")
            .and_then(|v| v.as_str())
            .expect("Message should contain 'amount' field");
        let expected = expected_by_id
            .get(&id)
            .unwrap_or_else(|| panic!("Unexpected id {} in message", id));
        assert_eq!(
            amount, *expected,
            "id {}: expected amount '{}', got '{}'",
            id, expected, amount
        );
    }
}

// ============================================================================
// Scenario 6: Assume a role before sending
// ============================================================================

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/partner-role";

/// Minimal STS endpoint: records each request's form parameters and answers
/// AssumeRole with short-lived credentials, or with AccessDenied.
struct MockSts {
    url: String,
    calls: std::sync::Arc<std::sync::Mutex<Vec<std::collections::HashMap<String, String>>>>,
}

impl MockSts {
    async fn start(deny: bool) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = calls.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let body = loop {
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf);
                        if let Some(end) = text.find("\r\n\r\n") {
                            let content_length = text[..end]
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().ok())?
                                })
                                .unwrap_or(0);
                            if buf.len() >= end + 4 + content_length {
                                break buf[end + 4..end + 4 + content_length].to_vec();
                            }
                        }
                    };
                    recorded
                        .lock()
                        .unwrap()
                        .push(url::form_urlencoded::parse(&body).into_owned().collect());
                    let (status, xml) = if deny {
                        (
                            "403 Forbidden",
                            r#"<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>not authorized to perform: sts:AssumeRole</Message></Error><RequestId>1</RequestId></ErrorResponse>"#.to_string(),
                        )
                    } else {
                        let expiration = chrono::Utc::now() + chrono::Duration::hours(1);
                        (
                            "200 OK",
                            format!(
                                r#"<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>ASIAMOCKASSUMED</AccessKeyId><SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken><Expiration>{}</Expiration></Credentials><AssumedRoleUser><AssumedRoleId>AROAMOCK:s</AssumedRoleId><Arn>arn:aws:sts::123456789012:assumed-role/partner-role/s</Arn></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>"#,
                                expiration.format("%Y-%m-%dT%H:%M:%SZ")
                            ),
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{xml}",
                        xml.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        MockSts { url, calls }
    }

    fn calls(&self) -> Vec<std::collections::HashMap<String, String>> {
        self.calls.lock().unwrap().clone()
    }
}

fn role_pipeline(input_topic: &str, queue_url: &str, endpoint_url: &str) -> String {
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  sqs_sink:
    type: sqs
    from: kafka_source
    queue_url: {queue_url}
    endpoint_url: {endpoint_url}
    region: us-east-1
    role_arn: {ROLE_ARN}
    external_id: from-yaml
"#
    )
}

/// The external ID set through env wins over the one in YAML, every
/// AssumeRole goes to STS rather than the SQS endpoint, and rows still arrive.
#[tokio::test]
async fn test_sqs_sink_assumes_role_with_env_external_id() {
    init_tracing();
    let sts = MockSts::start(false).await;

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");
    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records_to_produce = 5;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let status = ctx
        .run_pipeline_with_opts(
            &role_pipeline(&ctx.kafka_topic, &sqs.queue_url, &sqs.endpoint_url),
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("AWS_ENDPOINT_URL_STS", &sts.url)
                .env(
                    "STREAMLING__PLUGIN__SQS_SINK__EXTERNAL_ID",
                    "tenant:from-env",
                )
                .timeout(Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    let calls = sts.calls();
    assert!(!calls.is_empty(), "the sink never called STS");
    for call in &calls {
        assert_eq!(call.get("Action").map(String::as_str), Some("AssumeRole"));
        assert_eq!(call.get("RoleArn").map(String::as_str), Some(ROLE_ARN));
        assert_eq!(
            call.get("ExternalId").map(String::as_str),
            Some("tenant:from-env")
        );
        assert_eq!(
            call.get("RoleSessionName").map(String::as_str),
            Some("streamling-sqs-sink")
        );
    }

    let messages = sqs
        .receive_all_messages(records_to_produce as usize + 10, Duration::from_secs(10))
        .await
        .expect("Failed to receive messages from SQS");
    assert_eq!(messages.len(), records_to_produce as usize);
}

/// A denied AssumeRole fails the pipeline instead of writing anything.
#[tokio::test]
async fn test_sqs_sink_fails_when_role_is_denied() {
    init_tracing();
    let sts = MockSts::start(true).await;

    let ctx = TestContext::with_options(TestContextOptions::new().with_sqs())
        .await
        .expect("Failed to create test context");
    let sqs = ctx.sqs.as_ref().expect("SQS resource should be created");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    ctx.kafka
        .produce_avro_records(&[TestRecord {
            id: 1,
            value: "value_1".to_string(),
            timestamp: 1001,
        }])
        .await
        .expect("Failed to produce records");

    let status = ctx
        .run_pipeline_with_opts(
            &role_pipeline(&ctx.kafka_topic, &sqs.queue_url, &sqs.endpoint_url),
            PipelineOpts::new()
                .record_limit(1)
                .env("AWS_ENDPOINT_URL_STS", &sts.url)
                .env(
                    "STREAMLING__PLUGIN__SQS_SINK__EXTERNAL_ID",
                    "tenant:from-env",
                )
                .timeout(Duration::from_secs(60)),
        )
        .await;
    // The harness reports a non-zero exit as an error.
    assert!(
        status.map_or(true, |status| !status.success()),
        "a denied AssumeRole must fail the pipeline"
    );
    assert!(!sts.calls().is_empty(), "the sink never called STS");

    let messages = sqs
        .receive_all_messages(10, Duration::from_secs(5))
        .await
        .expect("Failed to receive messages from SQS");
    assert!(
        messages.is_empty(),
        "nothing may be written without the role"
    );
}
