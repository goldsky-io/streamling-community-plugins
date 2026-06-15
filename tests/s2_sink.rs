//! S2 sink plugin e2e tests backed by s2-lite.

use s2_sdk::types::{
    BasinName, EnsureBasinInput, EnsureStreamInput, ReadFrom, ReadInput, ReadLimits, ReadStart,
    ReadStop, StreamName,
};
use s2_testcontainers::S2Lite;
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, TestContext, TestContextOptions, init_tracing};

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "S2SinkTestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

#[tokio::test]
async fn test_s2_sink_writes_records_to_s2_lite() {
    if !s2_lite_enabled() {
        eprintln!("skipping s2-lite e2e; set E2E_S2_LITE=1 to run it");
        return;
    }

    init_tracing();

    let s2_lite = S2Lite::start().await.expect("failed to start s2-lite");
    let ctx = TestContext::with_options(TestContextOptions::new().with_plugin())
        .await
        .expect("failed to create test context");
    let s2 = s2_lite.client().expect("failed to construct s2 client");
    let basin = format!("basin-{}", &ctx.test_id[..8])
        .parse::<BasinName>()
        .expect("valid basin name");
    let stream = format!("stream-{}", &ctx.test_id[..8])
        .parse::<StreamName>()
        .expect("valid stream name");

    s2.ensure_basin(EnsureBasinInput::new(basin.clone()))
        .await
        .expect("failed to ensure s2-lite basin");
    s2.basin(basin.clone())
        .ensure_stream(EnsureStreamInput::new(stream.clone()))
        .await
        .expect("failed to ensure s2-lite stream");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");

    let records_to_produce = 25;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|id| TestRecord {
            id,
            value: format!("value_{id}"),
            timestamp: 1000 + id,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("failed to produce records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  s2_sink:
    type: s2_sink
    from: kafka_source
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__PLUGIN__S2_SINK__ACCESS_TOKEN", "ignored")
                .env("STREAMLING__PLUGIN__S2_SINK__BASIN", basin.to_string())
                .env("STREAMLING__PLUGIN__S2_SINK__STREAM", stream.to_string())
                .env("STREAMLING__PLUGIN__S2_SINK__ENDPOINT", s2_lite.endpoint())
                .env("STREAMLING__PLUGIN__S2_SINK__ENSURE_STREAM", "true")
                .env("STREAMLING__PLUGIN__S2_SINK__LINGER_MS", "0")
                .timeout(Duration::from_secs(90)),
        )
        .await
        .expect("streamling execution failed");

    assert!(status.success(), "streamling should exit successfully");

    let s2_records = s2
        .basin(basin)
        .stream(stream)
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)))
                .with_stop(
                    ReadStop::new()
                        .with_limits(ReadLimits::new().with_count(records_to_produce as usize)),
                ),
        )
        .await
        .expect("failed to read s2-lite records")
        .records;

    assert_eq!(
        s2_records.len(),
        records_to_produce as usize,
        "unexpected S2 record count"
    );

    let ids: BTreeSet<i64> = s2_records
        .iter()
        .map(|record| {
            let value: serde_json::Value =
                serde_json::from_slice(&record.body).expect("S2 record should be JSON");
            assert!(
                value.get("_gs_op").is_none(),
                "S2 record body should not duplicate operation metadata"
            );
            let op_header = record
                .headers
                .iter()
                .find(|header| header.name.as_ref() == b"dbz.op")
                .expect("S2 record should include operation header");
            assert_eq!(op_header.value.as_ref(), b"c");
            value
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .expect("S2 record should include id")
        })
        .collect();
    let expected_ids: BTreeSet<i64> = (1..=records_to_produce).collect();
    assert_eq!(ids, expected_ids);
}

fn s2_lite_enabled() -> bool {
    std::env::var("E2E_S2_LITE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}
