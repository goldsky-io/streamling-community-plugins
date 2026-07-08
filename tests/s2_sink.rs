//! S2 sink plugin e2e tests backed by s2-lite.

mod s2_common;

use s2_common::S2Fixture;
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, TestContextOptions};

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

async fn setup() -> S2Fixture {
    S2Fixture::setup(TestContextOptions::new().with_plugin()).await
}

/// Registers the Avro schema and produces `TestRecord`s with the given
/// per-id `value`.
async fn produce_records(fixture: &S2Fixture, count: i64, value: impl Fn(i64) -> String) {
    fixture
        .ctx
        .kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    let records: Vec<TestRecord> = (1..=count)
        .map(|id| TestRecord {
            id,
            value: value(id),
            timestamp: 1000 + id,
        })
        .collect();
    fixture
        .ctx
        .kafka
        .produce_avro_records(&records)
        .await
        .expect("failed to produce records");
}

fn kafka_pipeline(topic: &str) -> String {
    format!(
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
"#
    )
}

fn sink_opts(fixture: &S2Fixture, record_limit: u64) -> PipelineOpts {
    PipelineOpts::new()
        .record_limit(record_limit)
        .env("STREAMLING__RECORD_BATCH_SIZE", "1")
        .env("STREAMLING__PLUGIN__S2_SINK__ACCESS_TOKEN", "ignored")
        .env(
            "STREAMLING__PLUGIN__S2_SINK__BASIN",
            fixture.basin.to_string(),
        )
        .env("STREAMLING__PLUGIN__S2_SINK__ENDPOINT", &fixture.endpoint)
        .env("STREAMLING__PLUGIN__S2_SINK__LINGER_MS", "0")
        .timeout(Duration::from_secs(90))
}

#[tokio::test]
async fn test_s2_sink_writes_records_to_s2_lite() {
    let fixture = setup().await;
    let stream = fixture
        .create_stream(&format!("stream-{}", &fixture.ctx.test_id[..8]))
        .await;

    let records_to_produce = 25;
    produce_records(&fixture, records_to_produce, |id| format!("value_{id}")).await;

    let status = fixture
        .ctx
        .run_pipeline_with_opts(
            &kafka_pipeline(&fixture.ctx.kafka_topic),
            sink_opts(&fixture, records_to_produce as u64)
                .env("STREAMLING__PLUGIN__S2_SINK__STREAM", stream.to_string())
                .env("STREAMLING__PLUGIN__S2_SINK__ENSURE_STREAM", "true"),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit successfully");

    let s2_records = fixture
        .read_from_start(&stream, records_to_produce as usize)
        .await;
    assert_eq!(
        s2_records.len(),
        records_to_produce as usize,
        "unexpected S2 record count"
    );

    let ids: BTreeSet<i64> = s2_records
        .iter()
        .map(|record| {
            // Row kind travels as a Debezium-style header; kafka-sourced rows
            // are inserts.
            let op = record
                .headers
                .iter()
                .find(|h| h.name.as_ref() == b"dbz.op")
                .expect("S2 record should carry a dbz.op header");
            assert_eq!(op.value.as_ref(), b"c");

            let value: serde_json::Value =
                serde_json::from_slice(&record.body).expect("S2 record should be JSON");
            assert!(
                value.get("_gs_op").is_none(),
                "the op travels as a header, not payload; got {value}"
            );
            value
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .expect("S2 record should include id")
        })
        .collect();
    let expected_ids: BTreeSet<i64> = (1..=records_to_produce).collect();
    assert_eq!(ids, expected_ids);
}

/// `stream_template` routing: records fan out across streams resolved from
/// row values, with streams created lazily on first use.
#[tokio::test]
async fn test_s2_sink_routes_records_by_template() {
    let fixture = setup().await;

    // Two tenants interleaved; target streams are NOT pre-created — the sink
    // must ensure them lazily as the template resolves.
    let records_to_produce = 10;
    produce_records(&fixture, records_to_produce, |id| {
        format!("tenant-{}", id % 2)
    })
    .await;

    let stream_prefix = format!("routed-{}", &fixture.ctx.test_id[..8]);
    let status = fixture
        .ctx
        .run_pipeline_with_opts(
            &kafka_pipeline(&fixture.ctx.kafka_topic),
            sink_opts(&fixture, records_to_produce as u64).env(
                "STREAMLING__PLUGIN__S2_SINK__STREAM_TEMPLATE",
                format!("{stream_prefix}/{{value}}"),
            ),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit successfully");

    for tenant in ["tenant-0", "tenant-1"] {
        let stream = format!("{stream_prefix}/{tenant}")
            .parse()
            .expect("valid stream name");
        let records = fixture
            .read_from_start(&stream, records_to_produce as usize)
            .await;

        assert_eq!(records.len(), 5, "each tenant stream should have 5 records");
        for record in records {
            let value: serde_json::Value =
                serde_json::from_slice(&record.body).expect("S2 record should be JSON");
            assert_eq!(
                value.get("value").and_then(serde_json::Value::as_str),
                Some(tenant),
                "record routed to the wrong stream"
            );
        }
    }
}
