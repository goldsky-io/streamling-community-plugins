//! S2 source plugin e2e tests backed by s2-lite, sinking into ClickHouse.

use s2_sdk::S2;
use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, BasinName, EnsureBasinInput, EnsureStreamInput,
    Header, StreamName,
};
use s2_testcontainers::S2Lite;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, TestContext, TestContextOptions, init_tracing};

struct S2Fixture {
    ctx: TestContext,
    s2: S2,
    basin: BasinName,
    endpoint: String,
    _s2_lite: S2Lite,
}

async fn setup() -> S2Fixture {
    init_tracing();
    let s2_lite = S2Lite::start().await.expect("failed to start s2-lite");
    let ctx = TestContext::with_options(TestContextOptions::new().with_plugin().with_clickhouse())
        .await
        .expect("failed to create test context");
    let s2 = s2_lite.client().expect("failed to construct s2 client");
    let basin = format!("basin-{}", &ctx.test_id[..8])
        .parse::<BasinName>()
        .expect("valid basin name");
    s2.ensure_basin(EnsureBasinInput::new(basin.clone()))
        .await
        .expect("failed to ensure s2-lite basin");
    let endpoint = s2_lite.endpoint().to_string();
    S2Fixture {
        ctx,
        s2,
        basin,
        endpoint,
        _s2_lite: s2_lite,
    }
}

impl S2Fixture {
    async fn create_stream(&self, name: &str) -> StreamName {
        let stream = name.parse::<StreamName>().expect("valid stream name");
        self.s2
            .basin(self.basin.clone())
            .ensure_stream(EnsureStreamInput::new(stream.clone()))
            .await
            .expect("failed to ensure s2-lite stream");
        stream
    }

    async fn append(&self, stream: &StreamName, bodies: impl IntoIterator<Item = String>) {
        self.append_with_ops(stream, bodies.into_iter().map(|body| (body, None)))
            .await;
    }

    /// Appends records, optionally tagged with a Debezium-style `dbz.op`
    /// header (as the s2_sink writes them).
    async fn append_with_ops(
        &self,
        stream: &StreamName,
        records: impl IntoIterator<Item = (String, Option<&str>)>,
    ) {
        let records = records
            .into_iter()
            .map(|(body, op)| {
                let record = AppendRecord::new(body).expect("valid S2 record");
                match op {
                    Some(op) => record
                        .with_headers([Header::new("dbz.op", op.to_string())])
                        .expect("valid S2 record headers"),
                    None => record,
                }
            })
            .collect::<Vec<_>>();
        self.s2
            .basin(self.basin.clone())
            .stream(stream.clone())
            .append(AppendInput::new(
                AppendRecordBatch::try_from_iter(records).expect("valid S2 batch"),
            ))
            .await
            .expect("failed to append S2 records");
    }
}

/// Typed-schema mode with `stream_prefix` discovery: JSON events across two
/// prefixed streams land as typed rows in ClickHouse, with S2 metadata
/// columns attached — the events -> s2 -> streamling -> clickhouse path.
#[tokio::test]
async fn test_s2_source_typed_prefix_to_clickhouse() {
    let fixture = setup().await;
    let clickhouse = fixture.ctx.clickhouse.as_ref().expect("clickhouse");

    let stream_prefix = format!("events-{}-", &fixture.ctx.test_id[..8]);
    let records_per_stream = 6usize;
    let mut total = 0usize;
    for suffix in ["a", "b"] {
        let stream = fixture
            .create_stream(&format!("{stream_prefix}{suffix}"))
            .await;
        let bodies = (0..records_per_stream).map(|i| {
            let id = total + i + 1;
            format!(r#"{{"id":{id},"value":"value_{id}","amount":{id}.5}}"#)
        });
        fixture.append(&stream, bodies).await;
        total += records_per_stream;
    }

    let pipeline = r#"
sources:
  s2_source:
    type: s2_source
    schema: "id:int64,value:string,amount:float64"
    include_metadata: true
    batch_size: 5

transforms: {}

sinks:
  ch_sink:
    type: clickhouse
    from: s2_source
    table: s2_source_typed
    primary_key: id
"#;

    let status = fixture
        .ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(total as u64)
                .env("STREAMLING__PLUGIN__S2_SOURCE__ACCESS_TOKEN", "ignored")
                .env(
                    "STREAMLING__PLUGIN__S2_SOURCE__BASIN",
                    fixture.basin.to_string(),
                )
                .env(
                    "STREAMLING__PLUGIN__S2_SOURCE__STREAM_PREFIX",
                    &stream_prefix,
                )
                .env("STREAMLING__PLUGIN__S2_SOURCE__ENDPOINT", &fixture.endpoint)
                .timeout(Duration::from_secs(90)),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM s2_source_typed")
        .await
        .expect("failed to count rows");
    assert_eq!(count, total as u64);

    let matched = clickhouse
        .count(
            "SELECT COUNT(*) FROM s2_source_typed \
             WHERE id = 7 AND value = 'value_7' AND amount = 7.5",
        )
        .await
        .expect("failed to query row");
    assert_eq!(matched, 1);

    // Metadata columns: records 1..=6 came from stream "-a", 7..=12 from "-b".
    let from_first_stream = clickhouse
        .count(&format!(
            "SELECT COUNT(*) FROM s2_source_typed WHERE _s2_stream = '{stream_prefix}a'"
        ))
        .await
        .expect("failed to query metadata");
    assert_eq!(from_first_stream, records_per_stream as u64);
}

/// Raw mode with an exact stream: the S2 record envelope (stream, seq_num,
/// timestamp, headers, body) lands in ClickHouse without a configured schema.
#[tokio::test]
async fn test_s2_source_raw_envelope_to_clickhouse() {
    let fixture = setup().await;
    let clickhouse = fixture.ctx.clickhouse.as_ref().expect("clickhouse");

    let stream_name = format!("raw-{}", &fixture.ctx.test_id[..8]);
    let stream = fixture.create_stream(&stream_name).await;
    let total = 8usize;
    fixture
        .append(
            &stream,
            (0..total).map(|i| format!(r#"{{"event":"e_{i}"}}"#)),
        )
        .await;

    let pipeline = r#"
sources:
  s2_source:
    type: s2_source

transforms: {}

sinks:
  ch_sink:
    type: clickhouse
    from: s2_source
    table: s2_source_raw
    primary_key: seq_num
"#;

    let status = fixture
        .ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(total as u64)
                .env("STREAMLING__PLUGIN__S2_SOURCE__ACCESS_TOKEN", "ignored")
                .env(
                    "STREAMLING__PLUGIN__S2_SOURCE__BASIN",
                    fixture.basin.to_string(),
                )
                .env("STREAMLING__PLUGIN__S2_SOURCE__STREAM", &stream_name)
                .env("STREAMLING__PLUGIN__S2_SOURCE__ENDPOINT", &fixture.endpoint)
                .timeout(Duration::from_secs(90)),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM s2_source_raw")
        .await
        .expect("failed to count rows");
    assert_eq!(count, total as u64);

    let matched = clickhouse
        .count(&format!(
            "SELECT COUNT(*) FROM s2_source_raw \
             WHERE stream = '{stream_name}' AND seq_num = 3 AND body = '{{\"event\":\"e_3\"}}'"
        ))
        .await
        .expect("failed to query row");
    assert_eq!(matched, 1);
}

/// CDC round-trip: records carrying Debezium-style `dbz.op` headers (as the
/// s2_sink writes them) restore `_gs_op`, so updates and deletes apply in
/// ClickHouse instead of landing as inserts.
#[tokio::test]
async fn test_s2_source_restores_ops_for_cdc_round_trip() {
    let fixture = setup().await;
    let clickhouse = fixture.ctx.clickhouse.as_ref().expect("clickhouse");

    let stream_name = format!("cdc-{}", &fixture.ctx.test_id[..8]);
    let stream = fixture.create_stream(&stream_name).await;
    let records = [
        (r#"{"id":1,"value":"one"}"#, None), // no header → insert
        (r#"{"id":2,"value":"two"}"#, Some("c")),
        (r#"{"id":1,"value":"one-updated"}"#, Some("u")),
        (r#"{"id":2,"value":"two"}"#, Some("d")),
    ];
    fixture
        .append_with_ops(
            &stream,
            records.iter().map(|(body, op)| (body.to_string(), *op)),
        )
        .await;

    let pipeline = r#"
sources:
  s2_source:
    type: s2_source
    schema: "id:int64,value:string"

transforms: {}

sinks:
  ch_sink:
    type: clickhouse
    from: s2_source
    table: s2_source_cdc
    primary_key: id
"#;

    let status = fixture
        .ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__PLUGIN__S2_SOURCE__ACCESS_TOKEN", "ignored")
                .env(
                    "STREAMLING__PLUGIN__S2_SOURCE__BASIN",
                    fixture.basin.to_string(),
                )
                .env("STREAMLING__PLUGIN__S2_SOURCE__STREAM", &stream_name)
                .env("STREAMLING__PLUGIN__S2_SOURCE__ENDPOINT", &fixture.endpoint)
                .timeout(Duration::from_secs(90)),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit successfully");

    // The ClickHouse sink's default mode is ReplacingMergeTree with an
    // is_deleted column computed from _gs_op; FINAL collapses versions.
    let active = clickhouse
        .count("SELECT COUNT(*) FROM s2_source_cdc FINAL WHERE is_deleted = 0")
        .await
        .expect("failed to count active rows");
    assert_eq!(active, 1, "id=2 should be deleted, only id=1 active");

    let updated = clickhouse
        .count(
            "SELECT COUNT(*) FROM s2_source_cdc FINAL \
             WHERE id = 1 AND value = 'one-updated' AND is_deleted = 0",
        )
        .await
        .expect("failed to query updated row");
    assert_eq!(updated, 1, "id=1 should carry the updated value");
}
