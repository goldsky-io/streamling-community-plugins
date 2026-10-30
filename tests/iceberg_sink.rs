//! Iceberg sink plugin e2e tests against the Iceberg REST fixture.

mod iceberg_common;

use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
use iceberg::{Catalog, TableCreation};
use iceberg_common::IcebergFixture;
use serde::Serialize;
use streamling_e2e::{PipelineOpts, TestContextOptions};

const SINK_ENV: &str = "STREAMLING__PLUGIN__ICEBERG_SINK";
/// 2024-03-01T00:00:00Z.
const BASE_TS_MS: i64 = 1_709_251_200_000;
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
    category: String,
    ts: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "IcebergSinkTestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    ]
}"#;

fn record(id: i64) -> TestRecord {
    TestRecord {
        id,
        value: format!("value_{id}"),
        category: ["a", "b", "c"][id as usize % 3].to_string(),
        // Spread over three days.
        ts: BASE_TS_MS + (id % 3) * DAY_MS + id * 1000,
    }
}

async fn produce(fixture: &IcebergFixture, ids: RangeInclusive<i64>, op: &str) {
    fixture
        .ctx
        .kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("failed to register schema");
    let records: Vec<TestRecord> = ids.map(record).collect();
    fixture
        .ctx
        .kafka
        .produce_avro_records_with_op(&records, op)
        .await
        .expect("failed to produce records");
}

fn pipeline(topic: &str) -> String {
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
  lake:
    type: iceberg_sink
    from: kafka_source
"#
    )
}

fn sink_opts(fixture: &IcebergFixture, table: &str) -> PipelineOpts {
    PipelineOpts::new()
        .env("STREAMLING__RECORD_BATCH_SIZE", "4")
        .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
        .env(format!("{SINK_ENV}__CATALOG_URI"), &fixture.catalog_uri)
        .env(format!("{SINK_ENV}__TABLE"), fixture.table_option(table))
        .timeout(Duration::from_secs(120))
}

/// Total rows in the table's current snapshot, if it has one.
async fn committed_rows(fixture: &IcebergFixture, table: &str) -> Option<u64> {
    let table = fixture
        .catalog
        .load_table(&fixture.table_ident(table))
        .await
        .ok()?;
    let snapshot = table.metadata().current_snapshot()?;
    snapshot
        .summary()
        .additional_properties
        .get("total-records")?
        .parse()
        .ok()
}

async fn wait_for_committed(fixture: &IcebergFixture, table: &str, count: u64) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while committed_rows(fixture, table).await < Some(count) {
        assert!(Instant::now() < deadline, "rows were not committed in time");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Produces ids `1..=count`, runs the pipeline until a checkpoint has
/// committed all of them, then stops it.
///
/// The host stops a plugin sink as soon as the record limit is reached,
/// before any later checkpoint marker reaches it, so the run is given a
/// limit of `count + 1` and the extra (sentinel) record is produced only
/// once the first `count` rows are committed. The sentinel may or may not be
/// committed itself.
async fn run_until_committed(
    fixture: &IcebergFixture,
    table: &str,
    count: i64,
    opts: PipelineOpts,
) -> ExitStatus {
    produce(fixture, 1..=count, "c").await;
    let pipeline = pipeline(&fixture.ctx.kafka_topic);
    let run = fixture
        .ctx
        .run_pipeline_with_opts(&pipeline, opts.record_limit(count as u64 + 1));
    let stop = async {
        wait_for_committed(fixture, table, count as u64).await;
        produce(fixture, count + 1..=count + 1, "c").await;
    };
    let (status, ()) = tokio::join!(run, stop);
    status.expect("streamling execution failed")
}

fn rows_by_id(batches: &[RecordBatch]) -> BTreeMap<i64, HashMap<String, String>> {
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let mut values = HashMap::new();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                let rendered = if column.is_null(row) {
                    "null".to_string()
                } else {
                    arrow::util::display::array_value_to_string(column, row).expect("render")
                };
                values.insert(field.name().clone(), rendered);
            }
            assert!(
                rows.insert(ids.value(row), values).is_none(),
                "id {} was written twice",
                ids.value(row)
            );
        }
    }
    rows
}

#[tokio::test]
async fn test_iceberg_sink_appends_to_auto_created_partitioned_table() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    let count = 30;
    let status = run_until_committed(
        &fixture,
        "events",
        count,
        sink_opts(&fixture, "events").env(
            format!("{SINK_ENV}__PARTITION_BY"),
            "category, day(ts), bucket(4, id)",
        ),
    )
    .await;
    assert!(status.success(), "streamling should exit successfully");

    let table = fixture
        .catalog
        .load_table(&fixture.table_ident("events"))
        .await
        .expect("table was auto-created");
    let metadata = table.metadata();
    let spec: Vec<(String, Transform)> = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.transform))
        .collect();
    assert_eq!(
        spec,
        vec![
            ("category".to_string(), Transform::Identity),
            ("ts_day".to_string(), Transform::Day),
            ("id_bucket".to_string(), Transform::Bucket(4)),
        ]
    );
    assert_eq!(
        metadata.properties()["write.parquet.compression-codec"],
        "zstd"
    );
    let schema = metadata.current_schema();
    assert_eq!(
        *schema.field_by_name("ts").expect("ts column").field_type,
        Type::Primitive(PrimitiveType::Timestamp)
    );
    assert!(
        schema.field_by_name("_gs_op").is_some(),
        "_gs_op is kept by default"
    );

    // Every snapshot is a Streamling commit carrying its checkpoint epoch.
    let mut epochs = Vec::new();
    for snapshot in metadata.snapshots() {
        let props = &snapshot.summary().additional_properties;
        let epoch: u64 = props
            .get("streamling.checkpoint-epoch")
            .expect("snapshot carries the checkpoint epoch")
            .parse()
            .expect("numeric epoch");
        assert!(props.contains_key("streamling.writer-id"));
        epochs.push(epoch);
    }
    epochs.sort_unstable();
    epochs.dedup();
    assert_eq!(
        epochs.len(),
        metadata.snapshots().len(),
        "one snapshot per epoch"
    );

    let batches = fixture.read_table("events").await;
    let rows = rows_by_id(&batches);
    let ids: Vec<i64> = rows.keys().copied().filter(|id| *id <= count).collect();
    assert_eq!(
        ids,
        (1..=count).collect::<Vec<_>>(),
        "every record lands once"
    );
    for (id, row) in &rows {
        let expected = record(*id);
        assert_eq!(row["value"], expected.value);
        assert_eq!(row["category"], expected.category);
    }
    for batch in &batches {
        assert_eq!(
            batch.schema().field_with_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>();
        for row in 0..batch.num_rows() {
            assert_eq!(
                ts.value(row),
                record(ids.value(row)).ts * 1000,
                "millisecond timestamps are stored as exact microseconds"
            );
        }
    }
}

#[tokio::test]
async fn test_iceberg_sink_writes_existing_table_schema_with_op_column() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;

    // Pre-created table: different column order, a timestamptz column for a
    // tz-less input, an extra optional column, and the `_gs_op` changelog
    // column.
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "note", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(3, "value", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(5, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            NestedField::optional(6, "_gs_op", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    fixture
        .catalog
        .create_table(
            &fixture.namespace,
            TableCreation::builder()
                .name("changelog".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .expect("create table");

    let count = 12;
    let status = run_until_committed(
        &fixture,
        "changelog",
        count,
        sink_opts(&fixture, "changelog")
            .env(format!("{SINK_ENV}__AUTO_CREATE"), "false")
            .env(format!("{SINK_ENV}__INCLUDE_OP_COLUMN"), "true"),
    )
    .await;
    assert!(status.success(), "streamling should exit successfully");

    let rows = rows_by_id(&fixture.read_table("changelog").await);
    assert!(rows.len() >= count as usize, "got {} rows", rows.len());
    for (id, row) in &rows {
        assert_eq!(row["note"], "null", "missing optional column is null");
        assert_eq!(row["value"], format!("value_{id}"));
        assert_eq!(row["_gs_op"], "i");
    }
}

#[tokio::test]
async fn test_iceberg_sink_rejects_updates_without_op_column() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    let count = 5;
    produce(&fixture, 1..=count, "u").await;

    let result = fixture
        .ctx
        .run_pipeline_with_opts(
            &pipeline(&fixture.ctx.kafka_topic),
            sink_opts(&fixture, "rejected")
                .env(format!("{SINK_ENV}__INCLUDE_OP_COLUMN"), "false")
                .record_limit(count as u64),
        )
        .await;
    assert!(
        !result.is_ok_and(|status| status.success()),
        "updates must fail the pipeline"
    );

    let table = fixture
        .catalog
        .load_table(&fixture.table_ident("rejected"))
        .await
        .expect("table was auto-created at initialization");
    assert!(
        table.metadata().current_snapshot().is_none(),
        "nothing was committed"
    );
}

#[tokio::test]
async fn test_iceberg_sink_default_changelog_survives_restart() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    let mut expected = Vec::new();
    let mut previous = None;

    for run_number in 1..=2 {
        // Separate input topics make each process's input independent of Kafka
        // consumer-group recovery; the destination is deliberately unchanged.
        let kafka = fixture
            .ctx
            .create_kafka_topic(&format!("run_{run_number}"))
            .await
            .expect("create input topic");
        kafka.register_schema(TEST_SCHEMA).await.expect("schema");
        for (input_op, stored_op) in [("c", "i"), ("u", "u"), ("d", "d")] {
            let records: Vec<_> = (1..=2)
                .map(|id| TestRecord {
                    value: format!("run_{run_number}_{stored_op}_{id}"),
                    ..record(id)
                })
                .collect();
            expected.extend(
                records
                    .iter()
                    .map(|r| (r.id, r.value.clone(), stored_op.to_string())),
            );
            kafka
                .produce_avro_records_with_op(&records, input_op)
                .await
                .expect("produce changelog");
        }

        let yaml = pipeline(&kafka.topic);
        let run = fixture.ctx.run_pipeline_with_opts(
            &yaml,
            sink_opts(&fixture, "restart")
                .env("STREAMLING__RECORD_BATCH_SIZE", "6")
                .record_limit(7),
        );
        let stop = async {
            wait_for_committed(&fixture, "restart", run_number * 6).await;
            kafka
                .produce_avro_records(&[record(99)])
                .await
                .expect("produce stop sentinel");
        };
        let (status, ()) = tokio::join!(run, stop);
        assert!(status.expect("pipeline execution").success());

        let table = fixture
            .catalog
            .load_table(&fixture.table_ident("restart"))
            .await
            .expect("load reused table");
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().expect("committed snapshot");
        let writer = snapshot.summary().additional_properties["streamling.writer-id"].clone();
        if let Some((uuid, schema, snapshot_id, writer_id)) = previous.take() {
            assert_eq!(metadata.uuid(), uuid, "same table, not a replacement");
            assert_eq!(metadata.current_schema(), &schema, "schema is unchanged");
            assert!(metadata.snapshot_by_id(snapshot_id).is_some());
            assert_ne!(snapshot.snapshot_id(), snapshot_id);
            assert_ne!(writer, writer_id, "each process has its own writer id");
        }
        previous = Some((
            metadata.uuid(),
            metadata.current_schema().clone(),
            snapshot.snapshot_id(),
            writer,
        ));

        let mut actual = Vec::new();
        for batch in fixture.read_table("restart").await {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let values = batch.column_by_name("value").unwrap().as_string::<i32>();
            let ops = batch.column_by_name("_gs_op").unwrap().as_string::<i32>();
            for row in 0..batch.num_rows() {
                actual.push((
                    ids.value(row),
                    values.value(row).to_string(),
                    ops.value(row).to_string(),
                ));
            }
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "all i/u/d rows are appended, not merged");
    }
}

#[tokio::test]
async fn test_iceberg_sink_preserves_exact_numeric_boundaries() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    let decimals = [
        "9999999999999999999999999999999999.9999",
        "-1234567890123456789012345678901234.5000",
    ];
    let unsigned = [u64::MAX.to_string(), "0".to_string()];
    const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let records: Vec<_> = (0..2)
        .map(|i| TestRecord {
            value: decimals[i].to_string(),
            category: unsigned[i].clone(),
            ..record(i as i64 + 1)
        })
        .collect();
    fixture
        .ctx
        .kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("schema");
    fixture
        .ctx
        .kafka
        .produce_avro_records(&records)
        .await
        .expect("produce numeric strings");

    // v0.4.0's SQL planner supports BIGINT UNSIGNED, and to_u256 attaches
    // Streamling's extension metadata to its FixedSizeBinary(32) output.
    let yaml = pipeline(&fixture.ctx.kafka_topic)
        .replace(
            "transforms: {}",
            &format!(
                r#"transforms:
  exact_numbers:
    type: sql
    primary_key: id
    sql: "SELECT id, CAST(value AS DECIMAL(38,4)) AS amount, CAST(category AS BIGINT UNSIGNED) AS unsigned_value, to_u256(CASE WHEN id = 1 THEN '{U256_MAX}' ELSE '0' END) AS huge FROM kafka_source""#
            ),
        )
        .replace("from: kafka_source", "from: exact_numbers");
    let run = fixture.ctx.run_pipeline_with_opts(
        &yaml,
        sink_opts(&fixture, "numbers")
            .env("STREAMLING__RECORD_BATCH_SIZE", "2")
            .record_limit(3),
    );
    let stop = async {
        wait_for_committed(&fixture, "numbers", 2).await;
        fixture
            .ctx
            .kafka
            .produce_avro_records(&[TestRecord {
                id: 99,
                ..records[1].clone()
            }])
            .await
            .expect("produce stop sentinel");
    };
    let (status, ()) = tokio::join!(run, stop);
    assert!(status.expect("pipeline execution").success());

    let table = fixture
        .catalog
        .load_table(&fixture.table_ident("numbers"))
        .await
        .expect("auto-created numeric table");
    let schema = table.metadata().current_schema();
    for (column, primitive) in [
        (
            "amount",
            PrimitiveType::Decimal {
                precision: 38,
                scale: 4,
            },
        ),
        (
            "unsigned_value",
            PrimitiveType::Decimal {
                precision: 20,
                scale: 0,
            },
        ),
        ("huge", PrimitiveType::String),
    ] {
        assert_eq!(
            *schema
                .field_by_name(column)
                .expect("numeric column")
                .field_type,
            Type::Primitive(primitive)
        );
    }
    let rows = rows_by_id(&fixture.read_table("numbers").await);
    assert_eq!(rows.len(), 2);
    for (i, decimal) in decimals.iter().enumerate() {
        let row = &rows[&(i as i64 + 1)];
        assert_eq!(row["amount"], *decimal, "all 38 digits and scale survive");
        assert_eq!(row["unsigned_value"], unsigned[i]);
        assert_eq!(row["huge"], if i == 0 { U256_MAX } else { "0" });
    }
}
