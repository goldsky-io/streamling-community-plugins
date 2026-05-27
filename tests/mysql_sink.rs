//! MySQL sink plugin e2e tests.
//!
//! These tests verify that streamling can read from Kafka and write to MySQL
//! via the `mysql_sink` plugin. Requires:
//! - `E2E_MYSQL_URL` (e.g. `mysql://root:root@localhost:3306/test`)
//! - Kafka / Schema Registry
//! - The community_plugins shared library built and on the plugin path

use serde::Serialize;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, TestContext, TestContextOptions, init_tracing};

// ============================================================================
// Test Record Types
// ============================================================================

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

#[derive(Debug, Clone, Serialize)]
struct CompositePkRecord {
    id: i64,
    version: i64,
    value: String,
}

const COMPOSITE_PK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "CompositePkTestMessage",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "version", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

/// Helper: build a pipeline YAML that routes Kafka → mysql_sink plugin.
///
/// The mysql_sink options are injected via env vars so that secrets are not
/// baked into the YAML string.
fn mysql_pipeline(topic: &str, table: &str, primary_key: &str, extra: &str) -> String {
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: {primary_key}

transforms: {{}}

sinks:
  mysql_sink:
    type: mysql_sink
    from: kafka_source
    primary_key: {primary_key}
    table: {table}
    {extra}
"#
    )
}

/// Helper: build env-var overrides that pass MySQL connection details and
/// primary key to the plugin (the plugin reads `STREAMLING__PLUGIN__MYSQL_SINK__*`).
///
/// `primary_key` is passed via env var because the topology parser strips
/// the `primary_key` field from plugin options (it's a reserved topology field).
fn mysql_env(ctx: &TestContext, primary_key: &str) -> Vec<(String, String)> {
    let mysql = ctx.mysql.as_ref().expect("MySQL resource required");
    vec![
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__HOST".into(),
            mysql.host.clone(),
        ),
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__PORT".into(),
            mysql.port.to_string(),
        ),
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__USER".into(),
            mysql.user.clone(),
        ),
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__PASSWORD".into(),
            mysql.password.clone(),
        ),
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__DATABASE".into(),
            mysql.database.clone(),
        ),
        (
            "STREAMLING__PLUGIN__MYSQL_SINK__PRIMARY_KEY".into(),
            primary_key.to_string(),
        ),
    ]
}

async fn new_ctx() -> TestContext {
    TestContext::with_options(TestContextOptions::new().with_mysql())
        .await
        .expect("Failed to create test context")
}

// ============================================================================
// Scenario 1: Basic insert
// ============================================================================

#[tokio::test]
async fn test_basic_mysql_sink() {
    init_tracing();
    let ctx = new_ctx().await;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=10)
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

    let pipeline = mysql_pipeline(&ctx.kafka_topic, "test_basic", "id", "");
    let mut opts = PipelineOpts::new()
        .record_limit(10)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id") {
        opts = opts.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Pipeline failed");
    assert!(status.success(), "Pipeline should exit successfully");

    let mysql = ctx.mysql.as_ref().unwrap();
    let count = mysql
        .count("SELECT COUNT(*) FROM test_basic")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 10, "Should have 10 rows");

    #[derive(sqlx::FromRow, Debug)]
    struct Row {
        id: i64,
        value: String,
        timestamp: i64,
    }

    let rows: Vec<Row> = mysql
        .query("SELECT id, value, timestamp FROM test_basic WHERE id = 1")
        .await
        .expect("Failed to query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "value_1");
    assert_eq!(rows[0].timestamp, 1001);
}

// ============================================================================
// Scenario 2: Upsert / deduplication
// ============================================================================

#[tokio::test]
async fn test_mysql_upsert() {
    init_tracing();
    let ctx = new_ctx().await;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = vec![
        TestRecord {
            id: 1,
            value: "first".into(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "second".into(),
            timestamp: 200,
        },
        TestRecord {
            id: 1,
            value: "updated".into(),
            timestamp: 300,
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce");

    let pipeline = mysql_pipeline(&ctx.kafka_topic, "test_upsert", "id", "on_conflict: update");
    let mut opts = PipelineOpts::new()
        .record_limit(3)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id") {
        opts = opts.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Pipeline failed");
    assert!(status.success());

    let mysql = ctx.mysql.as_ref().unwrap();
    let count = mysql
        .count("SELECT COUNT(*) FROM test_upsert")
        .await
        .expect("count failed");
    assert_eq!(count, 2, "Should have 2 unique rows after upsert");

    #[derive(sqlx::FromRow, Debug)]
    struct Row {
        value: String,
    }

    let rows: Vec<Row> = mysql
        .query("SELECT value FROM test_upsert WHERE id = 1")
        .await
        .expect("query failed");
    assert_eq!(rows[0].value, "updated", "id=1 should have latest value");
}

// ============================================================================
// Scenario 3: Delete operations
// ============================================================================

#[tokio::test]
async fn test_mysql_delete() {
    init_tracing();
    let ctx = new_ctx().await;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let inserts = vec![
        TestRecord {
            id: 1,
            value: "v1".into(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "v2".into(),
            timestamp: 200,
        },
        TestRecord {
            id: 3,
            value: "v3".into(),
            timestamp: 300,
        },
    ];
    ctx.kafka
        .produce_avro_records(&inserts)
        .await
        .expect("produce inserts");

    ctx.kafka
        .produce_avro_records_with_op(
            &[
                TestRecord {
                    id: 1,
                    value: "".into(),
                    timestamp: 0,
                },
                TestRecord {
                    id: 2,
                    value: "".into(),
                    timestamp: 0,
                },
            ],
            "d",
        )
        .await
        .expect("produce deletes");

    let pipeline = mysql_pipeline(&ctx.kafka_topic, "test_delete", "id", "on_conflict: update");
    let mut opts = PipelineOpts::new()
        .record_limit(5)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id") {
        opts = opts.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Pipeline failed");
    assert!(status.success());

    let mysql = ctx.mysql.as_ref().unwrap();
    let count = mysql
        .count("SELECT COUNT(*) FROM test_delete")
        .await
        .expect("count");
    assert_eq!(count, 1, "Only id=3 should remain");

    #[derive(sqlx::FromRow, Debug)]
    struct Row {
        id: i64,
    }

    let rows: Vec<Row> = mysql.query("SELECT id FROM test_delete").await.expect("q");
    assert_eq!(rows[0].id, 3);
}

// ============================================================================
// Scenario 4: Composite primary key
// ============================================================================

#[tokio::test]
async fn test_mysql_composite_pk() {
    init_tracing();
    let ctx = new_ctx().await;

    ctx.kafka
        .register_schema(COMPOSITE_PK_SCHEMA)
        .await
        .expect("register schema");

    let records = vec![
        CompositePkRecord {
            id: 1,
            version: 1,
            value: "initial".into(),
        },
        CompositePkRecord {
            id: 1,
            version: 2,
            value: "v2".into(),
        },
        CompositePkRecord {
            id: 2,
            version: 1,
            value: "v1".into(),
        },
        CompositePkRecord {
            id: 1,
            version: 1,
            value: "updated".into(),
        },
    ];
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("produce");

    let pipeline = mysql_pipeline(
        &ctx.kafka_topic,
        "test_composite",
        "id,version",
        "on_conflict: update",
    );
    let mut opts = PipelineOpts::new()
        .record_limit(4)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id,version") {
        opts = opts.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Pipeline failed");
    assert!(status.success());

    let mysql = ctx.mysql.as_ref().unwrap();
    let count = mysql
        .count("SELECT COUNT(*) FROM test_composite")
        .await
        .expect("count");
    assert_eq!(count, 3, "Should have 3 unique (id,version) rows");

    #[derive(sqlx::FromRow, Debug)]
    struct Row {
        value: String,
    }

    let rows: Vec<Row> = mysql
        .query("SELECT value FROM test_composite WHERE id = 1 AND version = 1")
        .await
        .expect("q");
    assert_eq!(rows[0].value, "updated");
}

// ============================================================================
// Scenario 5: on_conflict = nothing (INSERT IGNORE)
// ============================================================================

#[tokio::test]
async fn test_mysql_on_conflict_nothing() {
    init_tracing();
    let ctx = new_ctx().await;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("register schema");

    // First batch: insert id=1
    ctx.kafka
        .produce_avro_records(&[TestRecord {
            id: 1,
            value: "original".into(),
            timestamp: 100,
        }])
        .await
        .expect("produce");

    let pipeline = mysql_pipeline(
        &ctx.kafka_topic,
        "test_ignore",
        "id",
        "on_conflict: nothing",
    );
    let mut opts = PipelineOpts::new()
        .record_limit(1)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id") {
        opts = opts.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Pipeline failed");
    assert!(status.success());

    // Produce a duplicate
    ctx.kafka
        .produce_avro_records(&[TestRecord {
            id: 1,
            value: "should_not_update".into(),
            timestamp: 200,
        }])
        .await
        .expect("produce");

    let mut opts2 = PipelineOpts::new()
        .record_limit(1)
        .timeout(Duration::from_secs(60));
    for (k, v) in mysql_env(&ctx, "id") {
        opts2 = opts2.env(k, v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts2)
        .await
        .expect("Pipeline failed");
    assert!(status.success());

    let mysql = ctx.mysql.as_ref().unwrap();

    #[derive(sqlx::FromRow, Debug)]
    struct Row {
        value: String,
    }

    let rows: Vec<Row> = mysql
        .query("SELECT value FROM test_ignore WHERE id = 1")
        .await
        .expect("q");
    assert_eq!(
        rows[0].value, "original",
        "Value should stay 'original' with on_conflict=nothing"
    );
}
