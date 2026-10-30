//! Iceberg source e2e tests: snapshot resume, initially empty tables, and
//! optional-column evolution against the REST catalog fixture.

mod iceberg_common;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_common::IcebergFixture;
use parquet::file::properties::WriterProperties;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use streamling_e2e::{PipelineOpts, PrintSinkOutput, TestContextOptions};

const TABLE: &str = "events";

async fn create_table(fixture: &IcebergFixture) {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    fixture
        .catalog
        .create_table(
            &fixture.namespace,
            TableCreation::builder()
                .name(TABLE.to_string())
                .schema(schema)
                .build(),
        )
        .await
        .expect("create table");
}

/// Commits one fast-append snapshot with one data file per id range.
async fn append(fixture: &IcebergFixture, files: &[RangeInclusive<i64>]) {
    let table = fixture
        .catalog
        .load_table(&fixture.table_ident(TABLE))
        .await
        .expect("load table");
    let schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).expect("arrow schema"));
    let mut data_files = Vec::new();
    for ids in files {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(ids.clone())),
            Arc::new(StringArray::from_iter_values(
                ids.clone().map(|i| format!("name_{i}")),
            )),
        ];
        if schema.field_by_name("category").is_some() {
            columns.push(Arc::new(StringArray::from_iter_values(
                ids.clone().map(|i| format!("category_{i}")),
            )));
        }
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).expect("batch");
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::default(), schema.clone()),
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata()).expect("location generator"),
            DefaultFileNameGenerator::new(
                uuid::Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .expect("data file writer");
        writer.write(batch).await.expect("write");
        data_files.extend(writer.close().await.expect("close writer"));
    }
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .expect("apply fast append");
    tx.commit(&fixture.catalog).await.expect("commit");
}

/// Ids of the printed rows, sorted, after checking each is an insert.
fn inserted_ids(output: &PrintSinkOutput) -> Vec<i64> {
    let mut ids: Vec<i64> = output
        .rows()
        .iter()
        .map(|row| {
            assert_eq!(row.row_kind, "Insert", "row {:?}", row.data);
            assert_eq!(row.data["_gs_op"], "i", "row {:?}", row.data);
            let id = row.data["id"].as_i64().expect("id");
            assert_eq!(row.data["name"], format!("name_{id}"));
            id
        })
        .collect();
    ids.sort_unstable();
    ids
}

const PIPELINE: &str = r#"
sources:
  iceberg_source:
    type: iceberg_source
    batch_size: 3

transforms: {}

sinks:
  print_sink:
    type: print
    from: iceberg_source
    sample_every: 1
"#;

fn source_opts(fixture: &IcebergFixture) -> PipelineOpts {
    let ctx = &fixture.ctx;
    let state_table = format!("iceberg_source_state_{}", ctx.test_id.replace('-', "_"));
    let application_id = format!("iceberg_source_{}", ctx.test_id);
    PipelineOpts::new()
        .env(
            "STREAMLING__PLUGIN__ICEBERG_SOURCE__CATALOG_URI",
            &fixture.catalog_uri,
        )
        .env(
            "STREAMLING__PLUGIN__ICEBERG_SOURCE__TABLE",
            fixture.table_option(TABLE),
        )
        .env(
            "STREAMLING__PLUGIN__ICEBERG_SOURCE__POLL_INTERVAL_SECS",
            "1",
        )
        .env("STREAMLING__APPLICATION_ID", &application_id)
        .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
        // Persist plugin state across runs.
        .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
            &ctx.postgres.host,
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
            ctx.postgres.port.to_string(),
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__USER",
            &ctx.postgres.user,
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD",
            &ctx.postgres.password,
        )
        .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
        .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
            &state_table,
        )
}

async fn run_source(fixture: &IcebergFixture) -> PrintSinkOutput {
    // Catch up, then idle through several checkpoints before SIGTERM.
    let (status, stderr) = fixture
        .ctx
        .run_pipeline_with_sigterm(
            PIPELINE,
            source_opts(fixture),
            Duration::from_secs(15),
            Duration::from_secs(30),
        )
        .await
        .expect("streamling execution failed");
    assert!(status.success(), "streamling should exit cleanly: {stderr}");
    assert!(
        !stderr.contains("iceberg_source: read failed"),
        "source must read without errors: {stderr}"
    );
    PrintSinkOutput::parse(&stderr)
}

/// A restart resumes after both seeded snapshots and reads only the new one.
#[tokio::test]
async fn test_iceberg_source_scan_then_resume_incremental() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    create_table(&fixture).await;
    append(&fixture, &[1..=4]).await;
    append(&fixture, &[5..=6, 7..=8]).await;

    let first = run_source(&fixture).await;
    assert_eq!(inserted_ids(&first), (1..=8).collect::<Vec<_>>());

    append(&fixture, &[9..=11]).await;
    let second = run_source(&fixture).await;
    assert_eq!(
        inserted_ids(&second),
        vec![9, 10, 11],
        "the restart must resume after the first run's rows"
    );
}

#[tokio::test]
async fn test_iceberg_source_empty_table_then_append() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    create_table(&fixture).await;
    let table = fixture
        .catalog
        .load_table(&fixture.table_ident(TABLE))
        .await
        .expect("load empty table");
    assert!(table.metadata().current_snapshot().is_none());

    // Let the default earliest source poll the empty table before its first append.
    let delayed_append = async {
        tokio::time::sleep(Duration::from_secs(3)).await;
        append(&fixture, &[1..=4]).await;
    };
    let (output, ()) = tokio::join!(run_source(&fixture), delayed_append);
    assert_eq!(inserted_ids(&output), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_iceberg_source_optional_column_evolution() {
    let fixture = IcebergFixture::setup(TestContextOptions::new().with_plugin()).await;
    create_table(&fixture).await;
    append(&fixture, &[1..=2]).await;

    let table = fixture
        .catalog
        .load_table(&fixture.table_ident(TABLE))
        .await
        .expect("load table");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "category",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .expect("apply optional-column addition");
    tx.commit(&fixture.catalog).await.expect("commit schema");
    append(&fixture, &[3..=4]).await;

    let output = run_source(&fixture).await;
    assert_eq!(inserted_ids(&output), vec![1, 2, 3, 4]);
    for row in output.rows() {
        let id = row.data["id"].as_i64().expect("id");
        let category = row
            .data
            .get("category")
            .expect("new column must be present");
        if id <= 2 {
            assert!(category.is_null(), "old row must have null: {:?}", row.data);
        } else {
            assert_eq!(category, &format!("category_{id}"));
        }
    }
}
