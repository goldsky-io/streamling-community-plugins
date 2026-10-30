use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, PartitionKey, PartitionSpec, Schema as IcebergSchema,
    SchemaRef as IcebergSchemaRef, Struct, TableProperties, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableCreation};
use iceberg_catalog_rest::RestCatalog;
use parquet::file::properties::{CdcOptions, WriterProperties};
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SinkPlugin};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info};
use uuid::Uuid;

use super::config::{IcebergSinkConfig, PartitionTerm, parse_config};
use super::schema::{BatchProjector, ensure_inserts_only, table_schema_for};
use crate::utils::iceberg::iceberg_error;
use crate::utils::parquet_compression::{
    CompressionError, LEVELED_CODECS, SUPPORTED_CODECS, parquet_compression,
};
use crate::utils::plugin_options::{PluginOptions, configuration_error};

/// Snapshot summary property: the Streamling checkpoint epoch a snapshot
/// commits.
pub const SNAPSHOT_PROP_EPOCH: &str = "streamling.checkpoint-epoch";
/// Snapshot summary property: the id of the sink instance that wrote it.
pub const SNAPSHOT_PROP_WRITER_ID: &str = "streamling.writer-id";

type FileWriterBuilder = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type DataWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Data files open for the current epoch.
enum EpochWriter {
    Unpartitioned(Box<UnpartitionedWriter<DataWriterBuilder>>),
    Partitioned(Box<FanoutWriter<DataWriterBuilder>>),
}

impl EpochWriter {
    async fn close(self) -> iceberg::Result<Vec<DataFile>> {
        match self {
            Self::Unpartitioned(writer) => writer.close().await,
            Self::Partitioned(writer) => writer.close().await,
        }
    }
}

/// How rows map to partitions of the table's default spec.
enum Partitioning {
    /// The default spec is the unpartitioned spec 0 with no fields.
    None,
    /// Every row lands in one all-null partition: the default spec has no
    /// fields but a non-zero id (partitioning removed by evolution), or only
    /// `void` fields. Files must still carry that spec's id and struct length.
    Constant(PartitionKey),
    /// Computes partition keys per row.
    Split(Box<RecordBatchPartitionSplitter>),
}

fn partitioning(spec: PartitionSpec, schema: IcebergSchemaRef) -> iceberg::Result<Partitioning> {
    if spec.spec_id() == 0 && spec.fields().is_empty() {
        Ok(Partitioning::None)
    } else if spec.is_unpartitioned() {
        let data = Struct::from_iter(spec.fields().iter().map(|_| None));
        Ok(Partitioning::Constant(PartitionKey::new(
            spec, schema, data,
        )))
    } else {
        RecordBatchPartitionSplitter::try_new_with_computed_values(schema, Arc::new(spec))
            .map(|splitter| Partitioning::Split(Box::new(splitter)))
    }
}

struct SinkState {
    catalog: RestCatalog,
    /// Latest committed table; the base of the next commit.
    table: Table,
    projector: BatchProjector,
    partitioning: Partitioning,
    /// Shared across epochs so file names (prefix + counter) stay unique.
    file_writer_builder: FileWriterBuilder,
    writer: Option<EpochWriter>,
}

impl SinkState {
    async fn write(&mut self, batch: RecordBatch) -> iceberg::Result<()> {
        let builder = &self.file_writer_builder;
        let writer = self.writer.get_or_insert_with(|| {
            let data_writer = DataFileWriterBuilder::new(builder.clone());
            match self.partitioning {
                Partitioning::None => {
                    EpochWriter::Unpartitioned(Box::new(UnpartitionedWriter::new(data_writer)))
                }
                _ => EpochWriter::Partitioned(Box::new(FanoutWriter::new(data_writer))),
            }
        });
        match (writer, &self.partitioning) {
            (EpochWriter::Partitioned(writer), Partitioning::Split(splitter)) => {
                for (key, part) in splitter.split(&batch)? {
                    writer.write(key, part).await?;
                }
                Ok(())
            }
            (EpochWriter::Partitioned(writer), Partitioning::Constant(key)) => {
                writer.write(key.clone(), batch).await
            }
            (EpochWriter::Unpartitioned(writer), _) => writer.write(batch).await,
            (EpochWriter::Partitioned(_), Partitioning::None) => Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                "partitioned writer for an unpartitioned table",
            )),
        }
    }
}

pub struct IcebergSink {
    config: IcebergSinkConfig,
    input_schema: SchemaRef,
    /// Per-instance id: data file name prefix and snapshot lineage.
    writer_id: Uuid,
    /// `namespace.table`, for logs and errors.
    table_name: String,
    state: OnceCell<Mutex<SinkState>>,
    metrics: PluginMetricsRecorder,
    running: AtomicBool,
}

impl IcebergSink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(options, "iceberg_sink", "STREAMLING__PLUGIN__ICEBERG_SINK");
        let config = parse_config(&opts).map_err(configuration_error)?;
        let table_name = format!(
            "{}.{}",
            config.table.namespace().as_ref().join("."),
            config.table.name()
        );
        Ok(Self {
            config,
            input_schema: schema,
            writer_id: Uuid::now_v7(),
            table_name,
            state: OnceCell::new(),
            metrics,
            running: AtomicBool::new(true),
        })
    }

    fn state(&self) -> Result<&Mutex<SinkState>, PluginError> {
        self.state
            .get()
            .ok_or_else(|| PluginError::Internal("Iceberg sink is not initialized".to_string()))
    }

    async fn load_or_create_table(&self, catalog: &RestCatalog) -> Result<Table, PluginError> {
        let ident = &self.config.table;
        match catalog.load_table(ident).await {
            Ok(table) => {
                if !self.config.partition_by.is_empty() {
                    info!(
                        table = %self.table_name,
                        "Iceberg sink: table exists; partition_by applies only to tables it creates"
                    );
                }
                Ok(table)
            }
            Err(e) if e.kind() == ErrorKind::TableNotFound && self.config.auto_create => {
                self.create_table(catalog).await
            }
            Err(e) if e.kind() == ErrorKind::TableNotFound => Err(PluginError::Internal(format!(
                "iceberg_sink: table '{}' does not exist and auto_create is false",
                self.table_name
            ))),
            Err(e) => Err(iceberg_error(
                &format!("load table '{}'", self.table_name),
                e,
            )),
        }
    }

    async fn create_table(&self, catalog: &RestCatalog) -> Result<Table, PluginError> {
        let ident = &self.config.table;
        ensure_namespace(catalog, ident.namespace()).await?;

        let schema = table_schema_for(&self.input_schema, self.config.include_op_column)?;
        let partition_spec = partition_spec(&schema, &self.config.partition_by)?;
        let mut properties = self.config.table_properties.clone();
        let format_version = match self.config.format_version {
            FormatVersion::V1 => "1",
            _ => "2",
        };
        // The REST create request carries the format version as a property.
        properties.insert("format-version".to_string(), format_version.to_string());
        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema)
            .partition_spec(partition_spec)
            .properties(properties)
            .format_version(self.config.format_version)
            .build();
        match catalog.create_table(ident.namespace(), creation).await {
            Ok(table) => {
                info!(
                    table = %self.table_name,
                    location = table.metadata().location(),
                    format_version,
                    partition_spec = ?table.metadata().default_partition_spec().fields(),
                    "Iceberg sink created table"
                );
                Ok(table)
            }
            // Lost a creation race with another writer: use its table.
            Err(e) if e.kind() == ErrorKind::TableAlreadyExists => catalog
                .load_table(ident)
                .await
                .map_err(|e| iceberg_error(&format!("load table '{}'", self.table_name), e)),
            Err(e) => Err(iceberg_error(
                &format!("create table '{}'", self.table_name),
                e,
            )),
        }
    }

    fn build_state(&self, catalog: RestCatalog, table: Table) -> Result<SinkState, PluginError> {
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let target = Arc::new(
            schema_to_arrow_schema(&schema)
                .map_err(|e| iceberg_error("convert the table schema to Arrow", e))?,
        );
        let projector =
            BatchProjector::try_new(&self.input_schema, target, self.config.include_op_column)
                .map_err(|e| PluginError::Internal(format!("table '{}': {e}", self.table_name)))?;

        let partitioning = partitioning(
            metadata.default_partition_spec().as_ref().clone(),
            schema.clone(),
        )
        .map_err(|e| iceberg_error("build the partition splitter", e))?;

        let table_props = metadata
            .table_properties()
            .map_err(|e| iceberg_error("read table properties", e))?;
        let parquet = ParquetWriterBuilder::new(
            parquet_writer_properties(metadata.properties(), &table_props)?,
            schema,
        );
        let file_writer_builder = RollingFileWriterBuilder::new(
            parquet,
            table_props.write_target_file_size_bytes,
            table.file_io().clone(),
            DefaultLocationGenerator::new(metadata)
                .map_err(|e| iceberg_error("build the data file location generator", e))?,
            DefaultFileNameGenerator::new(
                self.writer_id.to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        Ok(SinkState {
            catalog,
            table,
            projector,
            partitioning,
            file_writer_builder,
            writer: None,
        })
    }

    async fn process_batch_inner(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if !self.config.include_op_column {
            ensure_inserts_only(&batch)?;
        }
        let mut state = self.state()?.lock().await;
        let batch = state.projector.project(&batch)?;
        let rows = batch.num_rows();
        state
            .write(batch)
            .await
            .map_err(|e| iceberg_error("write data files", e))?;
        self.metrics
            .record_count("iceberg_sink.rows_written", rows as u64);
        debug!(table = %self.table_name, rows, "Iceberg sink wrote batch");
        Ok(())
    }

    async fn commit_epoch(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let mut state = self.state()?.lock().await;
        let Some(writer) = state.writer.take() else {
            debug!(table = %self.table_name, epoch = epoch.0, "Iceberg sink: nothing to commit");
            return Ok(());
        };
        let data_files = writer
            .close()
            .await
            .map_err(|e| iceberg_error("close data files", e))?;
        if data_files.is_empty() {
            return Ok(());
        }
        let files = data_files.len();
        let rows: u64 = data_files.iter().map(DataFile::record_count).sum();
        let bytes: u64 = data_files.iter().map(DataFile::file_size_in_bytes).sum();

        let started = Instant::now();
        let tx = Transaction::new(&state.table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(HashMap::from([
                (SNAPSHOT_PROP_EPOCH.to_string(), epoch.0.to_string()),
                (
                    SNAPSHOT_PROP_WRITER_ID.to_string(),
                    self.writer_id.to_string(),
                ),
            ]))
            .apply(tx)
            .map_err(|e| iceberg_error("build the append", e))?;
        // `commit` retries conflicts per the table's commit.retry.* settings,
        // re-applying the append onto the refreshed table.
        let table = tx.commit(&state.catalog).await.map_err(|e| {
            iceberg_error(
                &format!(
                    "commit {files} data file(s) for checkpoint epoch {} to '{}'",
                    epoch.0, self.table_name
                ),
                e,
            )
        })?;
        let latency = started.elapsed();
        let snapshot_id = table.metadata().current_snapshot_id();
        state.table = table;

        self.metrics
            .record_count("iceberg_sink.rows_committed", rows);
        self.metrics
            .record_count("iceberg_sink.files_committed", files as u64);
        self.metrics
            .record_count("iceberg_sink.bytes_committed", bytes);
        self.metrics
            .record_latency("iceberg_sink.commit_latency", latency);
        info!(
            table = %self.table_name,
            epoch = epoch.0,
            snapshot_id,
            files,
            rows,
            bytes,
            latency_ms = latency.as_millis() as u64,
            "Iceberg sink committed snapshot"
        );
        Ok(())
    }
}

/// Creates the namespace (and its parents) if missing. Tolerates concurrent
/// creation by another writer.
async fn ensure_namespace(
    catalog: &RestCatalog,
    namespace: &NamespaceIdent,
) -> Result<(), PluginError> {
    let parts = namespace.as_ref();
    for depth in 1..=parts.len() {
        let ns = NamespaceIdent::from_strs(&parts[..depth])
            .map_err(|e| iceberg_error("build namespace", e))?;
        let exists = catalog
            .namespace_exists(&ns)
            .await
            .map_err(|e| iceberg_error(&format!("check namespace '{ns}'"), e))?;
        if exists {
            continue;
        }
        match catalog.create_namespace(&ns, HashMap::new()).await {
            Ok(_) => info!(namespace = %ns, "Iceberg sink created namespace"),
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => {}
            Err(e) => return Err(iceberg_error(&format!("create namespace '{ns}'"), e)),
        }
    }
    Ok(())
}

/// Binds `partition_by` terms to the schema of a table being created.
pub(crate) fn partition_spec(
    schema: &IcebergSchema,
    terms: &[PartitionTerm],
) -> Result<UnboundPartitionSpec, PluginError> {
    // REST catalogs require a spec id in the create request.
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for term in terms {
        let field = schema.field_by_name(&term.column).ok_or_else(|| {
            PluginError::Internal(format!(
                "iceberg_sink: partition_by: column '{}' is not a written input column",
                term.column
            ))
        })?;
        term.transform.result_type(&field.field_type).map_err(|_| {
            PluginError::Internal(format!(
                "iceberg_sink: partition_by: transform {} cannot apply to column '{}' of \
                     type {}",
                term.transform, term.column, field.field_type
            ))
        })?;
        builder = builder
            .add_partition_field(field.id, term.field_name(), term.transform)
            .map_err(|e| {
                PluginError::Internal(format!("iceberg_sink: partition_by: {}", e.message()))
            })?;
    }
    Ok(builder.build())
}

const MIB: usize = 1024 * 1024;

/// Translates the table's `write.parquet.*` properties into Parquet writer
/// properties, with Iceberg's defaults (iceberg-rust 0.10 only maps the
/// content-defined-chunking keys itself).
fn parquet_writer_properties(
    props: &HashMap<String, String>,
    table_props: &TableProperties,
) -> Result<WriterProperties, PluginError> {
    let invalid = |key: &str, value: &str, reason: &dyn std::fmt::Display| {
        PluginError::Internal(format!(
            "iceberg_sink: invalid table property {key}='{value}': {reason}"
        ))
    };
    let size = |key: &str, default: usize| -> Result<usize, PluginError> {
        match props.get(key) {
            None => Ok(default),
            Some(v) => match v.trim().parse::<usize>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(invalid(key, v, &"expected a positive integer")),
            },
        }
    };

    const CODEC: &str = "write.parquet.compression-codec";
    const LEVEL: &str = "write.parquet.compression-level";
    let codec = props.get(CODEC).map_or("zstd", String::as_str);
    let level = props
        .get(LEVEL)
        .map(|v| v.trim().parse::<u32>().map_err(|e| invalid(LEVEL, v, &e)))
        .transpose()?;
    let compression = parquet_compression(codec, level).map_err(|e| match e {
        CompressionError::UnknownCodec => {
            invalid(CODEC, codec, &format_args!("expected {SUPPORTED_CODECS}"))
        }
        CompressionError::InvalidLevel(e) => {
            invalid(LEVEL, &level.map(|l| l.to_string()).unwrap_or_default(), &e)
        }
        CompressionError::LevelNotSupported => invalid(
            LEVEL,
            &level.map(|l| l.to_string()).unwrap_or_default(),
            &format_args!("not supported for {codec}: only {LEVELED_CODECS} take a level"),
        ),
    })?;
    let cdc = table_props.cdc_enabled.then_some(CdcOptions {
        min_chunk_size: table_props.cdc_min_chunk_size,
        max_chunk_size: table_props.cdc_max_chunk_size,
        norm_level: table_props.cdc_norm_level,
    });
    Ok(WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_bytes(Some(size("write.parquet.row-group-size-bytes", 128 * MIB)?))
        .set_data_page_size_limit(size("write.parquet.page-size-bytes", MIB)?)
        .set_dictionary_page_size_limit(size("write.parquet.dict-size-bytes", 2 * MIB)?)
        .set_content_defined_chunking(cdc)
        .build())
}

#[async_trait]
impl SupportsGracefulShutdown for IcebergSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Durability happens on checkpoint markers; data written since the last
    /// marker is uncommitted and is replayed after a restart.
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        info!(table = %self.table_name, "Iceberg sink terminated");
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for IcebergSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.state
            .get_or_try_init(|| async {
                let catalog = self.config.catalog.connect().await?;
                let table = self.load_or_create_table(&catalog).await?;
                let state = self.build_state(catalog, table)?;
                info!(
                    table = %self.table_name,
                    writer_id = %self.writer_id,
                    partitioned = !matches!(state.partitioning, Partitioning::None),
                    include_op_column = self.config.include_op_column,
                    "Iceberg sink initialized"
                );
                Ok::<_, PluginError>(Mutex::new(state))
            })
            .await?;
        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if !self.is_running() {
            return Err(PluginError::Internal(
                "Iceberg sink is not running, cannot process batch".to_string(),
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.process_batch_inner(batch).await
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.commit_epoch(epoch).await
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::iceberg::config::parse_partition_by;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use iceberg::spec::Transform;
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};

    fn input_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("_gs_op", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ])
    }

    #[test]
    fn binds_partition_terms_to_the_table_schema() {
        let schema = table_schema_for(&input_schema(), false).unwrap();
        let terms = parse_partition_by("day(ts), bucket(8, id), name").unwrap();
        let spec = partition_spec(&schema, &terms).unwrap();
        let fields: Vec<(i32, &str, Transform)> = spec
            .fields()
            .iter()
            .map(|f| (f.source_id, f.name.as_str(), f.transform))
            .collect();
        let id = |name| schema.field_by_name(name).unwrap().id;
        assert_eq!(
            fields,
            vec![
                (id("ts"), "ts_day", Transform::Day),
                (id("id"), "id_bucket", Transform::Bucket(8)),
                (id("name"), "name", Transform::Identity),
            ]
        );

        for (invalid, needle) in [
            ("day(name)", "name"),
            ("missing", "missing"),
            ("_gs_op", "_gs_op"),
            ("day(ts), day(ts)", "ts"),
        ] {
            let err = partition_spec(&schema, &parse_partition_by(invalid).unwrap()).unwrap_err();
            assert!(err.to_string().contains(needle), "for {invalid}: got {err}");
        }
    }

    #[test]
    fn evolved_unpartitioned_specs_keep_their_id_and_arity() {
        let schema = Arc::new(table_schema_for(&input_schema(), false).unwrap());
        let spec = |spec_id, void: bool| {
            let mut builder = PartitionSpec::builder(schema.clone()).with_spec_id(spec_id);
            if void {
                builder = builder
                    .add_partition_field("id", "id_void", Transform::Void)
                    .unwrap();
            }
            builder.build().unwrap()
        };
        assert!(matches!(
            partitioning(spec(0, false), schema.clone()).unwrap(),
            Partitioning::None
        ));
        // Partitioning dropped by evolution: empty spec with a new id.
        let Partitioning::Constant(key) = partitioning(spec(1, false), schema.clone()).unwrap()
        else {
            panic!("empty spec 1 must use a constant partition key");
        };
        assert_eq!((key.spec().spec_id(), key.data().fields().len()), (1, 0));
        // v1-style all-void spec: one null partition value per field.
        let Partitioning::Constant(key) = partitioning(spec(0, true), schema.clone()).unwrap()
        else {
            panic!("all-void spec must use a constant partition key");
        };
        assert_eq!(key.spec().spec_id(), 0);
        assert_eq!(key.data().fields(), &[None]);
    }

    #[test]
    fn maps_parquet_table_properties() {
        let defaults = TableProperties::try_from(&HashMap::new()).unwrap();
        let props = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let wp = parquet_writer_properties(&props(&[]), &defaults).unwrap();
        let col = parquet::schema::types::ColumnPath::from("c");
        assert_eq!(
            wp.compression(&col),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert_eq!(wp.max_row_group_bytes(), Some(128 * MIB));

        let wp = parquet_writer_properties(
            &props(&[
                ("write.parquet.compression-codec", "GZIP"),
                ("write.parquet.compression-level", "9"),
                ("write.parquet.row-group-size-bytes", "1024"),
            ]),
            &defaults,
        )
        .unwrap();
        assert_eq!(
            wp.compression(&col),
            Compression::GZIP(GzipLevel::try_new(9).unwrap())
        );
        assert_eq!(wp.max_row_group_bytes(), Some(1024));

        for (key, value) in [
            ("write.parquet.compression-codec", "lzma"),
            ("write.parquet.compression-level", "high"),
            ("write.parquet.row-group-size-bytes", "0"),
        ] {
            let err = parquet_writer_properties(&props(&[(key, value)]), &defaults).unwrap_err();
            assert!(err.to_string().contains(key), "for {key}: got {err}");
        }
    }
}
