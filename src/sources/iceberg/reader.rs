//! Pull-based table reader: turns snapshots into units of data files, reads
//! them in a deterministic order, and tracks a resumable [`Position`].
//!
//! A unit is either the full scan of the start snapshot ([`Phase::Scan`]) or
//! the data files one `append` snapshot added ([`Phase::Incremental`]). Files
//! of a unit are sorted by path, so `files_done` + `rows_done_in_file`
//! identify the next row exactly, and resuming re-plans the unit and skips
//! that prefix. Rows are counted after delete files are applied; the scan
//! of a fixed snapshot applies the same deletes every time, and files added
//! by an append have none.

use crate::sources::iceberg::config::OnNonAppendSnapshot;
use crate::sources::iceberg::snapshots::{SnapshotAction, classify, expired, snapshots_since};
use crate::utils::iceberg::iceberg_error;
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::compute::{cast, concat};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReader, schema_to_arrow_schema};
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask};
use iceberg::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, DataContentType, ManifestContentType, ManifestStatus, NameMapping,
    SchemaRef as IcebergSchemaRef, SnapshotRef, TableMetadata,
};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use streamling_plugin::PluginError;
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP;
use streamling_plugin::ffi::PluginMetricsRecorder;
use tracing::{debug, info};

/// Which kind of unit a [`Position`] refers to.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Every live data file of the start snapshot (deletes applied).
    Scan,
    /// The data files an `append` snapshot added.
    Incremental,
}

/// Checkpointed read progress.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub snapshot_id: i64,
    pub phase: Phase,
    /// Files of the unit (in path order) fully emitted.
    pub files_done: u64,
    /// Rows of the next file already emitted.
    pub rows_done_in_file: u64,
    /// The whole unit was emitted (or skipped); the next unit is the next
    /// snapshot after `snapshot_id`.
    pub complete: bool,
}

impl Position {
    pub fn start(snapshot_id: i64, phase: Phase) -> Self {
        Self {
            snapshot_id,
            phase,
            files_done: 0,
            rows_done_in_file: 0,
            complete: false,
        }
    }

    pub fn completed(snapshot_id: i64, phase: Phase) -> Self {
        Self {
            complete: true,
            ..Self::start(snapshot_id, phase)
        }
    }
}

/// The schema the source reads with, fixed at startup: the table's then
/// current schema, and the output Arrow schema derived from it.
#[derive(Debug, Clone)]
pub struct ReadSchema {
    iceberg: IcebergSchemaRef,
    field_ids: Vec<i32>,
    /// Data columns, without `_gs_op`.
    data: SchemaRef,
    output: SchemaRef,
}

impl ReadSchema {
    pub fn new(schema: IcebergSchemaRef) -> Result<Self, PluginError> {
        let arrow = schema_to_arrow_schema(&schema)
            .map_err(|e| iceberg_error("convert the table schema to Arrow", e))?;
        // Top-level `PARQUET:field_id` metadata is an Iceberg storage detail;
        // nested fields keep theirs, it is part of their data type.
        let data_fields: Vec<Field> = arrow
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_metadata(HashMap::new()))
            .collect();
        if data_fields
            .iter()
            .any(|f| f.name() == STREAMLING_COLUMN_NAME_OP)
        {
            return Err(PluginError::Internal(format!(
                "iceberg_source: table column '{STREAMLING_COLUMN_NAME_OP}' collides with the \
                 reserved streamling op column"
            )));
        }
        let mut output_fields = Vec::with_capacity(data_fields.len() + 1);
        output_fields.push(Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false));
        output_fields.extend(data_fields.iter().cloned());
        Ok(Self {
            field_ids: schema.as_struct().fields().iter().map(|f| f.id).collect(),
            iceberg: schema,
            data: Arc::new(Schema::new(data_fields)),
            output: Arc::new(Schema::new(output_fields)),
        })
    }

    pub fn output(&self) -> SchemaRef {
        self.output.clone()
    }

    /// Converts a batch from the Iceberg reader to the data columns. The
    /// reader projects by field id in schema order; types can still differ
    /// (e.g. run-end encoded partition constants), so mismatches are cast.
    fn data_columns(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, PluginError> {
        if batch.num_columns() != self.data.fields().len() {
            return Err(PluginError::Execution(format!(
                "iceberg_source: reader returned {} columns, expected {}",
                batch.num_columns(),
                self.data.fields().len()
            )));
        }
        batch
            .columns()
            .iter()
            .zip(self.data.fields())
            .map(|(column, field)| {
                if column.data_type() == field.data_type() {
                    Ok(column.clone())
                } else {
                    cast(column, field.data_type()).map_err(|e| {
                        PluginError::Execution(format!(
                            "iceberg_source: cannot convert column '{}' from {} to {}: {e}",
                            field.name(),
                            column.data_type(),
                            field.data_type()
                        ))
                    })
                }
            })
            .collect()
    }

    /// Builds one output batch (`_gs_op = 'i'` first) from reader slices.
    fn output_batch(&self, slices: &[RecordBatch]) -> Result<RecordBatch, PluginError> {
        let rows: usize = slices.iter().map(RecordBatch::num_rows).sum();
        let per_slice = slices
            .iter()
            .map(|s| self.data_columns(s))
            .collect::<Result<Vec<_>, _>>()?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output.fields().len());
        columns.push(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n("i", rows),
        )));
        for i in 0..self.data.fields().len() {
            let parts: Vec<&dyn arrow::array::Array> =
                per_slice.iter().map(|cols| cols[i].as_ref()).collect();
            columns.push(if parts.len() == 1 {
                per_slice[0][i].clone()
            } else {
                concat(&parts).map_err(arrow_error)?
            });
        }
        RecordBatch::try_new(self.output.clone(), columns).map_err(arrow_error)
    }
}

fn arrow_error(e: arrow::error::ArrowError) -> PluginError {
    PluginError::Execution(format!("iceberg_source: building output batch: {e}"))
}

/// One snapshot's files being read.
struct Unit {
    snapshot_id: i64,
    timestamp_ms: i64,
    phase: Phase,
    /// Sorted by data file path.
    files: Vec<FileScanTask>,
    /// Index of the file being read; `files.len()` once the unit is done.
    file: usize,
    /// Rows of `files[file]` consumed so far (emitted, or skipped on resume).
    rows_in_file: u64,
    /// Rows still to drop from the start of `files[file]` when resuming.
    skip: u64,
    stream: Option<ArrowRecordBatchStream>,
    /// Owned per unit: its delete-file cache is left waiting forever on an
    /// entry whose load failed, so a re-opened unit must not reuse it.
    arrow_reader: ArrowReader,
    /// Rows read from the current file but not emitted yet.
    leftover: Option<RecordBatch>,
}

impl Unit {
    fn is_done(&self) -> bool {
        self.file >= self.files.len()
    }

    fn position(&self) -> Position {
        if self.is_done() {
            return Position::completed(self.snapshot_id, self.phase);
        }
        Position {
            snapshot_id: self.snapshot_id,
            phase: self.phase,
            files_done: self.file as u64,
            rows_done_in_file: self.rows_in_file,
            complete: false,
        }
    }

    /// Reads up to `max` rows, crossing file boundaries within the unit.
    async fn read_up_to(
        &mut self,
        max: usize,
        metrics: &PluginMetricsRecorder,
    ) -> Result<Vec<RecordBatch>, PluginError> {
        let mut out = Vec::new();
        let mut rows = 0;
        while rows < max && !self.is_done() {
            let batch = match self.leftover.take() {
                Some(batch) => batch,
                None => match self.next_from_file().await? {
                    Some(batch) => batch,
                    None => {
                        metrics.record_count("iceberg_source.files_read", 1);
                        self.stream = None;
                        self.file += 1;
                        self.rows_in_file = 0;
                        continue;
                    }
                },
            };
            let take = batch.num_rows().min(max - rows);
            if take < batch.num_rows() {
                self.leftover = Some(batch.slice(take, batch.num_rows() - take));
            }
            if take > 0 {
                out.push(batch.slice(0, take));
                rows += take;
                self.rows_in_file += take as u64;
            }
        }
        Ok(out)
    }

    /// The next non-empty batch of the current file after the resume skip,
    /// or `None` at its end.
    async fn next_from_file(&mut self) -> Result<Option<RecordBatch>, PluginError> {
        let task = &self.files[self.file];
        let path = task.data_file_path.clone();
        if self.stream.is_none() {
            let tasks = futures::stream::iter([Ok(task.clone())]).boxed();
            let scan = self
                .arrow_reader
                .clone()
                .read(tasks)
                .map_err(|e| iceberg_error(&format!("read data file {path}"), e))?;
            self.stream = Some(scan.stream());
        }
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        loop {
            let Some(mut batch) = stream
                .try_next()
                .await
                .map_err(|e| iceberg_error(&format!("read data file {path}"), e))?
            else {
                if self.skip > 0 {
                    return Err(PluginError::Execution(format!(
                        "iceberg_source: data file {path} of snapshot {} has fewer rows than \
                         the checkpointed position; reset the source state",
                        self.snapshot_id
                    )));
                }
                return Ok(None);
            };
            if self.skip > 0 {
                let drop = batch.num_rows().min(self.skip as usize);
                self.skip -= drop as u64;
                batch = batch.slice(drop, batch.num_rows() - drop);
            }
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
    }
}

/// Reads a table snapshot by snapshot from a [`Position`]. Holds the most
/// recently loaded [`Table`]; the caller refreshes it via [`Self::set_table`].
pub struct TableReader {
    table: Table,
    schema: ReadSchema,
    batch_size: usize,
    policy: OnNonAppendSnapshot,
    /// Position after the last row returned by [`Self::next_batch`].
    cursor: Option<Position>,
    unit: Option<Unit>,
    /// Commit time of the last fully processed snapshot.
    last_processed_ms: Option<i64>,
    metrics: PluginMetricsRecorder,
}

impl TableReader {
    pub fn new(
        table: Table,
        schema: ReadSchema,
        batch_size: usize,
        policy: OnNonAppendSnapshot,
        cursor: Option<Position>,
        metrics: PluginMetricsRecorder,
    ) -> Self {
        // A completed cursor's snapshot is the last processed one.
        let last_processed_ms = cursor
            .as_ref()
            .filter(|p| p.complete)
            .and_then(|p| table.metadata().snapshot_by_id(p.snapshot_id))
            .map(|s| s.timestamp_ms());
        Self {
            table,
            schema,
            batch_size,
            policy,
            cursor,
            unit: None,
            last_processed_ms,
            metrics,
        }
    }

    /// Replaces the table with a freshly loaded one (new snapshots, possibly
    /// refreshed storage credentials).
    pub fn set_table(&mut self, table: Table) {
        // The in-flight unit reads its remaining files with the new FileIO.
        if let Some(unit) = self.unit.as_mut() {
            unit.arrow_reader = build_arrow_reader(&table, self.batch_size);
        }
        self.table = table;
    }

    pub fn cursor(&self) -> Option<&Position> {
        self.cursor.as_ref()
    }

    pub fn last_processed_ms(&self) -> Option<i64> {
        self.last_processed_ms
    }

    /// The next batch of at most `batch_size` rows, or `None` once caught up
    /// with the loaded table metadata. The cursor advances past the returned
    /// rows. Cancel- and error-safe: the in-flight unit is taken out while
    /// reading, so after an error or a dropped future the next call re-opens
    /// it from the cursor and returns the same rows again.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PluginError> {
        loop {
            let mut unit = match self.unit.take() {
                Some(unit) => unit,
                None => match self.open_next_unit().await? {
                    Some(unit) => unit,
                    None => return Ok(None),
                },
            };
            let slices = unit.read_up_to(self.batch_size, &self.metrics).await?;
            let batch = if slices.is_empty() {
                None
            } else {
                Some(self.schema.output_batch(&slices)?)
            };
            self.cursor = Some(unit.position());
            if unit.is_done() {
                self.finish_snapshot(unit.snapshot_id, unit.timestamp_ms, "read");
                self.metrics
                    .record_count("iceberg_source.snapshots_processed", 1);
            } else {
                self.unit = Some(unit);
            }
            if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }

    fn finish_snapshot(&mut self, snapshot_id: i64, timestamp_ms: i64, outcome: &str) {
        self.last_processed_ms = Some(timestamp_ms);
        info!(
            table = %self.table.identifier(),
            snapshot_id,
            outcome,
            "iceberg_source finished snapshot"
        );
    }

    /// Re-opens the cursor's unfinished unit, or opens the next snapshot to
    /// read, passing over skipped ones.
    async fn open_next_unit(&mut self) -> Result<Option<Unit>, PluginError> {
        if let Some(position) = self.cursor.clone()
            && !position.complete
        {
            return open_unit(&self.table, &self.schema, &position, self.batch_size)
                .await
                .map(Some);
        }
        let last = self.cursor.as_ref().map(|p| p.snapshot_id);
        for snapshot in snapshots_since(self.table.metadata(), last)? {
            let id = snapshot.snapshot_id();
            match classify(&snapshot, self.policy)? {
                SnapshotAction::Read => {
                    return open_unit(
                        &self.table,
                        &self.schema,
                        &Position::start(id, Phase::Incremental),
                        self.batch_size,
                    )
                    .await
                    .map(Some);
                }
                SnapshotAction::Skip => {
                    self.cursor = Some(Position::completed(id, Phase::Incremental));
                    let outcome = snapshot.summary().operation.as_str().to_string();
                    self.finish_snapshot(
                        id,
                        snapshot.timestamp_ms(),
                        &format!("skipped {outcome}"),
                    );
                    self.metrics
                        .record_count("iceberg_source.snapshots_skipped", 1);
                }
            }
        }
        Ok(None)
    }
}

// Free functions rather than `TableReader` methods: the reader holds a
// non-`Sync` record batch stream, so borrowing it across awaits would make
// `generate_batch`'s future non-`Send`.
async fn open_unit(
    table: &Table,
    schema: &ReadSchema,
    position: &Position,
    batch_size: usize,
) -> Result<Unit, PluginError> {
    let id = position.snapshot_id;
    let snapshot = table
        .metadata()
        .snapshot_by_id(id)
        .ok_or_else(|| expired(id))?
        .clone();
    let mut files = match position.phase {
        Phase::Scan => scan_files(table, schema, id).await?,
        Phase::Incremental => added_files(table, schema, &snapshot).await?,
    };
    files.sort_by(|a, b| a.data_file_path.cmp(&b.data_file_path));
    if position.files_done > files.len() as u64 {
        return Err(PluginError::Execution(format!(
            "iceberg_source: checkpointed position has {} files done but snapshot {id} has \
                 only {} to read; reset the source state",
            position.files_done,
            files.len()
        )));
    }
    debug!(
        snapshot_id = id,
        phase = ?position.phase,
        files = files.len(),
        files_done = position.files_done,
        rows_done_in_file = position.rows_done_in_file,
        "iceberg_source opened snapshot"
    );
    Ok(Unit {
        snapshot_id: id,
        timestamp_ms: snapshot.timestamp_ms(),
        phase: position.phase,
        files,
        file: position.files_done as usize,
        rows_in_file: position.rows_done_in_file,
        skip: position.rows_done_in_file,
        stream: None,
        leftover: None,
        arrow_reader: build_arrow_reader(table, batch_size),
    })
}

/// Every live data file of `snapshot_id`, deletes attached.
async fn scan_files(
    table: &Table,
    schema: &ReadSchema,
    snapshot_id: i64,
) -> Result<Vec<FileScanTask>, PluginError> {
    let context = format!("plan the scan of snapshot {snapshot_id}");
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .select_all()
        .build()
        .map_err(|e| iceberg_error(&context, e))?;
    let tasks: Vec<FileScanTask> = scan
        .plan_files()
        .await
        .map_err(|e| iceberg_error(&context, e))?
        .try_collect()
        .await
        .map_err(|e| iceberg_error(&context, e))?;
    // Read every snapshot with the fixed startup schema; older files
    // resolve by field id, so columns they lack read as null.
    Ok(tasks
        .into_iter()
        .map(|mut task| {
            task.schema = schema.iceberg.clone();
            task.project_field_ids = schema.field_ids.clone();
            task
        })
        .collect())
}

/// The data files `snapshot` added: entries with status ADDED and that
/// snapshot id, found in the manifests that snapshot wrote.
async fn added_files(
    table: &Table,
    schema: &ReadSchema,
    snapshot: &SnapshotRef,
) -> Result<Vec<FileScanTask>, PluginError> {
    let id = snapshot.snapshot_id();
    let context = format!("list the files added by snapshot {id}");
    let manifest_list = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .map_err(|e| iceberg_error(&context, e))?;
    let name_mapping = name_mapping(table.metadata())?;
    let mut tasks = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != id
            || !manifest_file.has_added_files()
        {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| iceberg_error(&context, e))?;
        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Added
                || entry.snapshot_id() != Some(id)
                || entry.content_type() != DataContentType::Data
            {
                continue;
            }
            tasks.push(
                FileScanTask::builder()
                    .with_file_size_in_bytes(entry.file_size_in_bytes())
                    .with_start(0)
                    .with_length(entry.file_size_in_bytes())
                    .with_record_count(Some(entry.record_count()))
                    .with_data_file_path(entry.file_path().to_string())
                    .with_data_file_format(entry.file_format())
                    .with_schema(schema.iceberg.clone())
                    .with_project_field_ids(schema.field_ids.clone())
                    .with_name_mapping(name_mapping.clone())
                    .with_case_sensitive(true)
                    .build(),
            );
        }
    }
    Ok(tasks)
}

fn build_arrow_reader(table: &Table, batch_size: usize) -> ArrowReader {
    table
        .reader_builder()
        .with_batch_size(batch_size)
        .with_data_file_concurrency_limit(1)
        .build()
}

/// The table's `schema.name-mapping.default`, for data files written without
/// field ids (e.g. imported Parquet).
fn name_mapping(metadata: &TableMetadata) -> Result<Option<Arc<NameMapping>>, PluginError> {
    metadata
        .properties()
        .get(DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|raw| {
            serde_json::from_str::<NameMapping>(raw)
                .map(Arc::new)
                .map_err(|e| {
                    PluginError::Execution(format!(
                        "iceberg_source: invalid table property {DEFAULT_SCHEMA_NAME_MAPPING}: {e}"
                    ))
                })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_support;
    use arrow::array::{Array, Int64Array};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{
        Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, TableCreation, TableIdent,
    };
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use parquet::file::properties::WriterProperties;
    use std::ops::RangeInclusive;
    use std::path::PathBuf;

    /// A MemoryCatalog table `lake.events (id long, name string)` in a
    /// `file://` temp warehouse.
    struct Fixture {
        catalog: MemoryCatalog,
        ident: TableIdent,
        dir: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    impl Fixture {
        async fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("iceberg-source-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        format!("file://{}", dir.display()),
                    )]),
                )
                .await
                .unwrap();
            let namespace = NamespaceIdent::new("lake".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .unwrap();
            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap();
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("events".to_string())
                        .schema(schema)
                        .build(),
                )
                .await
                .unwrap();
            Self {
                catalog,
                ident: TableIdent::new(namespace, "events".to_string()),
                dir,
            }
        }

        async fn table(&self) -> Table {
            self.catalog.load_table(&self.ident).await.unwrap()
        }

        /// Commits one fast-append snapshot with one data file per id range.
        async fn append(&self, files: &[RangeInclusive<i64>]) -> i64 {
            let table = self.table().await;
            let schema = table.metadata().current_schema().clone();
            let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
            let mut data_files = Vec::new();
            for ids in files {
                let batch = RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(ids.clone())),
                        Arc::new(StringArray::from_iter_values(
                            ids.clone().map(|i| format!("n{i}")),
                        )),
                    ],
                )
                .unwrap();
                let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                    ParquetWriterBuilder::new(WriterProperties::default(), schema.clone()),
                    table.file_io().clone(),
                    DefaultLocationGenerator::new(table.metadata()).unwrap(),
                    DefaultFileNameGenerator::new(
                        uuid::Uuid::new_v4().to_string(),
                        None,
                        DataFileFormat::Parquet,
                    ),
                );
                let mut writer = DataFileWriterBuilder::new(rolling)
                    .build(None)
                    .await
                    .unwrap();
                writer.write(batch).await.unwrap();
                data_files.extend(writer.close().await.unwrap());
            }
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(data_files)
                .apply(tx)
                .unwrap();
            let table = tx.commit(&self.catalog).await.unwrap();
            table.metadata().current_snapshot_id().unwrap()
        }

        async fn reader(&self, cursor: Option<Position>, batch_size: usize) -> TableReader {
            let table = self.table().await;
            let schema = ReadSchema::new(table.metadata().current_schema().clone()).unwrap();
            TableReader::new(
                table,
                schema,
                batch_size,
                OnNonAppendSnapshot::Error,
                cursor,
                test_support::metrics_recorder(),
            )
        }
    }

    fn ids(batch: &RecordBatch) -> Vec<i64> {
        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(ops.iter().all(|op| op == Some("i")));
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.values().to_vec()
    }

    /// Drains the reader: ids of every batch, and the cursor after each.
    async fn drain(reader: &mut TableReader) -> Vec<(Vec<i64>, Position)> {
        let mut out = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            assert_eq!(batch.schema(), reader.schema.output());
            out.push((ids(&batch), reader.cursor().cloned().unwrap()));
        }
        out
    }

    fn sorted(mut ids: Vec<i64>) -> Vec<i64> {
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn scans_then_reads_incrementally_and_resumes_exactly() {
        let fx = Fixture::new().await;
        fx.append(&[0..=4]).await;
        let start = fx.append(&[5..=8, 9..=11]).await;

        let mut reader = fx
            .reader(Some(Position::start(start, Phase::Scan)), 4)
            .await;
        let batches = drain(&mut reader).await;
        assert!(batches.iter().all(|(ids, _)| ids.len() <= 4));
        let all: Vec<i64> = batches.iter().flat_map(|(ids, _)| ids.clone()).collect();
        assert_eq!(sorted(all.clone()), (0..12).collect::<Vec<_>>());
        assert_eq!(
            reader.cursor(),
            Some(&Position::completed(start, Phase::Scan))
        );

        // Resuming from the cursor after any batch yields exactly the rest,
        // across file boundaries and mid-file.
        let mut emitted = 0;
        for (ids, cursor) in &batches {
            emitted += ids.len();
            let mut resumed = fx.reader(Some(cursor.clone()), 4).await;
            let rest: Vec<i64> = drain(&mut resumed)
                .await
                .into_iter()
                .flat_map(|(ids, _)| ids)
                .collect();
            assert_eq!(rest, all[emitted..], "resume from {cursor:?}");
        }

        // A new append is read incrementally after a refresh: only its rows.
        assert!(reader.next_batch().await.unwrap().is_none());
        let third = fx.append(&[12..=14]).await;
        reader.set_table(fx.table().await);
        let new: Vec<i64> = drain(&mut reader)
            .await
            .into_iter()
            .flat_map(|(ids, _)| ids)
            .collect();
        assert_eq!(new, vec![12, 13, 14]);
        assert_eq!(
            reader.cursor(),
            Some(&Position::completed(third, Phase::Incremental))
        );
    }

    #[tokio::test]
    async fn reads_only_snapshots_after_a_completed_position() {
        let fx = Fixture::new().await;
        let first = fx.append(&[0..=4]).await;
        let mut reader = fx
            .reader(Some(Position::completed(first, Phase::Incremental)), 3)
            .await;
        assert!(reader.next_batch().await.unwrap().is_none());

        let second = fx.append(&[5..=9]).await;
        reader.set_table(fx.table().await);
        let batch = reader.next_batch().await.unwrap().unwrap();
        assert_eq!(ids(&batch), vec![5, 6, 7]);
        let cursor = reader.cursor().cloned().unwrap();
        assert_eq!(
            cursor,
            Position {
                snapshot_id: second,
                phase: Phase::Incremental,
                files_done: 0,
                rows_done_in_file: 3,
                complete: false,
            }
        );

        // Resume mid-file: skips the 3 emitted rows.
        let mut resumed = fx.reader(Some(cursor), 3).await;
        let rest: Vec<i64> = drain(&mut resumed)
            .await
            .into_iter()
            .flat_map(|(ids, _)| ids)
            .collect();
        assert_eq!(rest, vec![8, 9]);
    }

    #[tokio::test]
    async fn without_a_position_reads_every_snapshot_from_the_root() {
        let fx = Fixture::new().await;
        let mut reader = fx.reader(None, 100).await;
        assert!(reader.next_batch().await.unwrap().is_none());
        fx.append(&[0..=1]).await;
        fx.append(&[2..=3]).await;
        reader.set_table(fx.table().await);
        let batches: Vec<Vec<i64>> = drain(&mut reader)
            .await
            .into_iter()
            .map(|(ids, _)| ids)
            .collect();
        assert_eq!(
            batches,
            vec![vec![0, 1], vec![2, 3]],
            "oldest snapshot first"
        );
    }

    #[tokio::test]
    async fn position_past_the_unit_is_an_error() {
        let fx = Fixture::new().await;
        let id = fx.append(&[0..=1]).await;
        let mut reader = fx
            .reader(
                Some(Position {
                    files_done: 0,
                    rows_done_in_file: 5,
                    ..Position::start(id, Phase::Incremental)
                }),
                10,
            )
            .await;
        let err = reader.next_batch().await.unwrap_err().to_string();
        assert!(err.contains("fewer rows"), "{err}");
        let mut reader = fx
            .reader(
                Some(Position {
                    files_done: 3,
                    ..Position::start(id, Phase::Incremental)
                }),
                10,
            )
            .await;
        let err = reader.next_batch().await.unwrap_err().to_string();
        assert!(err.contains("files done"), "{err}");
    }

    #[test]
    fn read_schema_strips_field_ids_and_rejects_gs_op() {
        let schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let read = ReadSchema::new(Arc::new(schema)).unwrap();
        let output = read.output();
        assert_eq!(output.field(0).name(), STREAMLING_COLUMN_NAME_OP);
        assert!(!output.field(0).is_nullable());
        assert_eq!(output.field(1).name(), "id");
        assert!(output.field(1).metadata().is_empty());

        let clash = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "_gs_op", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        assert!(ReadSchema::new(Arc::new(clash)).is_err());
    }
}
