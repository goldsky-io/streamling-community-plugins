//! Custom etl store backed by Streamling's plugin state backend.
//!
//! Replaces etl's built-in `PostgresStore`: replication metadata (table
//! states, versioned schemas, destination metadata, flush progress) is
//! persisted through the application's configured Streamling state backend
//! instead of an `etl` schema in Postgres.
//!
//! One aggregate value per slot-sharing group, stored under a fixed prefix so
//! every source in the group resolves the same physical key regardless of its
//! own reference name. Writes serialize the whole snapshot — metadata is KBs,
//! and the hot write (`upsert_replication_progress`) fires per flush cycle,
//! not per row.
//!
//! etl's `Pipeline::start()` calls the `load_*` methods once before workers
//! spawn; those hydrate the in-memory cache from the backend. All `get_*`
//! methods are pure cache reads; mutations write through (persist first, then
//! commit to the cache).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::replication::WorkerType;
use etl::state::{
    AppliedDestinationTableMetadata, DestinationTableMetadata, DestinationTableSchemaStatus,
    TableState,
};
use etl::store::lifecycle::{TableStateLifecycleStore, TableStateOperation};
use etl::store::schema::{SchemaStore, TableSchemaRetention};
use etl::store::state::{StateStore, TableStates};
use etl::types::{
    ColumnSchema, PgLsn, ReplicationMask, SnapshotId, TableId, TableName, TableSchema,
    convert_type_oid_to_type,
};
use serde::{Deserialize, Serialize};
use streamling_plugin::api::PluginStateBackend;
use tokio::sync::Mutex;

const STATE_KEY_PREFIX: &str = "postgres_cdc_source";

/// Aggregate persisted value: everything etl needs to resume a pipeline,
/// stored under one key for atomic snapshot writes.
///
/// Mirror structs hold only primitives (`Vec`s of entries instead of maps, to
/// avoid JSON map-key restrictions) because etl's schema/metadata types do not
/// derive serde. `TableState` does, so it is embedded directly — the same
/// rev-pinned serde coupling `PostgresStore` had.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct PersistedEtlState {
    table_states: Vec<PersistedTableStateEntry>,
    table_schemas: Vec<PersistedTableSchema>,
    destination_tables_metadata: Vec<PersistedDestinationMetadata>,
    /// `None` = the apply worker; `Some(oid)` = a table-sync worker. LSN as u64.
    replication_progress: Vec<(Option<u32>, u64)>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PersistedTableStateEntry {
    table_id: u32,
    current: TableState,
    history: Vec<TableState>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PersistedTableSchema {
    table_id: u32,
    schema: String,
    name: String,
    snapshot_id: u64,
    columns: Vec<PersistedColumn>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PersistedColumn {
    name: String,
    type_oid: u32,
    modifier: i32,
    ordinal_position: i32,
    primary_key_ordinal_position: Option<i32>,
    nullable: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PersistedDestinationMetadata {
    table_id: u32,
    destination_table_id: String,
    snapshot_id: u64,
    previous_snapshot_id: Option<u64>,
    applied: bool,
    replication_mask: Vec<u8>,
}

/// Live cache, shaped like etl's `MemoryStore`.
#[derive(Clone, Default)]
struct Inner {
    table_states: BTreeMap<TableId, TableState>,
    table_state_history: HashMap<TableId, Vec<TableState>>,
    /// Schema versions keyed first by table and then by snapshot.
    table_schemas: BTreeMap<TableId, BTreeMap<SnapshotId, Arc<TableSchema>>>,
    destination_tables_metadata: BTreeMap<TableId, DestinationTableMetadata>,
    replication_progress: HashMap<WorkerType, PgLsn>,
}

/// etl `PipelineStore` implementation persisting via a Streamling state
/// backend, keyed by the group's `slot_name`.
#[derive(Clone)]
pub struct StreamlingStore {
    inner: Arc<Mutex<Inner>>,
    backend: Arc<PluginStateBackend<PersistedEtlState>>,
    key: String,
}

impl StreamlingStore {
    pub fn new(backend: Arc<PluginStateBackend<PersistedEtlState>>, slot_name: &str) -> Self {
        backend.set_prefix(Some(STATE_KEY_PREFIX));
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            backend,
            key: slot_name.to_string(),
        }
    }

    /// Applies `f` to a clone of the cache, persists the result, then commits
    /// the clone. An `f` error persists nothing; a backend error leaves the
    /// cache untouched. Holding the mutex across the `put_kv` await serializes
    /// writes, so persisted snapshots are never torn.
    async fn mutate<T>(&self, f: impl FnOnce(&mut Inner) -> EtlResult<T>) -> EtlResult<T> {
        let mut inner = self.inner.lock().await;
        let mut updated = inner.clone();
        let result = f(&mut updated)?;
        self.backend
            .put_kv(&self.key, to_persisted(&updated))
            .await
            .map_err(backend_error)?;
        *inner = updated;
        Ok(result)
    }

    /// Replaces the whole cache from the backend (missing value = empty
    /// state). Called by each `load_*`; idempotent, and also restores
    /// `replication_progress`, which has no `load_*` of its own.
    async fn hydrate(&self) -> EtlResult<()> {
        let persisted = self
            .backend
            .get_kv(&self.key)
            .await
            .map_err(backend_error)?
            .unwrap_or_default();
        *self.inner.lock().await = from_persisted(persisted);
        Ok(())
    }
}

fn backend_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> EtlError {
    etl_error!(
        ErrorKind::IoError,
        "streamling state backend operation failed",
        source: e
    )
}

fn to_persisted(inner: &Inner) -> PersistedEtlState {
    let mut table_states = Vec::new();
    for (table_id, current) in &inner.table_states {
        // SyncWait/Catchup are in-memory-only (`should_store()` is false).
        // Chain history ++ current, drop unstorable entries; the last survivor
        // becomes the persisted current — reproducing PostgresStore's restart
        // semantics, where the persisted state is the last storable one.
        let history = inner
            .table_state_history
            .get(table_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut storable: Vec<TableState> = history
            .iter()
            .chain(std::iter::once(current))
            .filter(|state| state.as_type().should_store())
            .cloned()
            .collect();
        let Some(current) = storable.pop() else {
            continue;
        };
        table_states.push(PersistedTableStateEntry {
            table_id: table_id.into_inner(),
            current,
            history: storable,
        });
    }

    let table_schemas = inner
        .table_schemas
        .values()
        .flat_map(BTreeMap::values)
        .map(|schema| PersistedTableSchema {
            table_id: schema.id.into_inner(),
            schema: schema.name.schema.clone(),
            name: schema.name.name.clone(),
            snapshot_id: schema.snapshot_id.as_u64(),
            columns: schema
                .column_schemas
                .iter()
                .map(|column| PersistedColumn {
                    name: column.name.clone(),
                    type_oid: column.typ.oid(),
                    modifier: column.modifier,
                    ordinal_position: column.ordinal_position,
                    primary_key_ordinal_position: column.primary_key_ordinal_position,
                    nullable: column.nullable,
                })
                .collect(),
        })
        .collect();

    let destination_tables_metadata = inner
        .destination_tables_metadata
        .iter()
        .map(|(table_id, metadata)| PersistedDestinationMetadata {
            table_id: table_id.into_inner(),
            destination_table_id: metadata.destination_table_id.clone(),
            snapshot_id: metadata.snapshot_id.as_u64(),
            previous_snapshot_id: metadata.previous_snapshot_id.map(SnapshotId::as_u64),
            applied: metadata.is_applied(),
            replication_mask: metadata.replication_mask.to_bytes(),
        })
        .collect();

    let replication_progress = inner
        .replication_progress
        .iter()
        .map(|(worker, lsn)| {
            let table_oid = match worker {
                WorkerType::Apply => None,
                WorkerType::TableSync { table_id } => Some(table_id.into_inner()),
            };
            (table_oid, u64::from(*lsn))
        })
        .collect();

    PersistedEtlState {
        table_states,
        table_schemas,
        destination_tables_metadata,
        replication_progress,
    }
}

fn from_persisted(persisted: PersistedEtlState) -> Inner {
    let mut inner = Inner::default();

    for entry in persisted.table_states {
        let table_id = TableId::new(entry.table_id);
        inner.table_states.insert(table_id, entry.current);
        if !entry.history.is_empty() {
            inner.table_state_history.insert(table_id, entry.history);
        }
    }

    for schema in persisted.table_schemas {
        let table_id = TableId::new(schema.table_id);
        let snapshot_id = SnapshotId::from(schema.snapshot_id);
        let columns = schema
            .columns
            .into_iter()
            .map(|column| {
                ColumnSchema::new(
                    column.name,
                    convert_type_oid_to_type(column.type_oid),
                    column.modifier,
                    column.ordinal_position,
                    column.primary_key_ordinal_position,
                    column.nullable,
                )
            })
            .collect();
        let table_schema = TableSchema::with_snapshot_id(
            table_id,
            TableName::new(schema.schema, schema.name),
            columns,
            snapshot_id,
        );
        inner
            .table_schemas
            .entry(table_id)
            .or_default()
            .insert(snapshot_id, Arc::new(table_schema));
    }

    for metadata in persisted.destination_tables_metadata {
        let table_id = TableId::new(metadata.table_id);
        inner.destination_tables_metadata.insert(
            table_id,
            DestinationTableMetadata {
                destination_table_id: metadata.destination_table_id,
                snapshot_id: SnapshotId::from(metadata.snapshot_id),
                previous_snapshot_id: metadata.previous_snapshot_id.map(SnapshotId::from),
                schema_status: if metadata.applied {
                    DestinationTableSchemaStatus::Applied
                } else {
                    DestinationTableSchemaStatus::Applying
                },
                replication_mask: ReplicationMask::from_bytes(metadata.replication_mask),
            },
        );
    }

    for (table_oid, lsn) in persisted.replication_progress {
        let worker = match table_oid {
            None => WorkerType::Apply,
            Some(oid) => WorkerType::TableSync {
                table_id: TableId::new(oid),
            },
        };
        inner.replication_progress.insert(worker, PgLsn::from(lsn));
    }

    inner
}

impl StateStore for StreamlingStore {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_states.get(&table_id).cloned())
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        let inner = self.inner.lock().await;
        Ok(Arc::new(inner.table_states.clone()))
    }

    async fn load_table_states(&self) -> EtlResult<usize> {
        self.hydrate().await?;
        let inner = self.inner.lock().await;
        Ok(inner.table_states.len())
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        self.mutate(move |inner| {
            for (table_id, state) in updates {
                // Store the current state in history before updating.
                if let Some(current_state) = inner.table_states.get(&table_id).cloned() {
                    inner
                        .table_state_history
                        .entry(table_id)
                        .or_default()
                        .push(current_state);
                }
                inner.table_states.insert(table_id, state);
            }
            Ok(())
        })
        .await
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        self.mutate(move |inner| {
            let previous_state = inner
                .table_state_history
                .get_mut(&table_id)
                .and_then(Vec::pop)
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::StateRollbackError,
                        "No previous state available to roll back to"
                    )
                })?;
            inner.table_states.insert(table_id, previous_state.clone());
            Ok(previous_state)
        })
        .await
    }

    async fn get_replication_progress(&self, worker_type: WorkerType) -> EtlResult<Option<PgLsn>> {
        let inner = self.inner.lock().await;
        Ok(inner.replication_progress.get(&worker_type).copied())
    }

    async fn upsert_replication_progress(
        &self,
        worker_type: WorkerType,
        flush_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        self.mutate(move |inner| {
            let stored_lsn = inner
                .replication_progress
                .entry(worker_type)
                .and_modify(|stored_lsn| {
                    if flush_lsn > *stored_lsn {
                        *stored_lsn = flush_lsn;
                    }
                })
                .or_insert(flush_lsn);
            Ok(*stored_lsn)
        })
        .await
    }

    async fn delete_replication_progress(&self, worker_type: WorkerType) -> EtlResult<()> {
        self.mutate(move |inner| {
            inner.replication_progress.remove(&worker_type);
            Ok(())
        })
        .await
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.inner.lock().await;
        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
    }

    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<AppliedDestinationTableMetadata>> {
        let inner = self.inner.lock().await;
        inner
            .destination_tables_metadata
            .get(&table_id)
            .cloned()
            .map(DestinationTableMetadata::into_applied)
            .transpose()
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        self.hydrate().await?;
        let inner = self.inner.lock().await;
        Ok(inner.destination_tables_metadata.len())
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        self.mutate(move |inner| {
            inner.destination_tables_metadata.insert(table_id, metadata);
            Ok(())
        })
        .await
    }
}

impl SchemaStore for StreamlingStore {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;
        // Newest schema version at or before the requested snapshot.
        Ok(inner.table_schemas.get(table_id).and_then(|snapshots| {
            snapshots
                .range(..=snapshot_id)
                .next_back()
                .map(|(_, schema)| Arc::clone(schema))
        }))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .table_schemas
            .values()
            .flat_map(|snapshots| snapshots.values().map(Arc::clone))
            .collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        self.hydrate().await?;
        let inner = self.inner.lock().await;
        Ok(inner.table_schemas.values().map(BTreeMap::len).sum())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        self.mutate(move |inner| {
            let table_id = table_schema.id;
            let snapshot_id = table_schema.snapshot_id;
            let table_schema = Arc::new(table_schema);
            inner
                .table_schemas
                .entry(table_id)
                .or_default()
                .insert(snapshot_id, Arc::clone(&table_schema));
            Ok(table_schema)
        })
        .await
    }

    async fn prune_table_schemas(
        &self,
        table_schema_retentions: HashMap<TableId, TableSchemaRetention>,
    ) -> EtlResult<u64> {
        self.mutate(move |inner| {
            let mut removed_count = 0u64;
            for (table_id, snapshots) in &mut inner.table_schemas {
                let Some(retention) = table_schema_retentions.get(table_id) else {
                    continue;
                };
                // Keep the newest snapshot <= the retention LSN and everything
                // newer; skip the table if none qualifies (Postgres may replay
                // versions newer than the retention point).
                let retention_snapshot_id = SnapshotId::from(retention.to_lsn());
                let Some(retained_snapshot_id) = snapshots
                    .keys()
                    .rfind(|snapshot_id| **snapshot_id <= retention_snapshot_id)
                    .copied()
                else {
                    continue;
                };
                let before_count = snapshots.len();
                snapshots.retain(|snapshot_id, _| *snapshot_id >= retained_snapshot_id);
                removed_count += (before_count - snapshots.len()) as u64;
            }
            Ok(removed_count)
        })
        .await
    }
}

impl TableStateLifecycleStore for StreamlingStore {
    async fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> EtlResult<usize> {
        self.mutate(move |inner| match operation {
            TableStateOperation::PrepareForCopy { table_id } => {
                inner.table_schemas.remove(&table_id);
                inner.destination_tables_metadata.remove(&table_id);
                inner
                    .replication_progress
                    .remove(&WorkerType::TableSync { table_id });
                Ok(0)
            }
            TableStateOperation::ResetForResync => {
                let table_ids: Vec<TableId> = inner.table_states.keys().copied().collect();
                let reset_count = table_ids.len();
                for table_id in table_ids {
                    if let Some(current_state) = inner.table_states.get(&table_id).cloned() {
                        inner
                            .table_state_history
                            .entry(table_id)
                            .or_default()
                            .push(current_state);
                    }
                    inner.table_states.insert(table_id, TableState::Init);
                }
                inner.replication_progress.remove(&WorkerType::Apply);
                Ok(reset_count)
            }
            TableStateOperation::Delete { table_id } => {
                let affected_table_count = usize::from(inner.table_states.contains_key(&table_id));
                inner.table_states.remove(&table_id);
                inner.table_state_history.remove(&table_id);
                inner.table_schemas.remove(&table_id);
                inner.destination_tables_metadata.remove(&table_id);
                inner
                    .replication_progress
                    .remove(&WorkerType::TableSync { table_id });
                Ok(affected_table_count)
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_support;
    use etl::types::Type;

    fn backend() -> Arc<PluginStateBackend<PersistedEtlState>> {
        // The InMemory backend returns a fresh map per create(), so restart
        // tests must reuse ONE handle across two store instances.
        test_support::state_backend_factory("test_postgres_cdc_store").create()
    }

    fn test_schema(table_id: TableId, snapshot_id: u64) -> TableSchema {
        TableSchema::with_snapshot_id(
            table_id,
            TableName::new(
                "public".to_string(),
                format!("table_{}", table_id.into_inner()),
            ),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT8, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
            ],
            SnapshotId::from(snapshot_id),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn state_round_trips_across_store_instances() {
        let backend = backend();
        let store = StreamlingStore::new(backend.clone(), "slot_roundtrip");
        let table = TableId::new(1001);

        store
            .update_table_state(table, TableState::Init)
            .await
            .unwrap();
        store
            .update_table_state(table, TableState::DataSync)
            .await
            .unwrap();
        store
            .update_table_state(
                table,
                TableState::SyncDone {
                    lsn: PgLsn::from(42),
                },
            )
            .await
            .unwrap();
        store
            .store_table_schema(test_schema(table, 7))
            .await
            .unwrap();
        let metadata = DestinationTableMetadata {
            destination_table_id: "dest.users".to_string(),
            snapshot_id: SnapshotId::from(7),
            previous_snapshot_id: Some(SnapshotId::from(3)),
            schema_status: DestinationTableSchemaStatus::Applying,
            replication_mask: ReplicationMask::from_bytes(vec![1, 0]),
        };
        store
            .store_destination_table_metadata(table, metadata.clone())
            .await
            .unwrap();
        store
            .upsert_replication_progress(WorkerType::Apply, PgLsn::from(100))
            .await
            .unwrap();
        store
            .upsert_replication_progress(WorkerType::TableSync { table_id: table }, PgLsn::from(50))
            .await
            .unwrap();

        // A second store over the same backend handle simulates a restart.
        let restarted = StreamlingStore::new(backend, "slot_roundtrip");
        assert_eq!(
            restarted.load_destination_tables_metadata().await.unwrap(),
            1
        );
        assert_eq!(restarted.load_table_schemas().await.unwrap(), 1);
        assert_eq!(restarted.load_table_states().await.unwrap(), 1);

        assert_eq!(
            restarted.get_table_state(table).await.unwrap(),
            Some(TableState::SyncDone {
                lsn: PgLsn::from(42)
            })
        );
        let schema = restarted
            .get_table_schema(&table, SnapshotId::from(7))
            .await
            .unwrap()
            .expect("schema restored");
        assert_eq!(schema.name.schema, "public");
        assert_eq!(schema.column_schemas[0].typ, Type::INT8);
        assert_eq!(
            schema.column_schemas[0].primary_key_ordinal_position,
            Some(1)
        );
        assert!(!schema.column_schemas[0].nullable);
        assert_eq!(schema.column_schemas[1].typ, Type::TEXT);
        assert!(schema.column_schemas[1].nullable);
        assert_eq!(
            restarted
                .get_destination_table_metadata(table)
                .await
                .unwrap(),
            Some(metadata.clone())
        );
        // Applying status must be restored as-is (recovery reads it), so the
        // applied-only accessor must reject it.
        assert!(
            restarted
                .get_applied_destination_table_metadata(table)
                .await
                .is_err()
        );
        assert_eq!(
            restarted
                .get_replication_progress(WorkerType::Apply)
                .await
                .unwrap(),
            Some(PgLsn::from(100))
        );
        assert_eq!(
            restarted
                .get_replication_progress(WorkerType::TableSync { table_id: table })
                .await
                .unwrap(),
            Some(PgLsn::from(50))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replication_progress_is_monotonic() {
        let store = StreamlingStore::new(backend(), "slot_monotonic");
        let worker = WorkerType::Apply;
        assert_eq!(
            store
                .upsert_replication_progress(worker, PgLsn::from(100))
                .await
                .unwrap(),
            PgLsn::from(100)
        );
        assert_eq!(
            store
                .upsert_replication_progress(worker, PgLsn::from(50))
                .await
                .unwrap(),
            PgLsn::from(100)
        );
        assert_eq!(
            store
                .upsert_replication_progress(worker, PgLsn::from(200))
                .await
                .unwrap(),
            PgLsn::from(200)
        );
        assert_eq!(
            store.get_replication_progress(worker).await.unwrap(),
            Some(PgLsn::from(200))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rollback_pops_history_and_survives_restart() {
        let backend = backend();
        let store = StreamlingStore::new(backend.clone(), "slot_rollback");
        let table = TableId::new(2);
        store
            .update_table_state(table, TableState::Init)
            .await
            .unwrap();
        store
            .update_table_state(table, TableState::DataSync)
            .await
            .unwrap();
        store
            .update_table_state(table, TableState::FinishedCopy)
            .await
            .unwrap();

        let restarted = StreamlingStore::new(backend, "slot_rollback");
        restarted.load_table_states().await.unwrap();
        assert_eq!(
            restarted.rollback_table_state(table).await.unwrap(),
            TableState::DataSync
        );
        assert_eq!(
            restarted.rollback_table_state(table).await.unwrap(),
            TableState::Init
        );
        let err = restarted.rollback_table_state(table).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::StateRollbackError);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_versions_lookup_and_prune() {
        let store = StreamlingStore::new(backend(), "slot_schema");
        let table = TableId::new(3);
        store
            .store_table_schema(test_schema(table, 100))
            .await
            .unwrap();
        store
            .store_table_schema(test_schema(table, 300))
            .await
            .unwrap();

        assert!(
            store
                .get_table_schema(&table, SnapshotId::from(50))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_table_schema(&table, SnapshotId::from(250))
                .await
                .unwrap()
                .unwrap()
                .snapshot_id,
            SnapshotId::from(100)
        );

        store
            .store_table_schema(test_schema(table, 200))
            .await
            .unwrap();
        // Retention at 250 keeps the newest snapshot <= 250 (200) and
        // everything newer (300); only 100 is removed.
        let removed = store
            .prune_table_schemas(HashMap::from([(
                table,
                TableSchemaRetention::SnapshotId(SnapshotId::from(250)),
            )]))
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(
            store
                .get_table_schema(&table, SnapshotId::from(100))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_table_schema(&table, SnapshotId::max())
                .await
                .unwrap()
                .unwrap()
                .snapshot_id,
            SnapshotId::from(300)
        );

        // No snapshot at or before the retention point: table left untouched.
        let removed = store
            .prune_table_schemas(HashMap::from([(
                table,
                TableSchemaRetention::SnapshotId(SnapshotId::from(10)),
            )]))
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(store.get_table_schemas().await.unwrap().len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lifecycle_operations_apply_and_persist() {
        let backend = backend();
        let store = StreamlingStore::new(backend.clone(), "slot_lifecycle");
        let table = TableId::new(4);
        store
            .update_table_state(table, TableState::Ready)
            .await
            .unwrap();
        store
            .store_table_schema(test_schema(table, 0))
            .await
            .unwrap();
        store
            .store_destination_table_metadata(
                table,
                DestinationTableMetadata::new_applied(
                    "dest.t".to_string(),
                    SnapshotId::initial(),
                    ReplicationMask::from_bytes(vec![1, 1]),
                ),
            )
            .await
            .unwrap();
        store
            .upsert_replication_progress(WorkerType::Apply, PgLsn::from(10))
            .await
            .unwrap();
        store
            .upsert_replication_progress(WorkerType::TableSync { table_id: table }, PgLsn::from(5))
            .await
            .unwrap();

        // PrepareForCopy drops schemas + metadata + table-sync progress,
        // keeps the table state, returns 0.
        assert_eq!(
            store
                .apply_table_state_operation(TableStateOperation::PrepareForCopy {
                    table_id: table
                })
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store.get_table_state(table).await.unwrap(),
            Some(TableState::Ready)
        );
        assert!(
            store
                .get_table_schema(&table, SnapshotId::max())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_destination_table_metadata(table)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_replication_progress(WorkerType::TableSync { table_id: table })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_replication_progress(WorkerType::Apply)
                .await
                .unwrap()
                .is_some()
        );

        // ResetForResync pushes current states to history, resets to Init,
        // drops apply progress, returns the reset count.
        assert_eq!(
            store
                .apply_table_state_operation(TableStateOperation::ResetForResync)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store.get_table_state(table).await.unwrap(),
            Some(TableState::Init)
        );
        assert!(
            store
                .get_replication_progress(WorkerType::Apply)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.rollback_table_state(table).await.unwrap(),
            TableState::Ready
        );

        // Delete drops everything for the table; the count reflects whether a
        // state existed.
        assert_eq!(
            store
                .apply_table_state_operation(TableStateOperation::Delete { table_id: table })
                .await
                .unwrap(),
            1
        );
        assert!(store.get_table_state(table).await.unwrap().is_none());
        assert_eq!(
            store
                .apply_table_state_operation(TableStateOperation::Delete { table_id: table })
                .await
                .unwrap(),
            0
        );

        // The deletion is persisted, not just cached.
        let restarted = StreamlingStore::new(backend, "slot_lifecycle");
        assert_eq!(restarted.load_table_states().await.unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_memory_only_states_are_cached_but_not_persisted() {
        let backend = backend();
        let store = StreamlingStore::new(backend.clone(), "slot_syncwait");
        let table = TableId::new(5);
        store
            .update_table_state(table, TableState::Init)
            .await
            .unwrap();
        store
            .update_table_state(table, TableState::FinishedCopy)
            .await
            .unwrap();
        store
            .update_table_state(
                table,
                TableState::SyncWait {
                    lsn: PgLsn::from(9),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_table_state(table).await.unwrap(),
            Some(TableState::SyncWait {
                lsn: PgLsn::from(9)
            })
        );

        // Reload shows the prior storable state as current...
        let restarted = StreamlingStore::new(backend, "slot_syncwait");
        restarted.load_table_states().await.unwrap();
        assert_eq!(
            restarted.get_table_state(table).await.unwrap(),
            Some(TableState::FinishedCopy)
        );
        // ...and it was NOT also kept in history: one rollback reaches Init,
        // a second finds nothing.
        assert_eq!(
            restarted.rollback_table_state(table).await.unwrap(),
            TableState::Init
        );
        assert!(restarted.rollback_table_state(table).await.is_err());
    }
}
