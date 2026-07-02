//! Bridges etl's push-based `Destination` into a pull-based mpsc stream of
//! aligned CDC-row units.
//!
//! Each `write_events` / `write_table_rows` call fans out to one `Unit` per
//! subscriber table, carrying rows aligned to that table's output columns plus
//! a slice of the shared etl ack (`SourceAckHandle`). Acks are NOT resolved
//! here — each source arms its slice in the `AckLedger` once delivered and
//! releases it on checkpoint finalize, so etl confirms an LSN only once every
//! subscriber has durably checkpointed past it.
//!
//! A group of subscribers shares one publication: events for tables with no
//! subscriber are dropped (warned once per table by the destination). Truncate
//! events have no row representation in the table-shaped output and are
//! dropped with a warning. Begin/Commit/Relation events carry no row data
//! and are skipped.

use crate::postgres_cdc::arrow::CdcRow;
use crate::postgres_cdc::ledger::{SharedAck, SourceAckHandle, SourceId};
use etl::destination::Destination;
use etl::destination::async_result::AsyncResult;
use etl::error::EtlResult;
use etl::types::{Cell, Event, OldTableRow, ReplicatedTableSchema, TableRow, UpdatedTableRow};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// One destination write destined for a single subscriber: that table's
/// aligned rows + its slice of the shared etl ack.
pub struct Unit {
    pub rows: Vec<CdcRow>,
    pub ack: SourceAckHandle,
}

/// Aligns replication-event cells to the source's output data columns.
///
/// Output positions are fixed at construction (from the discovered schema);
/// event cells are matched by column name, so the conversion tolerates
/// replication masks and column reordering. Absent values stay `None`.
pub struct RowConverter {
    /// "schema.table" this source replicates.
    table: String,
    /// Output data-column name → position.
    col_idx: HashMap<String, usize>,
    n_cols: usize,
}

/// `schema.table` label for an etl table schema.
fn table_label(schema: &ReplicatedTableSchema) -> String {
    format!("{}.{}", schema.name().schema, schema.name().name)
}

impl RowConverter {
    /// `data_columns`: output column names in schema order (without `_gs_op`).
    pub fn new(table: String, data_columns: &[String]) -> Self {
        Self {
            table,
            col_idx: data_columns
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect(),
            n_cols: data_columns.len(),
        }
    }

    /// True when the event's table is the one this converter handles. In
    /// fan-out a foreign table is another subscriber's table; the destination
    /// owns the unconsumed-table warning, so this is a silent filter.
    fn matches_table(&self, schema: &ReplicatedTableSchema) -> bool {
        table_label(schema) == self.table
    }

    /// Aligns `values` (replicated-column order, `missing` indexes absent)
    /// to output positions by column name.
    fn aligned(
        &self,
        schema: &ReplicatedTableSchema,
        values: &[Cell],
        missing: &[usize],
    ) -> Vec<Option<Cell>> {
        let mut out = vec![None; self.n_cols];
        let mut vals = values.iter();
        // etl emits `missing` indexes sorted ascending: merge, don't scan.
        let mut missing = missing.iter().peekable();
        for (idx, col) in schema.column_schemas().enumerate() {
            if missing.next_if_eq(&&idx).is_some() {
                continue;
            }
            let Some(cell) = vals.next() else { break };
            if let Some(&i) = self.col_idx.get(&col.name) {
                out[i] = Some(cell.clone());
            }
        }
        out
    }

    /// Aligns a delete's old-row image. Key images carry identity columns
    /// only; the remaining output columns stay null.
    fn aligned_old(&self, schema: &ReplicatedTableSchema, old: &OldTableRow) -> Vec<Option<Cell>> {
        match old {
            OldTableRow::Full(row) => self.aligned(schema, row.values(), &[]),
            OldTableRow::Key(row) => {
                let mut out = vec![None; self.n_cols];
                for (col, cell) in schema.identity_column_schemas().zip(row.values()) {
                    if let Some(&i) = self.col_idx.get(&col.name) {
                        out[i] = Some(cell.clone());
                    }
                }
                out
            }
        }
    }

    /// Converts one streaming-event batch into aligned rows.
    pub fn convert_events(&self, events: &[Event]) -> Vec<CdcRow> {
        let mut rows = Vec::with_capacity(events.len());
        for event in events {
            match event {
                Event::Begin(_) | Event::Commit(_) => {}
                Event::Insert(e) => {
                    if self.matches_table(&e.replicated_table_schema) {
                        rows.push(CdcRow {
                            op: "insert",
                            values: self.aligned(
                                &e.replicated_table_schema,
                                e.table_row.values(),
                                &[],
                            ),
                        });
                    }
                }
                Event::Update(e) => {
                    if self.matches_table(&e.replicated_table_schema) {
                        let values = match &e.updated_table_row {
                            UpdatedTableRow::Full(row) => {
                                self.aligned(&e.replicated_table_schema, row.values(), &[])
                            }
                            // Unchanged-TOAST columns are absent from the new
                            // image and become null. Use REPLICA IDENTITY FULL
                            // on the source table to avoid this.
                            UpdatedTableRow::Partial(p) => self.aligned(
                                &e.replicated_table_schema,
                                p.values(),
                                p.missing_column_indexes(),
                            ),
                        };
                        rows.push(CdcRow {
                            op: "update",
                            values,
                        });
                    }
                }
                Event::Delete(e) => {
                    if self.matches_table(&e.replicated_table_schema) {
                        let Some(old) = &e.old_table_row else {
                            warn!(
                                table = %self.table,
                                "postgres_cdc: delete event without an old row \
                                 image; dropped (cannot key the delete)"
                            );
                            continue;
                        };
                        rows.push(CdcRow {
                            op: "delete",
                            values: self.aligned_old(&e.replicated_table_schema, old),
                        });
                    }
                }
                Event::Truncate(e) => {
                    for table in &e.truncated_tables {
                        if table_label(table) == self.table {
                            warn!(
                                table = %self.table,
                                "postgres_cdc: TRUNCATE observed on the replicated \
                                 table; it has no row representation and is NOT \
                                 propagated downstream"
                            );
                        }
                    }
                }
                Event::Relation(e) => {
                    debug!(
                        table = %table_label(&e.replicated_table_schema),
                        "postgres_cdc: schema change observed; output schema is \
                         fixed at startup (new columns are dropped, removed \
                         columns become null)"
                    );
                }
                _ => {
                    warn!("postgres_cdc: unsupported replication event skipped");
                }
            }
        }
        rows
    }

    /// Converts an initial-copy row batch into aligned `copy` rows.
    pub fn convert_copy_rows(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: &[TableRow],
    ) -> Vec<CdcRow> {
        if !self.matches_table(schema) {
            return Vec::new();
        }
        table_rows
            .iter()
            .map(|row| CdcRow {
                op: "copy",
                values: self.aligned(schema, row.values(), &[]),
            })
            .collect()
    }
}

/// One registered table within a shared group: its converter and channel.
pub struct Subscriber {
    pub source_id: SourceId,
    pub table_label: String,
    pub converter: RowConverter,
    pub tx: mpsc::Sender<Unit>,
}

/// etl `Destination` that fans each write out to per-table subscriber channels.
/// A group of one subscriber is the standalone case.
#[derive(Clone)]
pub struct ChannelDestination {
    subscribers: Arc<Vec<Subscriber>>,
    /// Publication tables seen with no subscriber, warned once each.
    warned_unconsumed: Arc<Mutex<HashSet<String>>>,
}

impl ChannelDestination {
    pub fn new(subscribers: Vec<Subscriber>) -> Self {
        Self {
            subscribers: Arc::new(subscribers),
            warned_unconsumed: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn warn_unconsumed(&self, label: &str) {
        let mut warned = self.warned_unconsumed.lock().expect("warned poisoned");
        if warned.insert(label.to_string()) {
            warn!(
                table = %label,
                "postgres_cdc: publication delivers a table with no subscribed source in this \
                 slot group; its rows are dropped (add a source for it or remove it from the \
                 publication)"
            );
        }
    }

    /// Sends one `Unit` per subscriber that produced rows; builds a `SharedAck`
    /// over exactly those subscribers (empty set ⇒ acks immediately). A failed
    /// send (receiver gone) leaves that subscriber's slice unreleased, so the
    /// etl ack never fires and etl stops the pipeline — correct on shutdown.
    async fn fan_out(
        &self,
        per_sub: Vec<(SourceId, mpsc::Sender<Unit>, Vec<CdcRow>)>,
        async_result: AsyncResult<()>,
    ) {
        let pending: HashSet<SourceId> = per_sub.iter().map(|(id, _, _)| *id).collect();
        let shared = SharedAck::new(async_result, pending);
        for (source_id, tx, rows) in per_sub {
            let unit = Unit {
                rows,
                ack: (shared.clone(), source_id),
            };
            let _ = tx.send(unit).await;
        }
    }
}

impl Destination for ChannelDestination {
    fn name() -> &'static str {
        "streamling_channel"
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: etl::destination::DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        // Nothing to drop downstream. A restarted copy re-emits rows
        // (at-least-once; sinks upsert by key).
        debug!(
            table = %table_label(replicated_table_schema),
            "postgres_cdc: table copy (re)start"
        );
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<etl::types::TableRow>,
        async_result: etl::destination::WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let label = table_label(replicated_table_schema);
        let mut per_sub = Vec::new();
        for sub in self.subscribers.iter() {
            if sub.table_label == label {
                let rows = sub
                    .converter
                    .convert_copy_rows(replicated_table_schema, &table_rows);
                if !rows.is_empty() {
                    per_sub.push((sub.source_id, sub.tx.clone(), rows));
                }
            }
        }
        if per_sub.is_empty() {
            self.warn_unconsumed(&label);
        }
        self.fan_out(per_sub, async_result).await;
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: etl::destination::WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let mut per_sub = Vec::new();
        for sub in self.subscribers.iter() {
            let rows = sub.converter.convert_events(&events);
            if !rows.is_empty() {
                per_sub.push((sub.source_id, sub.tx.clone(), rows));
            }
        }
        // Warn for data-event tables with no subscriber.
        let subscribed: HashSet<&str> = self
            .subscribers
            .iter()
            .map(|s| s.table_label.as_str())
            .collect();
        for label in data_event_tables(&events) {
            if !subscribed.contains(label.as_str()) {
                self.warn_unconsumed(&label);
            }
        }
        self.fan_out(per_sub, async_result).await;
        Ok(())
    }
}

/// Distinct `schema.table` labels of data events (insert/update/delete) in a
/// batch; used to warn about unsubscribed publication tables.
fn data_event_tables(events: &[Event]) -> HashSet<String> {
    let mut out = HashSet::new();
    for event in events {
        let schema = match event {
            Event::Insert(e) => Some(&e.replicated_table_schema),
            Event::Update(e) => Some(&e.replicated_table_schema),
            Event::Delete(e) => Some(&e.replicated_table_schema),
            _ => None,
        };
        if let Some(s) = schema {
            out.insert(table_label(s));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::{
        BeginEvent, ColumnSchema, CommitEvent, DeleteEvent, InsertEvent, PgLsn, TableId, TableName,
        TableSchema, TruncateEvent, Type, UpdateEvent,
    };
    use std::sync::Arc;

    fn users_schema() -> ReplicatedTableSchema {
        let ts = TableSchema::new(
            TableId::new(16384),
            TableName::new("public".into(), "users".into()),
            vec![
                ColumnSchema::new("id".into(), Type::INT8, -1, 1, Some(1), false),
                ColumnSchema::new("name".into(), Type::TEXT, -1, 2, None, true),
            ],
        );
        ReplicatedTableSchema::all(Arc::new(ts))
    }

    fn converter() -> RowConverter {
        RowConverter::new("public.users".into(), &["id".into(), "name".into()])
    }

    fn row(id: i64, name: &str) -> TableRow {
        TableRow::new(vec![Cell::I64(id), Cell::String(name.into())])
    }

    fn begin(commit_lsn: PgLsn) -> Event {
        Event::Begin(BeginEvent {
            start_lsn: PgLsn::from(990u64),
            commit_lsn,
            tx_ordinal: 0,
            timestamp: 0,
            xid: 42,
        })
    }

    #[test]
    fn insert_update_delete_convert_to_aligned_rows() {
        let s = users_schema();
        let c = converter();
        let commit_lsn = PgLsn::from(1000u64);

        let events = vec![
            begin(commit_lsn),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(991u64),
                commit_lsn,
                tx_ordinal: 1,
                replicated_table_schema: s.clone(),
                table_row: row(1, "ada"),
            }),
            Event::Update(UpdateEvent {
                start_lsn: PgLsn::from(992u64),
                commit_lsn,
                tx_ordinal: 2,
                replicated_table_schema: s.clone(),
                updated_table_row: UpdatedTableRow::Full(row(1, "grace")),
                old_table_row: None,
            }),
            Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(993u64),
                commit_lsn,
                tx_ordinal: 3,
                replicated_table_schema: s.clone(),
                old_table_row: Some(OldTableRow::Full(row(1, "grace"))),
            }),
            Event::Commit(CommitEvent {
                start_lsn: PgLsn::from(994u64),
                commit_lsn,
                tx_ordinal: 4,
                flags: 0,
                end_lsn: PgLsn::from(1001u64),
                timestamp: 0,
            }),
        ];

        let rows = c.convert_events(&events);
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].op, "insert");
        assert_eq!(
            rows[0].values,
            vec![Some(Cell::I64(1)), Some(Cell::String("ada".into()))]
        );

        assert_eq!(rows[1].op, "update");
        assert_eq!(
            rows[1].values,
            vec![Some(Cell::I64(1)), Some(Cell::String("grace".into()))]
        );

        // Delete carries the old image so sinks can key the delete.
        assert_eq!(rows[2].op, "delete");
        assert_eq!(
            rows[2].values,
            vec![Some(Cell::I64(1)), Some(Cell::String("grace".into()))]
        );
    }

    #[test]
    fn delete_with_key_image_fills_identity_columns_only() {
        let s = users_schema();
        let c = converter();
        let events = vec![Event::Delete(DeleteEvent {
            start_lsn: PgLsn::from(5u64),
            commit_lsn: PgLsn::from(9u64),
            tx_ordinal: 0,
            replicated_table_schema: s,
            old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I64(1)]))),
        })];
        let rows = c.convert_events(&events);
        assert_eq!(rows[0].op, "delete");
        assert_eq!(rows[0].values[0], Some(Cell::I64(1)));
        assert!(rows[0].values[1].is_none());
    }

    #[test]
    fn partial_update_leaves_missing_columns_none() {
        let s = users_schema();
        let c = converter();
        let partial = etl::types::PartialTableRow::new(
            2,
            TableRow::new(vec![Cell::String("ada".into())]),
            vec![0], // `id` missing (unchanged toast)
        );
        let events = vec![Event::Update(UpdateEvent {
            start_lsn: PgLsn::from(5u64),
            commit_lsn: PgLsn::from(9u64),
            tx_ordinal: 0,
            replicated_table_schema: s,
            updated_table_row: UpdatedTableRow::Partial(partial),
            old_table_row: None,
        })];
        let rows = c.convert_events(&events);
        assert!(rows[0].values[0].is_none());
        assert_eq!(rows[0].values[1], Some(Cell::String("ada".into())));
    }

    #[test]
    fn events_for_other_tables_are_dropped() {
        let other = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(99),
            TableName::new("public".into(), "orders".into()),
            vec![ColumnSchema::new(
                "id".into(),
                Type::INT8,
                -1,
                1,
                Some(1),
                false,
            )],
        )));
        let c = converter();
        let events = vec![Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(5u64),
            commit_lsn: PgLsn::from(9u64),
            tx_ordinal: 0,
            replicated_table_schema: other.clone(),
            table_row: TableRow::new(vec![Cell::I64(7)]),
        })];
        assert!(c.convert_events(&events).is_empty());
        assert!(
            c.convert_copy_rows(&other, &[TableRow::new(vec![Cell::I64(7)])])
                .is_empty()
        );
    }

    #[test]
    fn truncate_and_delete_without_old_image_emit_nothing() {
        let s = users_schema();
        let c = converter();
        let events = vec![
            Event::Truncate(TruncateEvent {
                start_lsn: PgLsn::from(50u64),
                commit_lsn: PgLsn::from(60u64),
                tx_ordinal: 1,
                options: 0,
                truncated_tables: vec![s.clone()],
            }),
            Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(51u64),
                commit_lsn: PgLsn::from(60u64),
                tx_ordinal: 2,
                replicated_table_schema: s,
                old_table_row: None,
            }),
        ];
        assert!(c.convert_events(&events).is_empty());
    }

    #[test]
    fn fan_out_groups_rows_per_subscriber_table() {
        let users = users_schema();
        let conv_users = RowConverter::new("public.users".into(), &["id".into(), "name".into()]);
        // An orders converter sees the same event stream but matches nothing here.
        let conv_orders = RowConverter::new("public.orders".into(), &["id".into()]);

        let events = vec![Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1u64),
            commit_lsn: PgLsn::from(2u64),
            tx_ordinal: 0,
            replicated_table_schema: users.clone(),
            table_row: row(1, "ada"),
        })];

        assert_eq!(conv_users.convert_events(&events).len(), 1);
        assert!(conv_orders.convert_events(&events).is_empty());
        assert_eq!(
            data_event_tables(&events),
            std::collections::HashSet::from(["public.users".to_string()])
        );
    }

    #[test]
    fn copy_rows_convert_aligned() {
        let s = users_schema();
        let c = converter();
        let rows = c.convert_copy_rows(&s, &[row(1, "ada"), row(2, "bo")]);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.op == "copy"));
        assert_eq!(
            rows[1].values,
            vec![Some(Cell::I64(2)), Some(Cell::String("bo".into()))]
        );
    }
}
