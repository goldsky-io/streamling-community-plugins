//! Typed RecordBatch construction from aligned CDC rows.
//!
//! The output schema is the replicated table's own columns plus `_gs_op`
//! (streamling `RowKind` encoding: "i" insert/copy, "u" update, "d" delete),
//! so downstream operators and sinks see the table exactly as other
//! streamling sources expose their data — no CDC envelope.
//!
//! Rows arrive pre-aligned to the output columns (`CdcRow::values[i]`
//! corresponds to data column `i`, i.e. schema field `i + 1`). `None` means
//! the value was absent from the replication image (key-only delete,
//! unchanged TOAST) and becomes null.

use crate::postgres_cdc::json::cell_to_json;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Timelike};
use etl::types::Cell;
use std::sync::Arc;
use tracing::warn;

/// One change event, aligned to the output data columns.
#[derive(Debug, Clone, PartialEq)]
pub struct CdcRow {
    /// "insert" | "update" | "delete" | "copy" (truncate is not emitted).
    pub op: &'static str,
    /// One entry per data column; `None` → null.
    pub values: Vec<Option<Cell>>,
}

/// Maps the CDC operation to streamling's `RowKind` string encoding.
fn gs_op_for(op: &str) -> &'static str {
    match op {
        "update" => "u",
        "delete" => "d",
        // insert and copy: applied as inserts by sinks.
        _ => "i",
    }
}

/// Per-column typed builder; one variant per `DataType` the discovery layer
/// can produce.
enum ColBuilder {
    Bool(BooleanBuilder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    U32(UInt32Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Bin(BinaryBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Ts(TimestampMicrosecondBuilder),
    TsTz(TimestampMicrosecondBuilder),
}

/// Stringifies any cell for a Utf8 column (numeric/uuid/json/arrays/unknown).
fn cell_to_utf8(cell: &Cell) -> String {
    match cell {
        Cell::String(s) => s.clone(),
        other => match cell_to_json(other) {
            serde_json::Value::String(s) => s,
            v => v.to_string(),
        },
    }
}

const UNIX_EPOCH_DAYS: i32 = 719_163; // days from CE to 1970-01-01

impl ColBuilder {
    fn new(dt: &DataType) -> Result<Self, ArrowError> {
        Ok(match dt {
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int16 => Self::I16(Int16Builder::new()),
            DataType::Int32 => Self::I32(Int32Builder::new()),
            DataType::Int64 => Self::I64(Int64Builder::new()),
            DataType::UInt32 => Self::U32(UInt32Builder::new()),
            DataType::Float32 => Self::F32(Float32Builder::new()),
            DataType::Float64 => Self::F64(Float64Builder::new()),
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::Binary => Self::Bin(BinaryBuilder::new()),
            DataType::Date32 => Self::Date(Date32Builder::new()),
            DataType::Time64(TimeUnit::Microsecond) => Self::Time(Time64MicrosecondBuilder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Self::Ts(TimestampMicrosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
                Self::TsTz(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
            other => {
                return Err(ArrowError::SchemaError(format!(
                    "postgres_cdc: unsupported output column type {other}"
                )));
            }
        })
    }

    /// Appends a cell, coercing to the column type; mismatches append null
    /// with a warning (the upstream type changed or decoding surprised us).
    fn append(&mut self, name: &str, value: Option<&Cell>) {
        let Some(cell) = value else {
            self.append_null();
            return;
        };
        if matches!(cell, Cell::Null) {
            self.append_null();
            return;
        }
        match (self, cell) {
            (Self::Bool(b), Cell::Bool(v)) => b.append_value(*v),
            (Self::I16(b), Cell::I16(v)) => b.append_value(*v),
            (Self::I32(b), Cell::I32(v)) => b.append_value(*v),
            (Self::I64(b), Cell::I64(v)) => b.append_value(*v),
            (Self::I64(b), Cell::I32(v)) => b.append_value(i64::from(*v)),
            (Self::I64(b), Cell::I16(v)) => b.append_value(i64::from(*v)),
            (Self::I32(b), Cell::I16(v)) => b.append_value(i32::from(*v)),
            (Self::U32(b), Cell::U32(v)) => b.append_value(*v),
            (Self::F32(b), Cell::F32(v)) => b.append_value(*v),
            (Self::F64(b), Cell::F64(v)) => b.append_value(*v),
            (Self::F64(b), Cell::F32(v)) => b.append_value(f64::from(*v)),
            (Self::Utf8(b), cell) => b.append_value(cell_to_utf8(cell)),
            (Self::Bin(b), Cell::Bytes(v)) => b.append_value(v),
            (Self::Date(b), Cell::Date(d)) => {
                b.append_value(d.num_days_from_ce() - UNIX_EPOCH_DAYS)
            }
            (Self::Time(b), Cell::Time(t)) => b.append_value(
                i64::from(t.num_seconds_from_midnight()) * 1_000_000
                    + i64::from(t.nanosecond() / 1_000),
            ),
            (Self::Ts(b), Cell::Timestamp(ts)) => b.append_value(ts.and_utc().timestamp_micros()),
            (Self::TsTz(b), Cell::TimestampTz(ts)) => b.append_value(ts.timestamp_micros()),
            (builder, cell) => {
                warn!(
                    column = name,
                    cell = ?std::mem::discriminant(cell),
                    "postgres_cdc: cell type does not match output column type; \
                     appending null"
                );
                builder.append_null();
            }
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::I16(b) => b.append_null(),
            Self::I32(b) => b.append_null(),
            Self::I64(b) => b.append_null(),
            Self::U32(b) => b.append_null(),
            Self::F32(b) => b.append_null(),
            Self::F64(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Bin(b) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Time(b) => b.append_null(),
            Self::Ts(b) => b.append_null(),
            Self::TsTz(b) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::I16(mut b) => Arc::new(b.finish()),
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::U32(mut b) => Arc::new(b.finish()),
            Self::F32(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Bin(mut b) => Arc::new(b.finish()),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
            Self::Ts(mut b) => Arc::new(b.finish()),
            Self::TsTz(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Builds a RecordBatch for `schema` (field 0 = `_gs_op`, then data columns)
/// from aligned rows.
pub fn rows_to_record_batch(
    schema: SchemaRef,
    rows: Vec<CdcRow>,
) -> Result<RecordBatch, ArrowError> {
    let data_fields = &schema.fields()[1..];
    let mut gs_op = StringBuilder::with_capacity(rows.len(), rows.len());
    let mut builders = data_fields
        .iter()
        .map(|f| ColBuilder::new(f.data_type()))
        .collect::<Result<Vec<_>, _>>()?;

    for row in &rows {
        if row.values.len() != builders.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "postgres_cdc: row has {} values for {} data columns",
                row.values.len(),
                builders.len()
            )));
        }
        gs_op.append_value(gs_op_for(row.op));
        for ((builder, field), value) in builders.iter_mut().zip(data_fields).zip(&row.values) {
            builder.append(field.name(), value.as_ref());
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(builders.len() + 1);
    columns.push(Arc::new(gs_op.finish()));
    columns.extend(builders.into_iter().map(ColBuilder::finish));
    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_cdc::discovery::{DiscoveredColumn, build_output_schema};
    use arrow::array::{
        Array, BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use chrono::{NaiveDate, TimeZone, Utc};
    use etl::types::Type;

    fn schema() -> SchemaRef {
        Arc::new(
            build_output_schema(&[
                DiscoveredColumn {
                    name: "id".into(),
                    type_oid: Type::INT8.oid(),
                },
                DiscoveredColumn {
                    name: "name".into(),
                    type_oid: Type::TEXT.oid(),
                },
                DiscoveredColumn {
                    name: "active".into(),
                    type_oid: Type::BOOL.oid(),
                },
                DiscoveredColumn {
                    name: "born".into(),
                    type_oid: Type::DATE.oid(),
                },
                DiscoveredColumn {
                    name: "seen_at".into(),
                    type_oid: Type::TIMESTAMPTZ.oid(),
                },
                DiscoveredColumn {
                    name: "balance".into(),
                    type_oid: Type::NUMERIC.oid(),
                },
            ])
            .unwrap(),
        )
    }

    #[test]
    fn typed_columns_round_trip_with_gs_op() {
        let born = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        let seen = Utc.with_ymd_and_hms(2026, 6, 11, 1, 2, 3).unwrap();
        let rows = vec![
            CdcRow {
                op: "insert",
                values: vec![
                    Some(Cell::I64(1)),
                    Some(Cell::String("ada".into())),
                    Some(Cell::Bool(true)),
                    Some(Cell::Date(born)),
                    Some(Cell::TimestampTz(seen)),
                    Some(Cell::String("12.50".into())),
                ],
            },
            CdcRow {
                op: "delete",
                // Key-only image: everything but the PK is absent.
                values: vec![Some(Cell::I64(1)), None, None, None, None, None],
            },
        ];
        let batch = rows_to_record_batch(schema(), rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let gs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(gs.value(0), "i");
        assert_eq!(gs.value(1), "d");

        let id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(0), 1);
        assert_eq!(id.value(1), 1);

        let name = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name.value(0), "ada");
        assert!(name.is_null(1));

        let active = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active.value(0));

        let born_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(i64::from(born_col.value(0)), (born - epoch).num_days());

        let seen_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(seen_col.value(0), seen.timestamp_micros());

        let balance = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(balance.value(0), "12.50");
    }

    #[test]
    fn type_mismatch_appends_null_not_panic() {
        let rows = vec![CdcRow {
            op: "insert",
            values: vec![
                Some(Cell::String("not-an-int".into())),
                None,
                None,
                None,
                None,
                None,
            ],
        }];
        let batch = rows_to_record_batch(schema(), rows).unwrap();
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn empty_rows_build_empty_batch() {
        let batch = rows_to_record_batch(schema(), vec![]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 7);
    }

    #[test]
    fn explicit_null_cells_become_null() {
        let rows = vec![CdcRow {
            op: "insert",
            values: vec![Some(Cell::I64(2)), Some(Cell::Null), None, None, None, None],
        }];
        let batch = rows_to_record_batch(schema(), rows).unwrap();
        assert!(batch.column(2).is_null(0));
    }
}
