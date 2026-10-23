//! Arrow ⇄ Iceberg schema handling for the Iceberg sink: deriving a table
//! schema from the input schema (auto-create), and projecting input batches
//! onto an existing table's schema by column name.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions};
use arrow::array::{StringArray, StringBuilder, new_null_array};
use arrow::compute::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use iceberg::spec::Schema as IcebergSchema;
use streamling_common::types::i256::{I256Type, bytes_to_i256, i256_to_string};
use streamling_common::types::u256::{U256Type, bytes_to_u256};
use streamling_plugin::PluginError;
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP;

use crate::utils::iceberg::iceberg_error;

/// Fails unless every row's `_gs_op` is an insert (`i`). An append-only
/// table cannot apply updates or deletes, and appending them as rows would
/// silently corrupt the table. A batch without `_gs_op` is all inserts.
pub(crate) fn ensure_inserts_only(batch: &RecordBatch) -> Result<(), PluginError> {
    let Some(column) = batch.column_by_name(STREAMLING_COLUMN_NAME_OP) else {
        return Ok(());
    };
    let ops = cast(column, &DataType::Utf8).map_err(PluginError::ArrowError)?;
    let ops = ops.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        PluginError::Internal(format!(
            "'{STREAMLING_COLUMN_NAME_OP}' is not a string column"
        ))
    })?;
    let mut rejected = ops.iter().filter(|op| *op != Some("i"));
    if let Some(first) = rejected.next() {
        return Err(PluginError::Internal(format!(
            "iceberg_sink: received {} non-insert row(s) (first {STREAMLING_COLUMN_NAME_OP}={:?}); \
             the sink appends only and cannot apply updates or deletes. Set \
             include_op_column=true to append the changelog with its {STREAMLING_COLUMN_NAME_OP} \
             column and apply it downstream (MERGE)",
            rejected.count() + 1,
            first.unwrap_or("null"),
        )));
    }
    Ok(())
}

/// The input columns the sink writes: `_gs_op` only with `include_op_column`.
fn written_fields(input: &ArrowSchema, include_op_column: bool) -> impl Iterator<Item = &Field> {
    input
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .filter(move |f| include_op_column || f.name() != STREAMLING_COLUMN_NAME_OP)
}

/// Derives the Iceberg schema of an auto-created table from the input
/// schema, after mapping types Iceberg cannot store (see [`storable_field`]).
pub(crate) fn table_schema_for(
    input: &ArrowSchema,
    include_op_column: bool,
) -> Result<IcebergSchema, PluginError> {
    let fields = written_fields(input, include_op_column)
        .map(storable_field)
        .collect::<Result<Vec<_>, _>>()?;
    if fields.is_empty() {
        return Err(PluginError::Internal(
            "iceberg_sink: the input schema has no columns to write".to_string(),
        ));
    }
    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&ArrowSchema::new(fields))
        .map_err(|e| iceberg_error("derive the Iceberg schema from the input schema", e))
}

/// Big-endian 256-bit integers stored as exact decimal text.
#[derive(Debug, Clone, Copy, PartialEq)]
enum DecimalText {
    U256,
    I256,
}

fn decimal_text_kind(field: &Field) -> Option<DecimalText> {
    if U256Type::is_u256_field(field) {
        Some(DecimalText::U256)
    } else if I256Type::is_i256_field(field) {
        Some(DecimalText::I256)
    } else {
        None
    }
}

/// Maps an input field to one Iceberg (v1/v2) can store. iceberg-rust's
/// converter already maps UInt8/16 → int, UInt32 → long, LargeUtf8/Utf8View
/// → string, and dictionaries to their value type; this adds the rest:
/// U256/I256 and Decimal256 → string (exact decimal text), UInt64 →
/// decimal(20,0), any timestamp → microseconds (zoned ones normalized to
/// UTC), Date64 → date, Time32/Time64(ns) → time, Float16 → float.
pub(crate) fn storable_field(field: &Field) -> Result<Field, PluginError> {
    let data_type = match decimal_text_kind(field) {
        Some(_) => DataType::Utf8,
        None => storable_type(field.name(), field.data_type())?,
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()))
}

fn storable_child(column: &str, child: &Field) -> Result<Arc<Field>, PluginError> {
    if decimal_text_kind(child).is_some() {
        return Err(PluginError::Internal(format!(
            "iceberg_sink: column '{column}': nested U256/I256 fields are not supported; \
             cast them to strings upstream"
        )));
    }
    Ok(Arc::new(Field::new(
        child.name(),
        storable_type(column, child.data_type())?,
        child.is_nullable(),
    )))
}

fn storable_type(column: &str, data_type: &DataType) -> Result<DataType, PluginError> {
    Ok(match data_type {
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Decimal256(_, _) => DataType::Utf8,
        DataType::Timestamp(_, tz) => DataType::Timestamp(
            TimeUnit::Microsecond,
            tz.as_ref().map(|_| Arc::from("+00:00")),
        ),
        DataType::Date64 => DataType::Date32,
        DataType::Time32(_) | DataType::Time64(_) => DataType::Time64(TimeUnit::Microsecond),
        DataType::Float16 => DataType::Float32,
        DataType::Dictionary(_, value) => storable_type(column, value)?,
        DataType::List(child) => DataType::List(storable_child(column, child)?),
        DataType::LargeList(child) => DataType::LargeList(storable_child(column, child)?),
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(storable_child(column, child)?, *size)
        }
        DataType::Map(entries, sorted) => DataType::Map(storable_child(column, entries)?, *sorted),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| storable_child(column, child))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        DataType::Null => {
            return Err(PluginError::Internal(format!(
                "iceberg_sink: column '{column}' has the Null type, which Iceberg cannot store; \
                 cast it to a concrete type upstream"
            )));
        }
        other => other.clone(),
    })
}

/// Where a table column's values come from.
#[derive(Debug)]
enum ColumnSource {
    /// The input column of the same name.
    Input(String),
    /// Absent from the input; the table column is optional.
    Null,
}

/// Builds batches in a table's Arrow schema (with Iceberg field ids) from
/// input batches, matching columns by name and converting types.
#[derive(Debug)]
pub(crate) struct BatchProjector {
    target: SchemaRef,
    sources: Vec<ColumnSource>,
}

impl BatchProjector {
    /// Validates the input schema against the table's Arrow schema: every
    /// written input column must exist in the table and be convertible to
    /// its type; table columns missing from the input must be optional.
    pub(crate) fn try_new(
        input: &ArrowSchema,
        target: SchemaRef,
        include_op_column: bool,
    ) -> Result<Self, PluginError> {
        let written: Vec<&Field> = written_fields(input, include_op_column).collect();
        let unknown: Vec<&str> = written
            .iter()
            .map(|f| f.name().as_str())
            .filter(|name| target.field_with_name(name).is_err())
            .collect();
        if !unknown.is_empty() {
            return Err(PluginError::Internal(format!(
                "iceberg_sink: input column(s) {unknown:?} do not exist in the table; the sink \
                 does not evolve schemas, so add them to the table (ALTER TABLE ... ADD COLUMN) \
                 or drop them upstream"
            )));
        }

        let mut problems = Vec::new();
        let mut sources = Vec::with_capacity(target.fields().len());
        for target_field in target.fields() {
            let name = target_field.name();
            match written.iter().find(|f| f.name() == name) {
                Some(input_field) => {
                    let to = target_field.data_type();
                    match decimal_text_kind(input_field) {
                        Some(_) if is_u256_target(to) => {}
                        Some(_) => problems.push(format!(
                            "column '{name}': cannot losslessly convert {} to the table \
                             type {to}; U256/I256 columns need a string, binary or fixed(32) \
                             table column",
                            input_field.data_type()
                        )),
                        None => check_conversion(name, input_field.data_type(), to, &mut problems),
                    }
                    sources.push(ColumnSource::Input(name.clone()));
                }
                None if target_field.is_nullable() => sources.push(ColumnSource::Null),
                None => problems.push(format!(
                    "required table column '{name}' is missing from the input"
                )),
            }
        }
        if !problems.is_empty() {
            return Err(PluginError::Internal(format!(
                "iceberg_sink: the input schema does not match the table: {}",
                problems.join("; ")
            )));
        }
        Ok(Self { target, sources })
    }

    pub(crate) fn project(&self, batch: &RecordBatch) -> Result<RecordBatch, PluginError> {
        let rows = batch.num_rows();
        let schema = batch.schema();
        let columns = self
            .target
            .fields()
            .iter()
            .zip(&self.sources)
            .map(|(target_field, source)| match source {
                ColumnSource::Null => Ok(new_null_array(target_field.data_type(), rows)),
                ColumnSource::Input(name) => {
                    let index = schema.index_of(name).map_err(|_| {
                        PluginError::Internal(format!(
                            "iceberg_sink: batch is missing input column '{name}'"
                        ))
                    })?;
                    convert_column(batch.column(index), schema.field(index), target_field)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new_with_options(
            self.target.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| {
            PluginError::Internal(format!(
                "iceberg_sink: invalid batch for the table schema: {e}"
            ))
        })
    }
}

/// Table types a U256/I256 column can be written to exactly: decimal text
/// for strings, the raw 32 big-endian bytes for binary and fixed(32).
fn is_u256_target(to: &DataType) -> bool {
    matches!(
        to,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(32)
    )
}

/// Records a problem unless `from` converts losslessly to `to`. Struct
/// fields match by name; list elements and map keys/values recurse.
fn check_conversion(path: &str, from: &DataType, to: &DataType, problems: &mut Vec<String>) {
    use DataType::*;
    if from == to {
        return;
    }
    match (from, to) {
        (Dictionary(_, value), _) => check_conversion(path, value, to, problems),
        (Struct(from_children), Struct(to_children)) => {
            for child in from_children {
                let child_path = format!("{path}.{}", child.name());
                match to_children.find(child.name()) {
                    Some((_, to_child)) => check_conversion(
                        &child_path,
                        child.data_type(),
                        to_child.data_type(),
                        problems,
                    ),
                    None => problems.push(format!(
                        "nested input field '{child_path}' does not exist in the table"
                    )),
                }
            }
            for to_child in to_children {
                if !to_child.is_nullable() && from_children.find(to_child.name()).is_none() {
                    problems.push(format!(
                        "required nested table field '{path}.{}' is missing from the input",
                        to_child.name()
                    ));
                }
            }
        }
        (
            List(from_child) | LargeList(from_child) | FixedSizeList(from_child, _),
            List(to_child) | LargeList(to_child),
        ) => check_conversion(
            &format!("{path}.element"),
            from_child.data_type(),
            to_child.data_type(),
            problems,
        ),
        (Map(from_entries, _), Map(to_entries, _)) => {
            // By position: Arrow names them keys/values, Iceberg key/value.
            if let (Struct(from_kv), Struct(to_kv)) =
                (from_entries.data_type(), to_entries.data_type())
                && from_kv.len() == 2
                && to_kv.len() == 2
            {
                for (i, part) in ["key", "value"].into_iter().enumerate() {
                    check_conversion(
                        &format!("{path}.{part}"),
                        from_kv[i].data_type(),
                        to_kv[i].data_type(),
                        problems,
                    );
                }
            } else {
                problems.push(format!("column '{path}': malformed map type"));
            }
        }
        _ if is_lossless(from, to) && can_cast_types(from, to) => {}
        _ => problems.push(format!(
            "column '{path}': cannot losslessly convert {from} to the table type {to}"
        )),
    }
}

/// Exact digits needed for every value of an integer type.
fn integer_digits(dt: &DataType) -> Option<u8> {
    use DataType::*;
    Some(match dt {
        Int8 | UInt8 => 3,
        Int16 | UInt16 => 5,
        Int32 | UInt32 => 10,
        Int64 => 19,
        UInt64 => 20,
        _ => return None,
    })
}

/// (signed, bits) of an integer type.
fn integer_width(dt: &DataType) -> Option<(bool, u8)> {
    use DataType::*;
    Some(match dt {
        Int8 => (true, 8),
        Int16 => (true, 16),
        Int32 => (true, 32),
        Int64 => (true, 64),
        UInt8 => (false, 8),
        UInt16 => (false, 16),
        UInt32 => (false, 32),
        UInt64 => (false, 64),
        _ => return None,
    })
}

/// Whether a unit change keeps every value; ns → µs (truncating sub-µs
/// digits) is the documented exception, as Iceberg v1/v2 store µs.
fn unit_ok(from: &TimeUnit, to: &TimeUnit) -> bool {
    let rank = |u: &TimeUnit| match u {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    };
    rank(from) <= rank(to) || (*from == TimeUnit::Nanosecond && *to == TimeUnit::Microsecond)
}

/// The allowlist of leaf conversions that preserve every value.
fn is_lossless(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    if let (Some((fs, fb)), Some((ts, tb))) = (integer_width(from), integer_width(to)) {
        return match (fs, ts) {
            (true, true) | (false, false) => fb <= tb,
            (false, true) => fb < tb,
            (true, false) => false,
        };
    }
    match (from, to) {
        (_, Decimal128(p, s)) if integer_digits(from).is_some() => {
            integer_digits(from).is_some_and(|d| i16::from(*p) - i16::from(*s) >= i16::from(d))
        }
        (Float16 | Float32, Float32 | Float64) | (Float64, Float64) => true,
        (Decimal128(fp, fs) | Decimal256(fp, fs), Decimal128(tp, ts)) => fs == ts && tp >= fp,
        (Decimal256(_, _), Utf8 | LargeUtf8) => true,
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View) => true,
        (
            Binary | LargeBinary | BinaryView | FixedSizeBinary(_),
            Binary | LargeBinary | BinaryView,
        ) => true,
        (Date32 | Date64, Date32) => true,
        // Same stored instants: zones normalize to UTC, tz-less values are UTC.
        (Timestamp(f, _), Timestamp(t, _)) => unit_ok(f, t),
        _ => false,
    }
}

/// Converts one input column to the table column's type. Conversion is
/// strict: values that overflow or fail to parse fail the batch instead of
/// becoming nulls.
fn convert_column(
    array: &ArrayRef,
    input_field: &Field,
    target_field: &Field,
) -> Result<ArrayRef, PluginError> {
    let column_error = |e: &dyn std::fmt::Display| {
        PluginError::Internal(format!(
            "iceberg_sink: column '{}': cannot convert {} to {}: {e}",
            input_field.name(),
            input_field.data_type(),
            target_field.data_type()
        ))
    };
    let array = match decimal_text_kind(input_field) {
        Some(kind)
            if matches!(
                target_field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8
            ) =>
        {
            decimal_text(array, kind).map_err(|e| column_error(&e))?
        }
        _ => array.clone(),
    };
    convert_array(&array, target_field.data_type()).map_err(|e| column_error(&e))
}

/// Casts `array` to `to`, building structs by field name (missing optional
/// fields become nulls) and recursing through lists and maps so nested
/// target fields keep their Iceberg field ids.
fn convert_array(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, arrow::error::ArrowError> {
    use arrow::array::{AsArray, LargeListArray, ListArray, MapArray, StructArray};
    let strict = CastOptions {
        safe: false,
        ..Default::default()
    };
    let from = array.data_type();
    if from == to {
        return Ok(array.clone());
    }
    match (from, to) {
        (DataType::Dictionary(_, value), DataType::Struct(_) | DataType::List(_))
        | (DataType::Dictionary(_, value), DataType::LargeList(_) | DataType::Map(_, _)) => {
            convert_array(&cast_with_options(array, value, &strict)?, to)
        }
        (DataType::Struct(_), DataType::Struct(to_children)) => {
            let input = array.as_struct();
            let columns = to_children
                .iter()
                .map(|child| match input.column_by_name(child.name()) {
                    Some(column) => convert_array(column, child.data_type()),
                    None => Ok(new_null_array(child.data_type(), input.len())),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(
                to_children.clone(),
                columns,
                input.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(to_child)) => {
            let list = array.as_list::<i32>();
            let values = convert_array(list.values(), to_child.data_type())?;
            Ok(Arc::new(ListArray::try_new(
                to_child.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(to_child)) => {
            let list = array.as_list::<i64>();
            let values = convert_array(list.values(), to_child.data_type())?;
            Ok(Arc::new(LargeListArray::try_new(
                to_child.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (
            DataType::List(from_child)
            | DataType::LargeList(from_child)
            | DataType::FixedSizeList(from_child, _),
            DataType::List(_) | DataType::LargeList(_),
        ) => {
            // Change only the container (offset width) first, then recurse.
            let container = match to {
                DataType::List(_) => DataType::List(from_child.clone()),
                _ => DataType::LargeList(from_child.clone()),
            };
            convert_array(&cast_with_options(array, &container, &strict)?, to)
        }
        (DataType::Map(_, _), DataType::Map(to_entries, ordered)) => {
            let map = array.as_map();
            let DataType::Struct(to_kv) = to_entries.data_type() else {
                return Err(arrow::error::ArrowError::CastError(
                    "map entries are not a struct".to_string(),
                ));
            };
            let entries = map.entries();
            let columns = to_kv
                .iter()
                .zip(entries.columns())
                .map(|(child, column)| convert_array(column, child.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            let entries = StructArray::try_new(to_kv.clone(), columns, entries.nulls().cloned())?;
            Ok(Arc::new(MapArray::try_new(
                to_entries.clone(),
                map.offsets().clone(),
                entries,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => cast_with_options(array, to, &strict),
    }
}

/// Renders big-endian 256-bit integers as exact decimal strings.
fn decimal_text(array: &ArrayRef, kind: DecimalText) -> Result<ArrayRef, String> {
    let values = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or("expected FixedSizeBinary(32) values")?;
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 20);
    for value in values.iter() {
        let Some(bytes) = value else {
            builder.append_null();
            continue;
        };
        let bytes: &[u8; 32] = bytes.try_into().map_err(|_| "expected 32-byte values")?;
        match kind {
            DecimalText::U256 => builder.append_value(bytes_to_u256(bytes).to_string()),
            DecimalText::I256 => builder.append_value(i256_to_string(&bytes_to_i256(bytes))),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal128Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, UInt64Array,
    };
    use iceberg::spec::{PrimitiveType, Type};

    fn u256_field(name: &str, kind: DecimalText) -> Field {
        let metadata = match kind {
            DecimalText::U256 => U256Type::metadata(),
            DecimalText::I256 => I256Type::metadata(),
        };
        Field::new(name, DataType::FixedSizeBinary(32), true).with_metadata(metadata)
    }

    fn be_bytes(value: u128, negative: bool) -> [u8; 32] {
        let mut bytes = [if negative { 0xff } else { 0 }; 32];
        let v = if negative {
            (value as i128).wrapping_neg() as u128
        } else {
            value
        };
        bytes[16..].copy_from_slice(&v.to_be_bytes());
        bytes
    }

    fn op_batch(ops: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(ops.to_vec())),
                Arc::new(Int64Array::from_iter_values(0..ops.len() as i64)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn derives_table_schema_with_normalized_types() {
        let input = ArrowSchema::new(vec![
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
            Field::new("id", DataType::UInt64, false),
            u256_field("amount", DecimalText::U256),
            u256_field("delta", DecimalText::I256),
            Field::new("big", DataType::Decimal256(60, 2), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "ts_ny",
                DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
                true,
            ),
            Field::new(
                "tag",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
                true,
            ),
            Field::new("small", DataType::UInt16, true),
        ]);
        let schema = table_schema_for(&input, false).unwrap();
        let types: Vec<(String, Type, bool)> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), (*f.field_type).clone(), f.required))
            .collect();
        let p = Type::Primitive;
        assert_eq!(
            types,
            vec![
                (
                    "id".into(),
                    p(PrimitiveType::Decimal {
                        precision: 20,
                        scale: 0
                    }),
                    true
                ),
                ("amount".into(), p(PrimitiveType::String), false),
                ("delta".into(), p(PrimitiveType::String), false),
                ("big".into(), p(PrimitiveType::String), false),
                ("ts".into(), p(PrimitiveType::Timestamp), false),
                ("ts_ny".into(), p(PrimitiveType::Timestamptz), false),
                ("tag".into(), p(PrimitiveType::String), false),
                ("small".into(), p(PrimitiveType::Int), false),
            ]
        );
        assert!(
            table_schema_for(&input, true)
                .unwrap()
                .field_by_name(STREAMLING_COLUMN_NAME_OP)
                .is_some()
        );
    }

    #[test]
    fn rejects_unstorable_types() {
        let nested_u256 = ArrowSchema::new(vec![Field::new(
            "amounts",
            DataType::List(Arc::new(u256_field("item", DecimalText::U256))),
            true,
        )]);
        let err = table_schema_for(&nested_u256, false).unwrap_err();
        assert!(err.to_string().contains("amounts"), "got {err}");

        let null = ArrowSchema::new(vec![Field::new("nothing", DataType::Null, true)]);
        let err = table_schema_for(&null, false).unwrap_err();
        assert!(err.to_string().contains("nothing"), "got {err}");
    }

    #[test]
    fn ensures_inserts_only() {
        assert!(ensure_inserts_only(&op_batch(&["i", "i"])).is_ok());
        let err = ensure_inserts_only(&op_batch(&["i", "u", "d"])).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("2 non-insert"), "got {msg}");
        assert!(msg.contains("include_op_column"), "got {msg}");
    }

    /// Projects through an auto-created table schema, as the sink does.
    fn projector_for(input: &ArrowSchema, include_op_column: bool) -> BatchProjector {
        let table = table_schema_for(input, include_op_column).unwrap();
        let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(&table).unwrap());
        BatchProjector::try_new(input, target, include_op_column).unwrap()
    }

    #[test]
    fn projection_converts_values_exactly() {
        let input = Arc::new(ArrowSchema::new(vec![
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
            Field::new("id", DataType::UInt64, false),
            u256_field("amount", DecimalText::U256),
            u256_field("delta", DecimalText::I256),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            input.clone(),
            vec![
                Arc::new(StringArray::from(vec!["i", "i"])),
                Arc::new(UInt64Array::from(vec![u64::MAX, 1])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(be_bytes(u128::MAX, false)), None].into_iter(),
                        32,
                    )
                    .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        [be_bytes(5, true), be_bytes(7, false)].into_iter(),
                    )
                    .unwrap(),
                ),
                Arc::new(TimestampMillisecondArray::from(vec![Some(1_500), None])),
            ],
        )
        .unwrap();

        let out = projector_for(&input, false).project(&batch).unwrap();
        assert_eq!(out.num_columns(), 4, "_gs_op is stripped");
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(ids.value_as_string(0), u64::MAX.to_string());
        let amount = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount.value(0), u128::MAX.to_string());
        assert!(amount.is_null(1));
        let delta = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((delta.value(0), delta.value(1)), ("-5", "7"));
        let ts = out
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_500_000);
        // Field ids travel with the projected schema for the Parquet writer.
        assert!(
            out.schema()
                .field(0)
                .metadata()
                .contains_key("PARQUET:field_id")
        );

        let with_op = projector_for(&input, true).project(&batch).unwrap();
        assert_eq!(with_op.schema().field(0).name(), STREAMLING_COLUMN_NAME_OP);
    }

    #[test]
    fn projection_fills_missing_optional_columns_with_nulls() {
        let table = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]);
        let target = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&table_schema_for(&table, false).unwrap())
                .unwrap(),
        );
        // Input in a different order, narrower id type, no `note`.
        let input = Arc::new(ArrowSchema::new(vec![
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            input.clone(),
            vec![
                Arc::new(StringArray::from(vec!["i", "i"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let out = BatchProjector::try_new(&input, target, false)
            .unwrap()
            .project(&batch)
            .unwrap();
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        assert_eq!(out.column(1).null_count(), 2);
    }

    #[test]
    fn projection_rejects_mismatched_schemas() {
        let table = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("required_note", DataType::Utf8, false),
        ]);
        let target = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&table_schema_for(&table, false).unwrap())
                .unwrap(),
        );

        let extra = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("required_note", DataType::Utf8, false),
            Field::new("surprise", DataType::Utf8, true),
        ]);
        let err = BatchProjector::try_new(&extra, target.clone(), false).unwrap_err();
        assert!(err.to_string().contains("surprise"), "got {err}");

        let missing_required = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let err = BatchProjector::try_new(&missing_required, target.clone(), false).unwrap_err();
        assert!(err.to_string().contains("required_note"), "got {err}");

        let incompatible = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "required_note",
                DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
                false,
            ),
        ]);
        let err = BatchProjector::try_new(&incompatible, target, false).unwrap_err();
        assert!(err.to_string().contains("required_note"), "got {err}");
    }

    fn target_for(table: Vec<Field>) -> SchemaRef {
        let table = ArrowSchema::new(table);
        Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&table_schema_for(&table, false).unwrap())
                .unwrap(),
        )
    }

    fn addr(children: &[(&str, bool)]) -> DataType {
        DataType::Struct(
            children
                .iter()
                .map(|(n, nullable)| Field::new(*n, DataType::Utf8, *nullable))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn nested_struct_fields_match_by_name() {
        use arrow::array::{AsArray, ListArray, StructArray};
        let list_of = |dt: DataType| DataType::List(Arc::new(Field::new("item", dt, true)));
        let target = target_for(vec![Field::new(
            "addrs",
            list_of(addr(&[("city", true), ("zip", true)])),
            true,
        )]);

        // Same arity, different names: arrow would pair them by position.
        let renamed = ArrowSchema::new(vec![Field::new(
            "addrs",
            list_of(addr(&[("zip", true), ("street", true)])),
            true,
        )]);
        let err = BatchProjector::try_new(&renamed, target.clone(), false).unwrap_err();
        assert!(
            err.to_string().contains("addrs.element.street"),
            "got {err}"
        );

        let required = target_for(vec![Field::new(
            "a",
            addr(&[("city", false), ("zip", true)]),
            true,
        )]);
        let no_city = ArrowSchema::new(vec![Field::new("a", addr(&[("zip", true)]), true)]);
        let err = BatchProjector::try_new(&no_city, required, false).unwrap_err();
        assert!(err.to_string().contains("a.city"), "got {err}");

        // Subset: zip lands in zip, city is null.
        let input = Arc::new(ArrowSchema::new(vec![Field::new(
            "addrs",
            list_of(addr(&[("zip", true)])),
            true,
        )]));
        let DataType::List(item) = input.field(0).data_type() else {
            unreachable!()
        };
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let structs = StructArray::new(
            fields.clone(),
            vec![Arc::new(StringArray::from(vec!["10115"]))],
            None,
        );
        let list = ListArray::new(
            item.clone(),
            arrow::buffer::OffsetBuffer::from_lengths([1]),
            Arc::new(structs),
            None,
        );
        let batch = RecordBatch::try_new(input.clone(), vec![Arc::new(list)]).unwrap();
        let out = BatchProjector::try_new(&input, target, false)
            .unwrap()
            .project(&batch)
            .unwrap();
        let values = out.column(0).as_list::<i32>().values().as_struct().clone();
        assert!(values.column_by_name("city").unwrap().is_null(0));
        assert_eq!(
            values
                .column_by_name("zip")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "10115"
        );
        // Nested field ids survive for the Parquet writer.
        assert!(
            values.fields()[1]
                .metadata()
                .contains_key("PARQUET:field_id")
        );
    }

    #[test]
    fn rejects_lossy_conversions() {
        for (from, to) in [
            (DataType::Float64, DataType::Int64),
            (DataType::Decimal128(10, 4), DataType::Decimal128(10, 2)),
            (DataType::Decimal128(10, 2), DataType::Decimal128(9, 2)),
            (DataType::Int64, DataType::Float64),
            (DataType::Int64, DataType::Int32),
            (DataType::UInt32, DataType::Int32),
            (DataType::Utf8, DataType::Decimal128(10, 2)),
            (DataType::Decimal128(10, 2), DataType::Float64),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Date32,
            ),
        ] {
            let target = target_for(vec![Field::new("c", to.clone(), true)]);
            let input = ArrowSchema::new(vec![Field::new("c", from.clone(), true)]);
            let err = BatchProjector::try_new(&input, target, false).unwrap_err();
            assert!(
                err.to_string().contains("column 'c'"),
                "{from} -> {to}: got {err}"
            );
        }
        for (from, to) in [
            (DataType::Int16, DataType::Int64),
            (DataType::UInt32, DataType::Int64),
            (DataType::Int64, DataType::Decimal128(19, 0)),
            (DataType::Float32, DataType::Float64),
            (DataType::Decimal128(10, 2), DataType::Decimal128(12, 2)),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            ),
        ] {
            let target = target_for(vec![Field::new("c", to.clone(), true)]);
            let input = ArrowSchema::new(vec![Field::new("c", from.clone(), true)]);
            assert!(
                BatchProjector::try_new(&input, target, false).is_ok(),
                "{from} -> {to}"
            );
        }
    }

    #[test]
    fn u256_into_binary_columns_keeps_raw_bytes() {
        use arrow::array::AsArray;
        let input = Arc::new(ArrowSchema::new(vec![u256_field("v", DecimalText::U256)]));
        let bytes = be_bytes(u128::MAX, false);
        let batch = RecordBatch::try_new(
            input.clone(),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap(),
            )],
        )
        .unwrap();
        for to in [DataType::Binary, DataType::FixedSizeBinary(32)] {
            let target = target_for(vec![Field::new("v", to.clone(), true)]);
            let out = BatchProjector::try_new(&input, target, false)
                .unwrap()
                .project(&batch)
                .unwrap();
            let value = match out.column(0).data_type() {
                DataType::FixedSizeBinary(_) => out.column(0).as_fixed_size_binary().value(0),
                _ => out.column(0).as_binary::<i64>().value(0),
            };
            assert_eq!(value, bytes.as_slice(), "into {to}");
        }
        let target = target_for(vec![Field::new("v", DataType::Decimal128(38, 0), true)]);
        let err = BatchProjector::try_new(&input, target, false).unwrap_err();
        assert!(err.to_string().contains("column 'v'"), "got {err}");
    }
}
