//! Utilities for converting Arrow RecordBatches to JSON suitable for sinks.
//!
//! Provides [`record_batch_to_line_delimited_json`] which converts a batch to
//! newline-delimited JSON rows. Top-level `decimal_arb` columns (wide integers
//! and arbitrary-precision decimals) are serialized as exact decimal strings;
//! arrow_json would print their canonical bytes as hex.

use arrow::array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_json::LineDelimitedWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use bytes::Bytes;
use std::sync::Arc;
use streamling_common::types::decimal_arb::{DecimalArbArray, DecimalArbType};
use streamling_common::types::decimal_arb_legacy::upgrade_legacy_wide_int_batch;

fn decimal_arb_as_text(array: &ArrayRef, field: &Field) -> Result<ArrayRef, ArrowError> {
    let err = |e: &dyn std::fmt::Display| {
        ArrowError::InvalidArgumentError(format!("decimal_arb column '{}': {e}", field.name()))
    };
    let values = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| err(&"expected LargeBinary values"))?;
    let values =
        DecimalArbArray::try_from_array_and_field(values.clone(), field).map_err(|e| err(&e))?;
    Ok(Arc::new(values.to_string_array().map_err(|e| err(&e))?))
}

fn decimal_arb_columns_as_text(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    // A plugin source may still emit the retired FixedSizeBinary(32) u256/i256 columns.
    let upgraded = upgrade_legacy_wide_int_batch(batch)
        .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
    let batch = upgraded.as_ref().unwrap_or(batch);
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| DecimalArbType::is_decimal_arb_field(f))
    {
        return Ok(batch.clone());
    }

    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if DecimalArbType::is_decimal_arb_field(field) {
            columns.push(decimal_arb_as_text(column, field)?);
            fields.push(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ));
        } else {
            columns.push(column.clone());
            fields.push(field.as_ref().clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Convert a RecordBatch to newline-delimited JSON, one JSON object per row.
///
/// `decimal_arb` columns are serialized as decimal strings (e.g.
/// `"12345678901234567890"`). Rows are zero-copy slices of one shared buffer.
pub fn record_batch_to_line_delimited_json(batch: &RecordBatch) -> Result<Vec<Bytes>, ArrowError> {
    let transformed = decimal_arb_columns_as_text(batch)?;

    let mut json_buffer = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut json_buffer);
    writer.write(&transformed)?;
    writer.finish()?;
    let json_buffer = Bytes::from(json_buffer);

    Ok(json_buffer
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| json_buffer.slice_ref(line))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Int64Array};
    use streamling_common::types::decimal_arb::DecimalArbArrayBuilder;

    fn rows(batch: &RecordBatch) -> Vec<String> {
        record_batch_to_line_delimited_json(batch)
            .unwrap()
            .iter()
            .map(|row| String::from_utf8(row.to_vec()).unwrap())
            .collect()
    }

    #[test]
    fn decimal_arb_columns_print_exact_decimal_text() {
        let field = DecimalArbType::field("amount", 80, 2, true).unwrap();
        let mut builder = DecimalArbArrayBuilder::with_capacity(2, "amount", 80, 2).unwrap();
        builder
            .append_str("-123456789012345678901234567890.5")
            .unwrap();
        builder.append_null();
        let amounts = builder.finish().into_inner().0;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                field,
            ])),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(amounts)],
        )
        .unwrap();
        assert_eq!(
            rows(&batch),
            [
                r#"{"id":1,"amount":"-123456789012345678901234567890.50"}"#,
                r#"{"id":2}"#
            ]
        );
    }

    #[test]
    fn legacy_wide_int_columns_print_decimal_text() {
        let field = Field::new("v", DataType::FixedSizeBinary(32), false).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "streamling.i256".to_string(),
            )]
            .into(),
        );
        let mut minus_five = [0xff; 32];
        minus_five[31] = 0xfb;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter([minus_five].into_iter()).unwrap(),
            )],
        )
        .unwrap();
        assert_eq!(rows(&batch), [r#"{"v":"-5"}"#]);
    }
}
