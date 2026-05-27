//! Utilities for converting Arrow RecordBatches to JSON suitable for sinks.
//!
//! Provides [`record_batch_to_line_delimited_json`] which converts a batch to
//! newline-delimited JSON rows. U256 columns (FixedSizeBinary(32) with streamling.u256
//! metadata) are serialized as decimal strings instead of hex, for compatibility with
//! streamling-core and downstream consumers.

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, StringArray};
use arrow_json::LineDelimitedWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use std::sync::Arc;

#[allow(clippy::manual_div_ceil)]
mod u256_impl {
    use uint::construct_uint;
    construct_uint! {
        pub struct U256(4);
    }
}

const U256_EXTENSION_NAME: &str = "streamling.u256";
const U256_METADATA_KEY: &str = "ARROW:extension:name";

fn is_u256_field(field: &Field) -> bool {
    matches!(field.data_type(), DataType::FixedSizeBinary(32))
        && field
            .metadata()
            .get(U256_METADATA_KEY)
            .map(|v| v == U256_EXTENSION_NAME)
            .unwrap_or(false)
}

fn transform_u256_columns(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let has_u256 = batch
        .schema()
        .fields()
        .iter()
        .any(|f| is_u256_field(f.as_ref()));
    if !has_u256 {
        return Ok(batch.clone());
    }

    let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let field_ref = field.as_ref();
        if is_u256_field(field_ref) {
            let col = batch.column(idx);
            let fsb = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "Expected FixedSizeBinaryArray for U256 field '{}'",
                        field_ref.name()
                    ))
                })?;

            let mut string_values: Vec<Option<String>> = Vec::with_capacity(fsb.len());
            for row_idx in 0..fsb.len() {
                if fsb.is_null(row_idx) {
                    string_values.push(None);
                } else {
                    let bytes = fsb.value(row_idx);
                    let mut fixed: [u8; 32] = [0u8; 32];
                    fixed.copy_from_slice(bytes);
                    let val = u256_impl::U256::from_big_endian(&fixed);
                    string_values.push(Some(val.to_string()));
                }
            }

            let string_array = StringArray::from(string_values);
            new_columns.push(Arc::new(string_array) as ArrayRef);
            new_fields.push(Field::new(
                field_ref.name(),
                DataType::Utf8,
                field_ref.is_nullable(),
            ));
        } else {
            new_columns.push(batch.column(idx).clone());
            new_fields.push(field_ref.clone());
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Convert a RecordBatch to newline-delimited JSON, one JSON object per row.
///
/// U256 columns are serialized as decimal strings (e.g. `"12345678901234567890"`)
/// instead of hex. Returns a vector of byte slices, one per row.
pub fn record_batch_to_line_delimited_json(
    batch: &RecordBatch,
) -> Result<Vec<Vec<u8>>, ArrowError> {
    let transformed = transform_u256_columns(batch)?;

    let mut json_buffer = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut json_buffer);
    writer.write(&transformed)?;
    writer.finish()?;

    let rows: Vec<Vec<u8>> = json_buffer
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.to_vec())
        .collect();

    Ok(rows)
}
