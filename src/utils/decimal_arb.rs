//! Helpers for Streamling's `decimal_arb` columns (wide integers and
//! arbitrary-precision decimals). Their storage is `LargeBinary` holding
//! canonical bytes, so anything that ignores the field metadata (arrow_json,
//! casts) silently mistakes them for plain binary.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray,
    MapArray, StructArray,
};
use arrow_schema::{ArrowError, DataType, Field};
use streamling_common::types::decimal_arb::{DecimalArbArray, DecimalArbType};

/// Whether a container type holds a `decimal_arb` leaf at any depth.
pub(crate) fn nests_decimal_arb(data_type: &DataType) -> bool {
    let holds =
        |f: &Field| DecimalArbType::is_decimal_arb_field(f) || nests_decimal_arb(f.data_type());
    match data_type {
        DataType::List(c)
        | DataType::LargeList(c)
        | DataType::ListView(c)
        | DataType::LargeListView(c)
        | DataType::FixedSizeList(c, _)
        | DataType::Map(c, _) => holds(c),
        DataType::Struct(children) => children.iter().any(|c| holds(c)),
        DataType::Union(children, _) => children.iter().any(|(_, c)| holds(c)),
        _ => false,
    }
}

/// A `decimal_arb` column as exact decimal strings at the column's scale.
pub(crate) fn decimal_arb_text(field: &Field, array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let err = |e: &dyn std::fmt::Display| {
        ArrowError::InvalidArgumentError(format!("decimal_arb field '{}': {e}", field.name()))
    };
    let values = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| err(&"expected LargeBinary values"))?;
    let values =
        DecimalArbArray::try_from_array_and_field(values.clone(), field).map_err(|e| err(&e))?;
    Ok(Arc::new(values.to_string_array().map_err(|e| err(&e))?))
}

/// Rewrites every `decimal_arb` leaf of `(field, array)`, top-level or nested
/// in structs, lists and maps, to Utf8 decimal text. Columns without one are
/// returned unchanged.
pub(crate) fn decimal_arb_leaves_as_text(
    field: &Field,
    array: &ArrayRef,
) -> Result<(Field, ArrayRef), ArrowError> {
    if DecimalArbType::is_decimal_arb_field(field) {
        let text = decimal_arb_text(field, array)?;
        return Ok((
            Field::new(field.name(), DataType::Utf8, field.is_nullable()),
            text,
        ));
    }
    if !nests_decimal_arb(field.data_type()) {
        return Ok((field.clone(), array.clone()));
    }
    let child = |c: &Arc<Field>, values: &ArrayRef| {
        decimal_arb_leaves_as_text(c, values).map(|(f, v)| (Arc::new(f), v))
    };
    let array: ArrayRef = match field.data_type() {
        DataType::Struct(children) => {
            let array = array.as_struct();
            let (fields, columns): (Vec<_>, Vec<_>) = children
                .iter()
                .zip(array.columns())
                .map(|(c, values)| child(c, values))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            Arc::new(StructArray::try_new(
                fields.into(),
                columns,
                array.nulls().cloned(),
            )?)
        }
        DataType::List(c) => {
            let array = array.as_list::<i32>();
            let (c, values) = child(c, array.values())?;
            Arc::new(ListArray::try_new(
                c,
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?)
        }
        DataType::LargeList(c) => {
            let array = array.as_list::<i64>();
            let (c, values) = child(c, array.values())?;
            Arc::new(LargeListArray::try_new(
                c,
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?)
        }
        DataType::FixedSizeList(c, size) => {
            let array = array.as_fixed_size_list();
            let (c, values) = child(c, array.values())?;
            Arc::new(FixedSizeListArray::try_new(
                c,
                *size,
                values,
                array.nulls().cloned(),
            )?)
        }
        DataType::Map(entries, sorted) => {
            let array = array.as_map();
            let (entries, values) = child(entries, &(Arc::new(array.entries().clone()) as _))?;
            Arc::new(MapArray::try_new(
                entries,
                array.offsets().clone(),
                values.as_struct().clone(),
                array.nulls().cloned(),
                *sorted,
            )?)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "field '{}': decimal_arb nested in {other} cannot be written as text; \
                 CAST it to TEXT upstream",
                field.name()
            )));
        }
    };
    Ok((
        Field::new(field.name(), array.data_type().clone(), field.is_nullable())
            .with_metadata(field.metadata().clone()),
        array,
    ))
}
