//! Read-only access to a string column in any of Arrow's string encodings.

use arrow::array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Schema};
use streamling_plugin::PluginError;

/// All three Arrow string encodings arrive in practice: the CSV reader yields
/// Utf8, other sources LargeUtf8, and DataFusion string functions in an
/// upstream SQL node Utf8View.
pub enum TextColumn {
    Small(StringArray),
    Large(LargeStringArray),
    View(StringViewArray),
}

impl TextColumn {
    pub fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Small(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Large(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::View(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }

    pub fn row_count(&self) -> usize {
        match self {
            Self::Small(array) => array.len(),
            Self::Large(array) => array.len(),
            Self::View(array) => array.len(),
        }
    }
}

pub fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

impl TryFrom<&ArrayRef> for TextColumn {
    /// The column's type when it is not a string column.
    type Error = DataType;

    fn try_from(column: &ArrayRef) -> Result<Self, DataType> {
        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Small(array.clone()));
        }
        if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Self::Large(array.clone()));
        }
        if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
            return Ok(Self::View(array.clone()));
        }
        Err(column.data_type().clone())
    }
}

/// Run at startup so a column typo fails initialization rather than the first batch.
pub fn validate_text_columns<'a>(
    schema: &Schema,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    for name in names {
        let field = schema
            .field_with_name(name)
            .map_err(|_| format!("input has no '{name}' column"))?;
        if !is_text(field.data_type()) {
            return Err(format!(
                "column '{name}' must be a string column, got {}",
                field.data_type()
            ));
        }
    }
    Ok(())
}

pub fn text_column(
    batch: &RecordBatch,
    name: &str,
    plugin: &str,
) -> Result<TextColumn, PluginError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| PluginError::Execution(format!("{plugin}: missing column '{name}'")))?;
    TextColumn::try_from(batch.column(index)).map_err(|data_type| {
        PluginError::Execution(format!(
            "{plugin}: column '{name}' must be a string column, got {data_type}"
        ))
    })
}
