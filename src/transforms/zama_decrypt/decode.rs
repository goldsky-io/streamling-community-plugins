//! Reads decoded ERC-7984 transfers from the columns an upstream
//! `erc7984_decode` step appended (`erc7984_token`, `erc7984_from`,
//! `erc7984_to`, `erc7984_amount_handle`), or from any columns with the same
//! content: `0x` hex addresses and a 32-byte `0x` hex handle.

use super::config::PLUGIN_NAME;
use crate::utils::text_column::{TextColumn, text_column, validate_text_columns};
use alloy_primitives::{Address, B256};
use arrow::array::RecordBatch;
use arrow_schema::Schema;
use std::collections::HashSet;
use streamling_plugin::PluginError;

/// Input column names; the values are `0x` hex strings.
#[derive(Clone, Debug)]
pub struct TransferColumns {
    pub token: String,
    pub from: String,
    pub to: String,
    pub handle: String,
}

impl TransferColumns {
    fn names(&self) -> [&str; 4] {
        [&self.token, &self.from, &self.to, &self.handle]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedTransfer {
    pub token: Address,
    pub from: Address,
    pub to: Address,
    pub handle: B256,
}

/// One entry per input row. `None` means the row is not a transfer of an
/// accepted token, or one of its fields is null or malformed.
pub fn decode_batch(
    batch: &RecordBatch,
    columns: &TransferColumns,
    tokens: &HashSet<Address>,
) -> Result<Vec<Option<DecodedTransfer>>, PluginError> {
    let [token, from, to, handle] = columns.names();
    let token = text_column(batch, token, PLUGIN_NAME)?;
    let from = text_column(batch, from, PLUGIN_NAME)?;
    let to = text_column(batch, to, PLUGIN_NAME)?;
    let handle = text_column(batch, handle, PLUGIN_NAME)?;
    Ok((0..batch.num_rows())
        .map(|row| decode_row(row, &token, &from, &to, &handle, tokens))
        .collect())
}

/// Run at startup so a column typo fails initialization rather than the first batch.
pub fn validate_columns(schema: &Schema, columns: &TransferColumns) -> Result<(), String> {
    validate_text_columns(schema, columns.names())
}

fn decode_row(
    row: usize,
    token: &TextColumn,
    from: &TextColumn,
    to: &TextColumn,
    handle: &TextColumn,
    tokens: &HashSet<Address>,
) -> Option<DecodedTransfer> {
    let token = token.value(row)?.trim().parse::<Address>().ok()?;
    if !tokens.is_empty() && !tokens.contains(&token) {
        return None;
    }
    Some(DecodedTransfer {
        token,
        from: from.value(row)?.trim().parse::<Address>().ok()?,
        to: to.value(row)?.trim().parse::<Address>().ok()?,
        handle: handle.value(row)?.trim().parse::<B256>().ok()?,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray, StringViewArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    pub const TOKEN: &str = "0x1111111111111111111111111111111111111111";
    pub const FROM: &str = "0x00000000000000000000000000000000000000aa";
    pub const TO: &str = "0x00000000000000000000000000000000000000bb";
    pub const HANDLE: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const NAMES: [&str; 4] = [
        "erc7984_token",
        "erc7984_from",
        "erc7984_to",
        "erc7984_amount_handle",
    ];

    pub fn columns() -> TransferColumns {
        TransferColumns {
            token: NAMES[0].into(),
            from: NAMES[1].into(),
            to: NAMES[2].into(),
            handle: NAMES[3].into(),
        }
    }

    /// Rows are (token, from, to, handle).
    pub fn batch(rows: &[[Option<&str>; 4]]) -> RecordBatch {
        let fields: Vec<Field> = NAMES
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();
        let arrays: Vec<ArrayRef> = (0..4)
            .map(|column| {
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row[column]).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
    }

    pub fn transfer() -> DecodedTransfer {
        DecodedTransfer {
            token: TOKEN.parse().expect("token"),
            from: FROM.parse().expect("from"),
            to: TO.parse().expect("to"),
            handle: HANDLE.parse().expect("handle"),
        }
    }

    #[test]
    fn decodes_a_transfer_and_skips_incomplete_rows() {
        let batch = batch(&[
            [Some(TOKEN), Some(FROM), Some(TO), Some(HANDLE)],
            [None, None, None, None],
            [Some(TOKEN), None, Some(TO), Some(HANDLE)],
            [Some("nonsense"), Some(FROM), Some(TO), Some(HANDLE)],
            [Some(TOKEN), Some(FROM), Some(TO), Some("0x1234")],
        ]);
        let decoded = decode_batch(&batch, &columns(), &HashSet::new()).expect("decode");
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], Some(transfer()));
        assert!(decoded[1..].iter().all(Option::is_none), "{decoded:?}");
    }

    #[test]
    fn utf8_view_columns_are_accepted() {
        let fields: Vec<Field> = NAMES
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8View, true))
            .collect();
        let arrays: Vec<ArrayRef> = [TOKEN, FROM, TO, HANDLE]
            .iter()
            .map(|value| Arc::new(StringViewArray::from(vec![Some(*value), None])) as ArrayRef)
            .collect();
        let schema = Arc::new(Schema::new(fields));
        validate_columns(&schema, &columns()).expect("view columns validate");
        let batch = RecordBatch::try_new(schema, arrays).expect("view batch");
        let decoded = decode_batch(&batch, &columns(), &HashSet::new()).expect("decode");
        assert_eq!(decoded[0], Some(transfer()));
        assert!(decoded[1].is_none());
    }

    #[test]
    fn token_allowlist_filters_contracts() {
        let batch = batch(&[[Some(TOKEN), Some(FROM), Some(TO), Some(HANDLE)]]);
        let allowed = HashSet::from(["0x2222222222222222222222222222222222222222"
            .parse::<Address>()
            .expect("address")]);
        let decoded = decode_batch(&batch, &columns(), &allowed).expect("decode");
        assert!(decoded[0].is_none());
    }

    #[test]
    fn missing_column_is_an_initialization_or_execution_error() {
        let batch = batch(&[]);
        let mut columns = columns();
        columns.token = "contract".into();
        assert!(validate_columns(&batch.schema(), &columns).is_err());
        assert!(matches!(
            decode_batch(&batch, &columns, &HashSet::new()),
            Err(PluginError::Execution(_))
        ));
    }
}
