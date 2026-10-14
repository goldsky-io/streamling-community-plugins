//! Decodes ERC-7984 `ConfidentialTransfer(address indexed from, address indexed to,
//! bytes32 indexed amount)` logs from a raw-logs batch. All three parameters are
//! indexed, so the whole event lives in topics 1 to 3 and `data` is empty.

use alloy_primitives::{Address, B256, keccak256};
use arrow::array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use std::collections::HashSet;
use std::sync::LazyLock;
use streamling_plugin::PluginError;

pub static CONFIDENTIAL_TRANSFER_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256("ConfidentialTransfer(address,address,bytes32)"));

/// Column names carrying the raw log fields, all hex strings.
#[derive(Clone, Debug)]
pub struct LogColumns {
    pub address: String,
    pub topics: [String; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedTransfer {
    pub token: Address,
    pub from: Address,
    pub to: Address,
    pub handle: B256,
}

/// One entry per input row. `None` means the row is not a `ConfidentialTransfer`
/// of an accepted token, or one of its fields is null or malformed.
pub fn decode_batch(
    batch: &RecordBatch,
    columns: &LogColumns,
    tokens: &HashSet<Address>,
) -> Result<Vec<Option<DecodedTransfer>>, PluginError> {
    let address = text_column(batch, &columns.address)?;
    let topics = [
        text_column(batch, &columns.topics[0])?,
        text_column(batch, &columns.topics[1])?,
        text_column(batch, &columns.topics[2])?,
        text_column(batch, &columns.topics[3])?,
    ];
    let mut decoded = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        decoded.push(decode_row(row, &address, &topics, tokens));
    }
    Ok(decoded)
}

/// Checks at startup that every configured log column exists and is a string
/// column, so a typo fails initialization rather than the first batch.
pub fn validate_columns(schema: &Schema, columns: &LogColumns) -> Result<(), String> {
    for name in std::iter::once(&columns.address).chain(&columns.topics) {
        let field = schema
            .field_with_name(name)
            .map_err(|_| format!("input has no '{name}' column"))?;
        if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(format!(
                "column '{name}' must be a string column, got {}",
                field.data_type()
            ));
        }
    }
    Ok(())
}

fn decode_row(
    row: usize,
    address: &TextColumn,
    topics: &[TextColumn; 4],
    tokens: &HashSet<Address>,
) -> Option<DecodedTransfer> {
    let topic0 = parse_word(topics[0].value(row)?)?;
    if topic0 != *CONFIDENTIAL_TRANSFER_TOPIC {
        return None;
    }
    let token = address.value(row)?.trim().parse::<Address>().ok()?;
    if !tokens.is_empty() && !tokens.contains(&token) {
        return None;
    }
    Some(DecodedTransfer {
        token,
        from: word_to_address(parse_word(topics[1].value(row)?)?)?,
        to: word_to_address(parse_word(topics[2].value(row)?)?)?,
        handle: parse_word(topics[3].value(row)?)?,
    })
}

fn parse_word(text: &str) -> Option<B256> {
    text.trim().parse::<B256>().ok()
}

/// An address topic is the address left-padded with 12 zero bytes.
fn word_to_address(word: B256) -> Option<Address> {
    word[..12]
        .iter()
        .all(|byte| *byte == 0)
        .then(|| Address::from_slice(&word[12..]))
}

/// Utf8 or LargeUtf8 column accessor; the CSV reader yields the former, other
/// sources may yield the latter.
enum TextColumn {
    Small(StringArray),
    Large(LargeStringArray),
}

impl TextColumn {
    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Small(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Large(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

fn text_column(batch: &RecordBatch, name: &str) -> Result<TextColumn, PluginError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| PluginError::Execution(format!("zama_decrypt: missing column '{name}'")))?;
    let column: &ArrayRef = batch.column(index);
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(TextColumn::Small(array.clone()));
    }
    if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(TextColumn::Large(array.clone()));
    }
    Err(PluginError::Execution(format!(
        "zama_decrypt: column '{name}' must be a string column, got {}",
        column.data_type()
    )))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    pub const TOKEN: &str = "0x1111111111111111111111111111111111111111";
    pub const FROM: &str = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    pub const TO: &str = "0x00000000000000000000000000000000000000000000000000000000000000bb";
    pub const HANDLE: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    pub fn columns() -> LogColumns {
        LogColumns {
            address: "address".into(),
            topics: [
                "topic0".into(),
                "topic1".into(),
                "topic2".into(),
                "topic3".into(),
            ],
        }
    }

    pub fn transfer_topic() -> String {
        format!("{:#x}", *CONFIDENTIAL_TRANSFER_TOPIC)
    }

    /// Rows are (address, topic0, topic1, topic2, topic3); `None` is a null cell.
    pub fn batch(rows: &[[Option<&str>; 5]]) -> RecordBatch {
        let names = ["address", "topic0", "topic1", "topic2", "topic3"];
        let fields: Vec<Field> = names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();
        let arrays: Vec<ArrayRef> = (0..5)
            .map(|column| {
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row[column]).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
    }

    #[test]
    fn topic_matches_solidity_signature() {
        // Reference value from `cast keccak "ConfidentialTransfer(address,address,bytes32)"`.
        assert_eq!(
            transfer_topic(),
            "0x67500e8d0ed826d2194f514dd0d8124f35648ab6e3fb5e6ed867134cffe661e9"
        );
    }

    #[test]
    fn decodes_a_transfer_and_skips_other_rows() {
        let topic = transfer_topic();
        let other = format!("{:#x}", keccak256("Transfer(address,address,uint256)"));
        let batch = batch(&[
            [
                Some(TOKEN),
                Some(&topic),
                Some(FROM),
                Some(TO),
                Some(HANDLE),
            ],
            [
                Some(TOKEN),
                Some(&other),
                Some(FROM),
                Some(TO),
                Some(HANDLE),
            ],
            [Some(TOKEN), Some(&topic), None, Some(TO), Some(HANDLE)],
            [
                Some("nonsense"),
                Some(&topic),
                Some(FROM),
                Some(TO),
                Some(HANDLE),
            ],
        ]);
        let decoded = decode_batch(&batch, &columns(), &HashSet::new()).expect("decode");
        assert_eq!(decoded.len(), 4);
        let first = decoded[0].expect("transfer");
        assert_eq!(first.token, TOKEN.parse::<Address>().expect("token"));
        assert_eq!(
            first.from,
            Address::from_slice(&[0; 19].into_iter().chain([0xaa]).collect::<Vec<_>>())
        );
        assert_eq!(
            first.to,
            Address::from_slice(&[0; 19].into_iter().chain([0xbb]).collect::<Vec<_>>())
        );
        assert_eq!(first.handle, HANDLE.parse::<B256>().expect("handle"));
        assert!(decoded[1..].iter().all(Option::is_none));
    }

    #[test]
    fn token_allowlist_filters_contracts() {
        let topic = transfer_topic();
        let batch = batch(&[[
            Some(TOKEN),
            Some(&topic),
            Some(FROM),
            Some(TO),
            Some(HANDLE),
        ]]);
        let allowed = HashSet::from(["0x2222222222222222222222222222222222222222"
            .parse::<Address>()
            .expect("address")]);
        let decoded = decode_batch(&batch, &columns(), &allowed).expect("decode");
        assert!(decoded[0].is_none());
    }

    #[test]
    fn address_topic_with_dirty_padding_is_rejected() {
        let topic = transfer_topic();
        let dirty = "0x01000000000000000000000000000000000000000000000000000000000000aa";
        let batch = batch(&[[
            Some(TOKEN),
            Some(&topic),
            Some(dirty),
            Some(TO),
            Some(HANDLE),
        ]]);
        let decoded = decode_batch(&batch, &columns(), &HashSet::new()).expect("decode");
        assert!(decoded[0].is_none());
    }

    #[test]
    fn missing_column_is_an_execution_error() {
        let batch = batch(&[]);
        let mut columns = columns();
        columns.address = "contract".into();
        assert!(matches!(
            decode_batch(&batch, &columns, &HashSet::new()),
            Err(PluginError::Execution(_))
        ));
    }
}
