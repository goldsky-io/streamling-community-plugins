//! Decodes the three ERC-7984 events from raw EVM logs. Signatures from
//! OpenZeppelin's `IERC7984`; `euint64` is `bytes32` on the wire.
//!
//! - `ConfidentialTransfer(address indexed from, address indexed to, bytes32 indexed amount)`
//! - `OperatorSet(address indexed holder, address indexed operator, uint48 until)`
//! - `AmountDisclosed(bytes32 indexed encryptedAmount, uint64 amount)`

use crate::utils::text_column::{TextColumn, is_text, text_column};
use alloy_primitives::{Address, B256, U256, keccak256};
use arrow::array::{Array, ArrayRef, LargeListArray, ListArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::collections::HashSet;
use std::sync::LazyLock;
use streamling_plugin::PluginError;

pub const PLUGIN_NAME: &str = "erc7984_decode";

pub static CONFIDENTIAL_TRANSFER_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256("ConfidentialTransfer(address,address,bytes32)"));
pub static OPERATOR_SET_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256("OperatorSet(address,address,uint48)"));
pub static AMOUNT_DISCLOSED_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256("AmountDisclosed(bytes32,uint64)"));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    ConfidentialTransfer {
        token: Address,
        from: Address,
        to: Address,
        amount_handle: B256,
    },
    OperatorSet {
        token: Address,
        holder: Address,
        operator: Address,
        until: u64,
    },
    AmountDisclosed {
        token: Address,
        encrypted_amount: B256,
        amount: u64,
    },
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ConfidentialTransfer { .. } => "ConfidentialTransfer",
            Self::OperatorSet { .. } => "OperatorSet",
            Self::AmountDisclosed { .. } => "AmountDisclosed",
        }
    }

    pub fn token(&self) -> Address {
        match self {
            Self::ConfidentialTransfer { token, .. }
            | Self::OperatorSet { token, .. }
            | Self::AmountDisclosed { token, .. } => *token,
        }
    }
}

#[derive(Clone, Debug)]
pub enum TopicColumns {
    /// One string column per topic, null when the log has fewer topics.
    Separate([String; 4]),
    /// One column holding all topics: a list of strings, or a single string
    /// with the topics separated by commas (`[` `]` and quotes are ignored).
    Joined(String),
}

#[derive(Clone, Debug)]
pub struct LogColumns {
    pub address: String,
    pub topics: TopicColumns,
    /// Hex string of the ABI-encoded non-indexed parameters.
    pub data: String,
}

/// Run at startup so a column typo fails initialization rather than the first batch.
pub fn validate_columns(schema: &Schema, columns: &LogColumns) -> Result<(), String> {
    let text_columns = match &columns.topics {
        TopicColumns::Separate(topics) => topics.iter().collect::<Vec<_>>(),
        TopicColumns::Joined(_) => Vec::new(),
    };
    for name in [&columns.address, &columns.data]
        .into_iter()
        .chain(text_columns)
    {
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
    if let TopicColumns::Joined(name) = &columns.topics {
        let field = schema
            .field_with_name(name)
            .map_err(|_| format!("input has no '{name}' column"))?;
        let child_is_text = match field.data_type() {
            DataType::List(child) | DataType::LargeList(child) => is_text(child.data_type()),
            other => is_text(other),
        };
        if !child_is_text {
            return Err(format!(
                "column '{name}' must be a string column or a list of strings, got {}",
                field.data_type()
            ));
        }
    }
    Ok(())
}

/// One entry per input row. `None` means the row is not one of the three
/// events of an accepted token, or one of its fields is null or malformed.
pub fn decode_batch(
    batch: &RecordBatch,
    columns: &LogColumns,
    tokens: &HashSet<Address>,
) -> Result<Vec<Option<Event>>, PluginError> {
    let address = text_column(batch, &columns.address, PLUGIN_NAME)?;
    let data = text_column(batch, &columns.data, PLUGIN_NAME)?;
    let topics = Topics::from_batch(batch, &columns.topics)?;
    Ok((0..batch.num_rows())
        .map(|row| decode_row(row, &address, &topics, &data, tokens))
        .collect())
}

enum Topics {
    Separate(Box<[TextColumn; 4]>),
    Joined(TextColumn),
    List(ListArray),
    LargeList(LargeListArray),
}

impl Topics {
    fn from_batch(batch: &RecordBatch, columns: &TopicColumns) -> Result<Self, PluginError> {
        match columns {
            TopicColumns::Separate([t0, t1, t2, t3]) => Ok(Self::Separate(Box::new([
                text_column(batch, t0, PLUGIN_NAME)?,
                text_column(batch, t1, PLUGIN_NAME)?,
                text_column(batch, t2, PLUGIN_NAME)?,
                text_column(batch, t3, PLUGIN_NAME)?,
            ]))),
            TopicColumns::Joined(name) => {
                let index = batch.schema().index_of(name).map_err(|_| {
                    PluginError::Execution(format!("{PLUGIN_NAME}: missing column '{name}'"))
                })?;
                let column: &ArrayRef = batch.column(index);
                if let Some(array) = column.as_any().downcast_ref::<ListArray>() {
                    return Ok(Self::List(array.clone()));
                }
                if let Some(array) = column.as_any().downcast_ref::<LargeListArray>() {
                    return Ok(Self::LargeList(array.clone()));
                }
                TextColumn::try_from(column)
                    .map(Self::Joined)
                    .map_err(|data_type| {
                        PluginError::Execution(format!(
                            "{PLUGIN_NAME}: column '{name}' must be a string column or a list of strings, got {data_type}"
                        ))
                    })
            }
        }
    }

    /// `None` when a present topic is not a 32-byte hex word.
    fn row(&self, row: usize) -> Option<Vec<B256>> {
        match self {
            Self::Separate(columns) => {
                let mut topics = Vec::with_capacity(4);
                for column in columns.iter() {
                    match column.value(row) {
                        Some(text) => topics.push(parse_word(text)?),
                        None => break,
                    }
                }
                Some(topics)
            }
            Self::Joined(column) => split_topics(column.value(row)?),
            Self::List(array) if array.is_null(row) => Some(Vec::new()),
            Self::List(array) => list_topics(&array.value(row)),
            Self::LargeList(array) if array.is_null(row) => Some(Vec::new()),
            Self::LargeList(array) => list_topics(&array.value(row)),
        }
    }
}

fn list_topics(values: &ArrayRef) -> Option<Vec<B256>> {
    let column = TextColumn::try_from(values).ok()?;
    (0..column.row_count())
        .map(|index| column.value(index).and_then(parse_word))
        .collect()
}

/// `0xa,0xb` or `["0xa","0xb"]`, as some log exports write the topics.
fn split_topics(text: &str) -> Option<Vec<B256>> {
    text.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|part| part.trim().trim_matches(|c| c == '"' || c == '\''))
        .filter(|part| !part.is_empty())
        .map(parse_word)
        .collect()
}

fn decode_row(
    row: usize,
    address: &TextColumn,
    topics: &Topics,
    data: &TextColumn,
    tokens: &HashSet<Address>,
) -> Option<Event> {
    let topics = topics.row(row)?;
    let selector = *topics.first()?;
    if selector != *CONFIDENTIAL_TRANSFER_TOPIC
        && selector != *OPERATOR_SET_TOPIC
        && selector != *AMOUNT_DISCLOSED_TOPIC
    {
        return None;
    }
    let token = address.value(row)?.trim().parse::<Address>().ok()?;
    if !tokens.is_empty() && !tokens.contains(&token) {
        return None;
    }
    if selector == *CONFIDENTIAL_TRANSFER_TOPIC {
        (topics.len() == 4).then_some(())?;
        Some(Event::ConfidentialTransfer {
            token,
            from: word_to_address(topics[1])?,
            to: word_to_address(topics[2])?,
            amount_handle: topics[3],
        })
    } else if selector == *OPERATOR_SET_TOPIC {
        (topics.len() == 3).then_some(())?;
        Some(Event::OperatorSet {
            token,
            holder: word_to_address(topics[1])?,
            operator: word_to_address(topics[2])?,
            until: word_to_u64(parse_word(data.value(row)?)?)?,
        })
    } else {
        (topics.len() == 2).then_some(())?;
        Some(Event::AmountDisclosed {
            token,
            encrypted_amount: topics[1],
            amount: word_to_u64(parse_word(data.value(row)?)?)?,
        })
    }
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

/// A `uint48` or `uint64` word is the value right-aligned in 32 bytes.
fn word_to_u64(word: B256) -> Option<u64> {
    u64::try_from(U256::from_be_bytes(word.0)).ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::{StringArray, StringViewArray};
    use arrow::buffer::OffsetBuffer;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    pub const TOKEN: &str = "0x1111111111111111111111111111111111111111";
    pub const FROM_WORD: &str =
        "0x00000000000000000000000000000000000000000000000000000000000000aa";
    pub const TO_WORD: &str = "0x00000000000000000000000000000000000000000000000000000000000000bb";
    pub const HANDLE: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const UNTIL_DATA: &str = "0x0000000000000000000000000000000000000000000000000000000000000539"; // 1337
    const AMOUNT_DATA: &str = "0x00000000000000000000000000000000000000000000000000000000000f4240"; // 1_000_000

    fn hex(topic: &B256) -> String {
        format!("{topic:#x}")
    }

    pub fn separate_columns() -> LogColumns {
        LogColumns {
            address: "address".into(),
            topics: TopicColumns::Separate([
                "topic0".into(),
                "topic1".into(),
                "topic2".into(),
                "topic3".into(),
            ]),
            data: "data".into(),
        }
    }

    pub struct Row<'a> {
        pub address: Option<&'a str>,
        pub topics: Vec<Option<&'a str>>,
        pub data: Option<&'a str>,
    }

    fn transfer_row<'a>(address: &'a str) -> Row<'a> {
        Row {
            address: Some(address),
            topics: vec![
                Some(Box::leak(
                    hex(&CONFIDENTIAL_TRANSFER_TOPIC).into_boxed_str(),
                )),
                Some(FROM_WORD),
                Some(TO_WORD),
                Some(HANDLE),
            ],
            data: Some("0x"),
        }
    }

    pub fn separate_batch(rows: &[Row<'_>]) -> RecordBatch {
        let mut fields = vec![Field::new("address", DataType::Utf8, true)];
        fields.extend((0..4).map(|i| Field::new(format!("topic{i}"), DataType::Utf8, true)));
        fields.push(Field::new("data", DataType::Utf8, true));
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            rows.iter().map(|row| row.address).collect::<Vec<_>>(),
        ))];
        for i in 0..4 {
            arrays.push(Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.topics.get(i).copied().flatten())
                    .collect::<Vec<_>>(),
            )));
        }
        arrays.push(Arc::new(StringArray::from(
            rows.iter().map(|row| row.data).collect::<Vec<_>>(),
        )));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
    }

    fn joined_columns() -> LogColumns {
        LogColumns {
            topics: TopicColumns::Joined("topics".into()),
            ..separate_columns()
        }
    }

    /// `data` is Utf8View so that encoding is covered too.
    fn list_batch(rows: &[Row<'_>]) -> RecordBatch {
        let fields = vec![
            Field::new("address", DataType::Utf8, true),
            Field::new(
                "topics",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("data", DataType::Utf8View, true),
        ];
        let values = StringArray::from(
            rows.iter()
                .flat_map(|row| row.topics.iter().copied())
                .collect::<Vec<_>>(),
        );
        let offsets = OffsetBuffer::from_lengths(rows.iter().map(|row| row.topics.len()));
        let topics = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.address).collect::<Vec<_>>(),
            )),
            Arc::new(topics),
            Arc::new(StringViewArray::from(
                rows.iter().map(|row| row.data).collect::<Vec<_>>(),
            )),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
    }

    fn joined_batch(rows: &[Row<'_>], wrap: bool) -> RecordBatch {
        let fields = vec![
            Field::new("address", DataType::Utf8, true),
            Field::new("topics", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
        ];
        let joined: Vec<String> = rows
            .iter()
            .map(|row| {
                let parts: Vec<&str> = row.topics.iter().flatten().copied().collect();
                if wrap {
                    format!(
                        "[{}]",
                        parts
                            .iter()
                            .map(|p| format!("\"{p}\""))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    parts.join(",")
                }
            })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.address).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                joined.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.data).collect::<Vec<_>>(),
            )),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
    }

    fn expected_transfer() -> Event {
        Event::ConfidentialTransfer {
            token: TOKEN.parse().expect("token"),
            from: Address::from_slice(&[0; 19].into_iter().chain([0xaa]).collect::<Vec<_>>()),
            to: Address::from_slice(&[0; 19].into_iter().chain([0xbb]).collect::<Vec<_>>()),
            amount_handle: HANDLE.parse().expect("handle"),
        }
    }

    #[test]
    fn topics_match_solidity_signatures() {
        // Reference values from `cast keccak "<signature>"`.
        assert_eq!(
            hex(&CONFIDENTIAL_TRANSFER_TOPIC),
            "0x67500e8d0ed826d2194f514dd0d8124f35648ab6e3fb5e6ed867134cffe661e9"
        );
        assert_eq!(
            hex(&OPERATOR_SET_TOPIC),
            "0x921a218a75d18e8ec5704851e6b234a85725b21a2521ce889622c35dedc1fa12"
        );
        assert_eq!(
            hex(&AMOUNT_DISCLOSED_TOPIC),
            "0x83bbbc07896439e5d950a6cead04cbc676180af7a61cecf43f5296475057f571"
        );
    }

    #[test]
    fn decodes_all_three_events_from_separate_columns() {
        let operator_set = hex(&OPERATOR_SET_TOPIC);
        let amount_disclosed = hex(&AMOUNT_DISCLOSED_TOPIC);
        let other = hex(&keccak256("Transfer(address,address,uint256)"));
        let rows = [
            transfer_row(TOKEN),
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&operator_set), Some(FROM_WORD), Some(TO_WORD), None],
                data: Some(UNTIL_DATA),
            },
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&amount_disclosed), Some(HANDLE), None, None],
                data: Some(AMOUNT_DATA),
            },
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&other), Some(FROM_WORD), Some(TO_WORD), None],
                data: Some(UNTIL_DATA),
            },
        ];
        let decoded = decode_batch(&separate_batch(&rows), &separate_columns(), &HashSet::new())
            .expect("decode");
        assert_eq!(decoded[0], Some(expected_transfer()));
        assert_eq!(
            decoded[1],
            Some(Event::OperatorSet {
                token: TOKEN.parse().expect("token"),
                holder: Address::from_slice(&[0; 19].into_iter().chain([0xaa]).collect::<Vec<_>>()),
                operator: Address::from_slice(
                    &[0; 19].into_iter().chain([0xbb]).collect::<Vec<_>>()
                ),
                until: 1337,
            })
        );
        assert_eq!(
            decoded[2],
            Some(Event::AmountDisclosed {
                token: TOKEN.parse().expect("token"),
                encrypted_amount: HANDLE.parse().expect("handle"),
                amount: 1_000_000,
            })
        );
        assert_eq!(decoded[3], None);
    }

    #[test]
    fn decodes_from_a_list_column() {
        let batch = list_batch(&[
            transfer_row(TOKEN),
            Row {
                address: Some(TOKEN),
                topics: vec![],
                data: None,
            },
        ]);
        validate_columns(&batch.schema(), &joined_columns()).expect("list column validates");
        let decoded = decode_batch(&batch, &joined_columns(), &HashSet::new()).expect("decode");
        assert_eq!(decoded[0], Some(expected_transfer()));
        assert_eq!(decoded[1], None);
    }

    #[test]
    fn decodes_from_a_joined_string_column() {
        for wrap in [false, true] {
            let batch = joined_batch(&[transfer_row(TOKEN)], wrap);
            validate_columns(&batch.schema(), &joined_columns()).expect("string column validates");
            let decoded = decode_batch(&batch, &joined_columns(), &HashSet::new()).expect("decode");
            assert_eq!(decoded[0], Some(expected_transfer()), "wrap={wrap}");
        }
    }

    #[test]
    fn malformed_rows_pass_through() {
        let transfer = hex(&CONFIDENTIAL_TRANSFER_TOPIC);
        let operator_set = hex(&OPERATOR_SET_TOPIC);
        let dirty = "0x01000000000000000000000000000000000000000000000000000000000000aa";
        let rows = [
            // null topic in the middle of a transfer
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&transfer), None, Some(TO_WORD), Some(HANDLE)],
                data: Some("0x"),
            },
            // address topic with dirty padding
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&transfer), Some(dirty), Some(TO_WORD), Some(HANDLE)],
                data: Some("0x"),
            },
            // unparsable emitting address
            transfer_row("nonsense"),
            // OperatorSet without data
            Row {
                address: Some(TOKEN),
                topics: vec![Some(&operator_set), Some(FROM_WORD), Some(TO_WORD), None],
                data: None,
            },
            // OperatorSet with too many topics
            Row {
                address: Some(TOKEN),
                topics: vec![
                    Some(&operator_set),
                    Some(FROM_WORD),
                    Some(TO_WORD),
                    Some(HANDLE),
                ],
                data: Some(UNTIL_DATA),
            },
        ];
        let decoded = decode_batch(&separate_batch(&rows), &separate_columns(), &HashSet::new())
            .expect("decode");
        assert!(decoded.iter().all(Option::is_none), "{decoded:?}");
    }

    #[test]
    fn token_allowlist_filters_contracts() {
        let batch = separate_batch(&[transfer_row(TOKEN)]);
        let allowed = HashSet::from(["0x2222222222222222222222222222222222222222"
            .parse::<Address>()
            .expect("address")]);
        let decoded = decode_batch(&batch, &separate_columns(), &allowed).expect("decode");
        assert!(decoded[0].is_none());
    }

    #[test]
    fn validation_rejects_missing_and_non_string_columns() {
        let batch = separate_batch(&[]);
        let mut columns = separate_columns();
        columns.data = "payload".into();
        assert!(validate_columns(&batch.schema(), &columns).is_err());
        let schema = Schema::new(vec![
            Field::new("address", DataType::Utf8, true),
            Field::new("topics", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
        ]);
        assert!(validate_columns(&schema, &joined_columns()).is_err());
        assert!(matches!(
            decode_batch(&batch, &columns, &HashSet::new()),
            Err(PluginError::Execution(_))
        ));
    }
}
