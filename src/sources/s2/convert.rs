//! S2 record → Arrow RecordBatch conversion.
//!
//! Two output modes:
//! - Raw: the S2 record envelope as columns — `stream`, `seq_num`,
//!   `timestamp` (ms, UTC), `headers` (JSON object, null when empty), `body`.
//!   Record bodies are emitted as UTF-8 text (invalid bytes are replaced).
//! - Typed: JSON record bodies decoded into user-configured columns, with
//!   optional `_s2_stream` / `_s2_seq_num` / `_s2_timestamp` metadata columns.
//!
//! Both modes lead with the streamling `_gs_op` row-kind column, always "i":
//! S2 streams are append-only, so every record is an insert.

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::compute::concat_batches;
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use std::sync::Arc;
use streamling_plugin::PluginError;
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP;
use tracing::warn;

pub(crate) const META_STREAM: &str = "_s2_stream";
pub(crate) const META_SEQ_NUM: &str = "_s2_seq_num";
pub(crate) const META_TIMESTAMP: &str = "_s2_timestamp";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMalformed {
    /// Fail the batch; the source retries the same records (stalls, no loss).
    Error,
    /// Drop undecodable records with a WARN log.
    Skip,
}

#[derive(Debug, Clone)]
pub enum OutputMode {
    Raw,
    Typed {
        fields: Vec<Field>,
        include_metadata: bool,
        on_malformed: OnMalformed,
    },
}

/// An S2 record plus its provenance, decoupled from SDK types so conversion
/// is independently testable.
#[derive(Debug, Clone)]
pub(crate) struct SourceRecord {
    pub stream: Arc<str>,
    pub seq_num: u64,
    /// Epoch milliseconds.
    pub timestamp: u64,
    pub headers: Vec<(Bytes, Bytes)>,
    pub body: Bytes,
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn metadata_fields() -> [Field; 3] {
    [
        Field::new(META_STREAM, DataType::Utf8, false),
        Field::new(META_SEQ_NUM, DataType::UInt64, false),
        Field::new(META_TIMESTAMP, timestamp_type(), false),
    ]
}

fn gs_op_field() -> Field {
    Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false)
}

/// The `_gs_op` column: every S2 record is an insert.
fn gs_op_array(len: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(std::iter::repeat_n("i", len)))
}

/// Parses a `name:type` comma-separated schema spec; `?` suffix marks a
/// field nullable (e.g. `id:int64,value:string?`).
pub fn parse_field_specs(spec: &str) -> Result<Vec<Field>, PluginError> {
    let mut fields = Vec::new();
    for field_spec in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, data_type) = field_spec.split_once(':').ok_or_else(|| {
            PluginError::Internal(format!(
                "s2_source: schema field '{field_spec}' must use name:type syntax"
            ))
        })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(PluginError::Internal(
                "s2_source: schema field name cannot be empty".to_string(),
            ));
        }
        let data_type = data_type.trim();
        let nullable = data_type.ends_with('?');
        let data_type = data_type.trim_end_matches('?').trim();
        fields.push(Field::new(name, parse_data_type(data_type)?, nullable));
    }
    if fields.is_empty() {
        return Err(PluginError::Internal(
            "s2_source: schema must contain at least one field".to_string(),
        ));
    }
    Ok(fields)
}

fn parse_data_type(data_type: &str) -> Result<DataType, PluginError> {
    match data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" | "long" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" | "float" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),
        "string" | "utf8" => Ok(DataType::Utf8),
        "large_string" | "large_utf8" => Ok(DataType::LargeUtf8),
        "date" | "date32" => Ok(DataType::Date32),
        "timestamp_s" | "timestamp_second" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        "timestamp" | "timestamp_ms" | "timestamp_millis" => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        "timestamp_us" | "timestamp_micros" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamp_ns" | "timestamp_nanos" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other => Err(PluginError::Internal(format!(
            "s2_source: unsupported schema field type '{other}'"
        ))),
    }
}

enum Mode {
    Raw,
    Typed {
        user_schema: SchemaRef,
        include_metadata: bool,
        on_malformed: OnMalformed,
    },
}

pub(crate) struct RecordConverter {
    schema: SchemaRef,
    mode: Mode,
}

impl RecordConverter {
    pub fn new(output: &OutputMode) -> Result<Self, PluginError> {
        match output {
            OutputMode::Raw => Ok(Self {
                schema: Arc::new(Schema::new(vec![
                    gs_op_field(),
                    Field::new("stream", DataType::Utf8, false),
                    Field::new("seq_num", DataType::UInt64, false),
                    Field::new("timestamp", timestamp_type(), false),
                    Field::new("headers", DataType::Utf8, true),
                    Field::new("body", DataType::Utf8, false),
                ])),
                mode: Mode::Raw,
            }),
            OutputMode::Typed {
                fields,
                include_metadata,
                on_malformed,
            } => {
                let mut all_fields = vec![gs_op_field()];
                all_fields.extend(fields.iter().cloned());
                if *include_metadata {
                    all_fields.extend(metadata_fields());
                }
                let mut reserved = vec![STREAMLING_COLUMN_NAME_OP];
                if *include_metadata {
                    reserved.extend([META_STREAM, META_SEQ_NUM, META_TIMESTAMP]);
                }
                for field in fields {
                    if reserved.contains(&field.name().as_str()) {
                        return Err(PluginError::Internal(format!(
                            "s2_source: schema field '{}' collides with a reserved column",
                            field.name()
                        )));
                    }
                }
                Ok(Self {
                    schema: Arc::new(Schema::new(all_fields)),
                    mode: Mode::Typed {
                        user_schema: Arc::new(Schema::new(fields.clone())),
                        include_metadata: *include_metadata,
                        on_malformed: *on_malformed,
                    },
                })
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn convert(&self, records: &[SourceRecord]) -> Result<RecordBatch, PluginError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        match &self.mode {
            Mode::Raw => self.convert_raw(records),
            Mode::Typed {
                user_schema,
                include_metadata,
                on_malformed,
            } => {
                let (user_batch, kept) = match decode_strict(user_schema, records) {
                    Ok(batch) => (batch, None),
                    Err(e) => match on_malformed {
                        OnMalformed::Error => return Err(e),
                        OnMalformed::Skip => {
                            let (batch, kept) = decode_skipping(user_schema, records)?;
                            (batch, Some(kept))
                        }
                    },
                };

                let mut columns = vec![gs_op_array(user_batch.num_rows())];
                columns.extend(user_batch.columns().iter().cloned());
                if *include_metadata {
                    let kept_records: Vec<&SourceRecord> = match &kept {
                        None => records.iter().collect(),
                        Some(kept) => kept.iter().map(|&i| &records[i]).collect(),
                    };
                    let (stream, seq_num, timestamp) = metadata_arrays(&kept_records);
                    columns.extend([stream, seq_num, timestamp]);
                }
                RecordBatch::try_new(self.schema.clone(), columns).map_err(PluginError::ArrowError)
            }
        }
    }

    fn convert_raw(&self, records: &[SourceRecord]) -> Result<RecordBatch, PluginError> {
        let all: Vec<&SourceRecord> = records.iter().collect();
        let (stream, seq_num, timestamp) = metadata_arrays(&all);
        let headers: ArrayRef = Arc::new(records.iter().map(headers_json).collect::<StringArray>());
        let body: ArrayRef = Arc::new(StringArray::from_iter_values(
            records.iter().map(|r| String::from_utf8_lossy(&r.body)),
        ));
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                gs_op_array(records.len()),
                stream,
                seq_num,
                timestamp,
                headers,
                body,
            ],
        )
        .map_err(PluginError::ArrowError)
    }
}

fn metadata_arrays(records: &[&SourceRecord]) -> (ArrayRef, ArrayRef, ArrayRef) {
    let stream = StringArray::from_iter_values(records.iter().map(|r| r.stream.as_ref()));
    let seq_num = UInt64Array::from_iter_values(records.iter().map(|r| r.seq_num));
    let timestamp =
        TimestampMillisecondArray::from_iter_values(records.iter().map(|r| r.timestamp as i64))
            .with_timezone("UTC");
    (Arc::new(stream), Arc::new(seq_num), Arc::new(timestamp))
}

/// Headers as a JSON object with lossy UTF-8 keys/values; None when empty.
/// Duplicate header names keep the last value.
fn headers_json(record: &SourceRecord) -> Option<String> {
    if record.headers.is_empty() {
        return None;
    }
    let map: serde_json::Map<String, serde_json::Value> = record
        .headers
        .iter()
        .map(|(name, value)| {
            (
                String::from_utf8_lossy(name).into_owned(),
                serde_json::Value::String(String::from_utf8_lossy(value).into_owned()),
            )
        })
        .collect();
    Some(serde_json::Value::Object(map).to_string())
}

/// Decodes every record body as exactly one JSON row; any malformed body
/// fails the whole batch.
fn decode_strict(
    user_schema: &SchemaRef,
    records: &[SourceRecord],
) -> Result<RecordBatch, PluginError> {
    let mut decoder = ReaderBuilder::new(user_schema.clone())
        .with_batch_size(records.len())
        .with_coerce_primitive(true)
        .build_decoder()
        .map_err(PluginError::ArrowError)?;

    for record in records {
        let mut buf = record.body.as_ref();
        while !buf.is_empty() {
            let consumed = decoder
                .decode(buf)
                .map_err(|e| decode_error(&record.stream, record.seq_num, &e.to_string()))?;
            if consumed == 0 {
                // The decoder's row budget (= record count) is exhausted, so
                // some body decoded to more than one row.
                return Err(decode_error(
                    &record.stream,
                    record.seq_num,
                    "record body contains more than one JSON value",
                ));
            }
            buf = &buf[consumed..];
        }
    }

    let batch = decoder
        .flush()
        .map_err(PluginError::ArrowError)?
        .unwrap_or_else(|| RecordBatch::new_empty(user_schema.clone()));
    if batch.num_rows() != records.len() {
        return Err(PluginError::Internal(format!(
            "s2_source: decoded {} rows from {} records; record bodies must \
             each be a single JSON object",
            batch.num_rows(),
            records.len()
        )));
    }
    Ok(batch)
}

/// Decodes record bodies one at a time, skipping (with a WARN) any that do
/// not decode to exactly one row. Returns the batch and the kept indices.
fn decode_skipping(
    user_schema: &SchemaRef,
    records: &[SourceRecord],
) -> Result<(RecordBatch, Vec<usize>), PluginError> {
    let mut batches = Vec::new();
    let mut kept = Vec::new();
    for (index, record) in records.iter().enumerate() {
        match decode_strict(user_schema, std::slice::from_ref(record)) {
            Ok(batch) => {
                batches.push(batch);
                kept.push(index);
            }
            Err(e) => warn!(
                stream = %record.stream,
                seq_num = record.seq_num,
                error = %e,
                "s2_source: skipping malformed record"
            ),
        }
    }
    let batch = if batches.is_empty() {
        RecordBatch::new_empty(user_schema.clone())
    } else {
        concat_batches(user_schema, batches.iter()).map_err(PluginError::ArrowError)?
    };
    Ok((batch, kept))
}

fn decode_error(stream: &str, seq_num: u64, message: &str) -> PluginError {
    PluginError::Internal(format!(
        "s2_source: failed to decode record body (stream '{stream}', seq_num {seq_num}): {message}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};

    pub(crate) fn record(stream: &str, seq_num: u64, body: &str) -> SourceRecord {
        SourceRecord {
            stream: Arc::from(stream),
            seq_num,
            timestamp: 1_700_000_000_000 + seq_num,
            headers: Vec::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    fn typed(schema: &str, include_metadata: bool, on_malformed: OnMalformed) -> RecordConverter {
        RecordConverter::new(&OutputMode::Typed {
            fields: parse_field_specs(schema).unwrap(),
            include_metadata,
            on_malformed,
        })
        .unwrap()
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap()
    }

    fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap()
    }

    #[test]
    fn parses_field_specs() {
        let fields = parse_field_specs("id:int64, value:string?, at:timestamp, day:date").unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(fields[1].is_nullable());
        assert_eq!(
            fields[2].data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(fields[3].data_type(), &DataType::Date32);

        assert!(parse_field_specs("").is_err());
        assert!(parse_field_specs("id").is_err());
        assert!(parse_field_specs("id:decimal").is_err());
    }

    #[test]
    fn raw_mode_emits_the_record_envelope() {
        let converter = RecordConverter::new(&OutputMode::Raw).unwrap();
        let mut with_headers = record("events-a", 7, r#"{"id":1}"#);
        with_headers.headers = vec![(Bytes::from_static(b"kind"), Bytes::from_static(b"click"))];
        let records = vec![with_headers, record("events-b", 3, "not json")];

        let batch = converter.convert(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            string_column(&batch, STREAMLING_COLUMN_NAME_OP).value(0),
            "i"
        );
        assert_eq!(string_column(&batch, "stream").value(0), "events-a");
        assert_eq!(string_column(&batch, "body").value(1), "not json");
        assert_eq!(
            string_column(&batch, "headers").value(0),
            r#"{"kind":"click"}"#
        );
        assert!(string_column(&batch, "headers").is_null(1));
        let seq_nums: &UInt64Array = batch
            .column_by_name("seq_num")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(seq_nums.value(0), 7);
        assert_eq!(seq_nums.value(1), 3);
    }

    #[test]
    fn typed_mode_decodes_json_bodies() {
        let converter = typed("id:int64,value:string", false, OnMalformed::Error);
        let records = vec![
            record("events", 0, r#"{"id":1,"value":"one"}"#),
            record("events", 1, r#"{"id":2,"value":"two"}"#),
        ];

        let batch = converter.convert(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(
            string_column(&batch, STREAMLING_COLUMN_NAME_OP).value(0),
            "i"
        );
        assert_eq!(int64_column(&batch, "id").value(1), 2);
        assert_eq!(string_column(&batch, "value").value(0), "one");
    }

    #[test]
    fn typed_mode_appends_metadata_columns() {
        let converter = typed("id:int64", true, OnMalformed::Error);
        let records = vec![record("events-a", 42, r#"{"id":1}"#)];

        let batch = converter.convert(&records).unwrap();
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(string_column(&batch, META_STREAM).value(0), "events-a");
        let seq_nums: &UInt64Array = batch
            .column_by_name(META_SEQ_NUM)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(seq_nums.value(0), 42);
    }

    #[test]
    fn metadata_column_collision_is_an_error() {
        let result = RecordConverter::new(&OutputMode::Typed {
            fields: parse_field_specs(&format!("{META_STREAM}:string")).unwrap(),
            include_metadata: true,
            on_malformed: OnMalformed::Error,
        });
        assert!(result.is_err());
    }

    #[test]
    fn malformed_body_fails_batch_in_error_mode() {
        let converter = typed("id:int64", false, OnMalformed::Error);
        let records = vec![
            record("events", 0, r#"{"id":1}"#),
            record("events", 1, "not json"),
        ];
        let err = converter.convert(&records).unwrap_err();
        assert!(err.to_string().contains("seq_num 1"), "got {err}");
    }

    #[test]
    fn malformed_body_is_dropped_in_skip_mode() {
        let converter = typed("id:int64", true, OnMalformed::Skip);
        let records = vec![
            record("events", 0, r#"{"id":1}"#),
            record("events", 1, "not json"),
            record("events", 2, r#"{"id":3}"#),
        ];

        let batch = converter.convert(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let ids = int64_column(&batch, "id");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
        let seq_nums: &UInt64Array = batch
            .column_by_name(META_SEQ_NUM)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(seq_nums.value(0), 0);
        assert_eq!(seq_nums.value(1), 2);
    }

    #[test]
    fn multi_value_body_is_rejected() {
        let converter = typed("id:int64", false, OnMalformed::Error);
        let records = vec![record("events", 0, r#"{"id":1}{"id":2}"#)];
        let err = converter.convert(&records).unwrap_err();
        assert!(
            err.to_string().contains("more than one JSON value")
                || err.to_string().contains("decoded"),
            "got {err}"
        );
    }

    #[test]
    fn empty_input_produces_empty_batch() {
        let converter = RecordConverter::new(&OutputMode::Raw).unwrap();
        assert_eq!(converter.convert(&[]).unwrap().num_rows(), 0);
    }
}
