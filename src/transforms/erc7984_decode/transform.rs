use super::decode::{Event, LogColumns, PLUGIN_NAME, TopicColumns, decode_batch, validate_columns};
use crate::utils::plugin_options::{PluginOptions, configuration_error};
use alloy_primitives::Address;
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginLabel, TransformPlugin,
};

const ENV_PREFIX: &str = "STREAMLING__PLUGIN__ERC7984_DECODE";

/// Appended to every row; all null when the row is not an ERC-7984 event.
pub const OUTPUT_COLUMNS: [&str; 10] = [
    "erc7984_event",
    "erc7984_token",
    "erc7984_from",
    "erc7984_to",
    "erc7984_amount_handle",
    "erc7984_holder",
    "erc7984_operator",
    "erc7984_until",
    "erc7984_encrypted_amount",
    "erc7984_amount",
];

pub struct Settings {
    pub columns: LogColumns,
    /// Empty set means every contract emitting the events is decoded.
    pub tokens: HashSet<Address>,
}

impl Settings {
    pub fn parse(options: HashMap<String, String>) -> Result<Self, PluginError> {
        let opts = PluginOptions::new(options, PLUGIN_NAME, ENV_PREFIX);
        let topics = match opts.lookup_non_empty("topics_column") {
            Some(name) => TopicColumns::Joined(name),
            None => TopicColumns::Separate([0, 1, 2, 3].map(|index| {
                opts.get_or(&format!("topic{index}_column"), &format!("topic{index}"))
            })),
        };
        let columns = LogColumns {
            address: opts.get_or("address_column", "address"),
            topics,
            data: opts.get_or("data_column", "data"),
        };
        let tokens = opts
            .lookup_non_empty("tokens")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| {
                        value.parse::<Address>().map_err(|_| {
                            PluginError::Internal(format!(
                                "{PLUGIN_NAME}: invalid token address '{value}'"
                            ))
                        })
                    })
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self { columns, tokens })
    }
}

pub struct Erc7984DecodeTransform {
    settings: Settings,
    output_schema: SchemaRef,
    metrics: PluginMetricsRecorder,
    running: Arc<AtomicBool>,
}

impl Erc7984DecodeTransform {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let settings = Settings::parse(options).map_err(configuration_error)?;
        validate_columns(&schema, &settings.columns).map_err(configuration)?;
        let output_schema = output_schema(&schema).map_err(configuration)?;
        Ok(Self {
            settings,
            output_schema,
            metrics,
            running: Arc::new(AtomicBool::new(true)),
        })
    }
}

fn configuration(message: String) -> PluginInitializationError {
    PluginInitializationError::Configuration(format!("{PLUGIN_NAME}: {message}").into())
}

pub fn output_schema(input: &SchemaRef) -> Result<SchemaRef, String> {
    let mut fields: Vec<Arc<Field>> = input.fields().iter().cloned().collect();
    for name in OUTPUT_COLUMNS {
        if input.index_of(name).is_ok() {
            return Err(format!("input already has a '{name}' column"));
        }
        fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}

fn append_columns(
    batch: &RecordBatch,
    schema: SchemaRef,
    events: &[Option<Event>],
) -> Result<RecordBatch, PluginError> {
    let mut values: [Vec<Option<String>>; 10] =
        std::array::from_fn(|_| Vec::with_capacity(events.len()));
    for event in events {
        let mut row: [Option<String>; 10] = Default::default();
        if let Some(event) = event {
            row[0] = Some(event.name().to_string());
            row[1] = Some(format!("{:#x}", event.token()));
            match event {
                Event::ConfidentialTransfer {
                    from,
                    to,
                    amount_handle,
                    ..
                } => {
                    row[2] = Some(format!("{from:#x}"));
                    row[3] = Some(format!("{to:#x}"));
                    row[4] = Some(format!("{amount_handle:#x}"));
                }
                Event::OperatorSet {
                    holder,
                    operator,
                    until,
                    ..
                } => {
                    row[5] = Some(format!("{holder:#x}"));
                    row[6] = Some(format!("{operator:#x}"));
                    row[7] = Some(until.to_string());
                }
                Event::AmountDisclosed {
                    encrypted_amount,
                    amount,
                    ..
                } => {
                    row[8] = Some(format!("{encrypted_amount:#x}"));
                    row[9] = Some(amount.to_string());
                }
            }
        }
        for (column, value) in values.iter_mut().zip(row) {
            column.push(value);
        }
    }
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.extend(
        values
            .into_iter()
            .map(|column| Arc::new(StringArray::from(column)) as ArrayRef),
    );
    RecordBatch::try_new(schema, columns).map_err(PluginError::ArrowError)
}

#[async_trait]
impl SupportsGracefulShutdown for Erc7984DecodeTransform {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for Erc7984DecodeTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.output_schema.clone())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        Vec::new()
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        let started = Instant::now();
        let events = decode_batch(&batch, &self.settings.columns, &self.settings.tokens)?;
        self.metrics
            .record_count("erc7984_decode.rows", batch.num_rows() as u64);
        self.metrics.record_count(
            "erc7984_decode.events",
            events.iter().flatten().count() as u64,
        );
        self.metrics
            .record_latency("erc7984_decode.batch", started.elapsed());
        append_columns(&batch, self.output_schema.clone(), &events)
    }

    async fn process_checkpoint_marker(&self, _epoch: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::decode::tests::{HANDLE, TOKEN, separate_batch, separate_columns};
    use super::*;
    use alloy_primitives::B256;
    use arrow::array::Array;

    fn settings(pairs: &[(&str, &str)]) -> Result<Settings, PluginError> {
        Settings::parse(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn options_default_to_separate_topic_columns() {
        let settings = settings(&[]).expect("valid");
        assert!(
            matches!(settings.columns.topics, TopicColumns::Separate(ref t) if t[3] == "topic3")
        );
        assert_eq!(settings.columns.data, "data");
        assert!(settings.tokens.is_empty());
    }

    #[test]
    fn topics_column_switches_to_the_joined_shape() {
        let parsed = settings(&[("topics_column", "topics"), ("tokens", TOKEN)]).expect("valid");
        assert!(matches!(parsed.columns.topics, TopicColumns::Joined(ref t) if t == "topics"));
        assert_eq!(parsed.tokens.len(), 1);
        assert!(settings(&[("tokens", "0x12")]).is_err());
    }

    #[test]
    fn output_schema_appends_nullable_text_columns_and_rejects_collisions() {
        let input = separate_batch(&[]).schema();
        let output = output_schema(&input).expect("schema");
        assert_eq!(output.fields().len(), input.fields().len() + 10);
        let field = output.field_with_name("erc7984_amount").expect("field");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
        let colliding = Arc::new(Schema::new(vec![Field::new(
            "erc7984_event",
            DataType::Utf8,
            true,
        )]));
        assert!(output_schema(&colliding).is_err());
    }

    #[test]
    fn appended_columns_line_up_with_rows() {
        let input = separate_batch(&[]);
        let token: Address = TOKEN.parse().expect("token");
        let events = [
            Some(Event::ConfidentialTransfer {
                token,
                from: Address::repeat_byte(0xaa),
                to: Address::repeat_byte(0xbb),
                amount_handle: HANDLE.parse::<B256>().expect("handle"),
            }),
            None,
            Some(Event::AmountDisclosed {
                token,
                encrypted_amount: HANDLE.parse::<B256>().expect("handle"),
                amount: 42,
            }),
        ];
        // Three input rows of nulls so the batch has the right length.
        let input = RecordBatch::try_new(
            input.schema(),
            input
                .columns()
                .iter()
                .map(|_| Arc::new(StringArray::from(vec![None::<&str>; 3])) as ArrayRef)
                .collect(),
        )
        .expect("batch");
        let schema = output_schema(&input.schema()).expect("schema");
        let output = append_columns(&input, schema, &events).expect("batch");
        let column = |name: &str| {
            output
                .column_by_name(name)
                .expect("column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .clone()
        };
        assert_eq!(column("erc7984_event").value(0), "ConfidentialTransfer");
        assert_eq!(column("erc7984_token").value(0), TOKEN);
        assert_eq!(column("erc7984_amount_handle").value(0), HANDLE);
        assert!(column("erc7984_amount").is_null(0));
        assert!(column("erc7984_event").is_null(1));
        assert_eq!(column("erc7984_event").value(2), "AmountDisclosed");
        assert_eq!(column("erc7984_amount").value(2), "42");
        assert!(column("erc7984_from").is_null(2));
        let _ = separate_columns();
    }
}
