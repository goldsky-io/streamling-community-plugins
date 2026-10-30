use super::config::{PLUGIN_NAME, Settings};
use super::daemon;
use super::decode::{decode_batch, validate_columns};
use super::decrypt::{Decrypted, Decryptor, SdkBackend};
use crate::utils::plugin_options::configuration_error;
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{
    CheckpointEpoch, PluginError, PluginInitializationError, PluginLabel, TransformPlugin,
};
use tokio::sync::OnceCell;

/// Appended to every row; all null when the row is not a decoded transfer.
pub const OUTPUT_COLUMNS: [&str; 2] = ["zama_amount", "zama_delegator"];
pub const ERROR_COLUMN: &str = "zama_error";

pub struct ZamaDecryptTransform {
    settings: Settings,
    output_schema: SchemaRef,
    /// No reconnect path: Streamling terminates a transform on any batch error
    /// and restarts the pipeline, which replays the batch into a new instance.
    decryptor: OnceCell<Decryptor<SdkBackend>>,
    metrics: PluginMetricsRecorder,
    running: Arc<AtomicBool>,
}

impl ZamaDecryptTransform {
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
            decryptor: OnceCell::new(),
            metrics,
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    async fn decryptor(&self) -> Result<&Decryptor<SdkBackend>, PluginError> {
        self.decryptor
            .get_or_try_init(|| async {
                let sdk = daemon::connect(&self.settings).await?;
                tracing::info!(
                    delegate = %self.settings.delegate_address(),
                    chain_id = self.settings.chain_id,
                    "{PLUGIN_NAME}: connected to daemon"
                );
                Ok(Decryptor::new(
                    SdkBackend(sdk),
                    self.settings.delegate_address(),
                    self.settings.max_concurrency,
                ))
            })
            .await
    }
}

fn configuration(message: String) -> PluginInitializationError {
    PluginInitializationError::Configuration(format!("{PLUGIN_NAME}: {message}").into())
}

pub fn output_schema(input: &SchemaRef) -> Result<SchemaRef, String> {
    let mut fields: Vec<Arc<Field>> = input.fields().iter().cloned().collect();
    for name in OUTPUT_COLUMNS.iter().chain([&ERROR_COLUMN]) {
        if input.index_of(name).is_ok() {
            return Err(format!("input already has a '{name}' column"));
        }
        fields.push(Arc::new(Field::new(*name, DataType::Utf8, true)));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}

fn append_columns(
    batch: &RecordBatch,
    schema: SchemaRef,
    decrypted: &[Decrypted],
) -> Result<RecordBatch, PluginError> {
    let text = |values: Vec<Option<String>>| Arc::new(StringArray::from(values)) as ArrayRef;
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(text(
        decrypted.iter().map(|row| row.amount.clone()).collect(),
    ));
    columns.push(text(
        decrypted
            .iter()
            .map(|row| row.delegator.map(|address| format!("{address:#x}")))
            .collect(),
    ));
    columns.push(text(
        decrypted.iter().map(|row| row.error.clone()).collect(),
    ));
    RecordBatch::try_new(schema, columns).map_err(PluginError::ArrowError)
}

#[async_trait]
impl SupportsGracefulShutdown for ZamaDecryptTransform {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(decryptor) = self.decryptor.get()
            && let Err(error) = decryptor.close().await
        {
            tracing::warn!("{PLUGIN_NAME}: closing SDK context failed: {error}");
        }
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for ZamaDecryptTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.decryptor().await?;
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.output_schema.clone())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        vec![PluginLabel::new(
            "chain_id",
            self.settings.chain_id.to_string(),
        )]
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        let started = Instant::now();
        let decoded = decode_batch(&batch, &self.settings.columns, &self.settings.tokens)?;
        let transfers = decoded.iter().flatten().count();
        let decrypted = if transfers == 0 {
            vec![Decrypted::default(); decoded.len()]
        } else {
            self.decryptor()
                .await?
                .decrypt(&decoded)
                .await
                .map_err(|error| {
                    PluginError::Execution(format!(
                        "{PLUGIN_NAME}: failing the batch for replay: {error}"
                    ))
                })?
        };
        let failed = decrypted.iter().filter(|row| row.error.is_some()).count();
        self.metrics
            .record_count("zama_decrypt.rows", batch.num_rows() as u64);
        self.metrics
            .record_count("zama_decrypt.transfers", transfers as u64);
        self.metrics
            .record_count("zama_decrypt.failed", failed as u64);
        self.metrics
            .record_latency("zama_decrypt.batch", started.elapsed());
        append_columns(&batch, self.output_schema.clone(), &decrypted)
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
    use super::super::decode::tests::{FROM, HANDLE, TO, TOKEN, batch};
    use super::*;
    use alloy_primitives::Address;
    use arrow::array::Array;

    #[test]
    fn output_schema_appends_nullable_text_columns() {
        let input = batch(&[]).schema();
        let output = output_schema(&input).expect("schema");
        assert_eq!(output.fields().len(), input.fields().len() + 3);
        let field = output.field_with_name("zama_amount").expect("field");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
    }

    #[test]
    fn output_schema_rejects_colliding_input() {
        let input = Arc::new(Schema::new(vec![Field::new(
            "zama_error",
            DataType::Utf8,
            true,
        )]));
        assert!(output_schema(&input).is_err());
    }

    #[test]
    fn appended_columns_line_up_with_rows() {
        let input = batch(&[
            [Some(TOKEN), Some(FROM), Some(TO), Some(HANDLE)],
            [Some(TOKEN), None, None, None],
        ]);
        let decrypted = vec![
            Decrypted {
                amount: Some("42".into()),
                delegator: Some(Address::ZERO),
                error: None,
            },
            Decrypted::default(),
        ];
        let schema = output_schema(&input.schema()).expect("schema");
        let output = append_columns(&input, schema, &decrypted).expect("batch");
        assert_eq!(output.num_rows(), 2);
        let amount = output
            .column_by_name("zama_amount")
            .expect("column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(amount.value(0), "42");
        assert!(amount.is_null(1));
        let delegator = output
            .column_by_name("zama_delegator")
            .expect("column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(delegator.value(0), format!("{:#x}", Address::ZERO));
        assert!(delegator.is_null(1));
    }
}
