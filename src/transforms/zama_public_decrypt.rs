//! `zama_public_decrypt` transform: decrypts Zama FHE handles that were made
//! publicly decryptable on-chain (`FHE.makePubliclyDecryptable`, e.g. ERC-7984
//! unwrap amounts) through the Zama SDK daemon -> relayer -> KMS. No signer,
//! permit or delegation is involved.
//!
//! Options (flat keys; `STREAMLING__PLUGIN__ZAMA_PUBLIC_DECRYPT__<KEY>` wins):
//! `chain_id`, `rpc_url`, `daemon_socket` (required); `handle_column`
//! (default `handle`, `0x` hex bytes32, any Arrow string type); `relayer_api_key`
//! (secret, required by the mainnet relayer only).
//!
//! Appends nullable Utf8 `zama_public_amount` (decimal) and `zama_public_error`.
//! A handle the KMS refuses (e.g. not public) is a row error; an unreachable
//! daemon or a retryable SDK error fails the batch so Streamling replays it.

use crate::utils::plugin_options::{PluginOptions, configuration_error};
use arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
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
use zama_sdk::{
    B256, ChainConfig, ClearValue, ClearValues, Client, ClientError, ErrorKind, RelayerAuth, Sdk,
    SdkConfig,
};

const PLUGIN_NAME: &str = "zama_public_decrypt";
const ENV_PREFIX: &str = "STREAMLING__PLUGIN__ZAMA_PUBLIC_DECRYPT";
const AMOUNT_COLUMN: &str = "zama_public_amount";
const ERROR_COLUMN: &str = "zama_public_error";

pub struct ZamaPublicDecryptTransform {
    chain_id: u64,
    rpc_url: String,
    daemon_socket: PathBuf,
    relayer_api_key: Option<String>,
    handle_column: String,
    output_schema: SchemaRef,
    sdk: OnceCell<Sdk>,
    metrics: PluginMetricsRecorder,
    running: AtomicBool,
}

impl ZamaPublicDecryptTransform {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let opts = PluginOptions::new(options, PLUGIN_NAME, ENV_PREFIX);
        let chain_id = opts
            .get("chain_id")
            .and_then(|value| opts.parse_value("chain_id", &value))
            .map_err(configuration_error)?;
        let rpc_url = opts.get("rpc_url").map_err(configuration_error)?;
        let daemon_socket = PathBuf::from(opts.get("daemon_socket").map_err(configuration_error)?);
        let handle_column = opts.get_or("handle_column", "handle");
        let relayer_api_key = opts
            .get_secret("relayer_api_key")
            .filter(|key| !key.is_empty());
        let output_schema = output_schema(&schema, &handle_column).map_err(|message| {
            PluginInitializationError::Configuration(format!("{PLUGIN_NAME}: {message}").into())
        })?;
        Ok(Self {
            chain_id,
            rpc_url,
            daemon_socket,
            relayer_api_key,
            handle_column,
            output_schema,
            sdk: OnceCell::new(),
            metrics,
            running: AtomicBool::new(true),
        })
    }

    async fn connect(&self) -> Result<Sdk, PluginError> {
        let internal = |message: String| PluginError::Internal(format!("{PLUGIN_NAME}: {message}"));
        let mut chain = ChainConfig::new(self.chain_id, self.rpc_url.clone());
        if let Some(key) = &self.relayer_api_key {
            chain = chain.with_auth(RelayerAuth::api_key(key.clone()));
        }
        let client = Client::connect(&self.daemon_socket)
            .await
            .map_err(|error| {
                internal(format!(
                    "connecting to daemon at {}: {error}",
                    self.daemon_socket.display()
                ))
            })?;
        client
            .sdk(SdkConfig::from_chains(self.chain_id, vec![chain]))
            .build()
            .await
            .map_err(|error| internal(format!("creating SDK context: {error}")))
    }
}

/// Input schema plus the two output columns; checks the handle column up front.
fn output_schema(input: &SchemaRef, handle_column: &str) -> Result<SchemaRef, String> {
    let field = input
        .field_with_name(handle_column)
        .map_err(|_| format!("input has no '{handle_column}' column"))?;
    if !matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(format!(
            "column '{handle_column}' must be a string column, got {}",
            field.data_type()
        ));
    }
    let mut fields: Vec<Arc<Field>> = input.fields().iter().cloned().collect();
    for name in [AMOUNT_COLUMN, ERROR_COLUMN] {
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

/// `None` for a null cell, `Err` for text that is not a bytes32 hex string.
fn read_handles(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<Option<Result<B256, String>>>, PluginError> {
    let parse = |cell: Option<&str>| {
        cell.map(|text| {
            text.trim()
                .parse::<B256>()
                .map_err(|_| format!("invalid handle '{text}'"))
        })
    };
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| PluginError::Execution(format!("{PLUGIN_NAME}: missing column '{name}'")))?;
    if let Some(array) = column.as_string_opt::<i32>() {
        return Ok(array.iter().map(parse).collect());
    }
    if let Some(array) = column.as_string_opt::<i64>() {
        return Ok(array.iter().map(parse).collect());
    }
    if let Some(array) = column.as_string_view_opt() {
        return Ok(array.iter().map(parse).collect());
    }
    Err(PluginError::Execution(format!(
        "{PLUGIN_NAME}: column '{name}' must be a string column, got {}",
        column.data_type()
    )))
}

/// The KMS refused the request itself; retrying the same input cannot help.
fn is_refusal(error: &ClientError) -> bool {
    matches!(error.kind(), ErrorKind::Sdk | ErrorKind::InvalidInput)
        && !error.sdk_error().is_some_and(|sdk| sdk.retryable)
}

fn render(value: &ClearValue) -> Result<String, String> {
    match value {
        ClearValue::BigInt(value) => Ok(value.to_string()),
        ClearValue::Number(value) => Ok(value.to_string()),
        ClearValue::Bool(value) => Ok(u8::from(*value).to_string()),
        ClearValue::String(value) => Ok(value.clone()),
        ClearValue::Undefined => Err("relayer returned an undefined value".into()),
    }
}

fn answer(values: &ClearValues, handle: &B256) -> Result<String, String> {
    values.get(handle).map_or_else(
        || Err("relayer returned no result for handle".into()),
        render,
    )
}

/// Decrypts every handle in one call. One refused handle fails the whole call,
/// so on a refusal the handles are asked one by one to pin the error to its row.
async fn decrypt_all<F, Fut>(
    handles: Vec<B256>,
    decrypt: F,
) -> Result<HashMap<B256, Result<String, String>>, ClientError>
where
    F: Fn(Vec<B256>) -> Fut,
    Fut: Future<Output = Result<ClearValues, ClientError>>,
{
    if handles.is_empty() {
        return Ok(HashMap::new());
    }
    match decrypt(handles.clone()).await {
        Ok(values) => Ok(handles
            .iter()
            .map(|handle| (*handle, answer(&values, handle)))
            .collect()),
        Err(error) if !is_refusal(&error) => Err(error),
        Err(error) if handles.len() == 1 => {
            Ok(HashMap::from([(handles[0], Err(error.to_string()))]))
        }
        Err(_) => {
            let mut answers = HashMap::with_capacity(handles.len());
            for handle in handles {
                let result = match decrypt(vec![handle]).await {
                    Ok(values) => answer(&values, &handle),
                    Err(error) if is_refusal(&error) => Err(error.to_string()),
                    Err(error) => return Err(error),
                };
                answers.insert(handle, result);
            }
            Ok(answers)
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for ZamaPublicDecryptTransform {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(sdk) = self.sdk.get()
            && let Err(error) = sdk.close().await
        {
            tracing::warn!("{PLUGIN_NAME}: closing SDK context failed: {error}");
        }
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for ZamaPublicDecryptTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        self.sdk.get_or_try_init(|| self.connect()).await?;
        tracing::info!(
            chain_id = self.chain_id,
            "{PLUGIN_NAME}: connected to daemon"
        );
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.output_schema.clone())
    }

    fn labels(&self) -> Vec<PluginLabel> {
        vec![PluginLabel::new("chain_id", self.chain_id.to_string())]
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        let started = Instant::now();
        let sdk = self.sdk.get().ok_or_else(|| {
            PluginError::Internal(format!("{PLUGIN_NAME}: batch before initialize"))
        })?;
        let rows = read_handles(&batch, &self.handle_column)?;
        let mut unique: Vec<B256> = rows.iter().flatten().flatten().copied().collect();
        unique.sort_unstable();
        unique.dedup();
        let handles = unique.len();
        let answers = decrypt_all(unique, |handles| async move {
            sdk.decryption()
                .decrypt_public_values(&handles, None)
                .await
                .map(|decryption| decryption.clear_values)
        })
        .await
        .map_err(|error| {
            PluginError::Execution(format!("{PLUGIN_NAME}: public decrypt failed: {error}"))
        })?;

        let (amounts, errors): (Vec<Option<String>>, Vec<Option<String>>) = rows
            .iter()
            .map(|row| match row {
                None => (None, None),
                Some(Err(error)) => (None, Some(error.clone())),
                Some(Ok(handle)) => match &answers[handle] {
                    Ok(amount) => (Some(amount.clone()), None),
                    Err(error) => (None, Some(error.clone())),
                },
            })
            .unzip();
        let failed = errors.iter().flatten().count();
        self.metrics
            .record_count("zama_public_decrypt.handles", handles as u64);
        self.metrics
            .record_count("zama_public_decrypt.failed", failed as u64);
        self.metrics
            .record_latency("zama_public_decrypt.batch", started.elapsed());

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(StringArray::from(amounts)));
        columns.push(Arc::new(StringArray::from(errors)));
        RecordBatch::try_new(self.output_schema.clone(), columns).map_err(PluginError::ArrowError)
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
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use zama_sdk::{BigInt, SdkError};

    const A: B256 = B256::repeat_byte(0xaa);
    const B: B256 = B256::repeat_byte(0xbb);

    fn refusal(retryable: bool) -> ClientError {
        ClientError::from(SdkError {
            code: "NOT_PUBLIC".into(),
            message: "handle is not publicly decryptable".into(),
            retryable,
            retry_after_seconds: None,
            revert_data: None,
        })
    }

    #[tokio::test]
    async fn one_call_decrypts_every_handle() {
        let calls = AtomicUsize::new(0);
        let answers = decrypt_all(vec![A, B], |handles| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(handles
                    .into_iter()
                    .map(|handle| (handle, ClearValue::BigInt(BigInt::from(2_000_000))))
                    .collect())
            }
        })
        .await
        .expect("decrypted");
        assert_eq!(answers[&A], Ok("2000000".to_string()));
        assert_eq!(answers[&B], Ok("2000000".to_string()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refused_handle_is_pinned_to_its_row() {
        let answers = decrypt_all(vec![A, B], |handles| async move {
            if handles.contains(&B) {
                Err(refusal(false))
            } else {
                Ok(HashMap::from([(A, ClearValue::Number(7))]))
            }
        })
        .await
        .expect("refusal is a row error");
        assert_eq!(answers[&A], Ok("7".to_string()));
        assert!(answers[&B].as_ref().unwrap_err().contains("NOT_PUBLIC"));
    }

    #[tokio::test]
    async fn retryable_error_fails_the_batch() {
        let result = decrypt_all(vec![A], |_| async { Err(refusal(true)) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn missing_and_undefined_results_are_row_errors() {
        let answers = decrypt_all(vec![A, B], |_| async {
            Ok(HashMap::from([(A, ClearValue::Undefined)]))
        })
        .await
        .expect("decrypted");
        assert!(answers[&A].is_err());
        assert_eq!(
            answers[&B],
            Err("relayer returned no result for handle".to_string())
        );
    }

    #[test]
    fn handles_parse_nulls_and_garbage() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "handle",
            DataType::Utf8,
            true,
        )]));
        let column = StringArray::from(vec![Some(format!("{A:#x}")), None, Some("0x12".into())]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        let rows = read_handles(&batch, "handle").expect("read");
        assert_eq!(rows[0], Some(Ok(A)));
        assert_eq!(rows[1], None);
        assert!(matches!(rows[2], Some(Err(_))));
        assert!(output_schema(&schema, "missing").is_err());
        assert_eq!(output_schema(&schema, "handle").unwrap().fields().len(), 3);
    }

    #[test]
    fn sql_string_views_are_accepted() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "handle",
            DataType::Utf8View,
            true,
        )]));
        let column = arrow::array::StringViewArray::from(vec![Some(format!("{B:#x}"))]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        assert!(output_schema(&schema, "handle").is_ok());
        assert_eq!(
            read_handles(&batch, "handle").expect("read")[0],
            Some(Ok(B))
        );
    }
}
