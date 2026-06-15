use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use sqlx::MySqlPool;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};
use tracing::{debug, info};

use crate::utils::plugin_options::PluginOptions;

static COLUMN_NAME_OP: &str = "_gs_op";

pub struct MySqlSink {
    opts: PluginOptions,
    schema: SchemaRef,
    pool: OnceLock<MySqlPool>,
    running: Arc<AtomicBool>,
    metrics_recorder: PluginMetricsRecorder,
}

impl MySqlSink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        MySqlSink {
            opts: PluginOptions::new(options, "mysql_sink", "STREAMLING__PLUGIN__MYSQL_SINK"),
            schema,
            pool: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
            metrics_recorder,
        }
    }

    fn pool(&self) -> Result<&MySqlPool, PluginError> {
        self.pool
            .get()
            .ok_or_else(|| PluginError::Internal("MySQL connection pool not initialized".into()))
    }

    fn primary_key_columns(&self) -> Vec<String> {
        self.opts
            .get("primary_key")
            .ok()
            .map(|pk| {
                pk.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn sink_column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .filter(|f| f.name() != COLUMN_NAME_OP)
            .map(|f| f.name().to_string())
            .collect()
    }

    fn sink_column_indices(&self) -> Vec<usize> {
        self.schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != COLUMN_NAME_OP)
            .map(|(idx, _)| idx)
            .collect()
    }

    async fn table_exists(&self, pool: &MySqlPool) -> Result<bool, PluginError> {
        let table = self.opts.get("table")?;
        let database = self.opts.get("database")?;
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1",
        )
        .bind(&database)
        .bind(&table)
        .fetch_optional(pool)
        .await
        .map_err(|e| PluginError::Internal(format!("failed to check table existence: {}", e)))?;
        Ok(row.is_some())
    }

    async fn create_table_if_needed(&self, pool: &MySqlPool) -> Result<(), PluginError> {
        if self.table_exists(pool).await? {
            return Ok(());
        }

        let table = self.opts.get("table")?;
        let database = self.opts.get("database")?;
        let pk_columns = self.primary_key_columns();

        let mut col_defs = Vec::new();
        for field in self.schema.fields() {
            if field.name() == COLUMN_NAME_OP {
                continue;
            }
            let mysql_type = arrow_to_mysql_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            col_defs.push(format!("`{}` {}{}", field.name(), mysql_type, nullable));
        }

        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}",
            database,
            table,
            col_defs.join(", ")
        );

        if !pk_columns.is_empty() {
            let pk_quoted: Vec<String> = pk_columns
                .iter()
                .map(|c| {
                    let needs_key_length = self
                        .schema
                        .field_with_name(c)
                        .ok()
                        .map(|f| pk_needs_key_length(f.data_type()))
                        .unwrap_or(false);
                    if needs_key_length {
                        format!("`{}`(255)", c)
                    } else {
                        format!("`{}`", c)
                    }
                })
                .collect();
            sql.push_str(&format!(", PRIMARY KEY ({})", pk_quoted.join(", ")));
        }
        sql.push(')');

        debug!("Creating MySQL table: {}", sql);

        sqlx::query(&sql).execute(pool).await.map_err(|e| {
            PluginError::Internal(format!(
                "failed to create table `{}`.`{}`: {}",
                database, table, e
            ))
        })?;

        Ok(())
    }

    async fn execute_upsert(
        &self,
        batch: &RecordBatch,
        row_indices: &[usize],
    ) -> Result<(), PluginError> {
        if row_indices.is_empty() {
            return Ok(());
        }

        let pool = self.pool()?;
        let table = self.opts.get("table")?;
        let database = self.opts.get("database")?;
        let on_conflict = self.opts.get_or("on_conflict", "update");
        let column_names = self.sink_column_names();
        let column_indices = self.sink_column_indices();
        let pk_columns = self.primary_key_columns();

        let quoted_cols: Vec<String> = column_names.iter().map(|c| format!("`{}`", c)).collect();
        let row_placeholders = format!("({})", vec!["?"; column_names.len()].join(", "));
        let all_values: Vec<String> = row_indices
            .iter()
            .map(|_| row_placeholders.clone())
            .collect();

        let mut sql = format!(
            "INSERT INTO `{}`.`{}` ({}) VALUES {}",
            database,
            table,
            quoted_cols.join(", "),
            all_values.join(", ")
        );

        if !pk_columns.is_empty() {
            if on_conflict == "nothing" {
                sql = format!(
                    "INSERT IGNORE INTO `{}`.`{}` ({}) VALUES {}",
                    database,
                    table,
                    quoted_cols.join(", "),
                    all_values.join(", ")
                );
            } else {
                let update_cols: Vec<String> = column_names
                    .iter()
                    .filter(|c| !pk_columns.contains(c))
                    .map(|c| format!("`{c}` = VALUES(`{c}`)"))
                    .collect();
                if !update_cols.is_empty() {
                    sql.push_str(&format!(
                        " ON DUPLICATE KEY UPDATE {}",
                        update_cols.join(", ")
                    ));
                }
            }
        }

        let mut query = sqlx::query(&sql);
        for &row_idx in row_indices {
            for &col_idx in &column_indices {
                let array = batch.column(col_idx);
                query = bind_arrow_value(query, array, row_idx)?;
            }
        }

        query.execute(pool).await.map_err(|e| {
            PluginError::Internal(format!(
                "failed to execute INSERT into `{}`.`{}`: {}",
                database, table, e
            ))
        })?;

        Ok(())
    }

    async fn execute_delete(
        &self,
        batch: &RecordBatch,
        row_indices: &[usize],
    ) -> Result<(), PluginError> {
        let pk_columns = self.primary_key_columns();
        if row_indices.is_empty() || pk_columns.is_empty() {
            return Ok(());
        }

        let pool = self.pool()?;
        let table = self.opts.get("table")?;
        let database = self.opts.get("database")?;

        let pk_indices: Vec<usize> = pk_columns
            .iter()
            .filter_map(|pk| self.schema.index_of(pk).ok())
            .collect();

        if pk_indices.is_empty() {
            return Ok(());
        }

        let row_condition = format!(
            "({})",
            pk_columns
                .iter()
                .map(|c| format!("`{}` = ?", c))
                .collect::<Vec<_>>()
                .join(" AND ")
        );
        let conditions: Vec<String> = row_indices.iter().map(|_| row_condition.clone()).collect();

        let sql = format!(
            "DELETE FROM `{}`.`{}` WHERE {}",
            database,
            table,
            conditions.join(" OR ")
        );

        let mut query = sqlx::query(&sql);
        for &row_idx in row_indices {
            for &pk_idx in &pk_indices {
                let array = batch.column(pk_idx);
                query = bind_arrow_value(query, array, row_idx)?;
            }
        }

        query.execute(pool).await.map_err(|e| {
            PluginError::Internal(format!(
                "failed to execute DELETE from `{}`.`{}`: {}",
                database, table, e
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl SupportsGracefulShutdown for MySqlSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        info!("Terminating MySQL sink");
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for MySqlSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.pool.get().is_some() {
            return Ok(());
        }

        let host = self.opts.get("host")?;
        let port: u16 = self
            .opts
            .get_or("port", "3306")
            .parse()
            .map_err(|_| PluginError::Internal("invalid port value".into()))?;
        let user = self.opts.get("user")?;
        let password = self.opts.get("password")?;
        let database = self.opts.get("database")?;
        let sslmode = self.opts.get_or("sslmode", "preferred");

        info!(
            "Connecting to MySQL: {}@{}:{}/{}",
            user, host, port, database
        );

        let ssl_mode = match sslmode.as_str() {
            "required" => sqlx::mysql::MySqlSslMode::Required,
            "verify_ca" | "verify-ca" => sqlx::mysql::MySqlSslMode::VerifyCa,
            "verify_identity" | "verify-identity" => sqlx::mysql::MySqlSslMode::VerifyIdentity,
            "preferred" => sqlx::mysql::MySqlSslMode::Preferred,
            _ => sqlx::mysql::MySqlSslMode::Disabled,
        };

        let options = MySqlConnectOptions::new()
            .host(&host)
            .port(port)
            .username(&user)
            .password(&password)
            .database(&database)
            .ssl_mode(ssl_mode);

        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .map_err(|e| PluginError::Internal(format!("failed to connect to MySQL: {}", e)))?;

        info!("Connected to MySQL successfully");

        self.create_table_if_needed(&pool).await?;

        let _ = self.pool.set(pool);
        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let start = Instant::now();
        let num_rows = batch.num_rows();

        let op_col_idx = self.schema.index_of(COLUMN_NAME_OP).ok();

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        if let Some(op_idx) = op_col_idx {
            let op_array = batch.column(op_idx).as_any().downcast_ref::<StringArray>();

            if let Some(op_arr) = op_array {
                for row in 0..batch.num_rows() {
                    match op_arr.value(row) {
                        "d" => delete_indices.push(row),
                        _ => insert_indices.push(row),
                    }
                }
            } else {
                insert_indices.extend(0..batch.num_rows());
            }
        } else {
            insert_indices.extend(0..batch.num_rows());
        }

        let chunk_size: usize = self
            .opts
            .get_or("batch_size", "1000")
            .parse()
            .unwrap_or(1000);

        for chunk in insert_indices.chunks(chunk_size) {
            self.execute_upsert(&batch, chunk).await?;
        }

        for chunk in delete_indices.chunks(chunk_size) {
            self.execute_delete(&batch, chunk).await?;
        }

        self.metrics_recorder
            .record_count("mysql_sink.rows_written", num_rows as u64);
        self.metrics_recorder
            .record_latency("mysql_sink.write_latency", start.elapsed());

        debug!(
            "MySQL sink: wrote {} rows ({} inserts, {} deletes) in {:?}",
            num_rows,
            insert_indices.len(),
            delete_indices.len(),
            start.elapsed()
        );

        Ok(())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        debug!("MySQL sink received checkpoint marker: {}", epoch.0);
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Arrow → MySQL type mapping
// ---------------------------------------------------------------------------

fn pk_needs_key_length(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
    )
}

fn arrow_to_mysql_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "TINYINT UNSIGNED".into(),
        DataType::UInt16 => "SMALLINT UNSIGNED".into(),
        DataType::UInt32 => "INT UNSIGNED".into(),
        DataType::UInt64 => "BIGINT UNSIGNED".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".into(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            "LONGBLOB".into()
        }
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Timestamp(_, _) => "DATETIME(6)".into(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME(6)".into(),
        DataType::Decimal128(p, s) => {
            let p = (*p).min(65);
            let s = (*s as u8).min(30).min(p);
            format!("DECIMAL({}, {})", p, s)
        }
        DataType::Decimal256(p, s) => {
            let p = (*p).min(65);
            let s = (*s as u8).min(30).min(p);
            format!("DECIMAL({}, {})", p, s)
        }
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => "JSON".into(),
        _ => "TEXT".into(),
    }
}

// ---------------------------------------------------------------------------
// Arrow value → sqlx MySQL query binding
// ---------------------------------------------------------------------------

type MySqlQuery<'q> = sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>;

fn bind_arrow_value<'q>(
    q: MySqlQuery<'q>,
    array: &Arc<dyn Array>,
    index: usize,
) -> Result<MySqlQuery<'q>, PluginError> {
    if array.is_null(index) {
        return Ok(bind_typed_null(q, array.data_type()));
    }

    let q = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            q.bind(arr.value(index) as i16)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            q.bind(arr.value(index) as i32)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            q.bind(arr.value(index) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            q.bind(arr.value(index).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            q.bind(arr.value(index).to_owned())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            q.bind(arr.value(index).to_owned())
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            q.bind(arr.value(index).to_owned())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            q.bind(arr.value(index).to_vec())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            q.bind(arr.value(index).to_vec())
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            q.bind(arr.value(index).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            q.bind(format_date_from_days(arr.value(index)))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let secs = arr.value(index) / 1000;
            q.bind(format_timestamp(secs, 0))
        }
        DataType::Timestamp(unit, _) => {
            let (secs, nanos) = extract_timestamp(array, index, unit);
            q.bind(format_timestamp(secs, nanos))
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            let s = extract_string_value(array, index).unwrap_or_default();
            q.bind(s)
        }
        DataType::Decimal128(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            q.bind(format_decimal(
                arr.value(index).to_string(),
                *scale as usize,
            ))
        }
        DataType::Decimal256(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            q.bind(format_decimal(
                arr.value(index).to_string(),
                *scale as usize,
            ))
        }
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => {
            let s = extract_string_value(array, index).unwrap_or_default();
            q.bind(s)
        }
        _ => {
            let s = extract_string_value(array, index).unwrap_or_default();
            q.bind(s)
        }
    };

    Ok(q)
}

fn bind_typed_null<'q>(q: MySqlQuery<'q>, dt: &DataType) -> MySqlQuery<'q> {
    match dt {
        DataType::Boolean => q.bind::<Option<bool>>(None),
        DataType::Int8 => q.bind::<Option<i8>>(None),
        DataType::Int16 => q.bind::<Option<i16>>(None),
        DataType::Int32 => q.bind::<Option<i32>>(None),
        DataType::Int64 => q.bind::<Option<i64>>(None),
        DataType::UInt8 => q.bind::<Option<i16>>(None),
        DataType::UInt16 => q.bind::<Option<i32>>(None),
        DataType::UInt32 => q.bind::<Option<i64>>(None),
        DataType::Float32 => q.bind::<Option<f32>>(None),
        DataType::Float64 => q.bind::<Option<f64>>(None),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            q.bind::<Option<Vec<u8>>>(None)
        }
        _ => q.bind::<Option<String>>(None),
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn extract_timestamp(
    array: &Arc<dyn Array>,
    index: usize,
    unit: &arrow::datatypes::TimeUnit,
) -> (i64, u32) {
    use arrow::datatypes::TimeUnit;
    match unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            (arr.value(index), 0)
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let ms = arr.value(index);
            (ms / 1000, ((ms % 1000) * 1_000_000) as u32)
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let us = arr.value(index);
            (us / 1_000_000, ((us % 1_000_000) * 1000) as u32)
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let ns = arr.value(index);
            (ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
        }
    }
}

fn format_timestamp(epoch_secs: i64, nanos: u32) -> String {
    const SECS_PER_DAY: i64 = 86400;

    let (mut days, mut day_secs) = (epoch_secs / SECS_PER_DAY, epoch_secs % SECS_PER_DAY);
    if day_secs < 0 {
        days -= 1;
        day_secs += SECS_PER_DAY;
    }

    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;
    let date = format_date_from_days((days + 719_163) as i32 - 719_163);

    if nanos > 0 {
        let micros = nanos / 1000;
        format!(
            "{} {:02}:{:02}:{:02}.{:06}",
            date, hours, minutes, seconds, micros
        )
    } else {
        format!("{} {:02}:{:02}:{:02}", date, hours, minutes, seconds)
    }
}

fn format_date_from_days(days_since_epoch: i32) -> String {
    let total_days = days_since_epoch as i64 + 719_468;
    let era = if total_days >= 0 {
        total_days / 146097
    } else {
        (total_days - 146096) / 146097
    };
    let doe = (total_days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn format_decimal(raw: String, scale: usize) -> String {
    if scale == 0 {
        return raw;
    }
    if let Some(dot) = raw.find('.') {
        let dec_part = &raw[dot + 1..];
        if dec_part.len() >= scale {
            raw[..dot + 1 + scale].to_string()
        } else {
            format!("{}{}", raw, "0".repeat(scale - dec_part.len()))
        }
    } else {
        format!("{}.{}", raw, "0".repeat(scale))
    }
}

fn extract_string_value(array: &Arc<dyn Array>, index: usize) -> Option<String> {
    if array.is_null(index) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(index).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>()?;
            Some(arr.value(index).to_string())
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>()?;
            Some(arr.value(index).to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            Some(arr.value(index).to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            Some(arr.value(index).to_string())
        }
        _ => {
            let col = array.slice(index, 1);
            Some(format!("{:?}", col))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_to_mysql_type() {
        assert_eq!(arrow_to_mysql_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_to_mysql_type(&DataType::Int32), "INT");
        assert_eq!(arrow_to_mysql_type(&DataType::Int64), "BIGINT");
        assert_eq!(arrow_to_mysql_type(&DataType::UInt64), "BIGINT UNSIGNED");
        assert_eq!(arrow_to_mysql_type(&DataType::Float64), "DOUBLE");
        assert_eq!(arrow_to_mysql_type(&DataType::Utf8), "TEXT");
        assert_eq!(arrow_to_mysql_type(&DataType::Binary), "LONGBLOB");
        assert_eq!(arrow_to_mysql_type(&DataType::Date32), "DATE");
        assert_eq!(
            arrow_to_mysql_type(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Second,
                None
            )),
            "DATETIME(6)"
        );
        assert_eq!(
            arrow_to_mysql_type(&DataType::Decimal128(10, 2)),
            "DECIMAL(10, 2)"
        );
        assert_eq!(
            arrow_to_mysql_type(&DataType::Decimal128(70, 10)),
            "DECIMAL(65, 10)"
        );
    }

    #[test]
    fn test_format_date_from_days() {
        assert_eq!(format_date_from_days(0), "1970-01-01");
        assert_eq!(format_date_from_days(1), "1970-01-02");
        assert_eq!(format_date_from_days(365), "1971-01-01");
        assert_eq!(format_date_from_days(-1), "1969-12-31");
    }

    #[test]
    fn test_format_timestamp() {
        assert_eq!(format_timestamp(0, 0), "1970-01-01 00:00:00");
        assert_eq!(format_timestamp(1000, 0), "1970-01-01 00:16:40");
        assert_eq!(
            format_timestamp(1000, 123_000_000),
            "1970-01-01 00:16:40.123000"
        );
    }

    #[test]
    fn test_format_decimal() {
        assert_eq!(format_decimal("123".into(), 0), "123");
        assert_eq!(format_decimal("123".into(), 2), "123.00");
        assert_eq!(format_decimal("123.4".into(), 3), "123.400");
        assert_eq!(format_decimal("123.456".into(), 2), "123.45");
    }
}
