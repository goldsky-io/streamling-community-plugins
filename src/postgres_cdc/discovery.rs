//! Output-schema discovery: reads the replicated table's columns from
//! Postgres at plugin construction and maps them to Arrow.
//!
//! The streamling host requires `output_schema` at plugin creation (before
//! `initialize()`), so discovery runs synchronously in the constructor on a
//! scratch thread with its own small runtime — the constructor may itself be
//! called from a tokio thread, where `block_on` would panic.
//!
//! All data columns are nullable in the output schema regardless of the
//! table's NOT NULL constraints: key-only delete images and unchanged-TOAST
//! update images legitimately omit column values.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use etl::config::PgConnectionConfig;
use etl::types::Type;
use secrecy::ExposeSecret;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use std::time::Duration;
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP;

/// One discovered table column.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredColumn {
    pub name: String,
    pub type_oid: u32,
}

/// Maps a Postgres type OID to the Arrow type the source emits.
///
/// Must stay in agreement with `arrow.rs`'s `Cell` → array conversion: every
/// OID mapped here must accept the `Cell` variant etl decodes for that type.
/// Unknown and complex types (numeric, uuid, json, arrays, enums, ...) fall
/// back to Utf8; `arrow.rs` stringifies their cells.
pub fn arrow_type_for(type_oid: u32) -> DataType {
    let Some(t) = Type::from_oid(type_oid) else {
        return DataType::Utf8;
    };
    if t == Type::BOOL {
        DataType::Boolean
    } else if t == Type::INT2 {
        DataType::Int16
    } else if t == Type::INT4 {
        DataType::Int32
    } else if t == Type::INT8 {
        DataType::Int64
    } else if t == Type::OID {
        DataType::UInt32
    } else if t == Type::FLOAT4 {
        DataType::Float32
    } else if t == Type::FLOAT8 {
        DataType::Float64
    } else if t == Type::BYTEA {
        DataType::Binary
    } else if t == Type::DATE {
        DataType::Date32
    } else if t == Type::TIME {
        DataType::Time64(TimeUnit::Microsecond)
    } else if t == Type::TIMESTAMP {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else if t == Type::TIMESTAMPTZ {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    } else {
        DataType::Utf8
    }
}

/// Builds the source output schema: `_gs_op` followed by one nullable field
/// per table column.
pub fn build_output_schema(columns: &[DiscoveredColumn]) -> Result<Schema, String> {
    if columns.is_empty() {
        return Err("postgres_cdc_source: table has no columns".to_string());
    }
    let mut fields = Vec::with_capacity(columns.len() + 1);
    fields.push(Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false));
    for col in columns {
        if col.name == STREAMLING_COLUMN_NAME_OP {
            return Err(format!(
                "postgres_cdc_source: table column '{}' collides with the reserved \
                 streamling op column",
                col.name
            ));
        }
        fields.push(Field::new(&col.name, arrow_type_for(col.type_oid), true));
    }
    Ok(Schema::new(fields))
}

/// Fetches the table's column names and type OIDs, blocking the caller.
///
/// Spawns a scratch thread with a current-thread runtime so it is safe to
/// call from both plain and tokio threads.
pub fn discover_columns_blocking(
    conn: &PgConnectionConfig,
    table_schema: &str,
    table_name: &str,
) -> Result<Vec<DiscoveredColumn>, String> {
    let mut opts = PgConnectOptions::new()
        .host(&conn.host)
        .port(conn.port)
        .database(&conn.name)
        .username(&conn.username)
        .ssl_mode(if conn.tls.enabled {
            PgSslMode::Require
        } else {
            PgSslMode::Prefer
        });
    if let Some(password) = &conn.password {
        opts = opts.password(password.expose_secret());
    }
    let (table_schema, table_name) = (table_schema.to_string(), table_name.to_string());

    std::thread::spawn(move || -> Result<Vec<DiscoveredColumn>, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("postgres_cdc_source: discovery runtime: {e}"))?;
        rt.block_on(async move {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(10))
                .connect_with(opts)
                .await
                .map_err(|e| format!("postgres_cdc_source: schema discovery connect: {e}"))?;
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT a.attname, a.atttypid::int8 \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
            )
            .bind(&table_schema)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| format!("postgres_cdc_source: schema discovery query: {e}"))?;
            pool.close().await;
            if rows.is_empty() {
                return Err(format!(
                    "postgres_cdc_source: table {table_schema}.{table_name} not found \
                     or has no columns"
                ));
            }
            Ok(rows
                .into_iter()
                .map(|(name, oid)| DiscoveredColumn {
                    name,
                    type_oid: oid as u32,
                })
                .collect())
        })
    })
    .join()
    .map_err(|_| "postgres_cdc_source: schema discovery thread panicked".to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: &Type) -> DiscoveredColumn {
        DiscoveredColumn {
            name: name.to_string(),
            type_oid: t.oid(),
        }
    }

    #[test]
    fn common_types_map_to_typed_arrow() {
        assert_eq!(arrow_type_for(Type::BOOL.oid()), DataType::Boolean);
        assert_eq!(arrow_type_for(Type::INT2.oid()), DataType::Int16);
        assert_eq!(arrow_type_for(Type::INT4.oid()), DataType::Int32);
        assert_eq!(arrow_type_for(Type::INT8.oid()), DataType::Int64);
        assert_eq!(arrow_type_for(Type::FLOAT4.oid()), DataType::Float32);
        assert_eq!(arrow_type_for(Type::FLOAT8.oid()), DataType::Float64);
        assert_eq!(arrow_type_for(Type::BYTEA.oid()), DataType::Binary);
        assert_eq!(arrow_type_for(Type::DATE.oid()), DataType::Date32);
        assert_eq!(
            arrow_type_for(Type::TIME.oid()),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            arrow_type_for(Type::TIMESTAMP.oid()),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            arrow_type_for(Type::TIMESTAMPTZ.oid()),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn text_like_complex_and_unknown_types_fall_back_to_utf8() {
        assert_eq!(arrow_type_for(Type::TEXT.oid()), DataType::Utf8);
        assert_eq!(arrow_type_for(Type::VARCHAR.oid()), DataType::Utf8);
        assert_eq!(arrow_type_for(Type::NUMERIC.oid()), DataType::Utf8);
        assert_eq!(arrow_type_for(Type::UUID.oid()), DataType::Utf8);
        assert_eq!(arrow_type_for(Type::JSONB.oid()), DataType::Utf8);
        assert_eq!(arrow_type_for(Type::INT8_ARRAY.oid()), DataType::Utf8);
        // OID 0 never names a real type.
        assert_eq!(arrow_type_for(0), DataType::Utf8);
    }

    #[test]
    fn output_schema_prepends_gs_op_and_makes_data_columns_nullable() {
        let schema =
            build_output_schema(&[col("id", &Type::INT8), col("name", &Type::TEXT)]).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["_gs_op", "id", "name"]);
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn output_schema_rejects_gs_op_column_collision_and_empty_tables() {
        assert!(
            build_output_schema(&[col("_gs_op", &Type::TEXT)])
                .unwrap_err()
                .contains("_gs_op")
        );
        assert!(build_output_schema(&[]).is_err());
    }
}
