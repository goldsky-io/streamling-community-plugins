//! Plugin option parsing → etl `PipelineConfig` + source-local settings.
//!
//! Required: `host`, `database`, `username`, `publication_name`, `table`
//! (schema-qualified, e.g. `public.users`; bare names default to `public` —
//! one source instance replicates exactly one table), `slot_name` (string;
//! replication-slot group key — sources sharing it share one slot).
//!
//! Optional: `port` (5432), `password`, `tls_enabled` (false),
//! `trusted_root_certs` (PEM), `store_host`/`store_port`/`store_database`/
//! `store_username`/`store_password` (default: source connection; the store
//! Postgres gets an `etl` schema created by migrations),
//! `batch_max_fill_ms` (1000), `batch_max_bytes` (8 MiB),
//! `max_table_sync_workers` (4), `batch_size` (1000 envelope rows),
//! `batch_interval_ms` (100), `max_buffered_units` (8).

use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Stable identity of a shared-slot group: every source sharing a `slot_name`
/// must agree on these. Derived from the parsed config; compared by value.
///
/// **Note:** the connection password is intentionally excluded. A shared `slot_name`
/// does not reject a mismatched password; the first-registered source's connection
/// (including its password) is authoritative for the group. Operators must keep
/// credentials consistent across sources that share a `slot_name`. Excluding the
/// password also keeps secrets out of this `Debug`/`PartialEq` struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupIdentity {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub publication_name: String,
}

/// Maps a `slot_name` to etl's `u64` pipeline id (first 8 bytes of SHA-256,
/// big-endian). Deterministic across runs and versions.
pub fn hash_slot_name(slot_name: &str) -> u64 {
    let digest = Sha256::digest(slot_name.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 >= 8 bytes"))
}

#[derive(Debug, Clone)]
pub struct SourceSettings {
    /// Schema of the replicated table (e.g. "public").
    pub table_schema: String,
    /// Name of the replicated table.
    pub table_name: String,
    /// Replication-slot group key; shared across sources that share a slot.
    pub slot_name: String,
    /// Max envelope rows per generated batch.
    pub batch_size: usize,
    /// Max wait for the first unit in generate_batch.
    pub batch_interval_ms: u64,
    /// Bounded channel capacity, in units.
    pub max_buffered_units: usize,
}

#[derive(Debug)]
pub struct ParsedConfig {
    pub pipeline: PipelineConfig,
    pub settings: SourceSettings,
}

impl ParsedConfig {
    /// Identity all sources sharing this `slot_name` must agree on.
    pub fn group_identity(&self) -> GroupIdentity {
        GroupIdentity {
            host: self.pipeline.pg_connection.host.clone(),
            port: self.pipeline.pg_connection.port,
            database: self.pipeline.pg_connection.name.clone(),
            username: self.pipeline.pg_connection.username.clone(),
            publication_name: self.pipeline.publication_name.clone(),
        }
    }
}

fn required<'a>(options: &'a HashMap<String, String>, key: &str) -> Result<&'a str, String> {
    options
        .get(key)
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("postgres_cdc_source: missing required option '{key}'"))
}

fn parse_opt<T: std::str::FromStr>(
    options: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, String> {
    match options.get(key) {
        None => Ok(default),
        Some(s) => s
            .parse()
            .map_err(|_| format!("postgres_cdc_source: invalid {key} '{s}'")),
    }
}

/// Splits a `table` option into (schema, name); bare names get "public".
fn parse_table(raw: &str) -> Result<(String, String), String> {
    let mut parts = raw.splitn(2, '.');
    let first = parts.next().unwrap_or_default();
    match parts.next() {
        Some(name) if !first.is_empty() && !name.is_empty() && !name.contains('.') => {
            Ok((first.to_string(), name.to_string()))
        }
        None if !first.is_empty() => Ok(("public".to_string(), first.to_string())),
        _ => Err(format!(
            "postgres_cdc_source: invalid table '{raw}' (expected 'name' or 'schema.name')"
        )),
    }
}

pub fn parse_options(options: &HashMap<String, String>) -> Result<ParsedConfig, String> {
    let slot_name = required(options, "slot_name")?.to_string();
    let pipeline_id = hash_slot_name(&slot_name);

    let tls = TlsConfig {
        trusted_root_certs: options
            .get("trusted_root_certs")
            .cloned()
            .unwrap_or_default(),
        enabled: parse_opt(options, "tls_enabled", false)?,
    };

    let pg_connection = PgConnectionConfig {
        host: required(options, "host")?.to_string(),
        hostaddr: None,
        port: parse_opt(options, "port", 5432u16)?,
        name: required(options, "database")?.to_string(),
        username: required(options, "username")?.to_string(),
        password: options.get("password").cloned().map(Into::into),
        tls: tls.clone(),
        keepalive: TcpKeepaliveConfig::default(),
    };

    let store_keys = ["store_host", "store_database", "store_username"];
    let store_present = store_keys
        .iter()
        .filter(|k| options.contains_key(**k))
        .count();
    let store_pg_connection = match store_present {
        0 => None,
        3 => Some(PgConnectionConfig {
            host: required(options, "store_host")?.to_string(),
            hostaddr: None,
            port: parse_opt(options, "store_port", 5432u16)?,
            name: required(options, "store_database")?.to_string(),
            username: required(options, "store_username")?.to_string(),
            password: options.get("store_password").cloned().map(Into::into),
            tls,
            keepalive: TcpKeepaliveConfig::default(),
        }),
        _ => {
            return Err(
                "postgres_cdc_source: store_host, store_database and store_username must be \
                 set together"
                    .to_string(),
            );
        }
    };

    let pipeline = PipelineConfig {
        id: pipeline_id,
        publication_name: required(options, "publication_name")?.to_string(),
        pg_connection,
        store_pg_connection,
        batch: BatchConfig {
            max_fill_ms: parse_opt(options, "batch_max_fill_ms", 1000u64)?,
            memory_budget_ratio: 0.2,
            max_bytes: parse_opt(options, "batch_max_bytes", 8usize * 1024 * 1024)?,
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: parse_opt(options, "max_table_sync_workers", 4u16)?,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let (table_schema, table_name) = parse_table(required(options, "table")?)?;

    let batch_size = parse_opt(options, "batch_size", 1000usize)?;
    if batch_size == 0 {
        return Err("postgres_cdc_source: batch_size must be greater than 0".to_string());
    }
    let max_buffered_units = parse_opt(options, "max_buffered_units", 8usize)?;
    if max_buffered_units == 0 {
        return Err("postgres_cdc_source: max_buffered_units must be greater than 0".to_string());
    }
    Ok(ParsedConfig {
        pipeline,
        settings: SourceSettings {
            table_schema,
            table_name,
            slot_name,
            batch_size,
            batch_interval_ms: parse_opt(options, "batch_interval_ms", 100u64)?,
            max_buffered_units,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_options() -> HashMap<String, String> {
        [
            ("host", "db.example.com"),
            ("database", "app"),
            ("username", "replicator"),
            ("password", "hunter2"),
            ("publication_name", "my_pub"),
            ("slot_name", "demo_slot"),
            ("table", "public.users"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse_options(&base_options()).unwrap();
        assert_eq!(cfg.pipeline.id, hash_slot_name("demo_slot"));
        assert_eq!(cfg.pipeline.publication_name, "my_pub");
        assert_eq!(cfg.pipeline.pg_connection.host, "db.example.com");
        assert_eq!(cfg.pipeline.pg_connection.port, 5432);
        assert_eq!(cfg.pipeline.pg_connection.name, "app");
        assert_eq!(cfg.pipeline.pg_connection.username, "replicator");
        assert!(cfg.pipeline.pg_connection.password.is_some());
        assert!(!cfg.pipeline.pg_connection.tls.enabled);
        assert!(cfg.pipeline.store_pg_connection.is_none());
        assert_eq!(cfg.pipeline.batch.max_fill_ms, 1000);
        assert_eq!(cfg.pipeline.batch.max_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.pipeline.max_table_sync_workers, 4);
        assert_eq!(cfg.settings.batch_size, 1000);
        assert_eq!(cfg.settings.batch_interval_ms, 100);
        assert_eq!(cfg.settings.max_buffered_units, 8);
    }

    #[test]
    fn missing_required_option_is_an_error() {
        for key in [
            "host",
            "database",
            "username",
            "publication_name",
            "slot_name",
        ] {
            let mut opts = base_options();
            opts.remove(key);
            let err = parse_options(&opts).unwrap_err();
            assert!(err.contains(key), "error {err:?} should name {key}");
        }
    }

    #[test]
    fn table_option_parses_qualified_and_bare_names() {
        let cfg = parse_options(&base_options()).unwrap();
        assert_eq!(cfg.settings.table_schema, "public");
        assert_eq!(cfg.settings.table_name, "users");

        let mut opts = base_options();
        opts.insert("table".into(), "sales.orders".into());
        let cfg = parse_options(&opts).unwrap();
        assert_eq!(cfg.settings.table_schema, "sales");
        assert_eq!(cfg.settings.table_name, "orders");

        opts.insert("table".into(), "orders".into());
        let cfg = parse_options(&opts).unwrap();
        assert_eq!(cfg.settings.table_schema, "public");
        assert_eq!(cfg.settings.table_name, "orders");

        opts.insert("table".into(), "a.b.c".into());
        assert!(parse_options(&opts).unwrap_err().contains("table"));
    }

    #[test]
    fn hash_slot_name_is_stable_and_distinct() {
        // Deterministic across calls/runs (first 8 bytes of SHA-256, BE).
        assert_eq!(hash_slot_name("demo_slot"), hash_slot_name("demo_slot"));
        assert_ne!(hash_slot_name("demo_slot"), hash_slot_name("other_slot"));
        // Pin one known value so an accidental algorithm change is caught.
        assert_eq!(hash_slot_name(""), 0xe3b0c44298fc1c14);
    }

    #[test]
    fn slot_name_derives_pipeline_id_and_is_exposed() {
        let cfg = parse_options(&base_options()).unwrap();
        assert_eq!(cfg.settings.slot_name, "demo_slot");
        assert_eq!(cfg.pipeline.id, hash_slot_name("demo_slot"));
    }

    #[test]
    fn empty_slot_name_is_an_error() {
        let mut opts = base_options();
        opts.insert("slot_name".into(), "".into());
        assert!(parse_options(&opts).unwrap_err().contains("slot_name"));
    }

    #[test]
    fn group_identity_matches_for_same_connection_and_publication() {
        let a = parse_options(&base_options()).unwrap().group_identity();
        let b = parse_options(&base_options()).unwrap().group_identity();
        assert_eq!(a, b);
        let mut opts = base_options();
        opts.insert("publication_name".into(), "other_pub".into());
        let c = parse_options(&opts).unwrap().group_identity();
        assert_ne!(a, c);
    }

    #[test]
    fn store_options_build_separate_store_connection() {
        let mut opts = base_options();
        opts.insert("store_host".into(), "state.example.com".into());
        opts.insert("store_database".into(), "etl_state".into());
        opts.insert("store_username".into(), "etl".into());
        let cfg = parse_options(&opts).unwrap();
        let store = cfg.pipeline.store_pg_connection.unwrap();
        assert_eq!(store.host, "state.example.com");
        assert_eq!(store.name, "etl_state");
        assert_eq!(store.port, 5432);
    }

    #[test]
    fn partial_store_options_are_an_error() {
        let mut opts = base_options();
        opts.insert("store_host".into(), "state.example.com".into());
        // store_database / store_username missing
        assert!(parse_options(&opts).unwrap_err().contains("store_"));
    }

    #[test]
    fn zero_batch_size_is_an_error() {
        let mut opts = base_options();
        opts.insert("batch_size".into(), "0".into());
        let err = parse_options(&opts).unwrap_err();
        assert!(err.contains("batch_size"), "got {err:?}");
    }

    #[test]
    fn zero_max_buffered_units_is_an_error() {
        let mut opts = base_options();
        opts.insert("max_buffered_units".into(), "0".into());
        let err = parse_options(&opts).unwrap_err();
        assert!(err.contains("max_buffered_units"), "got {err:?}");
    }

    #[test]
    fn tls_options_apply() {
        let mut opts = base_options();
        opts.insert("tls_enabled".into(), "true".into());
        opts.insert("trusted_root_certs".into(), "PEMPEM".into());
        let cfg = parse_options(&opts).unwrap();
        assert!(cfg.pipeline.pg_connection.tls.enabled);
        assert_eq!(cfg.pipeline.pg_connection.tls.trusted_root_certs, "PEMPEM");
    }
}
