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
//! `batch_interval_ms` (100), `max_buffered_units` (8),
//! `auto_create_publication` (true (default): create the publication / add
//! missing tables before starting — needs CREATE/ownership privileges),
//! `memory_backpressure_enabled` (true (default): etl pauses the replication
//! apply stream while *system* memory use is above the activate threshold; set
//! false to disable — useful on dev machines whose system memory sits high, where
//! the pause otherwise stalls live changes),
//! `memory_backpressure_activate_threshold` (0.85),
//! `memory_backpressure_resume_threshold` (0.75),
//! `emit_update_before_row` (false: emit an extra `d` row carrying an update's
//! old image before its new image, for sinks that retract prior state; requires
//! `REPLICA IDENTITY FULL`).
//!
//! Every option can also be set via `STREAMLING__PLUGIN__POSTGRES_CDC_SOURCE__<KEY>`
//! (uppercase key), which takes precedence over the YAML value.

use crate::utils::plugin_options::PluginOptions;
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use sha2::{Digest, Sha256};
use streamling_plugin::PluginError;

pub const PLUGIN_NAME: &str = "postgres_cdc_source";
pub const ENV_PREFIX: &str = "STREAMLING__PLUGIN__POSTGRES_CDC_SOURCE";

const DEFAULT_PORT: u16 = 5432;

/// Keys of the optional separate metadata-store connection. All or none.
const STORE_KEYS: [&str; 3] = ["store_host", "store_database", "store_username"];

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
    /// Create the publication if missing and `ALTER ... ADD TABLE` any
    /// registered tables not yet in it, before starting. Off by default —
    /// managing the publication is usually an operator concern. Requires a
    /// role with `CREATE` on the database / table ownership; see the README.
    pub auto_create_publication: bool,
    /// Precede each update's new image with a `d` row carrying the old image.
    /// Off by default: sinks that upsert by primary key would see a spurious
    /// delete. Requires `REPLICA IDENTITY FULL` on the replicated table.
    pub emit_update_before_row: bool,
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

/// Builds a connection from `{prefix}host`-style keys, so the replicated
/// database and the metadata store are configured the same way.
fn connection(
    options: &PluginOptions,
    prefix: &str,
    tls: TlsConfig,
) -> Result<PgConnectionConfig, PluginError> {
    Ok(PgConnectionConfig {
        host: options.get(&format!("{prefix}host"))?,
        hostaddr: None,
        port: options.get_parsed_or(&format!("{prefix}port"), DEFAULT_PORT)?,
        name: options.get(&format!("{prefix}database"))?,
        username: options.get(&format!("{prefix}username"))?,
        password: options
            .get_secret(&format!("{prefix}password"))
            .map(Into::into),
        tls,
        keepalive: TcpKeepaliveConfig::default(),
    })
}

/// Splits a `table` option into (schema, name); bare names get "public".
fn parse_table(raw: &str) -> Result<(String, String), PluginError> {
    let mut parts = raw.splitn(2, '.');
    let first = parts.next().unwrap_or_default();
    match parts.next() {
        Some(name) if !first.is_empty() && !name.is_empty() && !name.contains('.') => {
            Ok((first.to_string(), name.to_string()))
        }
        None if !first.is_empty() => Ok(("public".to_string(), first.to_string())),
        _ => Err(PluginError::Internal(format!(
            "{PLUGIN_NAME}: invalid table '{raw}' (expected 'name' or 'schema.name')"
        ))),
    }
}

/// Rejects an option that must be greater than zero, naming it.
fn positive(value: usize, key: &str) -> Result<usize, PluginError> {
    match value {
        0 => Err(PluginError::Internal(format!(
            "{PLUGIN_NAME}: {key} must be greater than 0"
        ))),
        n => Ok(n),
    }
}

/// Builds etl's memory-backpressure config. `memory_backpressure_enabled=false`
/// returns `None`, which disables etl's system-memory-driven pause of the
/// replication apply stream entirely (the pause can otherwise stall live changes
/// indefinitely on hosts whose overall memory use stays above the resume
/// threshold). When enabled, thresholds default to etl's own defaults and are
/// validated by [`MemoryBackpressureConfig::validate`]. Thresholds are ignored
/// (unparsed) when disabled.
fn parse_memory_backpressure(
    options: &PluginOptions,
) -> Result<Option<MemoryBackpressureConfig>, PluginError> {
    if !options.get_parsed_or("memory_backpressure_enabled", true)? {
        return Ok(None);
    }
    let config = MemoryBackpressureConfig {
        activate_threshold: options.get_parsed_or(
            "memory_backpressure_activate_threshold",
            MemoryBackpressureConfig::DEFAULT_ACTIVATE_THRESHOLD,
        )?,
        resume_threshold: options.get_parsed_or(
            "memory_backpressure_resume_threshold",
            MemoryBackpressureConfig::DEFAULT_RESUME_THRESHOLD,
        )?,
    };
    config.validate().map_err(|e| {
        PluginError::Internal(format!(
            "{PLUGIN_NAME}: invalid memory_backpressure config: {e}"
        ))
    })?;
    Ok(Some(config))
}

pub fn parse_options(options: &PluginOptions) -> Result<ParsedConfig, PluginError> {
    let slot_name = options.get("slot_name")?;
    let pipeline_id = hash_slot_name(&slot_name);

    let tls = TlsConfig {
        trusted_root_certs: options.get_or("trusted_root_certs", ""),
        enabled: options.get_parsed_or("tls_enabled", false)?,
    };

    let pg_connection = connection(options, "", tls.clone())?;

    let store_present = STORE_KEYS
        .iter()
        .filter(|k| options.lookup(k).is_some())
        .count();
    let store_pg_connection = match store_present {
        0 => None,
        n if n == STORE_KEYS.len() => Some(connection(options, "store_", tls)?),
        _ => {
            return Err(PluginError::Internal(format!(
                "{PLUGIN_NAME}: {} must be set together",
                STORE_KEYS.join(", ")
            )));
        }
    };

    let pipeline = PipelineConfig {
        id: pipeline_id,
        publication_name: options.get("publication_name")?,
        pg_connection,
        store_pg_connection,
        batch: BatchConfig {
            max_fill_ms: options.get_parsed_or("batch_max_fill_ms", 1000u64)?,
            memory_budget_ratio: 0.2,
            max_bytes: options.get_parsed_or("batch_max_bytes", 8usize * 1024 * 1024)?,
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: options.get_parsed_or("max_table_sync_workers", 4u16)?,
        memory_refresh_interval_ms: 100,
        memory_backpressure: parse_memory_backpressure(options)?,
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let (table_schema, table_name) = parse_table(&options.get("table")?)?;

    Ok(ParsedConfig {
        pipeline,
        settings: SourceSettings {
            table_schema,
            table_name,
            slot_name,
            batch_size: positive(
                options.get_parsed_or("batch_size", 1000usize)?,
                "batch_size",
            )?,
            batch_interval_ms: options.get_parsed_or("batch_interval_ms", 100u64)?,
            max_buffered_units: positive(
                options.get_parsed_or("max_buffered_units", 8usize)?,
                "max_buffered_units",
            )?,
            auto_create_publication: options.get_parsed_or("auto_create_publication", true)?,
            emit_update_before_row: options.get_parsed_or("emit_update_before_row", false)?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const TEST_ENV_PREFIX: &str = "STREAMLING__PLUGIN__POSTGRES_CDC_SOURCE_CONFIG_TEST";

    fn parse(options: &HashMap<String, String>) -> Result<ParsedConfig, String> {
        parse_options(&PluginOptions::new(
            options.clone(),
            PLUGIN_NAME,
            TEST_ENV_PREFIX,
        ))
        .map_err(|e| e.to_string())
    }

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
    fn env_vars_override_yaml_options() {
        const PREFIX: &str = "STREAMLING__PLUGIN__POSTGRES_CDC_SOURCE_ENV_TEST";
        let mut opts = base_options();
        opts.remove("password");
        unsafe {
            std::env::set_var(format!("{PREFIX}__PORT"), "6543");
            std::env::set_var(format!("{PREFIX}__HOST"), "from-env.example.com");
            std::env::set_var(format!("{PREFIX}__PASSWORD"), "from-env");
            std::env::set_var(format!("{PREFIX}__EMIT_UPDATE_BEFORE_ROW"), "true");
        }
        let cfg = parse_options(&PluginOptions::new(opts, PLUGIN_NAME, PREFIX)).unwrap();
        // Overrides a YAML value, supplies an absent secret, and flips a flag.
        assert_eq!(cfg.pipeline.pg_connection.port, 6543);
        assert_eq!(cfg.pipeline.pg_connection.host, "from-env.example.com");
        assert!(cfg.pipeline.pg_connection.password.is_some());
        assert!(cfg.settings.emit_update_before_row);
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse(&base_options()).unwrap();
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
        assert!(cfg.settings.auto_create_publication);
        assert_eq!(
            cfg.pipeline.memory_backpressure,
            Some(MemoryBackpressureConfig::default())
        );
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
            let err = parse(&opts).unwrap_err();
            assert!(err.contains(key), "error {err:?} should name {key}");
        }
    }

    #[test]
    fn table_option_parses_qualified_and_bare_names() {
        let cfg = parse(&base_options()).unwrap();
        assert_eq!(cfg.settings.table_schema, "public");
        assert_eq!(cfg.settings.table_name, "users");

        let mut opts = base_options();
        opts.insert("table".into(), "sales.orders".into());
        let cfg = parse(&opts).unwrap();
        assert_eq!(cfg.settings.table_schema, "sales");
        assert_eq!(cfg.settings.table_name, "orders");

        opts.insert("table".into(), "orders".into());
        let cfg = parse(&opts).unwrap();
        assert_eq!(cfg.settings.table_schema, "public");
        assert_eq!(cfg.settings.table_name, "orders");

        opts.insert("table".into(), "a.b.c".into());
        assert!(parse(&opts).unwrap_err().contains("table"));
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
        let cfg = parse(&base_options()).unwrap();
        assert_eq!(cfg.settings.slot_name, "demo_slot");
        assert_eq!(cfg.pipeline.id, hash_slot_name("demo_slot"));
    }

    #[test]
    fn empty_slot_name_is_an_error() {
        let mut opts = base_options();
        opts.insert("slot_name".into(), "".into());
        assert!(parse(&opts).unwrap_err().contains("slot_name"));
    }

    #[test]
    fn group_identity_matches_for_same_connection_and_publication() {
        let a = parse(&base_options()).unwrap().group_identity();
        let b = parse(&base_options()).unwrap().group_identity();
        assert_eq!(a, b);
        let mut opts = base_options();
        opts.insert("publication_name".into(), "other_pub".into());
        let c = parse(&opts).unwrap().group_identity();
        assert_ne!(a, c);
    }

    #[test]
    fn store_options_build_separate_store_connection() {
        let mut opts = base_options();
        opts.insert("store_host".into(), "state.example.com".into());
        opts.insert("store_database".into(), "etl_state".into());
        opts.insert("store_username".into(), "etl".into());
        let cfg = parse(&opts).unwrap();
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
        assert!(parse(&opts).unwrap_err().contains("store_"));
    }

    #[test]
    fn zero_batch_size_is_an_error() {
        let mut opts = base_options();
        opts.insert("batch_size".into(), "0".into());
        let err = parse(&opts).unwrap_err();
        assert!(err.contains("batch_size"), "got {err:?}");
    }

    #[test]
    fn zero_max_buffered_units_is_an_error() {
        let mut opts = base_options();
        opts.insert("max_buffered_units".into(), "0".into());
        let err = parse(&opts).unwrap_err();
        assert!(err.contains("max_buffered_units"), "got {err:?}");
    }

    #[test]
    fn tls_options_apply() {
        let mut opts = base_options();
        opts.insert("tls_enabled".into(), "true".into());
        opts.insert("trusted_root_certs".into(), "PEMPEM".into());
        let cfg = parse(&opts).unwrap();
        assert!(cfg.pipeline.pg_connection.tls.enabled);
        assert_eq!(cfg.pipeline.pg_connection.tls.trusted_root_certs, "PEMPEM");
    }
    #[test]
    fn auto_create_publication_defaults_true_and_parses() {
        // Defaults to true when unset.
        let cfg = parse(&base_options()).unwrap();
        assert!(cfg.settings.auto_create_publication);

        // Explicit opt-out.
        let mut opts = base_options();
        opts.insert("auto_create_publication".into(), "false".into());
        let cfg = parse(&opts).unwrap();
        assert!(!cfg.settings.auto_create_publication);
    }

    #[test]
    fn memory_backpressure_can_be_disabled_and_tuned() {
        // Disabled -> None (turns off etl's system-memory apply-stream pause).
        let mut opts = base_options();
        opts.insert("memory_backpressure_enabled".into(), "false".into());
        let cfg = parse(&opts).unwrap();
        assert!(cfg.pipeline.memory_backpressure.is_none());

        // Enabled with custom thresholds.
        let mut opts = base_options();
        opts.insert(
            "memory_backpressure_activate_threshold".into(),
            "0.95".into(),
        );
        opts.insert("memory_backpressure_resume_threshold".into(), "0.9".into());
        let bp = parse(&opts)
            .unwrap()
            .pipeline
            .memory_backpressure
            .expect("enabled by default");
        assert_eq!(bp.activate_threshold, 0.95);
        assert_eq!(bp.resume_threshold, 0.9);
    }

    #[test]
    fn invalid_memory_backpressure_is_an_error() {
        // resume must be lower than activate (etl's own validation).
        let mut opts = base_options();
        opts.insert(
            "memory_backpressure_activate_threshold".into(),
            "0.5".into(),
        );
        opts.insert("memory_backpressure_resume_threshold".into(), "0.8".into());
        let err = parse(&opts).unwrap_err();
        assert!(err.contains("memory_backpressure"), "got {err:?}");

        // Non-numeric threshold.
        let mut opts = base_options();
        opts.insert(
            "memory_backpressure_activate_threshold".into(),
            "high".into(),
        );
        let err = parse(&opts).unwrap_err();
        assert!(
            err.contains("memory_backpressure_activate_threshold"),
            "got {err:?}"
        );

        // Thresholds are ignored when disabled: out-of-range values do not error.
        let mut opts = base_options();
        opts.insert("memory_backpressure_enabled".into(), "false".into());
        opts.insert(
            "memory_backpressure_activate_threshold".into(),
            "0.1".into(),
        );
        opts.insert("memory_backpressure_resume_threshold".into(), "0.9".into());
        assert!(parse(&opts).unwrap().pipeline.memory_backpressure.is_none());
    }
}
