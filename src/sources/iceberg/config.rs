//! Plugin option parsing for the Iceberg source. See the module docs in
//! `mod.rs` for the full option list.

use crate::utils::iceberg::{CatalogConfig, parse_table_ident};
use crate::utils::plugin_options::PluginOptions;
use iceberg::TableIdent;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;
use streamling_plugin::PluginError;

pub const PLUGIN_NAME: &str = "iceberg_source";
pub const ENV_PREFIX: &str = "STREAMLING__PLUGIN__ICEBERG_SOURCE";

/// Where a source without persisted state begins.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StartPosition {
    /// Scan the current snapshot, then read new snapshots incrementally.
    Earliest,
    /// Only snapshots committed after the source first starts.
    Latest,
}

/// What to do with `overwrite` and `delete` snapshots.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OnNonAppendSnapshot {
    Error,
    Skip,
}

#[derive(Debug, Clone)]
pub struct IcebergSourceConfig {
    pub catalog: CatalogConfig,
    pub table: TableIdent,
    pub start_position: StartPosition,
    /// How often to reload table metadata once caught up.
    pub poll_interval: Duration,
    /// Max rows per generated Arrow batch.
    pub batch_size: usize,
    pub on_non_append_snapshot: OnNonAppendSnapshot,
}

fn parse_start_position(opts: &PluginOptions) -> Result<StartPosition, PluginError> {
    let value = opts.get_or("start_position", "earliest");
    match value.to_ascii_lowercase().as_str() {
        "earliest" => Ok(StartPosition::Earliest),
        "latest" => Ok(StartPosition::Latest),
        other => Err(PluginError::Internal(format!(
            "{PLUGIN_NAME}: start_position must be 'earliest' or 'latest', got '{other}'"
        ))),
    }
}

fn parse_on_non_append_snapshot(opts: &PluginOptions) -> Result<OnNonAppendSnapshot, PluginError> {
    let value = opts.get_or("on_non_append_snapshot", "error");
    match value.to_ascii_lowercase().as_str() {
        "error" => Ok(OnNonAppendSnapshot::Error),
        "skip" => Ok(OnNonAppendSnapshot::Skip),
        other => Err(PluginError::Internal(format!(
            "{PLUGIN_NAME}: on_non_append_snapshot must be 'error' or 'skip', got '{other}'"
        ))),
    }
}

pub fn parse_config(opts: &PluginOptions) -> Result<IcebergSourceConfig, PluginError> {
    Ok(IcebergSourceConfig {
        catalog: CatalogConfig::parse(opts)?,
        table: parse_table_ident(&opts.get("table")?)?,
        start_position: parse_start_position(opts)?,
        poll_interval: Duration::from_secs(
            opts.get_parsed::<NonZeroU64>("poll_interval_secs", "60")?
                .get(),
        ),
        batch_size: opts.get_parsed::<NonZeroUsize>("batch_size", "1000")?.get(),
        on_non_append_snapshot: parse_on_non_append_snapshot(opts)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::for_test(
            PLUGIN_NAME,
            "STREAMLING__PLUGIN__ICEBERG_SOURCE_CONFIG_TEST",
            pairs,
        )
    }

    const REQUIRED: [(&str, &str); 2] = [
        ("catalog_uri", "http://localhost:8181"),
        ("table", "lake.events"),
    ];

    fn with(extra: &[(&'static str, &'static str)]) -> PluginOptions {
        let mut pairs = REQUIRED.to_vec();
        pairs.extend_from_slice(extra);
        options(&pairs)
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse_config(&with(&[])).unwrap();
        assert_eq!(cfg.table.to_string(), "lake.events");
        assert_eq!(cfg.start_position, StartPosition::Earliest);
        assert_eq!(cfg.poll_interval, Duration::from_secs(60));
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.on_non_append_snapshot, OnNonAppendSnapshot::Error);
    }

    #[test]
    fn requires_catalog_uri_and_table() {
        let err = parse_config(&options(&[("table", "lake.events")])).unwrap_err();
        assert!(err.to_string().contains("catalog_uri"), "got {err}");
        let err = parse_config(&options(&[("catalog_uri", "http://x")])).unwrap_err();
        assert!(err.to_string().contains("table"), "got {err}");
        let err = parse_config(&options(&[
            ("catalog_uri", "http://x"),
            ("table", "events"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("namespace.table"), "got {err}");
    }

    #[test]
    fn parses_optional_options() {
        let cfg = parse_config(&with(&[
            ("start_position", "LATEST"),
            ("poll_interval_secs", "5"),
            ("batch_size", "10"),
            ("on_non_append_snapshot", "skip"),
        ]))
        .unwrap();
        assert_eq!(cfg.start_position, StartPosition::Latest);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
        assert_eq!(cfg.batch_size, 10);
        assert_eq!(cfg.on_non_append_snapshot, OnNonAppendSnapshot::Skip);
    }

    #[test]
    fn rejects_invalid_values() {
        for (key, value) in [
            ("start_position", "beginning"),
            ("on_non_append_snapshot", "ignore"),
            ("poll_interval_secs", "0"),
            ("batch_size", "0"),
            ("batch_size", "many"),
        ] {
            let err = parse_config(&with(&[(key, value)])).unwrap_err();
            assert!(err.to_string().contains(key), "{key}={value}: got {err}");
        }
    }
}
