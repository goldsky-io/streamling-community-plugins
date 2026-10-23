//! Plugin option parsing for the Iceberg sink. See the module docs in
//! `mod.rs` for the full option list.

use std::collections::HashMap;

use iceberg::TableIdent;
use iceberg::spec::{FormatVersion, Transform};
use streamling_plugin::PluginError;

use crate::utils::iceberg::{CatalogConfig, parse_table_ident};
use crate::utils::plugin_options::PluginOptions;

/// Table properties the sink sets on tables it creates; `table_properties`
/// entries override them.
const DEFAULT_TABLE_PROPERTIES: [(&str, &str); 3] = [
    ("write.parquet.compression-codec", "zstd"),
    ("write.metadata.delete-after-commit.enabled", "true"),
    ("write.metadata.previous-versions-max", "100"),
];

#[derive(Debug, Clone)]
pub(crate) struct IcebergSinkConfig {
    pub catalog: CatalogConfig,
    pub table: TableIdent,
    /// Create the namespace and table if missing.
    pub auto_create: bool,
    /// Partition spec for auto-created tables.
    pub partition_by: Vec<PartitionTerm>,
    /// Format version for auto-created tables.
    pub format_version: FormatVersion,
    /// Properties for auto-created tables: sink defaults merged with
    /// `table_properties`.
    pub table_properties: HashMap<String, String>,
    /// Keep `_gs_op` as a regular column instead of requiring inserts only.
    pub include_op_column: bool,
}

/// One `partition_by` entry: a transform over an input column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PartitionTerm {
    pub column: String,
    pub transform: Transform,
}

impl PartitionTerm {
    /// The partition field name, following the Iceberg Java naming
    /// convention (`col`, `col_day`, `col_bucket`, `col_trunc`, ...).
    pub fn field_name(&self) -> String {
        let suffix = match self.transform {
            Transform::Identity => return self.column.clone(),
            Transform::Year => "year",
            Transform::Month => "month",
            Transform::Day => "day",
            Transform::Hour => "hour",
            Transform::Bucket(_) => "bucket",
            Transform::Truncate(_) => "trunc",
            Transform::Void | Transform::Unknown => "void",
        };
        format!("{}_{suffix}", self.column)
    }
}

pub(crate) fn parse_config(opts: &PluginOptions) -> Result<IcebergSinkConfig, PluginError> {
    let format_version = match opts.get_or("format_version", "2").trim() {
        "1" => FormatVersion::V1,
        "2" => FormatVersion::V2,
        other => {
            return Err(PluginError::Internal(format!(
                "iceberg_sink: invalid format_version '{other}': expected 1 or 2"
            )));
        }
    };
    Ok(IcebergSinkConfig {
        catalog: CatalogConfig::parse(opts)?,
        table: parse_table_ident(&opts.get("table")?)?,
        auto_create: opts.get_parsed("auto_create", "true")?,
        partition_by: parse_partition_by(&opts.get_or("partition_by", ""))?,
        format_version,
        table_properties: parse_table_properties(&opts.get_or("table_properties", ""))?,
        include_op_column: opts.get_parsed("include_op_column", "true")?,
    })
}

/// Parses `table_properties` (`k=v,k=v`) merged over the sink defaults.
fn parse_table_properties(value: &str) -> Result<HashMap<String, String>, PluginError> {
    let mut props: HashMap<String, String> = DEFAULT_TABLE_PROPERTIES
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    for pair in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = pair
            .split_once('=')
            .map(|(k, v)| (k.trim(), v.trim()))
            .filter(|(k, _)| !k.is_empty())
            .ok_or_else(|| {
                PluginError::Internal(format!(
                    "iceberg_sink: table_properties: expected `key=value`, got `{pair}`"
                ))
            })?;
        if k == "format-version" {
            return Err(PluginError::Internal(
                "iceberg_sink: table_properties: set the format version with the \
                 `format_version` option, not `format-version`"
                    .to_string(),
            ));
        }
        props.insert(k.to_string(), v.to_string());
    }
    Ok(props)
}

/// Parses `partition_by`: a comma-separated list of `col`, `identity(col)`,
/// `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`,
/// `truncate(W, col)`.
pub(crate) fn parse_partition_by(value: &str) -> Result<Vec<PartitionTerm>, PluginError> {
    split_top_level(value)?
        .into_iter()
        .map(|term| {
            parse_partition_term(term).map_err(|reason| {
                PluginError::Internal(format!(
                    "iceberg_sink: partition_by: invalid term `{term}`: {reason}"
                ))
            })
        })
        .collect()
}

/// Splits on commas outside parentheses, dropping empty entries.
fn split_top_level(value: &str) -> Result<Vec<&str>, PluginError> {
    let unbalanced = || {
        PluginError::Internal(format!(
            "iceberg_sink: partition_by: unbalanced parentheses in `{value}`"
        ))
    };
    let mut terms = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in value.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1).ok_or_else(unbalanced)?,
            ',' if depth == 0 => {
                terms.push(&value[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(unbalanced());
    }
    terms.push(&value[start..]);
    Ok(terms
        .into_iter()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect())
}

fn parse_partition_term(term: &str) -> Result<PartitionTerm, String> {
    let Some((name, rest)) = term.split_once('(') else {
        return Ok(PartitionTerm {
            column: column_name(term)?,
            transform: Transform::Identity,
        });
    };
    let args = rest.strip_suffix(')').ok_or("expected `transform(args)`")?;
    let args: Vec<&str> = args.split(',').map(str::trim).collect();
    let name = name.trim().to_ascii_lowercase();
    let (transform, column) = match (name.as_str(), args.as_slice()) {
        ("identity", [col]) => (Transform::Identity, col),
        ("year", [col]) => (Transform::Year, col),
        ("month", [col]) => (Transform::Month, col),
        ("day", [col]) => (Transform::Day, col),
        ("hour", [col]) => (Transform::Hour, col),
        ("bucket", [n, col]) => (Transform::Bucket(positive_int(n)?), col),
        ("truncate", [w, col]) => (Transform::Truncate(positive_int(w)?), col),
        ("identity" | "year" | "month" | "day" | "hour", _) => {
            return Err(format!("`{name}` takes one column argument"));
        }
        ("bucket", _) => return Err("expected `bucket(N, column)`".to_string()),
        ("truncate", _) => return Err("expected `truncate(W, column)`".to_string()),
        _ => {
            return Err(format!(
                "unknown transform `{name}` (expected identity, year, month, day, hour, \
                 bucket or truncate)"
            ));
        }
    };
    Ok(PartitionTerm {
        column: column_name(column)?,
        transform,
    })
}

fn column_name(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.contains(['(', ')', ',']) || value.contains(char::is_whitespace) {
        return Err(format!("invalid column name `{value}`"));
    }
    Ok(value.to_string())
}

fn positive_int(value: &str) -> Result<u32, String> {
    match value.parse::<u32>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("expected a positive integer, got `{value}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        let mut all = vec![
            ("catalog_uri", "http://localhost:8181"),
            ("table", "lake.events"),
        ];
        all.extend_from_slice(pairs);
        PluginOptions::for_test(
            "iceberg_sink",
            "STREAMLING__PLUGIN__ICEBERG_SINK_CONFIG_TEST",
            &all,
        )
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse_config(&options(&[])).unwrap();
        assert_eq!(cfg.table.name(), "events");
        assert_eq!(cfg.table.namespace().as_ref(), &vec!["lake"]);
        assert!(cfg.auto_create);
        assert!(cfg.partition_by.is_empty());
        assert_eq!(cfg.format_version, FormatVersion::V2);
        assert!(cfg.include_op_column);
        assert_eq!(
            cfg.table_properties["write.parquet.compression-codec"],
            "zstd"
        );
        assert_eq!(cfg.table_properties.len(), DEFAULT_TABLE_PROPERTIES.len());
    }

    #[test]
    fn table_properties_merge_over_defaults() {
        let cfg = parse_config(&options(&[(
            "table_properties",
            "write.parquet.compression-codec=snappy, write.target-file-size-bytes=1048576",
        )]))
        .unwrap();
        assert_eq!(
            cfg.table_properties["write.parquet.compression-codec"],
            "snappy"
        );
        assert_eq!(
            cfg.table_properties["write.target-file-size-bytes"],
            "1048576"
        );
        assert_eq!(
            cfg.table_properties["write.metadata.delete-after-commit.enabled"],
            "true"
        );
    }

    #[test]
    fn invalid_options_are_errors() {
        for (key, value) in [
            ("table", "events"),
            ("auto_create", "maybe"),
            ("include_op_column", "yes please"),
            ("format_version", "3"),
            ("table_properties", "oops"),
            ("table_properties", "format-version=1"),
            ("partition_by", "day(ts"),
        ] {
            let err = parse_config(&options(&[(key, value)])).expect_err("bad option must fail");
            let msg = err.to_string();
            assert!(
                msg.contains(key) || msg.contains(&key.replace('_', "-")),
                "for {key}={value}: got {msg}"
            );
        }
        let missing = PluginOptions::for_test(
            "iceberg_sink",
            "STREAMLING__PLUGIN__ICEBERG_SINK_CONFIG_TEST",
            &[("catalog_uri", "http://localhost:8181")],
        );
        let err = parse_config(&missing).expect_err("table is required");
        assert!(err.to_string().contains("'table'"), "got {err}");
    }

    #[test]
    fn parses_partition_terms() {
        let terms = parse_partition_by(
            "category, identity(region), year(ts), MONTH(ts), day(ts), hour(ts), \
             bucket(16, id), truncate(4, name)",
        )
        .unwrap();
        let got: Vec<(&str, Transform, String)> = terms
            .iter()
            .map(|t| (t.column.as_str(), t.transform, t.field_name()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("category", Transform::Identity, "category".to_string()),
                ("region", Transform::Identity, "region".to_string()),
                ("ts", Transform::Year, "ts_year".to_string()),
                ("ts", Transform::Month, "ts_month".to_string()),
                ("ts", Transform::Day, "ts_day".to_string()),
                ("ts", Transform::Hour, "ts_hour".to_string()),
                ("id", Transform::Bucket(16), "id_bucket".to_string()),
                ("name", Transform::Truncate(4), "name_trunc".to_string()),
            ]
        );
        assert!(parse_partition_by("  ").unwrap().is_empty());
    }

    #[test]
    fn rejects_invalid_partition_terms() {
        for invalid in [
            "day(ts",
            "day)ts(",
            "bucket(id)",
            "bucket(0, id)",
            "bucket(-1, id)",
            "truncate(x, name)",
            "day(a, b)",
            "week(ts)",
            "day()",
            "two words",
            "day(ts) extra",
        ] {
            assert!(
                parse_partition_by(invalid).is_err(),
                "`{invalid}` should be rejected"
            );
        }
    }
}
