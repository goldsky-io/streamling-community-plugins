//! Shared Iceberg catalog configuration for `iceberg_sink` and `iceberg_source`.
//!
//! Both plugins talk to an Iceberg [REST catalog] — the interoperable standard
//! that Polaris, Lakekeeper, Nessie, Unity Catalog, Gravitino, and Tabular-style
//! services implement. Data and metadata files are read and written through
//! OpenDAL, resolved by the URL scheme of each path (`s3://`, `s3a://`,
//! `file://`).
//!
//! Catalog options (shared by both plugins):
//!
//! | Option | Required | Description |
//! |---|---|---|
//! | `catalog_uri` | yes | REST catalog base URL, e.g. `http://localhost:8181` |
//! | `warehouse` | no | Warehouse name or location passed to the catalog |
//! | `token` | no | Static bearer token (env var preferred) |
//! | `credential` | no | OAuth2 client credentials `client_id:client_secret` (env var preferred) |
//! | `s3_endpoint` / `s3_region` | no | S3-compatible endpoint and region |
//! | `s3_access_key_id` / `s3_secret_access_key` / `s3_session_token` | no | Static S3 credentials (env var preferred); omit for vended credentials or the AWS default chain |
//! | `s3_path_style_access` | no | Path-style S3 URLs (MinIO and most S3-compatible stores) |
//! | `catalog_properties` | no | Extra comma-separated `key=value` catalog/FileIO properties, e.g. `scope=PRINCIPAL_ROLE:ALL,header.X-Custom=1` |
//!
//! The catalog requests [vended credentials] by default
//! (`X-Iceberg-Access-Delegation: vended-credentials`). Catalogs that do not
//! vend credentials ignore the header.
//!
//! [REST catalog]: https://iceberg.apache.org/rest-catalog-spec/
//! [vended credentials]: https://iceberg.apache.org/docs/latest/rest-catalog/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::{CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use streamling_plugin::PluginError;

use crate::utils::plugin_options::PluginOptions;

const ACCESS_DELEGATION_HEADER: &str = "header.X-Iceberg-Access-Delegation";
/// Per-request bounds for catalog HTTP calls. Commit conflicts are retried by
/// iceberg-rust per the table's `commit.retry.*` properties, each attempt
/// bounded by these.
const CATALOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CATALOG_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Properties handed to the REST catalog. FileIO inherits them, merged with
/// any per-table config the catalog returns (e.g. vended credentials).
#[derive(Clone)]
pub struct CatalogConfig {
    props: HashMap<String, String>,
}

impl std::fmt::Debug for CatalogConfig {
    // Property values can hold secrets; print keys only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys: Vec<_> = self.props.keys().collect();
        keys.sort();
        f.debug_struct("CatalogConfig")
            .field("property_keys", &keys)
            .finish()
    }
}

impl CatalogConfig {
    pub fn parse(opts: &PluginOptions) -> Result<Self, PluginError> {
        let mut props = HashMap::from([(
            ACCESS_DELEGATION_HEADER.to_string(),
            "vended-credentials".to_string(),
        )]);
        if let Some(extra) = opts.lookup("catalog_properties") {
            for pair in extra.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                let (k, v) = pair.split_once('=').ok_or_else(|| {
                    PluginError::Internal(format!(
                        "catalog_properties: expected `key=value`, got `{pair}`"
                    ))
                })?;
                props.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
        props.insert(REST_CATALOG_PROP_URI.to_string(), opts.get("catalog_uri")?);
        let plain = [
            ("warehouse", REST_CATALOG_PROP_WAREHOUSE),
            ("s3_endpoint", "s3.endpoint"),
            ("s3_region", "s3.region"),
            ("s3_path_style_access", "s3.path-style-access"),
        ];
        for (key, prop) in plain {
            if let Some(v) = opts.lookup(key) {
                props.insert(prop.to_string(), v);
            }
        }
        let secrets = [
            ("token", "token"),
            ("credential", "credential"),
            ("s3_access_key_id", "s3.access-key-id"),
            ("s3_secret_access_key", "s3.secret-access-key"),
            ("s3_session_token", "s3.session-token"),
        ];
        for (key, prop) in secrets {
            if let Some(v) = opts.get_secret(key) {
                props.insert(prop.to_string(), v);
            }
        }
        // An empty value removes a property, e.g. the default delegation header.
        props.retain(|_, v| !v.is_empty());
        Ok(Self { props })
    }

    /// Connects to the REST catalog. Performs the `GET /v1/config` handshake,
    /// so a wrong URI or bad credentials fail here.
    pub async fn connect(&self) -> Result<RestCatalog, PluginError> {
        // reqwest's default client has no timeouts; a stalled catalog request
        // would otherwise wedge a checkpoint marker or a source poll forever.
        let client = reqwest::Client::builder()
            .connect_timeout(CATALOG_CONNECT_TIMEOUT)
            .timeout(CATALOG_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| {
                PluginError::Internal(format!("failed to build the catalog HTTP client: {e}"))
            })?;
        RestCatalogBuilder::default()
            .with_client(client)
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load("streamling", self.props.clone())
            .await
            .map_err(|e| iceberg_error("connect to the Iceberg REST catalog", e))
    }
}

/// Parses `namespace.table` (nested namespaces: `a.b.table`).
pub fn parse_table_ident(value: &str) -> Result<TableIdent, PluginError> {
    let parts: Vec<&str> = value.split('.').map(str::trim).collect();
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return Err(PluginError::Internal(format!(
            "table: expected `namespace.table`, got `{value}`"
        )));
    }
    TableIdent::from_strs(parts).map_err(|e| iceberg_error("parse table", e))
}

/// Wraps an iceberg error with what was being attempted.
pub fn iceberg_error(context: &str, e: iceberg::Error) -> PluginError {
    PluginError::Execution(format!("failed to {context}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_table_ident() {
        let ident = parse_table_ident("lake.raw.events").unwrap();
        assert_eq!(ident.namespace().as_ref(), &vec!["lake", "raw"]);
        assert_eq!(ident.name(), "events");
        assert!(parse_table_ident("events").is_err());
        assert!(parse_table_ident("lake..events").is_err());
    }

    #[test]
    fn maps_options_to_catalog_properties() {
        let opts = PluginOptions::for_test(
            "iceberg_sink",
            "STREAMLING__PLUGIN__ICEBERG_SINK_TEST_UNSET",
            &[
                ("catalog_uri", "http://localhost:8181"),
                ("warehouse", "wh"),
                ("s3_path_style_access", "true"),
                ("s3_access_key_id", "AK"),
                (
                    "catalog_properties",
                    "scope=PRINCIPAL_ROLE:ALL, header.X-Iceberg-Access-Delegation=",
                ),
            ],
        );
        let props = CatalogConfig::parse(&opts).unwrap().props;
        assert_eq!(props["uri"], "http://localhost:8181");
        assert_eq!(props["warehouse"], "wh");
        assert_eq!(props["s3.path-style-access"], "true");
        assert_eq!(props["s3.access-key-id"], "AK");
        assert_eq!(props["scope"], "PRINCIPAL_ROLE:ALL");
        // An empty value switches off the default delegation header.
        assert!(!props.contains_key(ACCESS_DELEGATION_HEADER));
    }

    #[test]
    fn rejects_malformed_catalog_properties() {
        let opts = PluginOptions::for_test(
            "iceberg_sink",
            "STREAMLING__PLUGIN__ICEBERG_SINK_TEST_UNSET",
            &[("catalog_uri", "http://x"), ("catalog_properties", "oops")],
        );
        assert!(CatalogConfig::parse(&opts).is_err());
    }
}
