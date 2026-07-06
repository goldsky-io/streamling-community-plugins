use s2_sdk::S2;
use s2_sdk::types::{AccountEndpoint, BasinEndpoint, RetryConfig, S2Config, S2Endpoints};
use std::time::Duration;
use streamling_common::data::RowKind;
use streamling_plugin::PluginError;

/// Builds an S2 client with the crate-standard bootstrap; `retry` of None
/// keeps the SDK's default (finite) policy.
pub fn s2_client(
    access_token: String,
    request_timeout: Duration,
    endpoints: Option<S2Endpoints>,
    retry: Option<RetryConfig>,
) -> Result<S2, PluginError> {
    // s2-sdk talks HTTP/2 over rustls; install the aws-lc-rs CryptoProvider
    // process-wide if nothing else has (install_default is idempotent).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut config = S2Config::new(access_token).with_request_timeout(request_timeout);
    if let Some(endpoints) = endpoints {
        config = config.with_endpoints(endpoints);
    }
    if let Some(retry) = retry {
        config = config.with_retry(retry);
    }
    S2::new(config)
        .map_err(|e| PluginError::Internal(format!("failed to construct S2 client: {e}")))
}

/// Record header carrying the row kind, Debezium-encoded — the same
/// convention streamling's Kafka sink uses for message headers. One
/// definition for both halves of the S2 round-trip contract.
pub const DBZ_OP_HEADER: &str = "dbz.op";

/// `_gs_op` row kind → Debezium op, as the sink writes it. Keyed on the
/// engine's `RowKind` (exhaustively — a new upstream variant fails
/// compilation here) with the same encoding as `RowKind::to_dbz_op`, but
/// returning `&'static str` for the per-record hot path.
pub fn dbz_op_from_row_kind(kind: &str) -> Option<&'static str> {
    Some(match kind.parse::<RowKind>().ok()? {
        RowKind::Insert => "c",
        RowKind::Update => "u",
        RowKind::Delete => "d",
    })
}

/// Debezium op → `_gs_op` row kind, as the source restores it; None for
/// unrecognized values (a non-panicking `RowKind::from_dbz_op`).
pub fn row_kind_from_dbz_op(op: &[u8]) -> Option<&'static str> {
    let kind = match op {
        b"c" | b"r" => RowKind::Insert,
        b"u" => RowKind::Update,
        b"d" => RowKind::Delete,
        _ => return None,
    };
    Some(match kind {
        RowKind::Insert => "i",
        RowKind::Update => "u",
        RowKind::Delete => "d",
    })
}

/// Builds S2 endpoints from a single endpoint override (e.g. s2-lite).
pub fn s2_endpoints(endpoint: &str) -> Result<S2Endpoints, PluginError> {
    S2Endpoints::new(
        AccountEndpoint::new(endpoint)
            .map_err(|e| PluginError::Internal(format!("invalid S2 account endpoint: {e}")))?,
        BasinEndpoint::new(endpoint)
            .map_err(|e| PluginError::Internal(format!("invalid S2 basin endpoint: {e}")))?,
    )
    .map_err(|e| PluginError::Internal(format!("invalid S2 endpoints: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plugin-side mapping must stay byte-identical to the engine's
    /// Kafka-sink/source encoding.
    #[test]
    fn dbz_mappings_match_the_engine() {
        for kind in [RowKind::Insert, RowKind::Update, RowKind::Delete] {
            assert_eq!(
                dbz_op_from_row_kind(&kind.to_str()),
                Some(kind.to_dbz_op().as_str()),
                "{kind:?} write mapping diverged from RowKind::to_dbz_op"
            );
        }
        for op in ["c", "r", "u", "d"] {
            assert_eq!(
                row_kind_from_dbz_op(op.as_bytes()),
                Some(RowKind::from_dbz_op(op).to_str().as_str()),
                "'{op}' read mapping diverged from RowKind::from_dbz_op"
            );
        }
        assert_eq!(row_kind_from_dbz_op(b"x"), None);
        assert_eq!(dbz_op_from_row_kind("x"), None);
    }
}
