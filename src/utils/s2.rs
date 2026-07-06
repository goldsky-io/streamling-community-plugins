use s2_sdk::types::{AccountEndpoint, BasinEndpoint, S2Endpoints};
use streamling_plugin::PluginError;

/// Record header carrying the row kind, Debezium-encoded — the same
/// convention streamling's Kafka sink uses for message headers. One
/// definition for both halves of the S2 round-trip contract.
pub const DBZ_OP_HEADER: &str = "dbz.op";

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
