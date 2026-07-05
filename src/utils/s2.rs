use s2_sdk::types::{AccountEndpoint, BasinEndpoint, S2Endpoints};
use streamling_plugin::PluginError;

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
