//! Shared unit-test fixtures for plugin construction.

use abi_stable::external_types::crossbeam_channel;
use streamling_plugin::PluginStateBackendConfig;
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::ffi::PluginMetricsRecorder;

pub(crate) fn state_backend_factory(plugin_name: &str) -> PluginStateBackendFactory {
    PluginStateBackendFactory::new(PluginStateBackendConfig::new(
        "test_app".to_string(),
        plugin_name.to_string(),
        r#"{"backend_type": "InMemory"}"#.to_string(),
    ))
}

/// A recorder whose receiver is dropped; dispatch tolerates that with a WARN.
pub(crate) fn metrics_recorder() -> PluginMetricsRecorder {
    let (sender, _receiver) = crossbeam_channel::bounded(1);
    PluginMetricsRecorder::new(sender)
}
