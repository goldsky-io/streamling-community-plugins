//! Shared unit-test fixtures for plugin construction.

use abi_stable::external_types::crossbeam_channel;
use std::sync::LazyLock;
use streamling_plugin::PluginStateBackendConfig;
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::r#async::{DirectTokioProxy, PluginAsyncRuntimeObj};
use streamling_plugin::ffi::PluginMetricsRecorder;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
});

thread_local! {
    static ENTERED: tokio::runtime::EnterGuard<'static> = RUNTIME.enter();
}

/// Async runtime on one process-wide Tokio runtime, entered for the rest of the
/// thread (plugin constructors call `Handle::current()`). Without it,
/// `DirectTokioProxy::new` parks a fresh runtime in a thread-local, and dropping
/// that from the TLS destructor at test-thread exit deadlocks on Windows.
pub(crate) fn async_runtime() -> PluginAsyncRuntimeObj {
    ENTERED.with(|_| ());
    DirectTokioProxy::new().into_async_runtime_obj()
}

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
