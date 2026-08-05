use std::collections::HashMap;
use streamling_plugin::{PluginError, PluginInitializationError};
use tracing::warn;

/// Maps a config-parse error to the initialization error a plugin
/// constructor must return.
pub fn configuration_error(e: PluginError) -> PluginInitializationError {
    PluginInitializationError::Configuration(e.to_string().into())
}

pub struct PluginOptions {
    options: HashMap<String, String>,
    env_prefix: String,
    plugin_name: String,
}

impl PluginOptions {
    pub fn new(options: HashMap<String, String>, plugin_name: &str, env_prefix: &str) -> Self {
        PluginOptions {
            options,
            env_prefix: env_prefix.to_string(),
            plugin_name: plugin_name.to_string(),
        }
    }

    #[cfg(test)]
    pub fn for_test(plugin_name: &str, env_prefix: &str, pairs: &[(&str, &str)]) -> Self {
        Self::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            plugin_name,
            env_prefix,
        )
    }

    fn env_key(&self, key: &str) -> String {
        format!("{}__{}", self.env_prefix, key.to_uppercase())
    }

    /// The option's value, environment variable first, or `None` when set in
    /// neither place. Use this over the raw options map so env overrides apply.
    pub fn lookup(&self, key: &str) -> Option<String> {
        std::env::var(self.env_key(key))
            .ok()
            .or_else(|| self.options.get(key).cloned())
    }

    /// A required option. An option set to the empty string counts as unset:
    /// no required option has a meaningful empty value, and reporting it here
    /// beats the downstream failure it would otherwise cause.
    pub fn get(&self, key: &str) -> Result<String, PluginError> {
        self.lookup(key).filter(|v| !v.is_empty()).ok_or_else(|| {
            PluginError::Internal(format!(
                "{}: required option '{}' is not specified",
                self.plugin_name, key
            ))
        })
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.lookup(key).unwrap_or_else(|| default.to_string())
    }

    /// Parses a typed option, naming the plugin, key, and offending value in
    /// the error.
    pub fn get_parsed<T: std::str::FromStr>(
        &self,
        key: &str,
        default: &str,
    ) -> Result<T, PluginError>
    where
        T::Err: std::fmt::Display,
    {
        let value = self.get_or(key, default);
        self.parse_value(key, &value)
    }

    /// Parses a typed option, falling back to an already-typed default so
    /// callers don't have to stringify one.
    pub fn get_parsed_or<T: std::str::FromStr>(
        &self,
        key: &str,
        default: T,
    ) -> Result<T, PluginError>
    where
        T::Err: std::fmt::Display,
    {
        match self.lookup(key) {
            None => Ok(default),
            Some(value) => self.parse_value(key, &value),
        }
    }

    /// Parses an already-read value (e.g. an SDK name type), naming the
    /// plugin, what the value is, and the offending value in the error.
    pub fn parse_value<T: std::str::FromStr>(
        &self,
        what: &str,
        value: &str,
    ) -> Result<T, PluginError>
    where
        T::Err: std::fmt::Display,
    {
        value.parse().map_err(|e| {
            PluginError::Internal(format!(
                "{}: invalid {what} '{value}': {e}",
                self.plugin_name
            ))
        })
    }

    pub fn get_secret(&self, key: &str) -> Option<String> {
        let env_key = self.env_key(key);
        std::env::var(&env_key).ok().or_else(|| {
            if let Some(val) = self.options.get(key) {
                warn!(
                    "{} is set in plaintext YAML configuration. \
                    Consider using environment variable {} instead.",
                    key, env_key
                );
                Some(val.clone())
            } else {
                None
            }
        })
    }
}
