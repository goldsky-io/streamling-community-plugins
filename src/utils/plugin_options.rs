use std::collections::HashMap;
use streamling_plugin::PluginError;
use tracing::warn;

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

    pub fn get(&self, key: &str) -> Result<String, PluginError> {
        let env_key = format!("{}_{}", self.env_prefix, key.to_uppercase());
        if let Ok(val) = std::env::var(&env_key) {
            return Ok(val);
        }
        self.options.get(key).cloned().ok_or_else(|| {
            PluginError::Internal(format!(
                "{}: required option '{}' is not specified",
                self.plugin_name, key
            ))
        })
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        let env_key = format!("{}_{}", self.env_prefix, key.to_uppercase());
        std::env::var(&env_key)
            .ok()
            .or_else(|| self.options.get(key).cloned())
            .unwrap_or_else(|| default.to_string())
    }

    pub fn get_secret(&self, key: &str) -> Option<String> {
        let env_key = format!("{}_{}", self.env_prefix, key.to_uppercase());
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
