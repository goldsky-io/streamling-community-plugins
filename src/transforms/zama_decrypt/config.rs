//! Plugin option parsing for the zama_decrypt transform. See the module docs in
//! `mod.rs` for the full option list.

use super::decode::TransferColumns;
use crate::utils::plugin_options::PluginOptions;
use alloy_primitives::{Address, B256};
use k256::ecdsa::SigningKey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use streamling_plugin::PluginError;

pub const PLUGIN_NAME: &str = "zama_decrypt";
pub const ENV_PREFIX: &str = "STREAMLING__PLUGIN__ZAMA_DECRYPT";

/// Options are read from the environment first and the YAML node second;
/// credentials come from the environment only.
pub struct Settings {
    pub chain_id: u64,
    pub rpc_url: String,
    pub daemon_socket: PathBuf,
    /// Empty set means every contract emitting the event is decoded.
    pub tokens: HashSet<Address>,
    pub columns: TransferColumns,
    pub max_concurrency: Option<u32>,
    /// Zeroized on drop.
    pub signer: SigningKey,
    pub relayer_api_key: Option<String>,
}

impl Settings {
    pub fn parse(options: HashMap<String, String>) -> Result<Self, PluginError> {
        let opts = PluginOptions::new(options, PLUGIN_NAME, ENV_PREFIX);
        let chain_id = opts.parse_value("chain_id", &opts.get("chain_id")?)?;
        let rpc_url = opts.get("rpc_url")?;
        let daemon_socket = PathBuf::from(opts.get("daemon_socket")?);
        if !daemon_socket.is_absolute() {
            return Err(invalid("daemon_socket must be an absolute path"));
        }
        let tokens = opts
            .lookup_non_empty("tokens")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| {
                        value
                            .parse::<Address>()
                            .map_err(|_| invalid(format!("invalid token address '{value}'")))
                    })
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        let columns = TransferColumns {
            token: opts.get_or("token_column", "erc7984_token"),
            from: opts.get_or("from_column", "erc7984_from"),
            to: opts.get_or("to_column", "erc7984_to"),
            handle: opts.get_or("handle_column", "erc7984_amount_handle"),
        };
        let max_concurrency = opts
            .lookup_non_empty("max_concurrency")
            .map(|value| opts.parse_value("max_concurrency", &value))
            .transpose()?;
        let signer = opts
            .get_env_only_secret("delegate_private_key")?
            .ok_or_else(|| {
                invalid(format!(
                    "required option 'delegate_private_key' is not specified; set {ENV_PREFIX}__DELEGATE_PRIVATE_KEY"
                ))
            })?
            .parse::<B256>()
            .ok()
            .and_then(|key| SigningKey::from_slice(key.as_slice()).ok())
            .ok_or_else(|| invalid("delegate_private_key is not a valid secp256k1 key"))?;
        let relayer_api_key = opts.get_env_only_secret("relayer_api_key")?;
        Ok(Self {
            chain_id,
            rpc_url,
            daemon_socket,
            tokens,
            columns,
            max_concurrency,
            signer,
            relayer_api_key,
        })
    }

    pub fn delegate_address(&self) -> Address {
        Address::from_public_key(self.signer.verifying_key())
    }
}

fn invalid(message: impl Into<String>) -> PluginError {
    PluginError::Internal(format!("{PLUGIN_NAME}: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialises this module's environment mutations; other modules' tests may
    // still read the environment concurrently, which is harmless.
    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    fn base() -> HashMap<String, String> {
        HashMap::from([
            ("chain_id".to_string(), "11155111".to_string()),
            ("rpc_url".to_string(), "https://rpc.example".to_string()),
            ("daemon_socket".to_string(), "/run/daemon.sock".to_string()),
        ])
    }

    fn with_env<T>(vars: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (key, value) in vars {
            // SAFETY: ENV_LOCK serialises every env mutation in this module.
            unsafe { std::env::set_var(format!("{ENV_PREFIX}__{key}"), value) };
        }
        let result = body();
        for (key, _) in vars {
            // SAFETY: same lock as above.
            unsafe { std::env::remove_var(format!("{ENV_PREFIX}__{key}")) };
        }
        result
    }

    #[test]
    fn required_options_are_enforced() {
        with_env(&[("DELEGATE_PRIVATE_KEY", KEY)], || {
            for key in ["chain_id", "rpc_url", "daemon_socket"] {
                let mut options = base();
                options.remove(key);
                let error = Settings::parse(options).err().expect("must fail");
                assert!(error.to_string().contains(key), "{error}");
            }
        });
        with_env(&[], || {
            let error = Settings::parse(base()).err().expect("must fail");
            assert!(
                error.to_string().contains("DELEGATE_PRIVATE_KEY"),
                "{error}"
            );
        });
    }

    #[test]
    fn defaults_apply() {
        with_env(&[("DELEGATE_PRIVATE_KEY", KEY)], || {
            let settings = Settings::parse(base()).expect("valid");
            assert_eq!(settings.chain_id, 11_155_111);
            assert!(settings.tokens.is_empty());
            assert_eq!(settings.columns.handle, "erc7984_amount_handle");
            assert_eq!(settings.max_concurrency, None);
            assert_eq!(settings.relayer_api_key, None);
            assert_eq!(
                settings.delegate_address(),
                "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
                    .parse::<Address>()
                    .expect("address")
            );
        });
    }

    #[test]
    fn credentials_in_yaml_are_rejected() {
        with_env(&[("DELEGATE_PRIVATE_KEY", KEY)], || {
            for key in ["delegate_private_key", "relayer_api_key"] {
                let mut options = base();
                options.insert(key.into(), "from-yaml".into());
                let error = Settings::parse(options).err().expect("must fail");
                assert!(error.to_string().contains("not in YAML"), "{error}");
                assert!(error.to_string().contains(key), "{error}");
            }
        });
    }

    #[test]
    fn env_overrides_yaml_for_options_and_supplies_credentials() {
        with_env(
            &[
                ("CHAIN_ID", "1"),
                ("DELEGATE_PRIVATE_KEY", KEY),
                ("RELAYER_API_KEY", "from-env"),
            ],
            || {
                let mut options = base();
                options.insert(
                    "tokens".into(),
                    "0x0000000000000000000000000000000000000001, 0x0000000000000000000000000000000000000002".into(),
                );
                options.insert("max_concurrency".into(), "4".into());
                let settings = Settings::parse(options).expect("valid");
                assert_eq!(settings.chain_id, 1);
                assert_eq!(settings.relayer_api_key.as_deref(), Some("from-env"));
                assert_eq!(settings.tokens.len(), 2);
                assert_eq!(settings.max_concurrency, Some(4));
            },
        );
    }

    #[test]
    fn invalid_values_are_rejected() {
        with_env(&[("DELEGATE_PRIVATE_KEY", KEY)], || {
            for (key, value) in [
                ("daemon_socket", "daemon.sock"),
                ("tokens", "0x1234"),
                ("chain_id", "mainnet"),
                ("max_concurrency", "many"),
            ] {
                let mut options = base();
                options.insert(key.into(), value.into());
                assert!(Settings::parse(options).is_err(), "{key}={value} must fail");
            }
        });
        with_env(&[("DELEGATE_PRIVATE_KEY", "0x00")], || {
            assert!(Settings::parse(base()).is_err(), "bad key must fail");
        });
    }
}
