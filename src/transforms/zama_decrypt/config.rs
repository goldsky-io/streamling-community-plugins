//! Plugin option parsing for the zama_decrypt transform. See the module docs in
//! `mod.rs` for the full option list.

use super::decode::LogColumns;
use crate::utils::plugin_options::PluginOptions;
use alloy_primitives::{Address, B256};
use k256::ecdsa::SigningKey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use streamling_plugin::PluginError;
use zama_sdk::{DerivationSecret, Storage};

pub const PLUGIN_NAME: &str = "zama_decrypt";
pub const ENV_PREFIX: &str = "STREAMLING__PLUGIN__ZAMA_DECRYPT";

/// Parsed plugin configuration. Every option, credentials included, is read
/// from the environment first and the YAML options second.
pub struct Settings {
    pub chain_id: u64,
    pub rpc_url: String,
    pub daemon_socket: PathBuf,
    /// Empty set means every contract emitting the event is decoded.
    pub tokens: HashSet<Address>,
    pub columns: LogColumns,
    pub max_concurrency: Option<u32>,
    /// Forwards the SDK relayer debug flag so the daemon prints relayer calls.
    pub relayer_debug: bool,
    pub storage: Storage,
    /// Zeroized on drop.
    pub signer: SigningKey,
    pub relayer_api_key: Option<String>,
    pub derivation_secret: Option<DerivationSecret>,
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
        let columns = LogColumns {
            address: opts.get_or("address_column", "address"),
            topics: [0, 1, 2, 3].map(|index| {
                opts.get_or(&format!("topic{index}_column"), &format!("topic{index}"))
            }),
        };
        let max_concurrency = opts
            .lookup_non_empty("max_concurrency")
            .map(|value| opts.parse_value("max_concurrency", &value))
            .transpose()?;
        let relayer_debug = opts.get_parsed_or("relayer_debug", false)?;
        let storage = match opts.lookup_non_empty("credential_storage").as_deref() {
            None | Some("memory") => Storage::Memory,
            Some("persistent") => {
                Storage::Persistent(opts.get_or("credential_store_name", PLUGIN_NAME))
            }
            Some(other) => {
                return Err(invalid(format!(
                    "credential_storage must be memory or persistent, got '{other}'"
                )));
            }
        };
        let signer = secret(&opts, "delegate_private_key")
            .ok_or_else(|| invalid("required option 'delegate_private_key' is not specified"))?
            .parse::<B256>()
            .ok()
            .and_then(|key| SigningKey::from_slice(key.as_slice()).ok())
            .ok_or_else(|| invalid("delegate_private_key is not a valid secp256k1 key"))?;
        let relayer_api_key = secret(&opts, "relayer_api_key");
        let derivation_secret = secret(&opts, "derivation_secret").map(DerivationSecret::text);
        Ok(Self {
            chain_id,
            rpc_url,
            daemon_socket,
            tokens,
            columns,
            max_concurrency,
            relayer_debug,
            storage,
            signer,
            relayer_api_key,
            derivation_secret,
        })
    }

    pub fn delegate_address(&self) -> Address {
        Address::from_public_key(self.signer.verifying_key())
    }
}

/// A credential: environment first, YAML second (with the shared warning).
fn secret(opts: &PluginOptions, key: &str) -> Option<String> {
    opts.get_secret(key).filter(|value| !value.is_empty())
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
            ("delegate_private_key".to_string(), KEY.to_string()),
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
        with_env(&[], || {
            for key in [
                "chain_id",
                "rpc_url",
                "daemon_socket",
                "delegate_private_key",
            ] {
                let mut options = base();
                options.remove(key);
                let error = Settings::parse(options).err().expect("must fail");
                assert!(error.to_string().contains(key), "{error}");
            }
        });
    }

    #[test]
    fn defaults_apply_and_yaml_secrets_are_accepted() {
        with_env(&[], || {
            let mut options = base();
            options.insert("relayer_api_key".into(), "from-yaml".into());
            let settings = Settings::parse(options).expect("valid");
            assert_eq!(settings.chain_id, 11_155_111);
            assert!(settings.tokens.is_empty());
            assert_eq!(settings.columns.topics[3], "topic3");
            assert_eq!(settings.relayer_api_key.as_deref(), Some("from-yaml"));
            assert!(matches!(settings.storage, Storage::Memory));
            assert_eq!(
                settings.delegate_address(),
                "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
                    .parse::<Address>()
                    .expect("address")
            );
        });
    }

    #[test]
    fn env_overrides_yaml_for_options_and_secrets() {
        with_env(
            &[("CHAIN_ID", "1"), ("RELAYER_API_KEY", "from-env")],
            || {
                let mut options = base();
                options.insert("relayer_api_key".into(), "from-yaml".into());
                options.insert(
                    "tokens".into(),
                    "0x0000000000000000000000000000000000000001, 0x0000000000000000000000000000000000000002".into(),
                );
                options.insert("credential_storage".into(), "persistent".into());
                let settings = Settings::parse(options).expect("valid");
                assert_eq!(settings.chain_id, 1);
                assert_eq!(settings.relayer_api_key.as_deref(), Some("from-env"));
                assert_eq!(settings.tokens.len(), 2);
                assert!(
                    matches!(settings.storage, Storage::Persistent(ref name) if name == PLUGIN_NAME)
                );
            },
        );
    }

    #[test]
    fn invalid_values_are_rejected() {
        with_env(&[], || {
            for (key, value) in [
                ("daemon_socket", "daemon.sock"),
                ("tokens", "0x1234"),
                ("credential_storage", "redis"),
                ("delegate_private_key", "0x00"),
                ("chain_id", "mainnet"),
            ] {
                let mut options = base();
                options.insert(key.into(), value.into());
                assert!(Settings::parse(options).is_err(), "{key}={value} must fail");
            }
        });
    }
}
