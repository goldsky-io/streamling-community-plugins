//! Option parsing for the EVM logs source. See the module docs in `mod.rs`.

use crate::utils::plugin_options::PluginOptions;
use std::time::Duration;
use streamling_plugin::PluginError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartBlock {
    /// The newest block that has `confirmations` blocks on top of it.
    Latest,
    At(u64),
}

#[derive(Debug, Clone)]
pub struct EvmLogsConfig {
    pub rpc_url: String,
    pub start_block: StartBlock,
    /// Last block to read, inclusive. Set: the source stops after it.
    pub end_block: Option<u64>,
    /// Lowercase `0x` contract addresses; empty means every contract.
    pub addresses: Vec<String>,
    /// Lowercase `0x` event signatures matched against `topic0`; empty means any.
    pub topic0: Vec<String>,
    pub confirmations: u64,
    pub blocks_per_request: u64,
    pub poll_interval: Duration,
}

pub fn parse_config(opts: &PluginOptions) -> Result<EvmLogsConfig, PluginError> {
    let invalid = |key: &str, why: String| {
        PluginError::Internal(format!("evm_logs_source: invalid {key}: {why}"))
    };

    let rpc_url = opts.get("rpc_url")?;
    if !(rpc_url.starts_with("http://") || rpc_url.starts_with("https://")) {
        return Err(invalid("rpc_url", "must be an http(s) URL".into()));
    }

    let start_block = match optional(opts, "start_block").as_deref() {
        None | Some("latest") => StartBlock::Latest,
        Some(value) => StartBlock::At(opts.parse_value("start_block", value)?),
    };
    let end_block = optional(opts, "end_block")
        .map(|value| opts.parse_value::<u64>("end_block", &value))
        .transpose()?;
    if let (StartBlock::At(start), Some(end)) = (start_block, end_block)
        && end < start
    {
        return Err(invalid(
            "end_block",
            format!("{end} is before start_block {start}"),
        ));
    }

    let addresses = hex_list(opts, "addresses", 20)?;
    let topic0 = hex_list(opts, "topic0", 32)?;

    let blocks_per_request: u64 = opts.get_parsed_or("blocks_per_request", 1000)?;
    if blocks_per_request == 0 {
        return Err(invalid("blocks_per_request", "must be at least 1".into()));
    }

    Ok(EvmLogsConfig {
        rpc_url,
        start_block,
        end_block,
        addresses,
        topic0,
        confirmations: opts.get_parsed_or("confirmations", 12)?,
        blocks_per_request,
        poll_interval: Duration::from_millis(opts.get_parsed_or("poll_interval_ms", 2000)?),
    })
}

fn optional(opts: &PluginOptions, key: &str) -> Option<String> {
    opts.lookup(key).filter(|value| !value.is_empty())
}

/// Comma-separated `0x` hex values of exactly `bytes` bytes, lowercased so they
/// compare equal to what the RPC returns.
fn hex_list(opts: &PluginOptions, key: &str, bytes: usize) -> Result<Vec<String>, PluginError> {
    let Some(raw) = optional(opts, key) else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let digits = value.strip_prefix("0x").unwrap_or("");
            if digits.len() != bytes * 2 || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(PluginError::Internal(format!(
                    "evm_logs_source: invalid {key}: '{value}' is not a 0x-prefixed {bytes}-byte hex value"
                )));
            }
            Ok(value.to_ascii_lowercase())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0x7c5BF43B851c1dff1a4feE8dB225b87f2C223639";
    const TRANSFER: &str = "0x67500e8d0ed826d2194f514dd0d8124f35648ab6e3fb5e6ed867134cffe661e9";

    fn parse(pairs: &[(&str, &str)]) -> Result<EvmLogsConfig, PluginError> {
        let opts = PluginOptions::for_test(
            "evm_logs_source",
            "STREAMLING__PLUGIN__EVM_LOGS_SOURCE_TEST",
            pairs,
        );
        parse_config(&opts)
    }

    #[test]
    fn rpc_url_is_required() {
        let err = parse(&[]).unwrap_err().to_string();
        assert!(err.contains("rpc_url"), "{err}");
    }

    #[test]
    fn defaults_follow_the_head_with_confirmations() {
        let config = parse(&[("rpc_url", "https://rpc.example")]).unwrap();
        assert_eq!(config.start_block, StartBlock::Latest);
        assert_eq!(config.end_block, None);
        assert_eq!(config.confirmations, 12);
        assert!(config.addresses.is_empty() && config.topic0.is_empty());
    }

    #[test]
    fn filters_are_lowercased_and_trimmed() {
        let config = parse(&[
            ("rpc_url", "https://rpc.example"),
            ("addresses", &format!(" {TOKEN} ,")),
            ("topic0", TRANSFER),
        ])
        .unwrap();
        assert_eq!(config.addresses, vec![TOKEN.to_ascii_lowercase()]);
        assert_eq!(config.topic0, vec![TRANSFER.to_string()]);
    }

    #[test]
    fn malformed_filters_are_rejected() {
        for (key, value) in [
            ("addresses", "0x1234"),
            ("topic0", TOKEN),
            ("addresses", "7c5b"),
        ] {
            let err = parse(&[("rpc_url", "https://rpc.example"), (key, value)])
                .unwrap_err()
                .to_string();
            assert!(err.contains(key), "{err}");
        }
    }

    #[test]
    fn end_block_before_start_block_is_rejected() {
        let err = parse(&[
            ("rpc_url", "https://rpc.example"),
            ("start_block", "100"),
            ("end_block", "99"),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("end_block"), "{err}");
    }

    #[test]
    fn non_http_rpc_url_and_zero_range_are_rejected() {
        assert!(parse(&[("rpc_url", "wss://rpc.example")]).is_err());
        assert!(
            parse(&[
                ("rpc_url", "https://rpc.example"),
                ("blocks_per_request", "0")
            ])
            .is_err()
        );
    }
}
