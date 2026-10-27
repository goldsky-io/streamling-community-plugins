//! Minimal Ethereum JSON-RPC client: `eth_blockNumber`, `eth_getLogs`, and
//! `eth_getBlockByNumber` for timestamps the log response lacks.

use futures::{StreamExt, TryStreamExt, stream};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;
use streamling_plugin::PluginError;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TIMESTAMP_CONCURRENCY: usize = 8;

/// One log as `eth_getLogs` returns it. Quantities stay hex until conversion.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub log_index: String,
    #[serde(default)]
    pub removed: bool,
    /// Returned by recent geth/reth/erigon; absent on older nodes.
    #[serde(default)]
    pub block_timestamp: Option<String>,
}

#[derive(Deserialize)]
struct Response<T> {
    result: Option<T>,
    error: Option<Value>,
}

#[derive(Deserialize)]
struct BlockHeader {
    timestamp: String,
}

pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    /// `scheme://host` only: provider URLs carry API keys in the path or query.
    redacted: String,
}

impl RpcClient {
    pub fn new(url: String) -> Result<Self, PluginError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| PluginError::Internal(format!("evm_logs_source: http client: {e}")))?;
        let redacted = match url.split_once("://") {
            Some((scheme, rest)) => format!(
                "{scheme}://{}",
                rest.split(['/', '?']).next().unwrap_or_default()
            ),
            None => "<rpc_url>".to_string(),
        };
        Ok(Self {
            http,
            url,
            redacted,
        })
    }

    pub fn redacted_url(&self) -> &str {
        &self.redacted
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, PluginError> {
        let fail = |why: String| {
            PluginError::Execution(format!(
                "evm_logs_source: {method} via {}: {why}",
                self.redacted
            ))
        };
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            // without_url: reqwest errors print the full URL, API key included.
            .map_err(|e| fail(e.without_url().to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(fail(format!("HTTP {status}: {}", truncate(&text))));
        }
        let parsed: Response<T> = response
            .json()
            .await
            .map_err(|e| fail(format!("bad response: {}", e.without_url())))?;
        match (parsed.result, parsed.error) {
            (_, Some(error)) => Err(fail(truncate(&error.to_string()))),
            (Some(result), None) => Ok(result),
            (None, None) => Err(fail("null result".into())),
        }
    }

    pub async fn block_number(&self) -> Result<u64, PluginError> {
        let hex: String = self.call("eth_blockNumber", json!([])).await?;
        parse_quantity(&hex)
    }

    pub async fn get_logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[String],
        topic0: &[String],
    ) -> Result<Vec<RpcLog>, PluginError> {
        let mut filter = json!({"fromBlock": format!("{from:#x}"), "toBlock": format!("{to:#x}")});
        if !addresses.is_empty() {
            filter["address"] = json!(addresses);
        }
        if !topic0.is_empty() {
            filter["topics"] = json!([topic0]);
        }
        self.call("eth_getLogs", json!([filter])).await
    }

    /// Timestamps for the given blocks, fetched concurrently.
    pub async fn block_timestamps(
        &self,
        blocks: impl IntoIterator<Item = u64>,
    ) -> Result<HashMap<u64, u64>, PluginError> {
        stream::iter(blocks)
            .map(|number| async move {
                let header: BlockHeader = self
                    .call(
                        "eth_getBlockByNumber",
                        json!([format!("{number:#x}"), false]),
                    )
                    .await?;
                Ok::<_, PluginError>((number, parse_quantity(&header.timestamp)?))
            })
            .buffer_unordered(TIMESTAMP_CONCURRENCY)
            .try_collect()
            .await
    }
}

pub fn parse_quantity(hex: &str) -> Result<u64, PluginError> {
    hex.strip_prefix("0x")
        .and_then(|digits| u64::from_str_radix(digits, 16).ok())
        .ok_or_else(|| PluginError::Execution(format!("evm_logs_source: bad hex quantity '{hex}'")))
}

fn truncate(text: &str) -> String {
    text.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hex_quantities() {
        assert_eq!(parse_quantity("0x0").unwrap(), 0);
        assert_eq!(parse_quantity("0x9a5b3c").unwrap(), 0x9a5b3c);
        assert!(parse_quantity("123").is_err());
        assert!(parse_quantity("0x").is_err());
    }

    #[test]
    fn redacts_path_and_query() {
        let client = RpcClient::new("https://rpc.example.com/v2/secret-key?x=1".into()).unwrap();
        assert_eq!(client.redacted_url(), "https://rpc.example.com");
    }

    #[test]
    fn decodes_a_log_without_timestamp() {
        let log: RpcLog = serde_json::from_value(json!({
            "address": "0x7c5bf43b851c1dff1a4fee8db225b87f2c223639",
            "topics": ["0x67500e8d0ed826d2194f514dd0d8124f35648ab6e3fb5e6ed867134cffe661e9"],
            "data": "0x",
            "blockNumber": "0x10",
            "blockHash": "0xaa",
            "transactionHash": "0xbb",
            "transactionIndex": "0x1",
            "logIndex": "0x2",
            "removed": false
        }))
        .unwrap();
        assert_eq!(log.block_timestamp, None);
        assert_eq!(parse_quantity(&log.log_index).unwrap(), 2);
    }
}
