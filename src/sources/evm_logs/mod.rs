//! EVM logs source — reads event logs from any Ethereum JSON-RPC endpoint with
//! `eth_getLogs` and emits one row per log, using the column names and types
//! of the `edge.evm.logs` dataset.
//!
//! ## Configuration
//!
//! - rpc_url (required) — HTTP(S) JSON-RPC endpoint: a Goldsky Edge/eRPC URL
//!   or any public RPC. Provider URLs often carry an API key; set it through
//!   `STREAMLING__PLUGIN__EVM_LOGS_SOURCE__RPC_URL` to keep it out of the YAML
//!   (Streamling logs YAML options at startup). Logs show only scheme and host.
//! - start_block (default `latest`) — block number to start at when there is
//!   no checkpoint, or `latest` for the newest confirmed block. A restored
//!   checkpoint always wins.
//! - end_block — last block to read, inclusive. When set the source stops
//!   after it, so the pipeline ends (bounded backfill).
//! - addresses — comma-separated contract addresses; only their logs are read.
//! - topic0 — comma-separated event signature hashes; only matching logs are
//!   read.
//! - confirmations (default 12) — blocks kept between the chain head and the
//!   newest block read. Logs are read only once confirmed and never revised,
//!   so this is the reorg protection: pick a depth the chain will not reorg
//!   past.
//! - blocks_per_request (default 1000) — max blocks per `eth_getLogs` call.
//!   When the RPC rejects a range (result or range limits) the source halves
//!   it until a request succeeds and keeps that size for the rest of the run.
//!   A single-block request that still fails fails the batch after a
//!   `poll_interval_ms` pause; Streamling retries it.
//! - poll_interval_ms (default 2000) — delay between head checks once caught
//!   up.
//!
//! The RPC must serve logs for the requested range: many free endpoints prune
//! old history (e.g. publicnode answers `pruned history unavailable`).
//!
//! Each option can be overridden by the matching
//! `STREAMLING__PLUGIN__EVM_LOGS_SOURCE__<KEY>` env var; the env var wins.
//!
//! ## Output
//!
//! `id` (`<block_hash>_<log_index>`), `block_number`, `block_hash`,
//! `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`,
//! `address`, `data`, `topics` (`List<Utf8>`), `removed`, `_gs_op` (`i`).
//! These match `edge.evm.logs`; that dataset's receipt and transaction columns
//! (`from_address`, `to_address`, `transaction_from_address`,
//! `transaction_to_address`, `input`) are not produced. Block timestamps come
//! from the log response when the node includes them, otherwise from one
//! `eth_getBlockByNumber` per block that has matching logs.
//!
//! ## Delivery
//!
//! At-least-once. The checkpoint is the first block not yet delivered; a
//! restart re-reads from it. Changing `addresses` or `topic0` keeps the
//! checkpoint, so earlier blocks are not re-read with the new filter.
//!
//! ## Metrics
//!
//! - `evm_logs_source.rpc_requests` (count) — `eth_getLogs` calls.
//! - `evm_logs_source.logs_emitted` (count) — rows emitted.
//! - `evm_logs_source.head_block` (gauge) — latest head seen.
//! - `evm_logs_source.next_block` (gauge) — first block not yet read.

mod config;
mod rpc;
mod source;

pub use source::EvmLogsSource;
