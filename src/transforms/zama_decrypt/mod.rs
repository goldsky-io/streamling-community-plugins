//! `zama_decrypt` transform: decodes ERC-7984 `ConfidentialTransfer` logs and
//! decrypts their amounts through the Zama SDK daemon, on behalf of wallets
//! that delegated decryption rights on-chain to the plugin's own signing key.
//!
//! # How a row flows through
//!
//! 1. **Decode.** A row is a candidate when `topic0` is
//!    `keccak256("ConfidentialTransfer(address,address,bytes32)")` and, with a
//!    `tokens` list configured, the emitting contract is on it. Rows that do not
//!    match, or whose fields are null or malformed, pass through with every
//!    output column null.
//! 2. **Pick a delegator.** The plugin asks the Zama ACL, through the daemon,
//!    whether the receiver has an active delegation to the plugin's address on
//!    that token, then the sender. The receiver is tried first because balances
//!    are what downstream consumers track. The zero address is skipped. Answers
//!    are cached: an active answer for at most five minutes and never past its
//!    on-chain expiry, an inactive answer for one minute. A relayer rejection
//!    evicts the entry so a revocation is seen on the next batch.
//! 3. **Decrypt.** Transfers are grouped by delegator and sent as one batch
//!    decryption per delegator; a handle that appears twice is asked once. The
//!    daemon holds the transport key pair and one signed permit per delegator,
//!    calls the Zama relayer, and returns the cleartext.
//! 4. **Sign permits on demand.** The first decryption for a delegator needs an
//!    EIP-712 permit from the delegate key. The daemon sends the typed data over
//!    its signer channel, the plugin checks that the primary type is one of the
//!    two decryption permit types, signs, and logs one line per signature. The
//!    key never leaves the Streamling process. Permits are valid for the SDK
//!    default of 30 days and cannot be revoked from the plugin.
//! 5. **Emit.** Seven nullable `Utf8` columns are appended (see below).
//!
//! # Failure semantics
//!
//! - A relayer or ACL answer that refuses one handle or one delegator is written
//!   to `zama_error` on the affected rows; the batch is still emitted and the
//!   checkpoint advances. Such rows are not retried: re-run them from the raw
//!   logs.
//! - A daemon that cannot be reached, or whose context is gone, fails the
//!   batch with an execution error. The session is dropped and rebuilt on the
//!   next attempt, so a daemon restart does not turn into silently undecrypted
//!   rows.
//! - A missing or non-string log column, a bad option, or an unreachable
//!   daemon at startup is an initialization error.
//!
//! # Configuration
//!
//! Every option can be set in YAML under `options`, or as
//! `STREAMLING__PLUGIN__ZAMA_DECRYPT__<KEY>` (key in upper case). The
//! environment wins. Credentials are accepted from both places so a hosted
//! deployment can inject them either way, but note that Streamling itself logs
//! every plugin's YAML options at INFO when it creates the plugin, so a
//! credential placed in YAML is written to the host log. Prefer the
//! environment for real credentials.
//!
//! | Option | Required | Default | Description |
//! |---|---|---|---|
//! | `chain_id` | yes | | EVM chain id; selects the SDK's relayer and contracts |
//! | `rpc_url` | yes | | JSON-RPC endpoint the daemon uses for public reads |
//! | `daemon_socket` | yes | | Absolute path of the daemon's Unix socket |
//! | `delegate_private_key` | yes | | Hex secp256k1 key of the address wallets delegate to |
//! | `relayer_api_key` | no | | Zama relayer credential; the public Sepolia relayer needs none |
//! | `derivation_secret` | no | | Wraps the transport key pair at rest, 64 characters or more |
//! | `tokens` | no | all | Comma-separated token contracts to decode; others pass through |
//! | `address_column` | no | `address` | Column holding the emitting contract |
//! | `topic0_column` to `topic3_column` | no | `topic0` to `topic3` | Columns holding the log topics |
//! | `max_concurrency` | no | SDK default | Relayer concurrency within one batch call |
//! | `relayer_debug` | no | `false` | Daemon prints relayer requests, payloads included, to its stdout; diagnostic only |
//! | `credential_storage` | no | `memory` | `memory` or `persistent` |
//! | `credential_store_name` | no | `zama_decrypt` | Daemon store name when persistent |
//!
//! With `memory` storage every restart generates a new transport key pair and
//! re-signs one permit per delegating wallet on first use. `persistent` keeps
//! them in the daemon's SQLite store; the daemon then needs
//! `ZAMA_SDK_DAEMON_STORAGE_DIR` pointing at a private (mode 0700) directory on
//! a durable volume, and a `derivation_secret` wraps the key pair at rest.
//!
//! # Output columns
//!
//! All input columns are kept; these are appended, all nullable `Utf8`:
//!
//! | Column | Content |
//! |---|---|
//! | `zama_token` | Emitting token contract, `0x` hex |
//! | `zama_from` | Sender |
//! | `zama_to` | Receiver |
//! | `zama_handle` | The encrypted amount handle, 32 bytes `0x` hex |
//! | `zama_amount` | Cleartext amount as a decimal string of the raw integer, not scaled by decimals |
//! | `zama_delegator` | The wallet whose delegation was used |
//! | `zama_error` | Why `zama_amount` is null for a decoded transfer |
//!
//! `zama_error` values: `no active delegation from either party`,
//! `delegation check failed: ...`, `batch decrypt failed: ...`,
//! `<code>: <message>` from the relayer for one handle,
//! `relayer returned no result for handle`, `relayer returned an undefined value`.
//!
//! # Metrics and logs
//!
//! Counts `zama_decrypt.rows`, `zama_decrypt.transfers`, `zama_decrypt.failed` and
//! latency `zama_decrypt.batch`, labelled with `chain_id`. The plugin logs the
//! delegate address on connect, the signer and delegator for each permit
//! signature, and the delegator, handle count and failure count for each batch.
//! Its own lines never contain a key, a permit or a cleartext amount; SDK error
//! messages are logged and copied into `zama_error` verbatim.
//!
//! # Deployment requirements
//!
//! - The plugin opens one connection: gRPC over the Unix socket at
//!   `daemon_socket`. It instantiates no HTTP or JSON-RPC client of its own.
//!   The daemon makes the outbound calls, to `rpc_url` and to the Zama relayer
//!   for `chain_id`, and needs egress to both.
//! - The daemon creates its socket with mode 0600. Streamling must run as the
//!   same UID, in a private socket directory. Anyone with socket access can open
//!   their own SDK context and, with `persistent` storage and the same store
//!   name, reuse the stored transport key and permits, so the socket is as
//!   sensitive as the storage directory.
//! - The delegate key is held in a zeroizing signer for the plugin's lifetime;
//!   the relayer API key and derivation secret are held as plain strings and
//!   sent to the daemon in plaintext over the socket at context creation. Use
//!   a key dedicated to this plugin, with no funds: it signs decryption permits
//!   only, but a compromised daemon would still obtain those signatures.
//! - One delegate key per tenant. Every wallet that delegates to the same
//!   address is decryptable by the same pipeline; nothing in the plugin
//!   enforces tenant isolation.
//! - The SDK client is a git dependency on a pinned commit of `zama-ai/sdk`
//!   (`clients/rust`), not yet published to crates.io. The commit hash is the
//!   audit unit, and it is bumped together with the daemon image.
//!
//! # Limitations
//!
//! - Only `ConfidentialTransfer` is decoded.
//! - Delegation status lives in process memory; there is no table of delegations
//!   for downstream consumers.
//! - Malformed rows pass through silently rather than reporting an error.
//! - The daemon protocol is beta.

mod config;
mod daemon;
mod decode;
mod decrypt;
mod transform;

pub use transform::ZamaDecryptTransform;
