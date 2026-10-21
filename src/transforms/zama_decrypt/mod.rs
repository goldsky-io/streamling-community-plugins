//! `zama_decrypt` transform: decrypts the amounts of decoded ERC-7984
//! `ConfidentialTransfer` events through the Zama SDK daemon, on behalf of
//! wallets that delegated decryption rights on-chain to the plugin's own
//! signing key. It reads the columns the `erc7984_decode` transform appends
//! and does no log parsing of its own.
//!
//! # How a row flows through
//!
//! 1. **Read the transfer.** A row is a candidate when its token, sender,
//!    receiver and amount-handle columns (by default `erc7984_token`,
//!    `erc7984_from`, `erc7984_to`, `erc7984_amount_handle`) are all set and,
//!    with a `tokens` list configured, the token is on it. Other rows, such as
//!    `OperatorSet` events or logs that did not decode, pass through with
//!    every output column null.
//! 2. **Pick a delegator.** The plugin asks the Zama ACL, through the daemon,
//!    whether the receiver has an active delegation to the plugin's address on
//!    that token, then the sender. The receiver is tried first because balances
//!    are what downstream consumers track. The zero address is skipped. The
//!    lookups of a batch run in two concurrent rounds (receivers, then the
//!    senders still needed), up to eight in flight. Answers are cached: an
//!    active answer for at most five minutes and never past its on-chain
//!    expiry, an inactive answer for one minute; stale entries are swept on
//!    every batch, so the cache holds the parties seen within one window. A
//!    relayer rejection evicts the entry so a revocation is seen on the next
//!    batch.
//! 3. **Decrypt.** Transfers are grouped by delegator and sent as one batch
//!    decryption per delegator; a handle that appears twice is asked once. The
//!    daemon holds the transport key pair and one signed permit per delegator,
//!    calls the Zama relayer, and returns the cleartext.
//! 4. **Sign permits on demand.** The first decryption for a delegator needs an
//!    EIP-712 permit from the delegate key. The daemon sends the typed data over
//!    its signer channel, the plugin checks that the primary type is one of the
//!    two decryption permit types and that the domain's chain id is the
//!    configured `chain_id`, signs, and logs one line per signature. The key
//!    never leaves the Streamling process. Permits are valid for the SDK
//!    default of 30 days and cannot be revoked from the plugin.
//! 5. **Emit.** Three nullable `Utf8` columns are appended (see below).
//!
//! # Failure semantics
//!
//! - A relayer or ACL answer that refuses one handle or one delegator for good
//!   (no entitlement, bad input) is written to `zama_error` on the affected
//!   rows; the batch is still emitted and the checkpoint advances. Such rows
//!   are not retried: re-run them from the raw logs.
//! - A failure that may clear on a retry fails the batch with an execution
//!   error: a daemon that cannot be reached or whose context is gone, and any
//!   SDK error flagged retryable (relayer timeout, RPC rate limit, a delegation
//!   not yet propagated). Streamling terminates the transform on a batch
//!   error, restarts the pipeline and replays the batch into a fresh plugin
//!   instance, which reconnects in `initialize`. A relayer blip therefore
//!   costs a restart, never a silently undecrypted row.
//! - A missing or non-string input column, a bad option, a credential in
//!   YAML, or an unreachable daemon at startup is an initialization error.
//!
//! # Configuration
//!
//! Options are flat keys on the transform node in the pipeline YAML (next to
//! `type` and `from`), or `STREAMLING__PLUGIN__ZAMA_DECRYPT__<KEY>` in the
//! environment (key in upper case). The environment wins. The two credentials
//! are accepted from the environment only: Streamling logs every plugin's
//! YAML options at INFO when it creates the plugin, so a credential in YAML
//! would be written to the host log, and the plugin refuses to start instead.
//!
//! | Option | Required | Default | Description |
//! |---|---|---|---|
//! | `chain_id` | yes | | EVM chain id; selects the SDK's relayer and contracts |
//! | `rpc_url` | yes | | JSON-RPC endpoint the daemon uses for public reads |
//! | `daemon_socket` | yes | | Absolute path of the daemon's Unix socket |
//! | `delegate_private_key` | yes | | Hex secp256k1 key of the address wallets delegate to; environment only |
//! | `relayer_api_key` | no | | Zama relayer credential; the public Sepolia relayer needs none; environment only |
//! | `tokens` | no | all | Comma-separated token contracts to decrypt; others pass through |
//! | `token_column` | no | `erc7984_token` | Column holding the token contract |
//! | `from_column`, `to_column` | no | `erc7984_from`, `erc7984_to` | Columns holding the transfer parties |
//! | `handle_column` | no | `erc7984_amount_handle` | Column holding the encrypted amount handle |
//! | `max_concurrency` | no | SDK default | Relayer concurrency within one batch call |
//!
//! ```yaml
//! transforms:
//!   decoded:
//!     type: erc7984_decode
//!     from: logs
//!   decrypted:
//!     type: zama_decrypt
//!     from: decoded
//!     chain_id: "11155111"
//!     rpc_url: https://ethereum-sepolia-rpc.publicnode.com
//!     daemon_socket: /run/zama-sdk/zama.sock
//!     # delegate_private_key: STREAMLING__PLUGIN__ZAMA_DECRYPT__DELEGATE_PRIVATE_KEY
//! ```
//!
//! The daemon keeps the transport key pair and the signed permits in memory:
//! every restart generates a new key pair and re-signs one permit per
//! delegating wallet on first use, which is one signature, not a transaction.
//!
//! # Output columns
//!
//! All input columns are kept; these are appended, all nullable `Utf8`:
//!
//! | Column | Content |
//! |---|---|
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
//!   same UID, in a private socket directory. Anyone with socket access can
//!   open their own SDK context on the daemon, so the socket is as sensitive
//!   as the daemon's egress.
//! - The delegate key is held in a zeroizing signer for the plugin's lifetime;
//!   the relayer API key is held as a plain string and sent to the daemon in
//!   plaintext over the socket at context creation. Use a key dedicated to
//!   this plugin, with no funds: it signs decryption permits only, but a
//!   compromised daemon would still obtain those signatures.
//! - One delegate key per tenant. Every wallet that delegates to the same
//!   address is decryptable by the same pipeline; nothing in the plugin
//!   enforces tenant isolation.
//! - The SDK client is the `zama_sdk` crate from crates.io, pinned to an exact
//!   pre-release version. The daemon protocol is beta, so the daemon is built
//!   from the `v<version>` tag of `zama-ai/sdk` that matches the crate, and the
//!   two are bumped together.
//!
//! # Limitations
//!
//! - Only `ConfidentialTransfer` amounts are decrypted; other decoded events
//!   pass through untouched.
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
