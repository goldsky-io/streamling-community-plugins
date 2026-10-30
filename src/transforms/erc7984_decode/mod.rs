//! `erc7984_decode` transform: decodes ERC-7984 events from raw EVM logs into
//! plain columns, so downstream steps (SQL, or `zama_decrypt`) never parse
//! topics themselves.
//!
//! # Events
//!
//! Signatures from OpenZeppelin's `IERC7984`; `euint64` is `bytes32` on the wire.
//!
//! | Event | Signature |
//! |---|---|
//! | `ConfidentialTransfer` | `(address indexed from, address indexed to, bytes32 indexed amount)` |
//! | `OperatorSet` | `(address indexed holder, address indexed operator, uint48 until)` |
//! | `AmountDisclosed` | `(bytes32 indexed encryptedAmount, uint64 amount)` |
//!
//! Other `OperatorSet` signatures (the `bool approved` variants of related
//! interfaces) have a different `topic0` and pass through.
//!
//! # Input
//!
//! Every row is a raw log. The emitting contract comes from `address_column`
//! and the non-indexed parameters from `data_column`, both hex strings. Topics
//! come either from four columns (`topic0_column` to `topic3_column`, null
//! when the log has fewer topics) or, when `topics_column` is set, from one
//! column holding all of them: a list of strings, or a single string with the
//! topics separated by commas (surrounding `[` `]` and quotes are ignored).
//! String columns may be `Utf8`, `LargeUtf8` or `Utf8View`.
//!
//! # Output
//!
//! All input columns are kept; these are appended, all nullable `Utf8`. Rows
//! that are not one of the three events, or whose fields are null or
//! malformed, pass through with every output column null.
//!
//! | Column | Content |
//! |---|---|
//! | `erc7984_event` | `ConfidentialTransfer`, `OperatorSet` or `AmountDisclosed` |
//! | `erc7984_token` | Emitting contract, `0x` hex |
//! | `erc7984_from`, `erc7984_to`, `erc7984_amount_handle` | `ConfidentialTransfer` parties and the encrypted amount handle (32 bytes `0x` hex) |
//! | `erc7984_holder`, `erc7984_operator`, `erc7984_until` | `OperatorSet` parties and the expiry as a decimal Unix timestamp |
//! | `erc7984_encrypted_amount`, `erc7984_amount` | `AmountDisclosed` handle and the cleartext amount as a decimal string |
//!
//! # Configuration
//!
//! Options are flat keys on the transform node, or
//! `STREAMLING__PLUGIN__ERC7984_DECODE__<KEY>` in the environment (key in
//! upper case); the environment wins.
//!
//! | Option | Required | Default | Description |
//! |---|---|---|---|
//! | `address_column` | no | `address` | Column holding the emitting contract |
//! | `topics_column` | no | | Single column holding all topics; overrides the four topic columns |
//! | `topic0_column` to `topic3_column` | no | `topic0` to `topic3` | Columns holding the log topics |
//! | `data_column` | no | `data` | Column holding the ABI-encoded non-indexed parameters |
//! | `tokens` | no | all | Comma-separated token contracts to decode; others pass through |
//!
//! ```yaml
//! transforms:
//!   decoded:
//!     type: erc7984_decode
//!     from: logs
//!   decrypted:
//!     type: zama_decrypt
//!     from: decoded
//!     # ...
//! ```
//!
//! # Metrics
//!
//! Counts `erc7984_decode.rows` and `erc7984_decode.events`, and the latency
//! `erc7984_decode.batch`. The transform keeps no state.

mod decode;
mod transform;

pub use transform::Erc7984DecodeTransform;
