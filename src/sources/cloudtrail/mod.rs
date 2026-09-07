//! AWS CloudTrail source - polls the CloudTrail LookupEvents API and emits
//! management events as Arrow batches with an Athena-style typed schema
//! (scalar columns plus raw-JSON Utf8 columns for nested fields such as
//! `user_identity` and `request_parameters`). No trail, S3 bucket, or SQS
//! queue setup is required — only the `cloudtrail:LookupEvents` IAM
//! permission.
//!
//! ## Configuration
//!
//! Every option is optional; with zero configuration the source uses the
//! default AWS credential chain and starts at "now".
//!
//! - region — AWS region override (default: the SDK chain, e.g. AWS_REGION).
//! - endpoint_url — custom endpoint (e.g. LocalStack).
//! - access_key_id / secret_access_key / session_token (secrets) — static
//!   credentials; supply via env STREAMLING__PLUGIN__CLOUDTRAIL_SOURCE__<KEY>
//!   when possible. Key id and secret must be set together; omit both to use
//!   the default chain (env, profile, IMDS, IRSA, ...).
//! - start_time (default `now`) — where a source with no checkpoint starts:
//!   `now`, `earliest` (the full 90-day retention), or an RFC 3339 timestamp.
//!   A restored checkpoint always wins.
//! - poll_interval_secs (default 60) — delay between poll cycles once caught
//!   up to now.
//! - lookback_secs (default 1800) — each cycle re-scans this far behind its
//!   watermark to pick up late-delivered events (CloudTrail delivers events
//!   with a typically-up-to-15-minute lag; the default doubles that). 0
//!   disables late-arrival protection.
//! - max_window_secs (default 3600) — max span of one poll window; bounds
//!   memory during backfills. Must be greater than lookback_secs.
//! - batch_size (default 1024) — max records per generated Arrow batch.
//! - lookup_attribute_key / lookup_attribute_value — server-side filter,
//!   both-or-neither. The key is matched case-insensitively against the
//!   LookupEvents attributes (EventId, EventName, ReadOnly, Username,
//!   ResourceType, ResourceName, EventSource, AccessKeyId).
//! - on_malformed (default `error`) — `error` fails the poll cycle when an
//!   event payload cannot be parsed (the source retries the same window, so
//!   it stalls rather than losing data); `skip` drops such events with a
//!   WARN log.
//!
//! Each option can be overridden by the matching
//! STREAMLING__PLUGIN__CLOUDTRAIL_SOURCE__<KEY> env var; the env var wins
//! when both are set.
//!
//! ## Metrics
//!
//! - `cloudtrail_source.api_requests` (count) — LookupEvents requests sent.
//! - `cloudtrail_source.throttled_requests` (count) — requests rejected with
//!   ThrottlingException.
//! - `cloudtrail_source.events_read` (count) — events received from the API,
//!   including duplicates.
//! - `cloudtrail_source.duplicates_filtered` (count) — already-emitted events
//!   dropped by the dedupe set (nonzero in steady state: the lookback window
//!   re-fetches recent history every cycle).
//! - `cloudtrail_source.malformed_skipped` (count) — events dropped by
//!   `on_malformed=skip`.
//! - `cloudtrail_source.records_emitted` (count) — rows emitted in generated
//!   batches.
//! - `cloudtrail_source.carry_records` (gauge) — records buffered ahead of
//!   the next batch.
//! - `cloudtrail_source.watermark_lag_ms` (gauge) — now minus the poller's
//!   watermark.
//!
//! ## Architecture
//!
//! A background task polls LookupEvents with sliding time windows: each cycle
//! scans `[watermark − lookback, start + max_window)` (the end cap is omitted
//! once the window reaches "now", so clock skew cannot cut off fresh events),
//! filters events already emitted (LookupEvents has no cursor — the lookback
//! overlap re-fetches recent history to catch late-delivered events), sorts
//! the new ones ascending by event time, and pushes them into a bounded
//! channel that `generate_batch` drains. Capped (backlogged) windows loop
//! immediately to catch up; a fully-scanned capped window advances the
//! watermark even when empty. The poller sleeps 1s between API requests,
//! self-throttling to half of the 2 req/s LookupEvents budget (shared with
//! the console's Event history page).
//!
//! ## Delivery semantics
//!
//! At-least-once. The max delivered event time is snapshotted at each
//! checkpoint marker and persisted when the checkpoint finalizes; a restart
//! resumes at `checkpoint − lookback`, so the in-memory dedupe set is not
//! persisted and the lookback window is re-emitted after every restart.
//! Late events are only caught while they are within `lookback_secs` of the
//! watermark — an event CloudTrail delivers later than that is **lost** (the
//! documented trade-off; raise `lookback_secs` for more safety at the cost
//! of more duplicate fetching). With `on_malformed=error` a poison event
//! stalls the source by design.
//!
//! ## Limits
//!
//! Inherited from LookupEvents, documented rather than mitigated:
//! - 2 requests/s per account/region, shared with the console Event history;
//!   at 50 events/page and the source's 1 req/s self-throttle this tops out
//!   around 50 events/s sustained. High-volume accounts need a trail
//!   (S3 + SQS notification), which this plugin does not implement.
//! - Management events only (no data events, no insight events).
//! - 90-day retention; `earliest` backfills reach at most that far.
//! - Events appear with a delivery lag, typically up to 15 minutes.
//! - One region per source; run one source per region for multi-region
//!   coverage.
//!
//! ## Example
//!
//! Write-mode management events flowing into S3:
//!
//! ```yaml
//! sources:
//!   audit:
//!     type: cloudtrail_source
//!     region: us-east-1
//!     start_time: earliest
//!     lookup_attribute_key: ReadOnly
//!     lookup_attribute_value: "false"
//!
//! transforms: {}
//!
//! sinks:
//!   archive:
//!     type: s3_sink
//!     from: audit
//!     bucket: my-audit-archive
//! ```

mod config;
mod convert;
mod poller;
mod source;

pub use source::CloudTrailSource;
