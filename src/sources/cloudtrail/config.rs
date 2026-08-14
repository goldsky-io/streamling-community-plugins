//! Plugin option parsing for the CloudTrail source. See the module docs in
//! `mod.rs` for the full option list.

use crate::utils::plugin_options::PluginOptions;
use std::num::{NonZeroU64, NonZeroUsize};
use streamling_plugin::PluginError;

/// The attribute keys LookupEvents accepts, in their canonical AWS spelling
/// (the API rejects any other casing).
const LOOKUP_ATTRIBUTE_KEYS: [&str; 8] = [
    "AccessKeyId",
    "EventId",
    "EventName",
    "EventSource",
    "ReadOnly",
    "ResourceName",
    "ResourceType",
    "Username",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartTime {
    /// Only events recorded after the pipeline first starts.
    Now,
    /// Everything LookupEvents retains (up to 90 days back).
    Earliest,
    /// Events at or after a fixed timestamp (epoch milliseconds).
    At(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMalformed {
    /// Fail the poll cycle; the source retries the same window (stalls, no
    /// loss).
    Error,
    /// Drop events whose payload cannot be parsed, with a WARN log.
    Skip,
}

#[derive(Debug, Clone)]
pub struct CloudTrailSourceConfig {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub start_time: StartTime,
    /// Delay between poll cycles once caught up to now.
    pub poll_interval_secs: u64,
    /// How far behind the watermark each window starts, re-reading recent
    /// history to pick up late-delivered events (0 = no protection).
    pub lookback_secs: u64,
    /// Max span of one poll window; bounds memory during backfill.
    pub max_window_secs: u64,
    /// Max records per generated Arrow batch.
    pub batch_size: usize,
    /// Server-side LookupEvents filter: (canonical key, value).
    pub lookup_attribute: Option<(String, String)>,
    pub on_malformed: OnMalformed,
}

fn optional(opts: &PluginOptions, key: &str) -> Option<String> {
    opts.lookup(key).filter(|value| !value.is_empty())
}

fn parse_start_time(opts: &PluginOptions) -> Result<StartTime, PluginError> {
    let value = opts.get_or("start_time", "now");
    match value.to_ascii_lowercase().as_str() {
        "now" => Ok(StartTime::Now),
        "earliest" => Ok(StartTime::Earliest),
        _ => chrono::DateTime::parse_from_rfc3339(&value)
            .map(|t| StartTime::At(t.timestamp_millis()))
            .map_err(|e| {
                PluginError::Internal(format!(
                    "cloudtrail_source: start_time must be 'now', 'earliest', or an \
                     RFC 3339 timestamp, got '{value}': {e}"
                ))
            }),
    }
}

fn parse_lookup_attribute(opts: &PluginOptions) -> Result<Option<(String, String)>, PluginError> {
    let key = opts.get_or("lookup_attribute_key", "");
    let key = key.trim();
    let value = opts.get_or("lookup_attribute_value", "");
    match (key.is_empty(), value.is_empty()) {
        (true, true) => Ok(None),
        (true, false) | (false, true) => Err(PluginError::Internal(
            "cloudtrail_source: lookup_attribute_key and lookup_attribute_value \
             must be set together"
                .to_string(),
        )),
        (false, false) => {
            let canonical = LOOKUP_ATTRIBUTE_KEYS
                .iter()
                .find(|candidate| candidate.eq_ignore_ascii_case(key))
                .ok_or_else(|| {
                    PluginError::Internal(format!(
                        "cloudtrail_source: invalid lookup_attribute_key '{key}'; \
                         must be one of {LOOKUP_ATTRIBUTE_KEYS:?}"
                    ))
                })?;
            Ok(Some((canonical.to_string(), value)))
        }
    }
}

fn parse_on_malformed(opts: &PluginOptions) -> Result<OnMalformed, PluginError> {
    match opts
        .get_or("on_malformed", "error")
        .to_ascii_lowercase()
        .as_str()
    {
        "error" => Ok(OnMalformed::Error),
        "skip" => Ok(OnMalformed::Skip),
        other => Err(PluginError::Internal(format!(
            "cloudtrail_source: on_malformed must be 'error' or 'skip', got '{other}'"
        ))),
    }
}

pub fn parse_config(opts: &PluginOptions) -> Result<CloudTrailSourceConfig, PluginError> {
    // A lone access key would silently fall back to the default credential
    // chain — and possibly a different account. Catch it before startup.
    let has_access_key = optional(opts, "access_key_id").is_some();
    let has_secret_key = optional(opts, "secret_access_key").is_some();
    if has_access_key != has_secret_key {
        return Err(PluginError::Internal(
            "cloudtrail_source: access_key_id and secret_access_key must be set \
             together (omit both to use the default AWS credential chain)"
                .to_string(),
        ));
    }

    let lookback_secs: u64 = opts.get_parsed("lookback_secs", "1800")?;
    let max_window_secs = opts
        .get_parsed::<NonZeroU64>("max_window_secs", "3600")?
        .get();
    // Each window starts at watermark − lookback and a fully-scanned capped
    // window advances the watermark to its end; the window must outspan the
    // lookback or the start never moves forward.
    if max_window_secs <= lookback_secs {
        return Err(PluginError::Internal(format!(
            "cloudtrail_source: max_window_secs ({max_window_secs}) must be greater \
             than lookback_secs ({lookback_secs})"
        )));
    }

    Ok(CloudTrailSourceConfig {
        region: optional(opts, "region"),
        endpoint_url: optional(opts, "endpoint_url"),
        start_time: parse_start_time(opts)?,
        poll_interval_secs: opts
            .get_parsed::<NonZeroU64>("poll_interval_secs", "60")?
            .get(),
        lookback_secs,
        max_window_secs,
        batch_size: opts.get_parsed::<NonZeroUsize>("batch_size", "1024")?.get(),
        lookup_attribute: parse_lookup_attribute(opts)?,
        on_malformed: parse_on_malformed(opts)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::for_test(
            "cloudtrail_source",
            "STREAMLING__PLUGIN__CLOUDTRAIL_SOURCE_CONFIG_TEST",
            pairs,
        )
    }

    #[test]
    fn parses_zero_config_with_defaults() {
        let cfg = parse_config(&options(&[])).unwrap();
        assert!(cfg.region.is_none());
        assert!(cfg.endpoint_url.is_none());
        assert_eq!(cfg.start_time, StartTime::Now);
        assert_eq!(cfg.poll_interval_secs, 60);
        assert_eq!(cfg.lookback_secs, 1800);
        assert_eq!(cfg.max_window_secs, 3600);
        assert_eq!(cfg.batch_size, 1024);
        assert!(cfg.lookup_attribute.is_none());
        assert_eq!(cfg.on_malformed, OnMalformed::Error);
    }

    #[test]
    fn parses_start_time_now_earliest_and_rfc3339() {
        let cfg = parse_config(&options(&[("start_time", "Now")])).unwrap();
        assert_eq!(cfg.start_time, StartTime::Now);

        let cfg = parse_config(&options(&[("start_time", "earliest")])).unwrap();
        assert_eq!(cfg.start_time, StartTime::Earliest);

        let cfg = parse_config(&options(&[("start_time", "2026-01-02T03:04:05Z")])).unwrap();
        assert_eq!(cfg.start_time, StartTime::At(1_767_323_045_000));
    }

    #[test]
    fn invalid_start_time_is_an_error() {
        let err = parse_config(&options(&[("start_time", "yesterday")])).unwrap_err();
        assert!(err.to_string().contains("start_time"), "got {err}");
    }

    #[test]
    fn zero_poll_interval_is_an_error() {
        let err = parse_config(&options(&[("poll_interval_secs", "0")])).unwrap_err();
        assert!(err.to_string().contains("poll_interval_secs"), "got {err}");
    }

    #[test]
    fn zero_batch_size_is_an_error() {
        let err = parse_config(&options(&[("batch_size", "0")])).unwrap_err();
        assert!(err.to_string().contains("batch_size"), "got {err}");
    }

    #[test]
    fn max_window_must_exceed_lookback() {
        let err = parse_config(&options(&[
            ("lookback_secs", "3600"),
            ("max_window_secs", "3600"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("max_window_secs"), "got {err}");

        parse_config(&options(&[
            ("lookback_secs", "3600"),
            ("max_window_secs", "3601"),
        ]))
        .unwrap();
    }

    #[test]
    fn lookup_attribute_requires_both_key_and_value() {
        for pair in [
            ("lookup_attribute_key", "EventName"),
            ("lookup_attribute_value", "ConsoleLogin"),
        ] {
            let err = parse_config(&options(&[pair])).unwrap_err();
            assert!(err.to_string().contains("together"), "got {err}");
        }
    }

    #[test]
    fn lookup_attribute_key_is_canonicalized_and_validated() {
        let cfg = parse_config(&options(&[
            ("lookup_attribute_key", "eventname"),
            ("lookup_attribute_value", "ConsoleLogin"),
        ]))
        .unwrap();
        assert_eq!(
            cfg.lookup_attribute,
            Some(("EventName".to_string(), "ConsoleLogin".to_string()))
        );

        let err = parse_config(&options(&[
            ("lookup_attribute_key", "EventTime"),
            ("lookup_attribute_value", "x"),
        ]))
        .unwrap_err();
        assert!(
            err.to_string().contains("lookup_attribute_key"),
            "got {err}"
        );
    }

    #[test]
    fn invalid_on_malformed_is_an_error() {
        let err = parse_config(&options(&[("on_malformed", "ignore")])).unwrap_err();
        assert!(err.to_string().contains("on_malformed"), "got {err}");
    }

    #[test]
    fn static_creds_require_both_keys() {
        for pair in [
            ("access_key_id", "AKIA123"),
            ("secret_access_key", "secret"),
        ] {
            let err = parse_config(&options(&[pair])).unwrap_err();
            assert!(err.to_string().contains("together"), "got {err}");
        }

        parse_config(&options(&[
            ("access_key_id", "AKIA123"),
            ("secret_access_key", "secret"),
        ]))
        .unwrap();
    }
}
