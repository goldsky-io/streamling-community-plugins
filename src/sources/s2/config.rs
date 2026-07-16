//! Plugin option parsing for the S2 source. See the module docs in `mod.rs`
//! for the full option list.

use crate::sources::s2::convert::{OnMalformed, OutputMode, parse_field_specs};
use crate::utils::plugin_options::PluginOptions;
use crate::utils::s2::optional_endpoints;
use s2_sdk::types::{BasinName, S2Endpoints, StreamName, StreamNamePrefix};
use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use streamling_plugin::PluginError;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StartPosition {
    Earliest,
    Latest,
}

#[derive(Debug, Clone)]
pub struct S2SourceConfig {
    pub basin: BasinName,
    /// Exact streams to read (from `streams`), deduplicated.
    pub streams: Vec<StreamName>,
    /// Read every stream with this prefix; refreshed periodically.
    pub stream_prefix: Option<StreamNamePrefix>,
    /// S2-compatible endpoint override (e.g. s2-lite).
    pub endpoints: Option<S2Endpoints>,
    pub start_position: StartPosition,
    pub output: OutputMode,
    /// Max records per generated Arrow batch.
    pub batch_size: usize,
    /// How often `stream_prefix` fetches the next listing page.
    pub update_streams_interval_secs: u64,
    pub request_timeout_ms: u64,
}

fn parse_streams(opts: &PluginOptions) -> Result<Vec<StreamName>, PluginError> {
    let mut seen = BTreeSet::new();
    opts.get_or("streams", "")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter(|s| seen.insert(s.to_string()))
        .map(|s| opts.parse_value("stream name", s))
        .collect()
}

fn parse_start_position(opts: &PluginOptions) -> Result<StartPosition, PluginError> {
    let value = opts.get_or("start_position", "earliest");
    match value.to_ascii_lowercase().as_str() {
        "earliest" => Ok(StartPosition::Earliest),
        "latest" => Ok(StartPosition::Latest),
        other => Err(PluginError::Internal(format!(
            "s2_source: start_position must be 'earliest' or 'latest', got '{other}'"
        ))),
    }
}

fn parse_output(opts: &PluginOptions) -> Result<OutputMode, PluginError> {
    let schema = opts.get_or("schema", "");
    let include_metadata = opts.get_or("include_metadata", "");
    let on_malformed = opts.get_or("on_malformed", "");

    if schema.trim().is_empty() {
        for (key, value) in [
            ("include_metadata", &include_metadata),
            ("on_malformed", &on_malformed),
        ] {
            if !value.is_empty() {
                return Err(PluginError::Internal(format!(
                    "s2_source: {key} only applies when 'schema' is set \
                     (raw mode always includes stream metadata)"
                )));
            }
        }
        return Ok(OutputMode::Raw);
    }

    let include_metadata = if include_metadata.is_empty() {
        false
    } else {
        include_metadata.parse().map_err(|e| {
            PluginError::Internal(format!("s2_source: invalid include_metadata: {e}"))
        })?
    };
    let on_malformed = match on_malformed.to_ascii_lowercase().as_str() {
        "" | "error" => OnMalformed::Error,
        "skip" => OnMalformed::Skip,
        other => {
            return Err(PluginError::Internal(format!(
                "s2_source: on_malformed must be 'error' or 'skip', got '{other}'"
            )));
        }
    };

    Ok(OutputMode::Typed {
        fields: parse_field_specs(&schema)?,
        include_metadata,
        on_malformed,
    })
}

pub fn parse_config(opts: &PluginOptions) -> Result<S2SourceConfig, PluginError> {
    let basin: BasinName = opts.parse_value("basin", &opts.get("basin")?)?;

    let streams = parse_streams(opts)?;
    let prefix = opts.get_or("stream_prefix", "");
    let stream_prefix = match prefix.trim() {
        "" => None,
        p => Some(opts.parse_value("stream_prefix", p)?),
    };
    if streams.is_empty() && stream_prefix.is_none() {
        return Err(PluginError::Internal(
            "s2_source: one of 'streams' or 'stream_prefix' is required".to_string(),
        ));
    }

    Ok(S2SourceConfig {
        basin,
        streams,
        stream_prefix,
        endpoints: optional_endpoints(opts)?,
        start_position: parse_start_position(opts)?,
        output: parse_output(opts)?,
        batch_size: opts.get_parsed::<NonZeroUsize>("batch_size", "1000")?.get(),
        update_streams_interval_secs: opts
            .get_parsed::<NonZeroU64>("update_streams_interval_secs", "60")?
            .get(),
        request_timeout_ms: opts.get_parsed("request_timeout_ms", "5000")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::for_test(
            "s2_source",
            "STREAMLING__PLUGIN__S2_SOURCE_CONFIG_TEST",
            pairs,
        )
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse_config(&options(&[("basin", "my-basin"), ("streams", "events")])).unwrap();
        assert_eq!(cfg.basin.to_string(), "my-basin");
        assert_eq!(cfg.streams.len(), 1);
        assert!(cfg.stream_prefix.is_none());
        assert!(cfg.endpoints.is_none());
        assert_eq!(cfg.start_position, StartPosition::Earliest);
        assert!(matches!(cfg.output, OutputMode::Raw));
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.update_streams_interval_secs, 60);
        assert_eq!(cfg.request_timeout_ms, 5000);
    }

    #[test]
    fn requires_basin_and_a_stream_selector() {
        assert!(parse_config(&options(&[("streams", "events")])).is_err());

        let err = parse_config(&options(&[("basin", "my-basin")])).unwrap_err();
        assert!(err.to_string().contains("stream_prefix"), "got {err}");
    }

    #[test]
    fn dedupes_and_trims_streams() {
        let cfg = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events, events-a, events-b,events"),
        ]))
        .unwrap();
        let names: Vec<String> = cfg.streams.iter().map(ToString::to_string).collect();
        assert_eq!(names, vec!["events", "events-a", "events-b"]);
    }

    #[test]
    fn parses_stream_prefix_and_start_position() {
        let cfg = parse_config(&options(&[
            ("basin", "my-basin"),
            ("stream_prefix", "events-"),
            ("start_position", "latest"),
        ]))
        .unwrap();
        assert!(cfg.streams.is_empty());
        assert!(cfg.stream_prefix.is_some());
        assert_eq!(cfg.start_position, StartPosition::Latest);

        let err = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("start_position", "bogus"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("start_position"));
    }

    #[test]
    fn parses_typed_output_options() {
        let cfg = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("schema", "id:int64,value:string?"),
            ("include_metadata", "true"),
            ("on_malformed", "skip"),
        ]))
        .unwrap();
        let OutputMode::Typed {
            fields,
            include_metadata,
            on_malformed,
        } = cfg.output
        else {
            panic!("expected typed output");
        };
        assert_eq!(fields.len(), 2);
        assert!(include_metadata);
        assert_eq!(on_malformed, OnMalformed::Skip);
    }

    #[test]
    fn typed_only_options_are_rejected_in_raw_mode() {
        for (key, value) in [("include_metadata", "true"), ("on_malformed", "skip")] {
            let err = parse_config(&options(&[
                ("basin", "my-basin"),
                ("streams", "events"),
                (key, value),
            ]))
            .unwrap_err();
            assert!(err.to_string().contains(key), "got {err}");
        }
    }

    #[test]
    fn invalid_on_malformed_is_an_error() {
        let err = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("schema", "id:int64"),
            ("on_malformed", "ignore"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("on_malformed"));
    }

    #[test]
    fn zero_update_streams_interval_is_an_error() {
        let err = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("update_streams_interval_secs", "0"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("update_streams_interval_secs"));
    }

    #[test]
    fn invalid_endpoint_is_a_config_error() {
        let err = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("endpoint", "not a url"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("endpoint"), "got {err}");
    }

    #[test]
    fn zero_batch_size_is_an_error() {
        let err = parse_config(&options(&[
            ("basin", "my-basin"),
            ("streams", "events"),
            ("batch_size", "0"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("batch_size"));
    }
}
