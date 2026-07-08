//! Plugin option parsing for the S2 sink. See the module docs in `sink.rs`
//! for the full option list.

use crate::utils::plugin_options::PluginOptions;
use crate::utils::s2::optional_endpoints;
use s2_sdk::types::{BasinName, S2Endpoints, StreamName};
use std::time::Duration;
use streamling_plugin::PluginError;

/// Where records go: one fixed stream, or a per-row stream name resolved
/// from a template with `{column}` placeholders.
#[derive(Debug, Clone)]
pub(crate) enum StreamTarget {
    Fixed(StreamName),
    Template(Vec<Segment>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Segment {
    Literal(String),
    Column(String),
}

#[derive(Debug, Clone)]
pub(crate) struct S2SinkConfig {
    pub basin: BasinName,
    pub target: StreamTarget,
    /// Create target streams if missing (idempotent).
    pub ensure_stream: bool,
    pub request_timeout: Duration,
    /// How long the SDK Producer waits for more records before flushing a
    /// partial batch.
    pub linger: Duration,
    /// S2-compatible endpoint override (e.g. s2-lite).
    pub endpoints: Option<S2Endpoints>,
}

pub(crate) fn parse_config(opts: &PluginOptions) -> Result<S2SinkConfig, PluginError> {
    Ok(S2SinkConfig {
        basin: opts.parse_value("basin", &opts.get("basin")?)?,
        target: stream_target_from_options(opts)?,
        ensure_stream: opts.get_parsed("ensure_stream", "true")?,
        request_timeout: Duration::from_millis(opts.get_parsed("request_timeout_ms", "5000")?),
        linger: Duration::from_millis(opts.get_parsed("linger_ms", "5")?),
        endpoints: optional_endpoints(opts)?,
    })
}

/// Reads the routing target: exactly one of `stream` (fixed) or
/// `stream_template` (per-row, with `{column}` placeholders).
fn stream_target_from_options(opts: &PluginOptions) -> Result<StreamTarget, PluginError> {
    let stream = opts.get_or("stream", "");
    let template = opts.get_or("stream_template", "");
    match (stream.is_empty(), template.is_empty()) {
        (false, true) => Ok(StreamTarget::Fixed(
            opts.parse_value("stream name", &stream)?,
        )),
        (true, false) => Ok(StreamTarget::Template(parse_stream_template(&template)?)),
        (false, false) => Err(PluginError::Internal(
            "s2_sink: 'stream' and 'stream_template' are mutually exclusive".to_string(),
        )),
        (true, true) => Err(PluginError::Internal(
            "s2_sink: one of 'stream' or 'stream_template' is required".to_string(),
        )),
    }
}

/// Parses `events/{tenant}`-style templates into literal and column segments.
pub(crate) fn parse_stream_template(template: &str) -> Result<Vec<Segment>, PluginError> {
    let mut segments = Vec::new();
    let mut literal = String::new();
    let mut chars = template.chars();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if !literal.is_empty() {
                    segments.push(Segment::Literal(std::mem::take(&mut literal)));
                }
                let mut column = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some('{') | None => {
                            return Err(PluginError::Internal(format!(
                                "s2_sink: unclosed '{{' in stream_template '{template}'"
                            )));
                        }
                        Some(c) => column.push(c),
                    }
                }
                if column.is_empty() {
                    return Err(PluginError::Internal(format!(
                        "s2_sink: empty column placeholder in stream_template '{template}'"
                    )));
                }
                segments.push(Segment::Column(column));
            }
            '}' => {
                return Err(PluginError::Internal(format!(
                    "s2_sink: unmatched '}}' in stream_template '{template}'"
                )));
            }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }
    if segments.is_empty() {
        return Err(PluginError::Internal(
            "s2_sink: stream_template cannot be empty".to_string(),
        ));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::for_test("s2_sink", "STREAMLING__PLUGIN__S2_SINK_CONFIG_TEST", pairs)
    }

    #[test]
    fn parses_minimal_options_with_defaults() {
        let cfg = parse_config(&options(&[("basin", "my-basin"), ("stream", "events")])).unwrap();
        assert_eq!(cfg.basin.to_string(), "my-basin");
        assert!(matches!(cfg.target, StreamTarget::Fixed(_)));
        assert!(cfg.ensure_stream);
        assert_eq!(cfg.request_timeout, Duration::from_millis(5000));
        assert_eq!(cfg.linger, Duration::from_millis(5));
        assert!(cfg.endpoints.is_none());
    }

    #[test]
    fn invalid_typed_options_are_errors() {
        for (key, value) in [
            ("basin", "NOT-A-BASIN"),
            ("ensure_stream", "nope"),
            ("request_timeout_ms", "fast"),
            ("linger_ms", "-1"),
            ("endpoint", "not a url"),
        ] {
            let err = parse_config(&options(&[
                ("basin", "my-basin"),
                ("stream", "events"),
                (key, value),
            ]))
            .expect_err("bad option must fail");
            assert!(err.to_string().contains(key), "for {key}: got {err}");
        }
    }

    #[test]
    fn stream_and_template_options_are_exclusive_and_required() {
        assert!(matches!(
            stream_target_from_options(&options(&[("stream", "events")])),
            Ok(StreamTarget::Fixed(_))
        ));
        assert!(matches!(
            stream_target_from_options(&options(&[("stream_template", "events/{tenant}")])),
            Ok(StreamTarget::Template(_))
        ));

        let err = stream_target_from_options(&options(&[
            ("stream", "events"),
            ("stream_template", "events/{tenant}"),
        ]))
        .expect_err("both set should fail");
        assert!(err.to_string().contains("mutually exclusive"), "got {err}");

        let err = stream_target_from_options(&options(&[])).expect_err("neither set should fail");
        assert!(err.to_string().contains("required"), "got {err}");
    }

    #[test]
    fn parses_stream_templates() {
        assert_eq!(
            parse_stream_template("events/{tenant}-{region}").expect("valid template"),
            vec![
                Segment::Literal("events/".to_string()),
                Segment::Column("tenant".to_string()),
                Segment::Literal("-".to_string()),
                Segment::Column("region".to_string()),
            ]
        );

        for invalid in ["events/{tenant", "events/{}", "events/}", "{a{b}}", ""] {
            assert!(
                parse_stream_template(invalid).is_err(),
                "'{invalid}' should be rejected"
            );
        }
    }
}
