//! SQS sink option parsing; runs at pipeline validation too, where runtime env is absent.

use crate::utils::plugin_options::PluginOptions;
use streamling_plugin::PluginError;

pub const DEFAULT_ROLE_SESSION_NAME: &str = "streamling-sqs-sink";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleConfig {
    pub role_arn: String,
    pub external_id: Option<String>,
    pub session_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqsSinkConfig {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub assume_role: Option<AssumeRoleConfig>,
}

impl SqsSinkConfig {
    pub fn from_options(opts: &PluginOptions) -> Result<Self, PluginError> {
        if opts.lookup_non_empty("access_key_id").is_some()
            != opts.lookup_non_empty("secret_access_key").is_some()
        {
            return Err(invalid(
                "access_key_id and secret_access_key must be set together",
            ));
        }

        // Operators may set `external_id` on every sqs sink, so alone it's ignored.
        let assume_role = match opts.lookup_non_empty("role_arn") {
            None => None,
            Some(role_arn) => {
                validate_role_arn(&role_arn)?;
                // Env only: the operator, not the pipeline author, chooses the external ID.
                let external_id = opts.env_non_empty("external_id");
                if let Some(id) = &external_id {
                    validate_external_id(id)?;
                }
                let session_name = opts
                    .lookup_non_empty("role_session_name")
                    .unwrap_or_else(|| DEFAULT_ROLE_SESSION_NAME.to_string());
                validate_session_name(&session_name)?;
                Some(AssumeRoleConfig {
                    role_arn,
                    external_id,
                    session_name,
                })
            }
        };

        // Without this, a pipeline with no `region` uses the host's AWS_REGION for the queue.
        let region = opts.lookup_non_empty("region").or_else(|| {
            opts.lookup_non_empty("queue_url")
                .and_then(|url| region_from_queue_url(&url))
        });

        Ok(SqsSinkConfig {
            region,
            endpoint_url: opts.lookup_non_empty("endpoint_url"),
            assume_role,
        })
    }
}

/// `https://sqs.<region>.amazonaws.com[.cn]/<account>/<queue>` → `<region>`.
fn region_from_queue_url(url: &str) -> Option<String> {
    let host = url.split("://").nth(1)?.split('/').next()?;
    let rest = host.strip_prefix("sqs.")?;
    let (region, domain) = rest.split_once('.')?;
    matches!(domain, "amazonaws.com" | "amazonaws.com.cn").then(|| region.to_string())
}

fn invalid(reason: impl std::fmt::Display) -> PluginError {
    PluginError::Internal(format!("sqs_sink: {reason}"))
}

/// `arn:<partition>:iam::<12-digit account>:role/<path/name>`.
fn validate_role_arn(arn: &str) -> Result<(), PluginError> {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let valid = parts.len() == 6
        && parts[0] == "arn"
        && parts[1].starts_with("aws")
        && parts[2] == "iam"
        && parts[3].is_empty()
        && parts[4].len() == 12
        && parts[4].bytes().all(|b| b.is_ascii_digit())
        && parts[5].len() > "role/".len()
        && parts[5].starts_with("role/");
    if valid {
        Ok(())
    } else {
        Err(invalid(format!(
            "role_arn '{arn}' is not an IAM role ARN (arn:aws:iam::<account-id>:role/<name>)"
        )))
    }
}

fn is_sts_name_char(c: char, extra: &str) -> bool {
    c.is_ascii_alphanumeric() || "_+=,.@-".contains(c) || extra.contains(c)
}

/// STS RoleSessionName: 2-64 chars of `[\w+=,.@-]`.
fn validate_session_name(name: &str) -> Result<(), PluginError> {
    if (2..=64).contains(&name.len()) && name.chars().all(|c| is_sts_name_char(c, "")) {
        Ok(())
    } else {
        Err(invalid(format!(
            "role_session_name '{name}' must be 2-64 characters of letters, digits and _+=,.@-"
        )))
    }
}

/// STS ExternalId: 2-1224 chars of `[\w+=,.@:/-]`.
fn validate_external_id(id: &str) -> Result<(), PluginError> {
    if (2..=1224).contains(&id.len()) && id.chars().all(|c| is_sts_name_char(c, ":/")) {
        Ok(())
    } else {
        Err(invalid(
            "external_id must be 2-1224 characters of letters, digits and _+=,.@:/-",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENV_PREFIX: &str = "STREAMLING__PLUGIN__SQS_SINK_CONFIG_TEST";

    fn parse(pairs: &[(&str, &str)]) -> Result<SqsSinkConfig, PluginError> {
        SqsSinkConfig::from_options(&PluginOptions::for_test("sqs_sink", ENV_PREFIX, pairs))
    }

    const ROLE: &str = "arn:aws:iam::123456789012:role/partner-role";

    #[test]
    fn no_role_by_default() {
        let config = parse(&[("region", "us-east-1")]).unwrap();
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.assume_role, None);
    }

    #[test]
    fn region_defaults_to_the_queue_url_region() {
        let url = "https://sqs.eu-central-1.amazonaws.com/123456789012/q";
        let region = |pairs: &[(&str, &str)]| parse(pairs).unwrap().region;
        assert_eq!(
            region(&[("queue_url", url)]).as_deref(),
            Some("eu-central-1")
        );
        assert_eq!(
            region(&[("queue_url", url), ("region", "us-east-1")]).as_deref(),
            Some("us-east-1")
        );
        let cn = "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/q";
        assert_eq!(region(&[("queue_url", cn)]).as_deref(), Some("cn-north-1"));
        assert_eq!(
            region(&[("queue_url", "http://localhost:9324/000000000000/q")]),
            None
        );
    }

    /// Each caller passes its own prefix: tests share the process environment.
    fn parse_with_env_external_id(
        prefix: &str,
        external_id: &str,
        pairs: &[(&str, &str)],
    ) -> Result<SqsSinkConfig, PluginError> {
        // SAFETY: the variable name is unique to the calling test.
        unsafe { std::env::set_var(format!("{prefix}__EXTERNAL_ID"), external_id) };
        SqsSinkConfig::from_options(&PluginOptions::for_test("sqs_sink", prefix, pairs))
    }

    #[test]
    fn role_with_external_id_and_default_session_name() {
        let config = parse_with_env_external_id(
            "STREAMLING__PLUGIN__SQS_SINK_CONFIG_TEST_EXT",
            "tenant:abc",
            &[("role_arn", ROLE), ("external_id", "from-yaml")],
        )
        .unwrap();
        assert_eq!(
            config.assume_role,
            Some(AssumeRoleConfig {
                role_arn: ROLE.to_string(),
                external_id: Some("tenant:abc".to_string()),
                session_name: DEFAULT_ROLE_SESSION_NAME.to_string(),
            })
        );
    }

    #[test]
    fn external_id_in_yaml_is_ignored() {
        let config = parse(&[("role_arn", ROLE), ("external_id", "tenant:abc")]).unwrap();
        assert_eq!(config.assume_role.unwrap().external_id, None);
    }

    #[test]
    fn external_id_without_role_is_ignored() {
        let config = parse(&[("external_id", "tenant:abc")]).unwrap();
        assert_eq!(config.assume_role, None);
    }

    #[test]
    fn rejects_half_a_static_key_pair() {
        assert!(parse(&[("access_key_id", "AKIA")]).is_err());
        assert!(parse(&[("secret_access_key", "s")]).is_err());
        assert!(parse(&[("access_key_id", "AKIA"), ("secret_access_key", "s")]).is_ok());
    }

    #[test]
    fn rejects_malformed_role_arns() {
        for arn in [
            "not-an-arn",
            "arn:aws:iam::123:role/x",
            "arn:aws:iam::123456789012:user/x",
            "arn:aws:iam::123456789012:role/",
            "arn:aws:sts::123456789012:role/x",
        ] {
            assert!(
                parse(&[("role_arn", arn)]).is_err(),
                "{arn} should be rejected"
            );
        }
        assert!(parse(&[("role_arn", "arn:aws-cn:iam::123456789012:role/a/b")]).is_ok());
    }

    #[test]
    fn rejects_invalid_session_names_and_external_ids() {
        assert!(parse(&[("role_arn", ROLE), ("role_session_name", "has space")]).is_err());
        assert!(parse(&[("role_arn", ROLE), ("role_session_name", &"a".repeat(65))]).is_err());
        let prefix = "STREAMLING__PLUGIN__SQS_SINK_CONFIG_TEST_BAD_EXT";
        for bad in ["x", "bad id"] {
            assert!(parse_with_env_external_id(prefix, bad, &[("role_arn", ROLE)]).is_err());
        }
    }
}
