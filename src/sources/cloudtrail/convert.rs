//! CloudTrail event → Arrow RecordBatch conversion.
//!
//! Each LookupEvents result carries the full CloudTrail record as a JSON
//! string (`CloudTrailEvent`). `CloudTrailRecord::from_sdk_event` parses it
//! once in the poller: scalar fields become typed columns, nested subtrees
//! (userIdentity, requestParameters, …) are re-serialized as compact JSON
//! strings — the Athena-style layout. The SDK envelope backfills `event_id`,
//! `event_time`, `event_name`, `event_source`, and `username` when the
//! payload omits them.

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use aws_sdk_cloudtrail::types::Event;
use serde_json::{Map, Value};
use std::sync::Arc;
use streamling_plugin::PluginError;
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP;

/// One CloudTrail event, parsed and decoupled from SDK types so conversion
/// is independently testable.
#[derive(Debug, Clone, Default)]
pub(crate) struct CloudTrailRecord {
    pub event_id: String,
    /// Epoch milliseconds.
    pub event_time_ms: i64,
    pub event_version: Option<String>,
    pub event_name: Option<String>,
    pub event_source: Option<String>,
    pub event_category: Option<String>,
    pub event_type: Option<String>,
    pub aws_region: Option<String>,
    pub source_ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub read_only: Option<bool>,
    pub management_event: Option<bool>,
    pub recipient_account_id: Option<String>,
    pub request_id: Option<String>,
    pub shared_event_id: Option<String>,
    pub vpc_endpoint_id: Option<String>,
    pub vpc_endpoint_account_id: Option<String>,
    pub api_version: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub username: Option<String>,
    pub session_credential_from_console: Option<String>,
    /// Nested subtrees, re-serialized as compact JSON.
    pub user_identity: Option<String>,
    pub request_parameters: Option<String>,
    pub response_elements: Option<String>,
    pub resources: Option<String>,
    pub additional_event_data: Option<String>,
    pub service_event_details: Option<String>,
    pub tls_details: Option<String>,
    pub addendum: Option<String>,
    pub event_context: Option<String>,
    pub edge_device_details: Option<String>,
}

/// A scalar field as a string. CloudTrail is inconsistent about scalar types
/// across services (booleans and numbers appear both bare and quoted), so
/// non-string scalars are stringified rather than dropped.
fn string_field(json: &Map<String, Value>, key: &str) -> Option<String> {
    match json.get(key)? {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Lenient boolean: CloudTrail emits both `true` and `"true"`.
fn bool_field(json: &Map<String, Value>, key: &str) -> Option<bool> {
    lenient_bool(json.get(key)?)
}

fn lenient_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

/// A nested subtree re-serialized as compact JSON; JSON null becomes SQL null.
fn json_field(json: &Map<String, Value>, key: &str) -> Option<String> {
    json.get(key).filter(|v| !v.is_null()).map(Value::to_string)
}

impl CloudTrailRecord {
    pub fn from_sdk_event(event: &Event) -> Result<CloudTrailRecord, String> {
        let payload = event
            .cloud_trail_event()
            .ok_or("event has no CloudTrailEvent payload")?;
        let json: Value = serde_json::from_str(payload)
            .map_err(|e| format!("invalid CloudTrailEvent JSON: {e}"))?;
        let Value::Object(json) = json else {
            return Err("CloudTrailEvent JSON is not an object".to_string());
        };

        let event_id = string_field(&json, "eventID")
            .or_else(|| event.event_id().map(str::to_string))
            .ok_or("event has no eventID")?;
        let event_time_ms = match string_field(&json, "eventTime") {
            Some(s) => chrono::DateTime::parse_from_rfc3339(&s)
                .map_err(|e| format!("invalid eventTime '{s}': {e}"))?
                .timestamp_millis(),
            None => event
                .event_time()
                .map(|t| t.to_millis())
                .transpose()
                .map_err(|e| format!("event time out of range: {e}"))?
                .ok_or("event has no eventTime")?,
        };

        Ok(CloudTrailRecord {
            event_id,
            event_time_ms,
            event_version: string_field(&json, "eventVersion"),
            event_name: string_field(&json, "eventName")
                .or_else(|| event.event_name().map(str::to_string)),
            event_source: string_field(&json, "eventSource")
                .or_else(|| event.event_source().map(str::to_string)),
            event_category: string_field(&json, "eventCategory"),
            event_type: string_field(&json, "eventType"),
            aws_region: string_field(&json, "awsRegion"),
            source_ip_address: string_field(&json, "sourceIPAddress"),
            user_agent: string_field(&json, "userAgent"),
            read_only: bool_field(&json, "readOnly").or_else(|| {
                event
                    .read_only()
                    .and_then(|s| lenient_bool(&Value::from(s)))
            }),
            management_event: bool_field(&json, "managementEvent"),
            recipient_account_id: string_field(&json, "recipientAccountId"),
            request_id: string_field(&json, "requestID"),
            shared_event_id: string_field(&json, "sharedEventID"),
            vpc_endpoint_id: string_field(&json, "vpcEndpointId"),
            vpc_endpoint_account_id: string_field(&json, "vpcEndpointAccountId"),
            api_version: string_field(&json, "apiVersion"),
            error_code: string_field(&json, "errorCode"),
            error_message: string_field(&json, "errorMessage"),
            username: json
                .get("userIdentity")
                .and_then(|identity| identity.get("userName"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| event.username().map(str::to_string)),
            session_credential_from_console: string_field(&json, "sessionCredentialFromConsole"),
            user_identity: json_field(&json, "userIdentity"),
            request_parameters: json_field(&json, "requestParameters"),
            response_elements: json_field(&json, "responseElements"),
            resources: json_field(&json, "resources"),
            additional_event_data: json_field(&json, "additionalEventData"),
            service_event_details: json_field(&json, "serviceEventDetails"),
            tls_details: json_field(&json, "tlsDetails"),
            addendum: json_field(&json, "addendum"),
            event_context: json_field(&json, "eventContext"),
            edge_device_details: json_field(&json, "edgeDeviceDetails"),
        })
    }
}

pub(crate) struct RecordConverter {
    schema: SchemaRef,
}

fn utf8_column<'a>(
    records: &'a [CloudTrailRecord],
    get: impl Fn(&'a CloudTrailRecord) -> Option<&'a str>,
) -> ArrayRef {
    Arc::new(StringArray::from_iter(records.iter().map(get)))
}

impl RecordConverter {
    pub fn new() -> Self {
        let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
        let boolean = |name: &str| Field::new(name, DataType::Boolean, true);
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
                Field::new("event_id", DataType::Utf8, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                utf8("event_version"),
                utf8("event_name"),
                utf8("event_source"),
                utf8("event_category"),
                utf8("event_type"),
                utf8("aws_region"),
                utf8("source_ip_address"),
                utf8("user_agent"),
                boolean("read_only"),
                boolean("management_event"),
                utf8("recipient_account_id"),
                utf8("request_id"),
                utf8("shared_event_id"),
                utf8("vpc_endpoint_id"),
                utf8("vpc_endpoint_account_id"),
                utf8("api_version"),
                utf8("error_code"),
                utf8("error_message"),
                utf8("username"),
                utf8("session_credential_from_console"),
                utf8("user_identity"),
                utf8("request_parameters"),
                utf8("response_elements"),
                utf8("resources"),
                utf8("additional_event_data"),
                utf8("service_event_details"),
                utf8("tls_details"),
                utf8("addendum"),
                utf8("event_context"),
                utf8("edge_device_details"),
            ])),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn convert(&self, records: &[CloudTrailRecord]) -> Result<RecordBatch, PluginError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        // Column order must match the schema built in `new` exactly.
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                // Management events are append-only facts: always inserts.
                records.iter().map(|_| "i"),
            )),
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.event_id.as_str()),
            )),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    records.iter().map(|r| r.event_time_ms),
                )
                .with_timezone("UTC"),
            ),
            utf8_column(records, |r| r.event_version.as_deref()),
            utf8_column(records, |r| r.event_name.as_deref()),
            utf8_column(records, |r| r.event_source.as_deref()),
            utf8_column(records, |r| r.event_category.as_deref()),
            utf8_column(records, |r| r.event_type.as_deref()),
            utf8_column(records, |r| r.aws_region.as_deref()),
            utf8_column(records, |r| r.source_ip_address.as_deref()),
            utf8_column(records, |r| r.user_agent.as_deref()),
            Arc::new(BooleanArray::from_iter(records.iter().map(|r| r.read_only))),
            Arc::new(BooleanArray::from_iter(
                records.iter().map(|r| r.management_event),
            )),
            utf8_column(records, |r| r.recipient_account_id.as_deref()),
            utf8_column(records, |r| r.request_id.as_deref()),
            utf8_column(records, |r| r.shared_event_id.as_deref()),
            utf8_column(records, |r| r.vpc_endpoint_id.as_deref()),
            utf8_column(records, |r| r.vpc_endpoint_account_id.as_deref()),
            utf8_column(records, |r| r.api_version.as_deref()),
            utf8_column(records, |r| r.error_code.as_deref()),
            utf8_column(records, |r| r.error_message.as_deref()),
            utf8_column(records, |r| r.username.as_deref()),
            utf8_column(records, |r| r.session_credential_from_console.as_deref()),
            utf8_column(records, |r| r.user_identity.as_deref()),
            utf8_column(records, |r| r.request_parameters.as_deref()),
            utf8_column(records, |r| r.response_elements.as_deref()),
            utf8_column(records, |r| r.resources.as_deref()),
            utf8_column(records, |r| r.additional_event_data.as_deref()),
            utf8_column(records, |r| r.service_event_details.as_deref()),
            utf8_column(records, |r| r.tls_details.as_deref()),
            utf8_column(records, |r| r.addendum.as_deref()),
            utf8_column(records, |r| r.event_context.as_deref()),
            utf8_column(records, |r| r.edge_device_details.as_deref()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).map_err(PluginError::ArrowError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use aws_sdk_cloudtrail::primitives::DateTime;

    /// A complete CloudTrail v1.11 management-event record.
    const FULL_RECORD: &str = r#"{
        "eventVersion": "1.11",
        "userIdentity": {"type": "IAMUser", "principalId": "EXAMPLE", "userName": "alice"},
        "eventTime": "2026-01-02T03:04:05Z",
        "eventSource": "s3.amazonaws.com",
        "eventName": "CreateBucket",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "192.0.2.1",
        "userAgent": "aws-cli/2.15",
        "errorCode": "AccessDenied",
        "errorMessage": "denied",
        "requestParameters": {"bucketName": "my-bucket"},
        "responseElements": null,
        "additionalEventData": {"bytesTransferredIn": 0},
        "requestID": "req-1",
        "eventID": "11111111-2222-3333-4444-555555555555",
        "readOnly": false,
        "resources": [{"ARN": "arn:aws:s3:::my-bucket"}],
        "eventType": "AwsApiCall",
        "apiVersion": "2006-03-01",
        "managementEvent": true,
        "recipientAccountId": "123456789012",
        "serviceEventDetails": {"reason": "test"},
        "sharedEventID": "shared-1",
        "vpcEndpointId": "vpce-1",
        "vpcEndpointAccountId": "210987654321",
        "eventCategory": "Management",
        "addendum": {"reason": "UPDATED_DATA"},
        "sessionCredentialFromConsole": "true",
        "edgeDeviceDetails": {"type": "outpost"},
        "tlsDetails": {"tlsVersion": "TLSv1.3"},
        "eventContext": {"requestContext": {}}
    }"#;

    fn event(payload: &str) -> Event {
        Event::builder().cloud_trail_event(payload).build()
    }

    #[test]
    fn schema_has_expected_columns() {
        let schema = RecordConverter::new().schema();
        assert_eq!(schema.fields().len(), 33);
        assert!(!schema.field_with_name("_gs_op").unwrap().is_nullable());
        assert!(!schema.field_with_name("event_id").unwrap().is_nullable());
        assert_eq!(
            schema.field_with_name("event_time").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field_with_name("read_only").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            schema.field_with_name("user_identity").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn parses_full_v1_11_record_fixture() {
        let record = CloudTrailRecord::from_sdk_event(&event(FULL_RECORD)).unwrap();
        assert_eq!(record.event_id, "11111111-2222-3333-4444-555555555555");
        assert_eq!(record.event_time_ms, 1_767_323_045_000);
        assert_eq!(record.event_version.as_deref(), Some("1.11"));
        assert_eq!(record.event_name.as_deref(), Some("CreateBucket"));
        assert_eq!(record.event_source.as_deref(), Some("s3.amazonaws.com"));
        assert_eq!(record.event_category.as_deref(), Some("Management"));
        assert_eq!(record.event_type.as_deref(), Some("AwsApiCall"));
        assert_eq!(record.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(record.source_ip_address.as_deref(), Some("192.0.2.1"));
        assert_eq!(record.user_agent.as_deref(), Some("aws-cli/2.15"));
        assert_eq!(record.read_only, Some(false));
        assert_eq!(record.management_event, Some(true));
        assert_eq!(record.recipient_account_id.as_deref(), Some("123456789012"));
        assert_eq!(record.request_id.as_deref(), Some("req-1"));
        assert_eq!(record.shared_event_id.as_deref(), Some("shared-1"));
        assert_eq!(record.vpc_endpoint_id.as_deref(), Some("vpce-1"));
        assert_eq!(
            record.vpc_endpoint_account_id.as_deref(),
            Some("210987654321")
        );
        assert_eq!(record.api_version.as_deref(), Some("2006-03-01"));
        assert_eq!(record.error_code.as_deref(), Some("AccessDenied"));
        assert_eq!(record.error_message.as_deref(), Some("denied"));
        assert_eq!(record.username.as_deref(), Some("alice"));
        assert_eq!(
            record.session_credential_from_console.as_deref(),
            Some("true")
        );
        // Key order depends on serde_json features; compare parsed values.
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(record.user_identity.as_deref().unwrap())
                .unwrap(),
            serde_json::json!({"type": "IAMUser", "principalId": "EXAMPLE", "userName": "alice"})
        );
        assert_eq!(
            record.request_parameters.as_deref(),
            Some(r#"{"bucketName":"my-bucket"}"#)
        );
        // JSON null → SQL null, not the string "null".
        assert_eq!(record.response_elements, None);
        assert_eq!(
            record.resources.as_deref(),
            Some(r#"[{"ARN":"arn:aws:s3:::my-bucket"}]"#)
        );
        assert_eq!(
            record.tls_details.as_deref(),
            Some(r#"{"tlsVersion":"TLSv1.3"}"#)
        );
        assert_eq!(
            record.addendum.as_deref(),
            Some(r#"{"reason":"UPDATED_DATA"}"#)
        );
        assert_eq!(
            record.event_context.as_deref(),
            Some(r#"{"requestContext":{}}"#)
        );
        assert_eq!(
            record.edge_device_details.as_deref(),
            Some(r#"{"type":"outpost"}"#)
        );
    }

    #[test]
    fn missing_optional_fields_become_nulls() {
        let record = CloudTrailRecord::from_sdk_event(&event(
            r#"{"eventID": "id-1", "eventTime": "2026-01-02T03:04:05Z"}"#,
        ))
        .unwrap();
        assert_eq!(record.event_id, "id-1");
        assert_eq!(record.event_version, None);
        assert_eq!(record.read_only, None);
        assert_eq!(record.management_event, None);
        assert_eq!(record.username, None);
        assert_eq!(record.user_identity, None);
    }

    #[test]
    fn sdk_fields_backfill_missing_json_fields() {
        let event = Event::builder()
            .cloud_trail_event("{}")
            .event_id("envelope-id")
            .event_time(DateTime::from_millis(1_767_323_045_000))
            .event_name("ConsoleLogin")
            .event_source("signin.amazonaws.com")
            .username("bob")
            .read_only("false")
            .build();
        let record = CloudTrailRecord::from_sdk_event(&event).unwrap();
        assert_eq!(record.event_id, "envelope-id");
        assert_eq!(record.event_time_ms, 1_767_323_045_000);
        assert_eq!(record.event_name.as_deref(), Some("ConsoleLogin"));
        assert_eq!(record.event_source.as_deref(), Some("signin.amazonaws.com"));
        assert_eq!(record.username.as_deref(), Some("bob"));
        assert_eq!(record.read_only, Some(false));
    }

    #[test]
    fn lenient_boolean_parsing() {
        for (payload, expected) in [
            (r#""readOnly": true"#, Some(true)),
            (r#""readOnly": "true""#, Some(true)),
            (r#""readOnly": "False""#, Some(false)),
            (r#""readOnly": "maybe""#, None),
        ] {
            let record = CloudTrailRecord::from_sdk_event(&event(&format!(
                r#"{{"eventID": "id-1", "eventTime": "2026-01-02T03:04:05Z", {payload}}}"#
            )))
            .unwrap();
            assert_eq!(record.read_only, expected, "payload: {payload}");
        }
    }

    #[test]
    fn missing_or_malformed_cloud_trail_event_is_an_error() {
        let err = CloudTrailRecord::from_sdk_event(&Event::builder().build()).unwrap_err();
        assert!(err.contains("no CloudTrailEvent"), "got {err}");

        let err = CloudTrailRecord::from_sdk_event(&event("not json")).unwrap_err();
        assert!(err.contains("invalid CloudTrailEvent JSON"), "got {err}");

        let err = CloudTrailRecord::from_sdk_event(&event("[]")).unwrap_err();
        assert!(err.contains("not an object"), "got {err}");

        // Non-null columns cannot be backfilled from an empty envelope.
        let err = CloudTrailRecord::from_sdk_event(&event("{}")).unwrap_err();
        assert!(err.contains("eventID"), "got {err}");

        let err = CloudTrailRecord::from_sdk_event(&event(r#"{"eventID": "id-1"}"#)).unwrap_err();
        assert!(err.contains("eventTime"), "got {err}");
    }

    #[test]
    fn converts_records_to_batch() {
        let converter = RecordConverter::new();
        let records = vec![
            CloudTrailRecord::from_sdk_event(&event(FULL_RECORD)).unwrap(),
            CloudTrailRecord::from_sdk_event(&event(
                r#"{"eventID": "id-2", "eventTime": "2026-01-02T03:04:06Z"}"#,
            ))
            .unwrap(),
        ];
        let batch = converter.convert(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 33);

        let column = |name: &str| batch.column_by_name(name).unwrap();
        let ops: &StringArray = column("_gs_op").as_any().downcast_ref().unwrap();
        assert_eq!(ops.value(0), "i");
        assert_eq!(ops.value(1), "i");
        let ids: &StringArray = column("event_id").as_any().downcast_ref().unwrap();
        assert_eq!(ids.value(1), "id-2");
        let times: &TimestampMillisecondArray =
            column("event_time").as_any().downcast_ref().unwrap();
        assert_eq!(times.value(0), 1_767_323_045_000);
        assert_eq!(times.value(1), 1_767_323_046_000);
        let read_only: &BooleanArray = column("read_only").as_any().downcast_ref().unwrap();
        assert!(!read_only.value(0));
        assert!(read_only.is_null(1));
        let identities: &StringArray = column("user_identity").as_any().downcast_ref().unwrap();
        assert!(identities.value(0).contains("IAMUser"));
        assert!(identities.is_null(1));
    }

    #[test]
    fn empty_input_produces_empty_batch() {
        let batch = RecordConverter::new().convert(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 33);
    }
}
