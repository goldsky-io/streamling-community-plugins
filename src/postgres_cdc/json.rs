//! etl `Cell` → `serde_json::Value` conversion for envelope row images.
//!
//! Mapping: ints/floats/bools native JSON; `Numeric` as decimal string
//! (exactness over convenience); `Bytes` as base64; timestamps RFC3339;
//! `Date`/`Time` ISO-8601 strings; `Uuid` string; `Json` passthrough;
//! arrays as JSON arrays with null elements preserved.

use base64::Engine;
use etl::types::{ArrayCell, Cell};
use serde_json::{Value, json};

pub fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::String(s) => json!(s),
        Cell::I16(v) => json!(v),
        Cell::I32(v) => json!(v),
        Cell::U32(v) => json!(v),
        Cell::I64(v) => json!(v),
        Cell::F32(v) => json!(v),
        Cell::F64(v) => json!(v),
        Cell::Numeric(n) => json!(n.to_string()),
        Cell::Date(d) => json!(d.format("%Y-%m-%d").to_string()),
        Cell::Time(t) => json!(t.format("%H:%M:%S%.6f").to_string()),
        Cell::Timestamp(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
        Cell::TimestampTz(ts) => json!(ts.to_rfc3339()),
        Cell::Uuid(u) => json!(u.to_string()),
        Cell::Json(v) => v.clone(),
        Cell::Bytes(b) => json!(base64::engine::general_purpose::STANDARD.encode(b)),
        Cell::Array(arr) => array_to_json(arr),
    }
}

fn array_to_json(arr: &ArrayCell) -> Value {
    fn map<T, F: Fn(&T) -> Value>(items: &[Option<T>], f: F) -> Value {
        Value::Array(
            items
                .iter()
                .map(|v| v.as_ref().map(&f).unwrap_or(Value::Null))
                .collect(),
        )
    }

    match arr {
        ArrayCell::Bool(v) => map(v, |b| json!(b)),
        ArrayCell::String(v) => map(v, |s| json!(s)),
        ArrayCell::I16(v) => map(v, |x| json!(x)),
        ArrayCell::I32(v) => map(v, |x| json!(x)),
        ArrayCell::U32(v) => map(v, |x| json!(x)),
        ArrayCell::I64(v) => map(v, |x| json!(x)),
        ArrayCell::F32(v) => map(v, |x| json!(x)),
        ArrayCell::F64(v) => map(v, |x| json!(x)),
        ArrayCell::Numeric(v) => map(v, |n| json!(n.to_string())),
        ArrayCell::Date(v) => map(v, |d| json!(d.format("%Y-%m-%d").to_string())),
        ArrayCell::Time(v) => map(v, |t| json!(t.format("%H:%M:%S%.6f").to_string())),
        ArrayCell::Timestamp(v) => map(v, |ts| {
            json!(ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }),
        ArrayCell::TimestampTz(v) => map(v, |ts| json!(ts.to_rfc3339())),
        ArrayCell::Uuid(v) => map(v, |u| json!(u.to_string())),
        ArrayCell::Json(v) => map(v, |j| j.clone()),
        ArrayCell::Bytes(v) => map(v, |b| {
            json!(base64::engine::general_purpose::STANDARD.encode(b))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};

    #[test]
    fn scalars_map_to_native_json() {
        assert_eq!(cell_to_json(&Cell::Null), Value::Null);
        assert_eq!(cell_to_json(&Cell::Bool(true)), json!(true));
        assert_eq!(cell_to_json(&Cell::String("x".into())), json!("x"));
        assert_eq!(cell_to_json(&Cell::I16(-3)), json!(-3));
        assert_eq!(cell_to_json(&Cell::I32(42)), json!(42));
        assert_eq!(cell_to_json(&Cell::U32(7)), json!(7));
        assert_eq!(cell_to_json(&Cell::I64(1_i64 << 40)), json!(1_i64 << 40));
        assert_eq!(cell_to_json(&Cell::F32(1.5)), json!(1.5));
        assert_eq!(cell_to_json(&Cell::F64(2.25)), json!(2.25));
    }

    #[test]
    fn bytes_map_to_base64() {
        assert_eq!(cell_to_json(&Cell::Bytes(vec![0xde, 0xad])), json!("3q0="));
    }

    #[test]
    fn temporal_types_map_to_iso_strings() {
        let d = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        assert_eq!(cell_to_json(&Cell::Date(d)), json!("2026-06-11"));

        let t = NaiveTime::from_hms_micro_opt(1, 2, 3, 500).unwrap();
        assert_eq!(cell_to_json(&Cell::Time(t)), json!("01:02:03.000500"));

        let ndt = d.and_time(t);
        assert_eq!(
            cell_to_json(&Cell::Timestamp(ndt)),
            json!("2026-06-11T01:02:03.000500")
        );

        let tz = Utc.with_ymd_and_hms(2026, 6, 11, 1, 2, 3).unwrap();
        assert_eq!(
            cell_to_json(&Cell::TimestampTz(tz)),
            json!("2026-06-11T01:02:03+00:00")
        );
    }

    #[test]
    fn uuid_and_json_pass_through() {
        let u = uuid::Uuid::nil();
        assert_eq!(
            cell_to_json(&Cell::Uuid(u)),
            json!("00000000-0000-0000-0000-000000000000")
        );
        assert_eq!(
            cell_to_json(&Cell::Json(json!({"a": [1, null]}))),
            json!({"a": [1, null]})
        );
    }

    #[test]
    fn arrays_preserve_null_elements() {
        let arr = Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]));
        assert_eq!(cell_to_json(&arr), json!([1, null, 3]));

        let sarr = Cell::Array(ArrayCell::String(vec![Some("a".into()), None]));
        assert_eq!(cell_to_json(&sarr), json!(["a", null]));
    }
}
