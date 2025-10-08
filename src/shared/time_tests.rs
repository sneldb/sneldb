use crate::shared::time::{TimeKind, TimeParser};
use chrono::{TimeZone, Utc};

#[test]
fn parses_rfc3339_z_to_epoch_seconds() {
    let s = "2025-09-07T12:34:56Z";
    let got = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::DateTime).unwrap();
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 12, 34, 56)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(got, expected);
}

#[test]
fn parses_rfc3339_with_offset() {
    let s = "2025-09-07T12:34:56+02:30";
    let got = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::DateTime).unwrap();
    // 12:34:56+02:30 == 10:04:56Z
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 10, 4, 56)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(got, expected);
}

#[test]
fn parses_date_only_to_midnight_utc() {
    let s = "2025-09-07";
    let got_dt = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::DateTime).unwrap();
    let got_date = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::Date).unwrap();
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(got_dt, expected);
    assert_eq!(got_date, expected);
}

#[test]
fn numeric_strings_normalize_units() {
    // seconds
    assert_eq!(
        TimeParser::parse_str_to_epoch_seconds("1600000000", TimeKind::DateTime),
        Some(1_600_000_000)
    );
    // milliseconds
    assert_eq!(
        TimeParser::parse_str_to_epoch_seconds("1600000000000", TimeKind::DateTime),
        Some(1_600_000_000)
    );
    // microseconds
    assert_eq!(
        TimeParser::parse_str_to_epoch_seconds("1600000000000000", TimeKind::DateTime),
        Some(1_600_000_000)
    );
    // nanoseconds
    assert_eq!(
        TimeParser::parse_str_to_epoch_seconds("1600000000000000000", TimeKind::DateTime),
        Some(1_600_000_000)
    );
}

#[test]
fn supports_negative_seconds() {
    // 1969-12-31T23:59:00Z
    assert_eq!(
        TimeParser::parse_str_to_epoch_seconds("-60", TimeKind::DateTime),
        Some(-60)
    );
}

#[test]
fn normalize_json_value_variants() {
    // integer seconds
    let mut v = serde_json::json!(1_600_000_000);
    TimeParser::normalize_json_value(&mut v, TimeKind::DateTime).unwrap();
    assert_eq!(v, serde_json::json!(1_600_000_000));

    // integer milliseconds
    let mut v = serde_json::json!(1_600_000_000_000u64);
    TimeParser::normalize_json_value(&mut v, TimeKind::DateTime).unwrap();
    assert_eq!(v, serde_json::json!(1_600_000_000));

    // float seconds (floor)
    let mut v = serde_json::json!(1_600_000_000.9);
    TimeParser::normalize_json_value(&mut v, TimeKind::DateTime).unwrap();
    assert_eq!(v, serde_json::json!(1_600_000_000));

    // string RFC3339
    let mut v = serde_json::json!("2025-09-07T00:00:00Z");
    TimeParser::normalize_json_value(&mut v, TimeKind::DateTime).unwrap();
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(v, serde_json::json!(expected));

    // invalid type
    let mut v = serde_json::json!({"x":1});
    let err = TimeParser::normalize_json_value(&mut v, TimeKind::DateTime).unwrap_err();
    assert!(err.contains("Time field must be a number or string"));
}
