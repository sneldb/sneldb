use crate::shared::time::{TimeKind, TimeParser, format_timestamp, now};
use chrono::{TimeZone, Utc};
use std::time::{SystemTime, UNIX_EPOCH};

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

#[test]
fn test_now_returns_valid_timestamp() {
    let timestamp = now();

    // Should be a reasonable timestamp (after 2020, before 2100)
    assert!(timestamp > 1_600_000_000, "Timestamp should be after 2020");
    assert!(timestamp < 4_000_000_000, "Timestamp should be before 2100");

    // Should be close to system time
    let system_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    assert!(
        timestamp.abs_diff(system_now) < 2,
        "Timestamp should be within 2 seconds of system time"
    );
}

#[test]
fn test_now_is_monotonic() {
    let t1 = now();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let t2 = now();

    assert!(t2 >= t1, "Timestamps should be monotonically increasing");
}

#[test]
fn test_format_timestamp_rfc3339() {
    // Known timestamp: 2023-11-14T22:13:20Z
    let timestamp = 1700000000;
    let formatted = format_timestamp(timestamp);

    assert_eq!(formatted, "2023-11-14T22:13:20+00:00");
}

#[test]
fn test_format_timestamp_zero() {
    let formatted = format_timestamp(0);
    assert_eq!(formatted, "1970-01-01T00:00:00+00:00");
}

#[test]
fn test_format_timestamp_various_times() {
    // Test various timestamps
    let test_cases = vec![
        (1_000_000_000, "2001-09-09T01:46:40+00:00"),
        (1_500_000_000, "2017-07-14T02:40:00+00:00"),
        (1_700_000_000, "2023-11-14T22:13:20+00:00"),
    ];

    for (timestamp, expected) in test_cases {
        let formatted = format_timestamp(timestamp);
        assert_eq!(
            formatted, expected,
            "Timestamp {} should format to {}",
            timestamp, expected
        );
    }
}

#[test]
fn test_format_timestamp_invalid() {
    // Very large timestamp that might be invalid
    let timestamp = u64::MAX;
    let formatted = format_timestamp(timestamp);

    // Should handle gracefully, either format or return error string
    assert!(
        formatted.contains("invalid") || formatted.contains("+"),
        "Should either format or indicate invalid: {}",
        formatted
    );
}

#[test]
fn test_format_timestamp_roundtrip() {
    // Test that formatting and parsing work together
    let original = 1700000000u64;
    let formatted = format_timestamp(original);
    let parsed = TimeParser::parse_str_to_epoch_seconds(&formatted, TimeKind::DateTime).unwrap();

    assert_eq!(
        parsed, original as i64,
        "Roundtrip should preserve timestamp"
    );
}

#[test]
fn test_now_and_format_together() {
    let timestamp = now();
    let formatted = format_timestamp(timestamp);

    // Should produce valid RFC3339 format
    assert!(formatted.contains("T"), "Should contain T separator");
    assert!(
        formatted.contains("+") || formatted.contains("Z"),
        "Should contain timezone"
    );

    // Should be parseable back
    let parsed = TimeParser::parse_str_to_epoch_seconds(&formatted, TimeKind::DateTime);
    assert!(parsed.is_some(), "Formatted timestamp should be parseable");
}

#[test]
fn test_format_timestamp_recent() {
    // Test with a recent timestamp (2024)
    let timestamp = 1704067200; // 2024-01-01T00:00:00Z
    let formatted = format_timestamp(timestamp);

    assert!(
        formatted.starts_with("2024-01-01"),
        "Should format 2024 correctly"
    );
}

#[test]
fn test_format_timestamp_consistency() {
    // Same timestamp should always format the same way
    let timestamp = 1700000000;
    let formatted1 = format_timestamp(timestamp);
    let formatted2 = format_timestamp(timestamp);

    assert_eq!(formatted1, formatted2, "Formatting should be deterministic");
}
