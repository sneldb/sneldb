use crate::engine::schema::PayloadTimeNormalizer;
use crate::test_helpers::factories::MiniSchemaFactory;
use chrono::{TimeZone, Utc};

#[test]
fn normalizes_timestamp_and_date_fields_from_strings() {
    let schema = MiniSchemaFactory::empty()
        .with("ts", "datetime")
        .with("d", "date")
        .with("name", "string")
        .create();

    let mut payload = serde_json::json!({
        "ts": "2025-09-07T12:34:56Z",
        "d": "2025-09-07",
        "name": "alice",
        "extra": 1,
    });

    let normalizer = PayloadTimeNormalizer::new(&schema);
    normalizer
        .normalize(&mut payload)
        .expect("normalize should succeed");

    let expected_ts = Utc
        .with_ymd_and_hms(2025, 9, 7, 12, 34, 56)
        .single()
        .unwrap()
        .timestamp();
    let expected_d = Utc
        .with_ymd_and_hms(2025, 9, 7, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();

    assert_eq!(payload["ts"], serde_json::json!(expected_ts));
    assert_eq!(payload["d"], serde_json::json!(expected_d));
    assert_eq!(payload["name"], serde_json::json!("alice"));
    assert_eq!(payload["extra"], serde_json::json!(1));
}

#[test]
fn normalizes_numeric_units_and_numeric_strings() {
    let schema = MiniSchemaFactory::empty()
        .with("ts_sec", "datetime")
        .with("ts_millis", "datetime")
        .with("ts_float", "datetime")
        .with("ts_num_str_millis", "datetime")
        .create();

    let mut payload = serde_json::json!({
        "ts_sec": 1_600_000_000,
        "ts_millis": 1_600_000_000_000u64,
        "ts_float": 1_600_000_000.9,
        "ts_num_str_millis": "1600000000000",
    });

    let normalizer = PayloadTimeNormalizer::new(&schema);
    normalizer
        .normalize(&mut payload)
        .expect("normalize should succeed");

    assert_eq!(payload["ts_sec"], serde_json::json!(1_600_000_000));
    assert_eq!(payload["ts_millis"], serde_json::json!(1_600_000_000));
    assert_eq!(payload["ts_float"], serde_json::json!(1_600_000_000));
    assert_eq!(
        payload["ts_num_str_millis"],
        serde_json::json!(1_600_000_000)
    );
}

#[test]
fn optional_time_fields_are_normalized_or_left_null() {
    let schema = MiniSchemaFactory::empty()
        .with_optional("opt_ts", "datetime")
        .with_optional("opt_date", "date")
        .create();

    let mut payload = serde_json::json!({
        "opt_ts": "2025-09-07T00:00:00Z",
        "opt_date": null,
    });

    let normalizer = PayloadTimeNormalizer::new(&schema);
    normalizer
        .normalize(&mut payload)
        .expect("normalize should succeed");

    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(payload["opt_ts"], serde_json::json!(expected));
    assert!(payload["opt_date"].is_null());
}

#[test]
fn missing_fields_are_ignored_and_extras_untouched() {
    let schema = MiniSchemaFactory::empty()
        .with("ts", "datetime")
        .with("d", "date")
        .create();

    let mut payload = serde_json::json!({
        "unrelated": 42,
    });

    let normalizer = PayloadTimeNormalizer::new(&schema);
    normalizer
        .normalize(&mut payload)
        .expect("normalize should succeed");

    assert_eq!(payload, serde_json::json!({"unrelated": 42}));
}

#[test]
fn invalid_time_string_returns_error() {
    let schema = MiniSchemaFactory::empty().with("ts", "datetime").create();
    let mut payload = serde_json::json!({"ts": "not-a-date"});

    let normalizer = PayloadTimeNormalizer::new(&schema);
    let err = normalizer
        .normalize(&mut payload)
        .expect_err("should return error for invalid time string");
    assert!(err.contains("Invalid time string"));
}

#[test]
fn payload_must_be_object() {
    let schema = MiniSchemaFactory::empty().with("ts", "datetime").create();
    let mut payload = serde_json::json!(["2025-09-07T00:00:00Z"]);

    let normalizer = PayloadTimeNormalizer::new(&schema);
    let err = normalizer
        .normalize(&mut payload)
        .expect_err("should return error when payload not object");
    assert!(err.contains("Payload must be a JSON object"));
}
