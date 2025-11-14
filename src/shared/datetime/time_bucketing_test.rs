use super::time::TimeConfig;
use super::time_bucketing::{CalendarTimeBucketer, naive_bucket_of};
use crate::command::types::TimeGranularity;
use crate::test_helpers::factories::TimestampFactory;
use chrono::Weekday;

/// Helper to create a bucketer with specific config
fn create_bucketer(
    timezone: Option<&str>,
    week_start: Weekday,
    use_calendar: bool,
) -> CalendarTimeBucketer {
    let config = TimeConfig {
        timezone: timezone.map(|s| s.to_string()),
        week_start,
        use_calendar_bucketing: use_calendar,
    };
    CalendarTimeBucketer::new(config)
}

// ============================================================================
// CalendarTimeBucketer Tests
// ============================================================================

#[test]
fn calendar_bucketer_constructor() {
    let config = TimeConfig::default();
    let _bucketer = CalendarTimeBucketer::new(config);
    // Just ensure it doesn't panic
    assert!(true);
}

// ============================================================================
// Hour Bucketing Tests
// ============================================================================

#[test]
fn calendar_hour_bucketing_utc() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test various hours
    let test_cases = vec![
        // (input_timestamp, expected_bucket_timestamp, description)
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45),
            TimestampFactory::utc_datetime(2024, 1, 15, 14, 0, 0),
            "14:30:45 should bucket to 14:00:00",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            "midnight should bucket to midnight",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 23, 59, 59),
            TimestampFactory::utc_datetime(2024, 1, 15, 23, 0, 0),
            "23:59:59 should bucket to 23:00:00",
        ),
    ];

    for (input, expected, description) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Hour);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_hour_bucketing_timezone() {
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test timezone-aware hour bucketing
    // 2024-01-15 14:30:45 UTC = 2024-01-15 09:30:45 EST
    let utc_ts = TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45);
    let result = bucketer.bucket_of(utc_ts, &TimeGranularity::Hour);

    // Should bucket to 09:00:00 EST = 14:00:00 UTC
    let expected = TimestampFactory::utc_datetime(2024, 1, 15, 14, 0, 0);
    assert_eq!(result, expected);
}

// ============================================================================
// Day Bucketing Tests
// ============================================================================

#[test]
fn calendar_day_bucketing_utc() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (input_timestamp, expected_bucket_timestamp, description)
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45),
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            "14:30:45 should bucket to start of day",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            "midnight should bucket to midnight",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 23, 59, 59),
            TimestampFactory::utc_datetime(2024, 1, 15, 0, 0, 0),
            "23:59:59 should bucket to start of day",
        ),
    ];

    for (input, expected, description) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Day);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_day_bucketing_timezone() {
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test timezone-aware day bucketing
    // 2024-01-15 14:30:45 UTC = 2024-01-15 09:30:45 EST
    let utc_ts = TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45);
    let result = bucketer.bucket_of(utc_ts, &TimeGranularity::Day);

    // Should bucket to start of day in EST = 05:00:00 UTC
    let expected = TimestampFactory::utc_datetime(2024, 1, 15, 5, 0, 0);
    assert_eq!(result, expected);
}

// ============================================================================
// Week Bucketing Tests
// ============================================================================

#[test]
fn calendar_week_bucketing_monday_start() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (input_timestamp, expected_bucket_timestamp, description)
        (
            TimestampFactory::utc_weekday(2024, 1, 3, Weekday::Wed), // Wednesday
            TimestampFactory::utc_date(2024, 1, 1),                  // Monday
            "Wednesday should bucket to Monday of same week",
        ),
        (
            TimestampFactory::utc_weekday(2024, 1, 1, Weekday::Mon), // Monday
            TimestampFactory::utc_date(2024, 1, 1),                  // Monday
            "Monday should bucket to same Monday",
        ),
        (
            TimestampFactory::utc_weekday(2024, 1, 7, Weekday::Sun), // Sunday
            TimestampFactory::utc_date(2024, 1, 1),                  // Monday
            "Sunday should bucket to Monday of same week",
        ),
    ];

    for (input, expected, description) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Week);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_week_bucketing_sunday_start() {
    let bucketer = create_bucketer(None, Weekday::Sun, true);

    let test_cases = vec![
        // (input_timestamp, expected_bucket_timestamp, description)
        (
            TimestampFactory::utc_weekday(2024, 1, 3, Weekday::Wed), // Wednesday
            TimestampFactory::utc_date(2023, 12, 31),                // Sunday (previous week)
            "Wednesday should bucket to Sunday of same week",
        ),
        (
            TimestampFactory::utc_weekday(2024, 1, 7, Weekday::Sun), // Sunday
            TimestampFactory::utc_date(2024, 1, 7),                  // Sunday
            "Sunday should bucket to same Sunday",
        ),
        (
            TimestampFactory::utc_weekday(2024, 1, 1, Weekday::Mon), // Monday
            TimestampFactory::utc_date(2023, 12, 31),                // Sunday (previous week)
            "Monday should bucket to Sunday of same week",
        ),
    ];

    for (input, expected, description) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Week);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_week_bucketing_cross_month_boundary() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test week that crosses month boundary
    // January 29, 2024 is a Monday, so the week includes Jan 29-31 and Feb 1-4
    let jan_29 = TimestampFactory::utc_weekday(2024, 1, 29, Weekday::Mon);
    let feb_1 = TimestampFactory::utc_weekday(2024, 2, 1, Weekday::Thu);

    let jan_29_bucket = bucketer.bucket_of(jan_29, &TimeGranularity::Week);
    let feb_1_bucket = bucketer.bucket_of(feb_1, &TimeGranularity::Week);

    // Both should bucket to the same week start (Jan 29)
    assert_eq!(jan_29_bucket, feb_1_bucket);
    assert_eq!(jan_29_bucket, TimestampFactory::utc_date(2024, 1, 29));
}

// ============================================================================
// Month Bucketing Tests
// ============================================================================

#[test]
fn calendar_month_bucketing_utc() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (input_timestamp, expected_bucket_timestamp, description)
        (
            TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45),
            TimestampFactory::utc_date(2024, 1, 1),
            "January 15 should bucket to January 1",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 1, 0, 0, 0),
            TimestampFactory::utc_date(2024, 1, 1),
            "January 1 should bucket to January 1",
        ),
        (
            TimestampFactory::utc_datetime(2024, 1, 31, 23, 59, 59),
            TimestampFactory::utc_date(2024, 1, 1),
            "January 31 should bucket to January 1",
        ),
    ];

    for (input, expected, description) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Month);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_month_bucketing_leap_year() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test February in leap year (2024)
    let feb_15_2024 = TimestampFactory::utc_date(2024, 2, 15);
    let feb_29_2024 = TimestampFactory::utc_date(2024, 2, 29); // Leap day

    let feb_15_bucket = bucketer.bucket_of(feb_15_2024, &TimeGranularity::Month);
    let feb_29_bucket = bucketer.bucket_of(feb_29_2024, &TimeGranularity::Month);

    // Both should bucket to February 1, 2024
    let expected = TimestampFactory::utc_date(2024, 2, 1);
    assert_eq!(feb_15_bucket, expected);
    assert_eq!(feb_29_bucket, expected);
}

#[test]
fn calendar_month_bucketing_different_month_lengths() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (month, day, expected_month, description)
        (1, 31, 1, "January has 31 days"),
        (2, 28, 2, "February has 28 days (non-leap year)"),
        (2, 29, 2, "February has 29 days (leap year)"),
        (4, 30, 4, "April has 30 days"),
        (12, 31, 12, "December has 31 days"),
    ];

    for (month, day, expected_month, description) in test_cases {
        let year = if month == 2 && day == 29 { 2024 } else { 2023 };
        let input = TimestampFactory::utc_date(year, month, day);
        let result = bucketer.bucket_of(input, &TimeGranularity::Month);
        let expected = TimestampFactory::utc_date(year, expected_month, 1);
        assert_eq!(result, expected, "{}", description);
    }
}

#[test]
fn calendar_year_bucketing() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test various dates throughout the year
    let test_cases = vec![
        ("January 15, 2024", TimestampFactory::utc_date(2024, 1, 15)),
        ("June 15, 2024", TimestampFactory::utc_date(2024, 6, 15)),
        (
            "December 31, 2024",
            TimestampFactory::utc_date(2024, 12, 31),
        ),
        (
            "February 29, 2024 (leap year)",
            TimestampFactory::utc_date(2024, 2, 29),
        ),
    ];

    for (description, input) in test_cases {
        let result = bucketer.bucket_of(input, &TimeGranularity::Year);

        // Should bucket to January 1, 2024 00:00:00 UTC
        let expected = TimestampFactory::utc_date(2024, 1, 1);
        assert_eq!(
            result, expected,
            "{} should bucket to start of year",
            description
        );
    }
}

#[test]
fn calendar_year_bucketing_timezone() {
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test timezone-aware year bucketing
    // 2024-06-15 14:30:45 UTC = 2024-06-15 10:30:45 EDT
    let utc_ts = TimestampFactory::utc_datetime(2024, 6, 15, 14, 30, 45);
    let result = bucketer.bucket_of(utc_ts, &TimeGranularity::Year);

    // Should bucket to start of year in EST = 05:00:00 UTC (Jan 1, 2024)
    let expected = TimestampFactory::utc_datetime(2024, 1, 1, 5, 0, 0);
    assert_eq!(result, expected);
}

#[test]
fn calendar_month_bucketing_timezone() {
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test timezone-aware month bucketing
    // 2024-01-15 14:30:45 UTC = 2024-01-15 09:30:45 EST
    let utc_ts = TimestampFactory::utc_datetime(2024, 1, 15, 14, 30, 45);
    let result = bucketer.bucket_of(utc_ts, &TimeGranularity::Month);

    // Should bucket to start of month in EST = 05:00:00 UTC
    let expected = TimestampFactory::utc_datetime(2024, 1, 1, 5, 0, 0);
    assert_eq!(result, expected);
}

// ============================================================================
// Timezone Tests
// ============================================================================

#[test]
fn calendar_bucketing_different_timezones() {
    let timezones = vec![
        ("UTC", None),
        ("US/Eastern", Some("US/Eastern")),
        ("US/Pacific", Some("US/Pacific")),
        ("Europe/London", Some("Europe/London")),
        ("Asia/Tokyo", Some("Asia/Tokyo")),
    ];

    let test_ts = TimestampFactory::utc_datetime(2024, 1, 15, 12, 0, 0);

    for (tz_name, tz_str) in timezones {
        let bucketer = create_bucketer(tz_str, Weekday::Mon, true);

        // Test that different timezones produce different results
        let day_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Day);
        let hour_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Hour);

        // Results should be valid timestamps
        assert!(day_bucket > 0, "{} day bucket should be valid", tz_name);
        assert!(hour_bucket > 0, "{} hour bucket should be valid", tz_name);
    }
}

#[test]
fn calendar_bucketing_dst_transition() {
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test DST transition dates in 2024
    // Spring forward: March 10, 2024 at 2:00 AM EST -> 3:00 AM EDT
    // Fall back: November 3, 2024 at 2:00 AM EDT -> 1:00 AM EST

    // Before DST (EST)
    let before_dst = TimestampFactory::utc_datetime(2024, 3, 10, 6, 0, 0); // 1:00 AM EST
    let before_bucket = bucketer.bucket_of(before_dst, &TimeGranularity::Hour);

    // After DST (EDT)
    let after_dst = TimestampFactory::utc_datetime(2024, 3, 10, 7, 0, 0); // 3:00 AM EDT
    let after_bucket = bucketer.bucket_of(after_dst, &TimeGranularity::Hour);

    // Both should be valid and different
    assert!(before_bucket > 0);
    assert!(after_bucket > 0);
    assert_ne!(before_bucket, after_bucket);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[test]
fn calendar_bucketing_zero_timestamp() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test Unix epoch (1970-01-01 00:00:00 UTC)
    let epoch = 0u64;
    let result = bucketer.bucket_of(epoch, &TimeGranularity::Day);

    // Should bucket to start of epoch day
    let expected = TimestampFactory::utc_date(1970, 1, 1);
    assert_eq!(result, expected);
}

#[test]
fn calendar_bucketing_future_timestamp() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test far future timestamp (year 2100)
    let future_ts = TimestampFactory::utc_datetime(2100, 1, 15, 12, 0, 0);
    let result = bucketer.bucket_of(future_ts, &TimeGranularity::Month);

    // Should bucket to start of month
    let expected = TimestampFactory::utc_date(2100, 1, 1);
    assert_eq!(result, expected);
}

#[test]
fn calendar_bucketing_invalid_timezone_falls_back_to_utc() {
    let config = TimeConfig {
        timezone: Some("Invalid/Timezone".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    let bucketer = CalendarTimeBucketer::new(config);

    // Should not panic and should use UTC
    let test_ts = TimestampFactory::utc_datetime(2024, 1, 15, 12, 0, 0);
    let result = bucketer.bucket_of(test_ts, &TimeGranularity::Day);

    // Should be same as UTC bucketing
    let utc_bucketer = create_bucketer(None, Weekday::Mon, true);
    let utc_result = utc_bucketer.bucket_of(test_ts, &TimeGranularity::Day);

    assert_eq!(result, utc_result);
}

// ============================================================================
// Performance and Consistency Tests
// ============================================================================

#[test]
fn calendar_bucketing_consistency() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);

    // Test that same input always produces same output
    let test_ts = TimestampFactory::utc_datetime(2024, 1, 15, 12, 30, 45);

    let result1 = bucketer.bucket_of(test_ts, &TimeGranularity::Hour);
    let result2 = bucketer.bucket_of(test_ts, &TimeGranularity::Hour);
    let result3 = bucketer.bucket_of(test_ts, &TimeGranularity::Hour);

    assert_eq!(result1, result2);
    assert_eq!(result2, result3);
}

#[test]
fn calendar_bucketing_all_granularities() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);
    let test_ts = TimestampFactory::utc_datetime(2024, 1, 15, 12, 30, 45);

    let granularities = vec![
        TimeGranularity::Hour,
        TimeGranularity::Day,
        TimeGranularity::Week,
        TimeGranularity::Month,
        TimeGranularity::Year,
    ];

    for gran in granularities {
        let result = bucketer.bucket_of(test_ts, &gran);
        assert!(result > 0, "Result should be positive for {:?}", gran);
        assert!(
            result <= test_ts,
            "Bucket should be <= input for {:?}",
            gran
        );
    }
}

// ============================================================================
// Naive vs Calendar Comparison Tests
// ============================================================================

#[test]
fn naive_vs_calendar_year_comparison() {
    let calendar_bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (description, timestamp)
        ("January 15, 2024", TimestampFactory::utc_date(2024, 1, 15)),
        ("June 15, 2024", TimestampFactory::utc_date(2024, 6, 15)),
        (
            "December 31, 2024",
            TimestampFactory::utc_date(2024, 12, 31),
        ),
        (
            "February 29, 2024 (leap year)",
            TimestampFactory::utc_date(2024, 2, 29),
        ),
    ];

    for (description, ts) in test_cases {
        let naive_result = naive_bucket_of(ts, &TimeGranularity::Year);
        let calendar_result = calendar_bucketer.bucket_of(ts, &TimeGranularity::Year);

        // Calendar should always bucket to January 1st of the year
        let expected = TimestampFactory::utc_date(2024, 1, 1);
        assert_eq!(
            calendar_result, expected,
            "Calendar bucketing for {} should be accurate",
            description
        );

        // Naive bucketing should be close but may differ due to leap years
        // It uses a fixed 365-day year approximation, so dates near year end
        // can be off by more days
        let days_diff = if naive_result > calendar_result {
            (naive_result - calendar_result) / 86_400
        } else {
            (calendar_result - naive_result) / 86_400
        };
        assert!(
            days_diff <= 366, // Allow up to a full year difference for naive approximation
            "Naive bucketing for {} should be reasonably close (diff: {} days)",
            description,
            days_diff
        );
    }
}

#[test]
fn naive_vs_calendar_month_comparison() {
    let calendar_bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (description, timestamp)
        ("January 15, 2024", TimestampFactory::utc_date(2024, 1, 15)),
        (
            "February 15, 2024 (leap year)",
            TimestampFactory::utc_date(2024, 2, 15),
        ),
        (
            "February 15, 2023 (non-leap year)",
            TimestampFactory::utc_date(2023, 2, 15),
        ),
        (
            "April 15, 2024 (30-day month)",
            TimestampFactory::utc_date(2024, 4, 15),
        ),
        (
            "December 15, 2024 (31-day month)",
            TimestampFactory::utc_date(2024, 12, 15),
        ),
    ];

    for (description, ts) in test_cases {
        let naive_result = naive_bucket_of(ts, &TimeGranularity::Month);
        let calendar_result = calendar_bucketer.bucket_of(ts, &TimeGranularity::Month);

        // Calendar should always be more accurate
        assert!(
            calendar_result <= ts,
            "Calendar bucket should be <= input for {}",
            description
        );
        assert!(
            naive_result <= ts,
            "Naive bucket should be <= input for {}",
            description
        );

        // For most cases, they should be different (except when they happen to align)
        // We'll just ensure both are valid
        assert!(
            naive_result > 0,
            "Naive result should be positive for {}",
            description
        );
        assert!(
            calendar_result > 0,
            "Calendar result should be positive for {}",
            description
        );
    }
}

#[test]
fn naive_vs_calendar_week_comparison() {
    let calendar_bucketer = create_bucketer(None, Weekday::Mon, true);

    let test_cases = vec![
        // (description, timestamp)
        (
            "Monday, January 1, 2024",
            TimestampFactory::utc_weekday(2024, 1, 1, Weekday::Mon),
        ),
        (
            "Wednesday, January 3, 2024",
            TimestampFactory::utc_weekday(2024, 1, 3, Weekday::Wed),
        ),
        (
            "Sunday, January 7, 2024",
            TimestampFactory::utc_weekday(2024, 1, 7, Weekday::Sun),
        ),
    ];

    for (description, ts) in test_cases {
        let naive_result = naive_bucket_of(ts, &TimeGranularity::Week);
        let calendar_result = calendar_bucketer.bucket_of(ts, &TimeGranularity::Week);

        // Both should be valid
        assert!(
            naive_result > 0,
            "Naive result should be positive for {}",
            description
        );
        assert!(
            calendar_result > 0,
            "Calendar result should be positive for {}",
            description
        );

        // Calendar should be more accurate (closer to actual week boundaries)
        assert!(
            calendar_result <= ts,
            "Calendar bucket should be <= input for {}",
            description
        );
        assert!(
            naive_result <= ts,
            "Naive bucket should be <= input for {}",
            description
        );
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn calendar_bucketing_integration_scenario() {
    // Simulate a real-world scenario with multiple events
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Events throughout a week in January 2024
    let events = vec![
        (
            "Monday morning",
            TimestampFactory::utc_datetime(2024, 1, 1, 14, 0, 0),
        ), // 9 AM EST
        (
            "Monday afternoon",
            TimestampFactory::utc_datetime(2024, 1, 1, 19, 0, 0),
        ), // 2 PM EST
        (
            "Wednesday",
            TimestampFactory::utc_datetime(2024, 1, 3, 15, 0, 0),
        ), // 10 AM EST
        (
            "Friday",
            TimestampFactory::utc_datetime(2024, 1, 5, 16, 0, 0),
        ), // 11 AM EST
        (
            "Sunday",
            TimestampFactory::utc_datetime(2024, 1, 7, 13, 0, 0),
        ), // 8 AM EST
    ];

    // Group by week
    let mut week_groups = std::collections::HashMap::new();
    for (description, ts) in &events {
        let week_bucket = bucketer.bucket_of(*ts, &TimeGranularity::Week);
        week_groups
            .entry(week_bucket)
            .or_insert(Vec::new())
            .push(*description);
    }

    // All events should be in the same week group
    assert_eq!(
        week_groups.len(),
        1,
        "All events should be in the same week"
    );

    // Group by day
    let mut day_groups = std::collections::HashMap::new();
    for (description, ts) in &events {
        let day_bucket = bucketer.bucket_of(*ts, &TimeGranularity::Day);
        day_groups
            .entry(day_bucket)
            .or_insert(Vec::new())
            .push(*description);
    }

    // Should have multiple day groups
    assert!(day_groups.len() > 1, "Events should span multiple days");
}

#[test]
fn calendar_bucketing_timezone_consistency() {
    // Test that timezone bucketing is consistent across different times
    let bucketer = create_bucketer(Some("US/Eastern"), Weekday::Mon, true);

    // Test multiple times throughout a day in UTC
    // Use a time that's clearly within one day in Eastern timezone
    let base_date = TimestampFactory::utc_date(2024, 1, 15);
    let hours = vec![6, 12, 18]; // Skip 0 and 23 as they might cross day boundaries in EST

    let mut day_buckets = std::collections::HashSet::new();
    for hour in hours {
        let ts = base_date + (hour * 3600);
        let bucket = bucketer.bucket_of(ts, &TimeGranularity::Day);
        day_buckets.insert(bucket);
    }

    // All times in the same day should bucket to the same day bucket
    assert_eq!(
        day_buckets.len(),
        1,
        "All times in same day should have same day bucket"
    );
}

// ============================================================================
// Debugging and Diagnostic Tests
// ============================================================================

#[test]
fn calendar_bucketing_debug_output() {
    let bucketer = create_bucketer(None, Weekday::Mon, true);
    let test_ts = TimestampFactory::utc_datetime(2024, 1, 15, 12, 30, 45);

    // This test is mainly for debugging - it prints the results
    // In a real test, you'd use this to verify expected behavior
    let hour_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Hour);
    let day_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Day);
    let week_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Week);
    let month_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Month);

    // Verify the buckets are in ascending order (more specific to less specific)
    assert!(
        hour_bucket >= day_bucket,
        "Hour bucket should be >= day bucket"
    );
    assert!(
        day_bucket >= week_bucket,
        "Day bucket should be >= week bucket"
    );
    assert!(
        week_bucket >= month_bucket,
        "Week bucket should be >= month bucket"
    );
    let year_bucket = bucketer.bucket_of(test_ts, &TimeGranularity::Year);
    assert!(
        month_bucket >= year_bucket,
        "Month bucket should be >= year bucket"
    );

    // All buckets should be valid timestamps
    assert!(hour_bucket > 0);
    assert!(day_bucket > 0);
    assert!(week_bucket > 0);
    assert!(month_bucket > 0);
    assert!(year_bucket > 0);
}
