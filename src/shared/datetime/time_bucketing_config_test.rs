use super::time_bucketing_config::TimeBucketingConfig;
use chrono::Weekday;

#[test]
fn test_default_config() {
    let config = TimeBucketingConfig::default();
    assert_eq!(config.timezone, None);
    assert_eq!(config.week_start, Weekday::Mon);
    assert_eq!(config.use_calendar_bucketing, true);
}

#[test]
fn test_timezone_parsing() {
    let config = TimeBucketingConfig {
        timezone: Some("US/Eastern".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_some());
}

#[test]
fn test_invalid_timezone() {
    let config = TimeBucketingConfig {
        timezone: Some("Invalid/Timezone".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_none());
}

#[test]
fn test_config_creation() {
    let config = TimeBucketingConfig {
        timezone: Some("Europe/London".to_string()),
        week_start: Weekday::Sun,
        use_calendar_bucketing: false,
    };

    assert_eq!(config.timezone, Some("Europe/London".to_string()));
    assert_eq!(config.week_start, Weekday::Sun);
    assert_eq!(config.use_calendar_bucketing, false);
}

#[test]
fn test_utc_timezone_parsing() {
    let config = TimeBucketingConfig {
        timezone: Some("UTC".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_some());
}

#[test]
fn test_pacific_timezone_parsing() {
    let config = TimeBucketingConfig {
        timezone: Some("US/Pacific".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_some());
}

#[test]
fn test_european_timezone_parsing() {
    let config = TimeBucketingConfig {
        timezone: Some("Europe/Paris".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_some());
}

#[test]
fn test_asia_timezone_parsing() {
    let config = TimeBucketingConfig {
        timezone: Some("Asia/Tokyo".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_some());
}

#[test]
fn test_different_week_starts() {
    let config_monday = TimeBucketingConfig {
        timezone: None,
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    let config_sunday = TimeBucketingConfig {
        timezone: None,
        week_start: Weekday::Sun,
        use_calendar_bucketing: true,
    };

    let config_wednesday = TimeBucketingConfig {
        timezone: None,
        week_start: Weekday::Wed,
        use_calendar_bucketing: true,
    };

    assert_eq!(config_monday.week_start, Weekday::Mon);
    assert_eq!(config_sunday.week_start, Weekday::Sun);
    assert_eq!(config_wednesday.week_start, Weekday::Wed);
}

#[test]
fn test_calendar_bucketing_flags() {
    let config_calendar = TimeBucketingConfig {
        timezone: None,
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    let config_naive = TimeBucketingConfig {
        timezone: None,
        week_start: Weekday::Mon,
        use_calendar_bucketing: false,
    };

    assert_eq!(config_calendar.use_calendar_bucketing, true);
    assert_eq!(config_naive.use_calendar_bucketing, false);
}

#[test]
fn test_from_app_config() {
    let config = TimeBucketingConfig::from_app_config();
    // Should return default config for now
    assert_eq!(config.timezone, None);
    assert_eq!(config.week_start, Weekday::Mon);
    assert_eq!(config.use_calendar_bucketing, true);
}

#[test]
fn test_config_equality() {
    let config1 = TimeBucketingConfig {
        timezone: Some("US/Eastern".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    let config2 = TimeBucketingConfig {
        timezone: Some("US/Eastern".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    let config3 = TimeBucketingConfig {
        timezone: Some("US/Pacific".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    assert_eq!(config1, config2);
    assert_ne!(config1, config3);
}

#[test]
fn test_config_clone() {
    let config = TimeBucketingConfig {
        timezone: Some("Europe/London".to_string()),
        week_start: Weekday::Sun,
        use_calendar_bucketing: false,
    };

    let cloned_config = config.clone();
    assert_eq!(config, cloned_config);
}

#[test]
fn test_config_debug() {
    let config = TimeBucketingConfig {
        timezone: Some("UTC".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };

    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("UTC"));
    assert!(debug_str.contains("Mon"));
    assert!(debug_str.contains("true"));
}

#[test]
fn test_empty_timezone_string() {
    let config = TimeBucketingConfig {
        timezone: Some("".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_none());
}

#[test]
fn test_malformed_timezone_string() {
    let config = TimeBucketingConfig {
        timezone: Some("Not/A/Timezone".to_string()),
        week_start: Weekday::Mon,
        use_calendar_bucketing: true,
    };
    assert!(config.parse_timezone().is_none());
}

#[test]
fn test_all_weekdays() {
    let weekdays = vec![
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Fri,
        Weekday::Sat,
        Weekday::Sun,
    ];

    for weekday in weekdays {
        let config = TimeBucketingConfig {
            timezone: None,
            week_start: weekday,
            use_calendar_bucketing: true,
        };
        assert_eq!(config.week_start, weekday);
    }
}
