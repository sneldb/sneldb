use super::time::TimeConfig;
use chrono::Weekday;

#[test]
fn test_default_config() {
    let config = TimeConfig::default();
    assert_eq!(config.timezone, None);
    assert_eq!(config.week_start, Weekday::Mon);
    assert_eq!(config.use_calendar_bucketing, true);
}

#[test]
fn test_from_app_config_reads_time_section() {
    // With test config, timezone is set to UTC
    let config = TimeConfig::from_app_config();
    assert_eq!(config.timezone, Some("UTC".to_string()));
    assert_eq!(config.week_start, Weekday::Mon);
    assert_eq!(config.use_calendar_bucketing, true);
}
