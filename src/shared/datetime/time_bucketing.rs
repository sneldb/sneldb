use super::time_bucketing_config::TimeBucketingConfig;
use crate::command::types::TimeGranularity;
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};

/// Calendar-aware time bucketing implementation
pub struct CalendarTimeBucketer {
    config: TimeBucketingConfig,
}

impl CalendarTimeBucketer {
    pub fn new(config: TimeBucketingConfig) -> Self {
        Self { config }
    }

    /// Calculate the bucket start timestamp for a given granularity
    pub fn bucket_of(&self, ts: u64, gran: &TimeGranularity) -> u64 {
        if let Some(tz) = self.config.parse_timezone() {
            let dt = DateTime::from_timestamp(ts as i64, 0)
                .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
                .with_timezone(&tz);

            let bucket_dt = match gran {
                TimeGranularity::Hour => self.bucket_hour(dt),
                TimeGranularity::Day => self.bucket_day(dt),
                TimeGranularity::Week => self.bucket_week(dt),
                TimeGranularity::Month => self.bucket_month(dt),
            };

            bucket_dt.timestamp() as u64
        } else {
            let dt = DateTime::from_timestamp(ts as i64, 0)
                .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());

            let bucket_dt = match gran {
                TimeGranularity::Hour => self.bucket_hour(dt),
                TimeGranularity::Day => self.bucket_day(dt),
                TimeGranularity::Week => self.bucket_week(dt),
                TimeGranularity::Month => self.bucket_month(dt),
            };

            bucket_dt.timestamp() as u64
        }
    }

    fn bucket_hour<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<T> {
        dt.date_naive()
            .and_hms_opt(dt.hour(), 0, 0)
            .unwrap()
            .and_local_timezone(dt.timezone())
            .unwrap()
    }

    fn bucket_day<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<T> {
        dt.date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(dt.timezone())
            .unwrap()
    }

    fn bucket_week<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<T> {
        let days_since_week_start = (dt.weekday().num_days_from_monday()
            + (7 - self.config.week_start.num_days_from_monday()))
            % 7;

        let week_start = dt.date_naive() - chrono::Duration::days(days_since_week_start as i64);
        week_start
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(dt.timezone())
            .unwrap()
    }

    fn bucket_month<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<T> {
        dt.date_naive()
            .with_day(1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(dt.timezone())
            .unwrap()
    }
}

/// Fallback to naive implementation for performance-critical paths
pub fn naive_bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    match gran {
        TimeGranularity::Hour => (ts / 3600) * 3600,
        TimeGranularity::Day => (ts / 86_400) * 86_400,
        TimeGranularity::Week => (ts / 604_800) * 604_800,
        TimeGranularity::Month => (ts / 2_592_000) * 2_592_000, // naive 30-day month bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Weekday;

    #[test]
    fn test_calendar_month_bucketing() {
        let config = TimeBucketingConfig::default();
        let bucketer = CalendarTimeBucketer::new(config);

        // Test February 15, 2024 (leap year)
        let feb_15_2024 = 1708012800; // 2024-02-15 00:00:00 UTC
        let bucket = bucketer.bucket_of(feb_15_2024, &TimeGranularity::Month);

        // Should bucket to February 1, 2024
        let expected = 1706745600; // 2024-02-01 00:00:00 UTC
        assert_eq!(bucket, expected);
    }

    #[test]
    fn test_calendar_week_bucketing() {
        let config = TimeBucketingConfig {
            timezone: None,
            week_start: Weekday::Mon,
            use_calendar_bucketing: true,
        };
        let bucketer = CalendarTimeBucketer::new(config);

        // Test Wednesday, January 3, 2024
        let wed_jan_3 = 1704240000; // 2024-01-03 00:00:00 UTC
        let bucket = bucketer.bucket_of(wed_jan_3, &TimeGranularity::Week);

        // Should bucket to Monday, January 1, 2024 (week start)
        let expected = 1704067200; // 2024-01-01 00:00:00 UTC
        assert_eq!(bucket, expected);
    }

    #[test]
    fn test_timezone_aware_bucketing() {
        let config = TimeBucketingConfig {
            timezone: Some("US/Eastern".to_string()),
            week_start: Weekday::Mon,
            use_calendar_bucketing: true,
        };
        let bucketer = CalendarTimeBucketer::new(config);

        // Test a timestamp that crosses timezone boundaries
        let ts = 1704067200; // 2024-01-01 00:00:00 UTC
        let bucket = bucketer.bucket_of(ts, &TimeGranularity::Day);

        // Should be bucketed to start of day in Eastern timezone
        // This will be different from UTC day start
        assert!(bucket != naive_bucket_of(ts, &TimeGranularity::Day));
    }
}
