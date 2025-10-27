use crate::command::types::TimeGranularity;
use crate::shared::datetime::time_bucketing::{
    CalendarTimeBucketer, naive_bucket_of as naive_bucket,
};
use crate::shared::datetime::time_bucketing_config::TimeBucketingConfig;

// Global configuration for time bucketing
lazy_static::lazy_static! {
    static ref TIME_BUCKETING_CONFIG: TimeBucketingConfig = TimeBucketingConfig::default();
    static ref TIME_BUCKETER: CalendarTimeBucketer = CalendarTimeBucketer::new(TIME_BUCKETING_CONFIG.clone());
}

pub fn bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    if TIME_BUCKETING_CONFIG.use_calendar_bucketing {
        // Use calendar-aware bucketing for accurate results
        TIME_BUCKETER.bucket_of(ts, gran)
    } else {
        // Fall back to naive bucketing for performance
        naive_bucket(ts, gran)
    }
}

// Keep naive implementation available for performance-critical paths
#[allow(dead_code)]
pub fn naive_bucket_of(ts: u64, gran: &TimeGranularity) -> u64 { naive_bucket(ts, gran) }
