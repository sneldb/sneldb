use crate::command::types::TimeGranularity;
use crate::shared::datetime::time::TimeConfig;
use crate::shared::datetime::time_bucketing::{
    CalendarTimeBucketer, naive_bucket_of as naive_bucket,
};

pub fn bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    let cfg = TimeConfig::from_app_config();
    if cfg.use_calendar_bucketing {
        let bucketer = CalendarTimeBucketer::new(cfg);
        bucketer.bucket_of(ts, gran)
    } else {
        naive_bucket(ts, gran)
    }
}

// Keep naive implementation available for performance-critical paths
#[allow(dead_code)]
pub fn naive_bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    naive_bucket(ts, gran)
}
