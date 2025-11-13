use crate::command::types::TimeGranularity;
use crate::shared::datetime::time::TimeConfig;
use crate::shared::datetime::time_bucketing::{
    CalendarTimeBucketer, naive_bucket_of as naive_bucket,
};
use std::sync::OnceLock;

// Cache the CalendarTimeBucketer instance to avoid recreating it on every call
// This is safe because TimeConfig is typically set at startup and doesn't change
static CALENDAR_BUCKETER_CACHE: OnceLock<(TimeConfig, CalendarTimeBucketer)> = OnceLock::new();

pub fn bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    // Cache the config check as well to avoid repeated calls
    static USE_CALENDAR: OnceLock<bool> = OnceLock::new();
    let use_calendar =
        *USE_CALENDAR.get_or_init(|| TimeConfig::from_app_config().use_calendar_bucketing);

    if use_calendar {
        // Get or create cached bucketer
        // Note: OnceLock doesn't support updates, so if config changes at runtime,
        // we'll use the cached one (config typically doesn't change during runtime)
        let bucketer = CALENDAR_BUCKETER_CACHE.get_or_init(|| {
            let cfg = TimeConfig::from_app_config();
            let bucketer = CalendarTimeBucketer::new(cfg.clone());
            (cfg, bucketer)
        });

        // Use cached bucketer
        bucketer.1.bucket_of(ts, gran)
    } else {
        naive_bucket(ts, gran)
    }
}
