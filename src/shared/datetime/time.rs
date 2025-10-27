use crate::shared::config::CONFIG;
use chrono::Weekday;
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// Global time configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeConfig {
    /// Timezone for calendar-based calculations (None = UTC)
    pub timezone: Option<String>,
    /// Week start day (Monday = 1, Sunday = 0)
    pub week_start: Weekday,
    /// Use calendar-aware bucketing (true) or naive bucketing (false)
    pub use_calendar_bucketing: bool,
}

impl Default for TimeConfig {
    fn default() -> Self {
        Self {
            timezone: None,               // UTC by default
            week_start: Weekday::Mon,     // Monday start by default
            use_calendar_bucketing: true, // Use calendar-aware by default
        }
    }
}

impl TimeConfig {
    /// Parse timezone string to chrono_tz::Tz
    pub fn parse_timezone(&self) -> Option<Tz> {
        self.timezone
            .as_ref()
            .and_then(|tz_str| tz_str.parse().ok())
    }

    /// Create from application configuration
    pub fn from_app_config() -> Self {
        CONFIG.time.clone().unwrap_or_else(|| Self::default())
    }
}
