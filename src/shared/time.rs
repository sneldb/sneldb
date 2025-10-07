use chrono::{DateTime, NaiveDate, TimeZone, Utc};

/// Logical kind of time field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeKind {
    /// Full datetime with timezone information; normalized to UTC seconds.
    DateTime,
    /// Calendar date without time; normalized to midnight UTC seconds.
    Date,
}

/// Utility for parsing and normalizing time inputs to epoch seconds (i64).
pub struct TimeParser;

impl TimeParser {
    /// Parse a string representing a time instant into epoch seconds (UTC).
    /// Supports RFC3339/ISO-8601 and date-only (YYYY-MM-DD).
    pub fn parse_str_to_epoch_seconds(input: &str, kind: TimeKind) -> Option<i64> {
        let s = input.trim();
        // Try RFC3339/ISO-8601 first
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            let utc = dt.with_timezone(&Utc);
            return Some(utc.timestamp());
        }
        // Try date-only (YYYY-MM-DD)
        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            let dt = match kind {
                TimeKind::Date => Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?),
                TimeKind::DateTime => Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?),
            };
            return Some(dt.timestamp());
        }
        // Fallback: numeric string
        if let Ok(num) = s.parse::<i128>() {
            return Self::normalize_integer_epoch(num);
        }
        None
    }

    /// Normalize a JSON value representing time to epoch seconds in-place.
    /// Returns Err with a human-readable message if the value cannot be parsed.
    pub fn normalize_json_value(
        value: &mut serde_json::Value,
        kind: TimeKind,
    ) -> Result<(), String> {
        match value {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    let norm = Self::normalize_integer_epoch(i as i128)
                        .ok_or_else(|| format!("Unrecognized integer time magnitude: {i}"))?;
                    *value = serde_json::Value::Number(norm.into());
                    Ok(())
                } else if let Some(u) = n.as_u64() {
                    let norm = Self::normalize_integer_epoch(u as i128)
                        .ok_or_else(|| format!("Unrecognized integer time magnitude: {u}"))?;
                    *value = serde_json::Value::Number(norm.into());
                    Ok(())
                } else if let Some(f) = n.as_f64() {
                    // Treat float as seconds.
                    let secs = f.floor() as i64;
                    *value = serde_json::Value::Number(secs.into());
                    Ok(())
                } else {
                    Err("Unsupported numeric time value".to_string())
                }
            }
            serde_json::Value::String(s) => {
                let parsed = Self::parse_str_to_epoch_seconds(s, kind)
                    .ok_or_else(|| format!("Invalid time string: '{s}'"))?;
                *value = serde_json::Value::Number(parsed.into());
                Ok(())
            }
            _ => Err("Time field must be a number or string".to_string()),
        }
    }

    /// Heuristic normalization of integer magnitudes to seconds.
    /// - 10..=11 digits: seconds
    /// - 12..=14: milliseconds
    /// - 15..=16: microseconds
    /// - 18..=19: nanoseconds
    fn normalize_integer_epoch(n: i128) -> Option<i64> {
        let abs = n.unsigned_abs();
        let digits = num_digits_u128(abs);
        let secs = match digits {
            0..=11 => n,                  // seconds (and small negatives)
            12..=14 => n / 1_000,         // ms -> s
            15..=16 => n / 1_000_000,     // µs -> s
            17..=19 => n / 1_000_000_000, // ns -> s
            _ => return None,
        };
        i64::try_from(secs).ok()
    }
}

fn num_digits_u128(mut x: u128) -> u32 {
    if x == 0 {
        return 1;
    }
    let mut c = 0;
    while x > 0 {
        x /= 10;
        c += 1;
    }
    c
}
