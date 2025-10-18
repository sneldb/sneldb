use chrono::{Datelike, TimeZone, Utc, Weekday};

/// Factory for creating test timestamps with known dates
pub struct TimestampFactory;

impl TimestampFactory {
    /// Create timestamp for a specific UTC date/time
    pub fn utc_datetime(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> u64 {
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, min, sec)
            .single()
            .unwrap();
        dt.timestamp() as u64
    }

    /// Create timestamp for start of day
    pub fn utc_date(year: i32, month: u32, day: u32) -> u64 {
        Self::utc_datetime(year, month, day, 0, 0, 0)
    }

    /// Create timestamp for specific weekday
    pub fn utc_weekday(year: i32, month: u32, day: u32, weekday: Weekday) -> u64 {
        let dt = Utc
            .with_ymd_and_hms(year, month, day, 12, 0, 0)
            .single()
            .unwrap();
        assert_eq!(dt.weekday(), weekday);
        dt.timestamp() as u64
    }

    /// Create timestamp for Unix epoch (1970-01-01 00:00:00 UTC)
    pub fn epoch() -> u64 {
        0
    }

    /// Create timestamp for a specific year
    pub fn year(year: i32) -> u64 {
        Self::utc_date(year, 1, 1)
    }

    /// Create timestamp for a specific month
    pub fn month(year: i32, month: u32) -> u64 {
        Self::utc_date(year, month, 1)
    }

    /// Create timestamp for a specific hour of a day
    pub fn hour(year: i32, month: u32, day: u32, hour: u32) -> u64 {
        Self::utc_datetime(year, month, day, hour, 0, 0)
    }

    /// Create timestamp for a specific minute of an hour
    pub fn minute(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
        Self::utc_datetime(year, month, day, hour, minute, 0)
    }

    /// Create timestamp for a specific second
    pub fn second(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
        Self::utc_datetime(year, month, day, hour, minute, second)
    }

    /// Create timestamp for a specific day of the year
    pub fn day_of_year(year: i32, day_of_year: u32) -> u64 {
        let dt = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().unwrap()
            + chrono::Duration::days((day_of_year - 1) as i64);
        dt.timestamp() as u64
    }

    /// Create timestamp for a specific week of the year
    pub fn week_of_year(year: i32, week: u32) -> u64 {
        let jan_1 = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().unwrap();
        let days_to_add = (week - 1) * 7;
        let dt = jan_1 + chrono::Duration::days(days_to_add as i64);
        dt.timestamp() as u64
    }

    /// Create timestamp for a specific quarter
    pub fn quarter(year: i32, quarter: u32) -> u64 {
        let month = (quarter - 1) * 3 + 1;
        Self::utc_date(year, month, 1)
    }

    /// Create timestamp for a specific decade
    pub fn decade(year: i32) -> u64 {
        let decade_year = (year / 10) * 10;
        Self::utc_date(decade_year, 1, 1)
    }

    /// Create timestamp for a specific century
    pub fn century(year: i32) -> u64 {
        let century_year = (year / 100) * 100;
        Self::utc_date(century_year, 1, 1)
    }

    /// Create timestamp for a specific millennium
    pub fn millennium(year: i32) -> u64 {
        let millennium_year = (year / 1000) * 1000;
        Self::utc_date(millennium_year, 1, 1)
    }

    /// Create timestamp for a specific timezone offset
    pub fn with_timezone_offset(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        offset_hours: i32,
    ) -> u64 {
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, min, sec)
            .single()
            .unwrap();
        let offset_dt = dt + chrono::Duration::hours(offset_hours as i64);
        offset_dt.timestamp() as u64
    }

    /// Create timestamp for a specific duration from epoch
    pub fn from_epoch(seconds: i64) -> u64 {
        (seconds as u64).max(0)
    }

    /// Create timestamp for a specific duration from a base timestamp
    pub fn from_timestamp(base_ts: u64, offset_seconds: i64) -> u64 {
        ((base_ts as i64) + offset_seconds).max(0) as u64
    }

    /// Create timestamp for a specific duration from a base timestamp (days)
    pub fn from_timestamp_days(base_ts: u64, offset_days: i64) -> u64 {
        Self::from_timestamp(base_ts, offset_days * 86400)
    }

    /// Create timestamp for a specific duration from a base timestamp (hours)
    pub fn from_timestamp_hours(base_ts: u64, offset_hours: i64) -> u64 {
        Self::from_timestamp(base_ts, offset_hours * 3600)
    }

    /// Create timestamp for a specific duration from a base timestamp (minutes)
    pub fn from_timestamp_minutes(base_ts: u64, offset_minutes: i64) -> u64 {
        Self::from_timestamp(base_ts, offset_minutes * 60)
    }

    /// Create timestamp for a specific duration from a base timestamp (weeks)
    pub fn from_timestamp_weeks(base_ts: u64, offset_weeks: i64) -> u64 {
        Self::from_timestamp_days(base_ts, offset_weeks * 7)
    }

    /// Create timestamp for a specific duration from a base timestamp (months)
    pub fn from_timestamp_months(base_ts: u64, offset_months: i64) -> u64 {
        // This is approximate - months have different lengths
        Self::from_timestamp_days(base_ts, offset_months * 30)
    }

    /// Create timestamp for a specific duration from a base timestamp (years)
    pub fn from_timestamp_years(base_ts: u64, offset_years: i64) -> u64 {
        // This is approximate - years have different lengths
        Self::from_timestamp_days(base_ts, offset_years * 365)
    }

    /// Create timestamp for a specific duration from a base timestamp (leap years)
    pub fn from_timestamp_leap_years(base_ts: u64, offset_years: i64) -> u64 {
        // This is approximate - leap years have 366 days
        Self::from_timestamp_days(base_ts, offset_years * 366)
    }

    /// Create timestamp for a specific duration from a base timestamp (quarters)
    pub fn from_timestamp_quarters(base_ts: u64, offset_quarters: i64) -> u64 {
        Self::from_timestamp_months(base_ts, offset_quarters * 3)
    }

    /// Create timestamp for a specific duration from a base timestamp (decades)
    pub fn from_timestamp_decades(base_ts: u64, offset_decades: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_decades * 10)
    }

    /// Create timestamp for a specific duration from a base timestamp (centuries)
    pub fn from_timestamp_centuries(base_ts: u64, offset_centuries: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_centuries * 100)
    }

    /// Create timestamp for a specific duration from a base timestamp (millennia)
    pub fn from_timestamp_millennia(base_ts: u64, offset_millennia: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_millennia * 1000)
    }

    /// Create timestamp for a specific duration from a base timestamp (fortnights)
    pub fn from_timestamp_fortnights(base_ts: u64, offset_fortnights: i64) -> u64 {
        Self::from_timestamp_weeks(base_ts, offset_fortnights * 2)
    }

    /// Create timestamp for a specific duration from a base timestamp (bienniums)
    pub fn from_timestamp_bienniums(base_ts: u64, offset_bienniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_bienniums * 2)
    }

    /// Create timestamp for a specific duration from a base timestamp (trienniums)
    pub fn from_timestamp_trienniums(base_ts: u64, offset_trienniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_trienniums * 3)
    }

    /// Create timestamp for a specific duration from a base timestamp (quadrenniums)
    pub fn from_timestamp_quadrenniums(base_ts: u64, offset_quadrenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_quadrenniums * 4)
    }

    /// Create timestamp for a specific duration from a base timestamp (quinquenniums)
    pub fn from_timestamp_quinquenniums(base_ts: u64, offset_quinquenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_quinquenniums * 5)
    }

    /// Create timestamp for a specific duration from a base timestamp (sexenniums)
    pub fn from_timestamp_sexenniums(base_ts: u64, offset_sexenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_sexenniums * 6)
    }

    /// Create timestamp for a specific duration from a base timestamp (septenniums)
    pub fn from_timestamp_septenniums(base_ts: u64, offset_septenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_septenniums * 7)
    }

    /// Create timestamp for a specific duration from a base timestamp (octenniums)
    pub fn from_timestamp_octenniums(base_ts: u64, offset_octenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_octenniums * 8)
    }

    /// Create timestamp for a specific duration from a base timestamp (novenniums)
    pub fn from_timestamp_novenniums(base_ts: u64, offset_novenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_novenniums * 9)
    }

    /// Create timestamp for a specific duration from a base timestamp (decenniums)
    pub fn from_timestamp_decenniums(base_ts: u64, offset_decenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_decenniums * 10)
    }

    /// Create timestamp for a specific duration from a base timestamp (vicenniums)
    pub fn from_timestamp_vicenniums(base_ts: u64, offset_vicenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_vicenniums * 20)
    }

    /// Create timestamp for a specific duration from a base timestamp (tricenniums)
    pub fn from_timestamp_tricenniums(base_ts: u64, offset_tricenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_tricenniums * 30)
    }

    /// Create timestamp for a specific duration from a base timestamp (quadricenniums)
    pub fn from_timestamp_quadricenniums(base_ts: u64, offset_quadricenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_quadricenniums * 40)
    }

    /// Create timestamp for a specific duration from a base timestamp (quinquagenniums)
    pub fn from_timestamp_quinquagenniums(base_ts: u64, offset_quinquagenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_quinquagenniums * 50)
    }

    /// Create timestamp for a specific duration from a base timestamp (sexagenniums)
    pub fn from_timestamp_sexagenniums(base_ts: u64, offset_sexagenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_sexagenniums * 60)
    }

    /// Create timestamp for a specific duration from a base timestamp (septuagenniums)
    pub fn from_timestamp_septuagenniums(base_ts: u64, offset_septuagenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_septuagenniums * 70)
    }

    /// Create timestamp for a specific duration from a base timestamp (octogenniums)
    pub fn from_timestamp_octogenniums(base_ts: u64, offset_octogenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_octogenniums * 80)
    }

    /// Create timestamp for a specific duration from a base timestamp (nonagenniums)
    pub fn from_timestamp_nonagenniums(base_ts: u64, offset_nonagenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_nonagenniums * 90)
    }

    /// Create timestamp for a specific duration from a base timestamp (centenniums)
    pub fn from_timestamp_centenniums(base_ts: u64, offset_centenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_centenniums * 100)
    }

    /// Create timestamp for a specific duration from a base timestamp (millenniums)
    pub fn from_timestamp_millenniums(base_ts: u64, offset_millenniums: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_millenniums * 1000)
    }

    /// Create timestamp for a specific duration from a base timestamp (myriads)
    pub fn from_timestamp_myriads(base_ts: u64, offset_myriads: i64) -> u64 {
        Self::from_timestamp_years(base_ts, offset_myriads * 10000)
    }
}
