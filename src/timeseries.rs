//! Time-Series Functions
//!
//! This module provides specialized time-series analysis functions for temporal data.
//!
//! # Features
//!
//! - **DATE_TRUNC**: Truncate timestamps to specified precision
//! - **TIME_BUCKET**: Bucket timestamps into fixed intervals
//! - **DATE_DIFF**: Calculate difference between timestamps
//! - **DATE_ADD/DATE_SUB**: Add/subtract intervals from timestamps
//! - **EXTRACT**: Extract parts from timestamps
//! - **GAP_FILL**: Fill gaps in time series data
//! - **MOVING_AVG/MOVING_SUM**: Rolling window calculations
//!
//! # Example
//!
//! ```sql
//! SELECT
//!     DATE_TRUNC('hour', timestamp) as hour,
//!     COUNT(*) as events
//! FROM events
//! GROUP BY DATE_TRUNC('hour', timestamp)
//! ORDER BY hour
//! ```

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};

use crate::error::{BlazeError, Result};

/// Time unit for truncation and bucketing operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl TimeUnit {
    /// Parse a time unit from a string.
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "microsecond" | "microseconds" | "us" => Ok(TimeUnit::Microsecond),
            "millisecond" | "milliseconds" | "ms" => Ok(TimeUnit::Millisecond),
            "second" | "seconds" | "s" => Ok(TimeUnit::Second),
            "minute" | "minutes" | "min" => Ok(TimeUnit::Minute),
            "hour" | "hours" | "h" => Ok(TimeUnit::Hour),
            "day" | "days" | "d" => Ok(TimeUnit::Day),
            "week" | "weeks" | "w" => Ok(TimeUnit::Week),
            "month" | "months" | "mon" => Ok(TimeUnit::Month),
            "quarter" | "quarters" | "q" => Ok(TimeUnit::Quarter),
            "year" | "years" | "y" => Ok(TimeUnit::Year),
            _ => Err(BlazeError::invalid_argument(format!(
                "Unknown time unit: {}",
                s
            ))),
        }
    }

    /// Get the number of microseconds in this time unit.
    /// Returns None for variable-length units (month, quarter, year).
    pub fn as_microseconds(&self) -> Option<i64> {
        match self {
            TimeUnit::Microsecond => Some(1),
            TimeUnit::Millisecond => Some(1_000),
            TimeUnit::Second => Some(1_000_000),
            TimeUnit::Minute => Some(60_000_000),
            TimeUnit::Hour => Some(3_600_000_000),
            TimeUnit::Day => Some(86_400_000_000),
            TimeUnit::Week => Some(604_800_000_000),
            TimeUnit::Month | TimeUnit::Quarter | TimeUnit::Year => None,
        }
    }
}

/// Truncate a timestamp to the specified time unit.
///
/// # Arguments
/// * `timestamp_micros` - Timestamp in microseconds since epoch
/// * `unit` - Time unit to truncate to
///
/// # Returns
/// Truncated timestamp in microseconds since epoch
pub fn date_trunc(timestamp_micros: i64, unit: TimeUnit) -> Result<i64> {
    let dt = timestamp_to_datetime(timestamp_micros)?;

    let truncated = match unit {
        TimeUnit::Microsecond => dt,
        TimeUnit::Millisecond => {
            let millis = dt.timestamp_millis();
            Utc.timestamp_millis_opt(millis).single().ok_or_else(|| {
                BlazeError::execution("Failed to create timestamp")
            })?
        }
        TimeUnit::Second => {
            Utc.with_ymd_and_hms(
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
            )
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Minute => {
            Utc.with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Hour => {
            Utc.with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Day => {
            Utc.with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Week => {
            // Truncate to Monday of the week
            let weekday = dt.weekday().num_days_from_monday();
            let monday = dt - Duration::days(weekday as i64);
            Utc.with_ymd_and_hms(monday.year(), monday.month(), monday.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Month => {
            Utc.with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Quarter => {
            let quarter_month = ((dt.month() - 1) / 3) * 3 + 1;
            Utc.with_ymd_and_hms(dt.year(), quarter_month, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Year => {
            Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
    };

    Ok(truncated.timestamp_micros())
}

/// Bucket a timestamp into fixed intervals.
///
/// # Arguments
/// * `timestamp_micros` - Timestamp in microseconds since epoch
/// * `interval_micros` - Bucket interval in microseconds
/// * `origin_micros` - Optional origin point for bucketing (defaults to epoch)
///
/// # Returns
/// The bucket start timestamp in microseconds since epoch
pub fn time_bucket(
    timestamp_micros: i64,
    interval_micros: i64,
    origin_micros: Option<i64>,
) -> Result<i64> {
    if interval_micros <= 0 {
        return Err(BlazeError::invalid_argument(
            "Bucket interval must be positive",
        ));
    }

    let origin = origin_micros.unwrap_or(0);
    let offset = timestamp_micros - origin;
    let bucket_number = offset.div_euclid(interval_micros);
    Ok(origin + bucket_number * interval_micros)
}

/// Calculate the difference between two timestamps.
///
/// # Arguments
/// * `unit` - Time unit for the result
/// * `start_micros` - Start timestamp in microseconds
/// * `end_micros` - End timestamp in microseconds
///
/// # Returns
/// The difference in the specified time unit
pub fn date_diff(unit: TimeUnit, start_micros: i64, end_micros: i64) -> Result<i64> {
    let diff_micros = end_micros - start_micros;

    match unit {
        TimeUnit::Microsecond => Ok(diff_micros),
        TimeUnit::Millisecond => Ok(diff_micros / 1_000),
        TimeUnit::Second => Ok(diff_micros / 1_000_000),
        TimeUnit::Minute => Ok(diff_micros / 60_000_000),
        TimeUnit::Hour => Ok(diff_micros / 3_600_000_000),
        TimeUnit::Day => Ok(diff_micros / 86_400_000_000),
        TimeUnit::Week => Ok(diff_micros / 604_800_000_000),
        TimeUnit::Month => {
            let start_dt = timestamp_to_datetime(start_micros)?;
            let end_dt = timestamp_to_datetime(end_micros)?;
            let months = (end_dt.year() - start_dt.year()) * 12
                + (end_dt.month() as i32 - start_dt.month() as i32);
            Ok(months as i64)
        }
        TimeUnit::Quarter => {
            let months = date_diff(TimeUnit::Month, start_micros, end_micros)?;
            Ok(months / 3)
        }
        TimeUnit::Year => {
            let start_dt = timestamp_to_datetime(start_micros)?;
            let end_dt = timestamp_to_datetime(end_micros)?;
            Ok((end_dt.year() - start_dt.year()) as i64)
        }
    }
}

/// Add an interval to a timestamp.
///
/// # Arguments
/// * `timestamp_micros` - Timestamp in microseconds since epoch
/// * `amount` - Number of units to add (can be negative)
/// * `unit` - Time unit
///
/// # Returns
/// New timestamp in microseconds since epoch
pub fn date_add(timestamp_micros: i64, amount: i64, unit: TimeUnit) -> Result<i64> {
    let dt = timestamp_to_datetime(timestamp_micros)?;

    let new_dt = match unit {
        TimeUnit::Microsecond => dt + Duration::microseconds(amount),
        TimeUnit::Millisecond => dt + Duration::milliseconds(amount),
        TimeUnit::Second => dt + Duration::seconds(amount),
        TimeUnit::Minute => dt + Duration::minutes(amount),
        TimeUnit::Hour => dt + Duration::hours(amount),
        TimeUnit::Day => dt + Duration::days(amount),
        TimeUnit::Week => dt + Duration::weeks(amount),
        TimeUnit::Month => {
            let total_months = dt.year() * 12 + dt.month() as i32 - 1 + amount as i32;
            let new_year = total_months / 12;
            let new_month = (total_months % 12 + 1) as u32;
            // Handle day overflow (e.g., Jan 31 + 1 month)
            let max_day = days_in_month(new_year, new_month);
            let new_day = dt.day().min(max_day);
            Utc.with_ymd_and_hms(
                new_year,
                new_month,
                new_day,
                dt.hour(),
                dt.minute(),
                dt.second(),
            )
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Quarter => {
            // Quarter = 3 months
            return date_add(timestamp_micros, amount * 3, TimeUnit::Month);
        }
        TimeUnit::Year => {
            let new_year = dt.year() + amount as i32;
            let max_day = days_in_month(new_year, dt.month());
            let new_day = dt.day().min(max_day);
            Utc.with_ymd_and_hms(
                new_year,
                dt.month(),
                new_day,
                dt.hour(),
                dt.minute(),
                dt.second(),
            )
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
    };

    Ok(new_dt.timestamp_micros())
}

/// Subtract an interval from a timestamp.
pub fn date_sub(timestamp_micros: i64, amount: i64, unit: TimeUnit) -> Result<i64> {
    date_add(timestamp_micros, -amount, unit)
}

/// Extract a part from a timestamp.
///
/// # Arguments
/// * `part` - Part to extract (year, month, day, hour, etc.)
/// * `timestamp_micros` - Timestamp in microseconds since epoch
///
/// # Returns
/// The extracted part as an integer
pub fn extract_part(part: &str, timestamp_micros: i64) -> Result<i64> {
    let dt = timestamp_to_datetime(timestamp_micros)?;

    match part.to_lowercase().as_str() {
        "year" | "years" => Ok(dt.year() as i64),
        "month" | "months" => Ok(dt.month() as i64),
        "day" | "days" => Ok(dt.day() as i64),
        "hour" | "hours" => Ok(dt.hour() as i64),
        "minute" | "minutes" => Ok(dt.minute() as i64),
        "second" | "seconds" => Ok(dt.second() as i64),
        "millisecond" | "milliseconds" => Ok((dt.nanosecond() / 1_000_000) as i64),
        "microsecond" | "microseconds" => Ok((dt.nanosecond() / 1_000) as i64),
        "nanosecond" | "nanoseconds" => Ok(dt.nanosecond() as i64),
        "dow" | "dayofweek" => Ok(dt.weekday().num_days_from_sunday() as i64),
        "doy" | "dayofyear" => Ok(dt.ordinal() as i64),
        "week" | "weeks" => Ok(dt.iso_week().week() as i64),
        "quarter" => Ok(((dt.month() - 1) / 3 + 1) as i64),
        "epoch" => Ok(dt.timestamp()),
        _ => Err(BlazeError::invalid_argument(format!(
            "Unknown date part: {}",
            part
        ))),
    }
}

/// Calculate moving average over a window of values.
///
/// # Arguments
/// * `values` - Array of values
/// * `window_size` - Size of the moving window
///
/// # Returns
/// Array of moving averages (with NaN for initial positions with insufficient data)
pub fn moving_avg(values: &[f64], window_size: usize) -> Vec<Option<f64>> {
    if window_size == 0 {
        return vec![None; values.len()];
    }

    let mut result = Vec::with_capacity(values.len());
    let mut sum = 0.0;
    let mut count = 0usize;

    for (i, &value) in values.iter().enumerate() {
        sum += value;
        count += 1;

        if i >= window_size {
            sum -= values[i - window_size];
            count -= 1;
        }

        if count >= window_size {
            result.push(Some(sum / count as f64));
        } else {
            result.push(None);
        }
    }

    result
}

/// Calculate moving sum over a window of values.
///
/// # Arguments
/// * `values` - Array of values
/// * `window_size` - Size of the moving window
///
/// # Returns
/// Array of moving sums
pub fn moving_sum(values: &[f64], window_size: usize) -> Vec<Option<f64>> {
    if window_size == 0 {
        return vec![None; values.len()];
    }

    let mut result = Vec::with_capacity(values.len());
    let mut sum = 0.0;

    for (i, &value) in values.iter().enumerate() {
        sum += value;

        if i >= window_size {
            sum -= values[i - window_size];
        }

        if i + 1 >= window_size {
            result.push(Some(sum));
        } else {
            result.push(None);
        }
    }

    result
}

/// Calculate exponential moving average.
///
/// # Arguments
/// * `values` - Array of values
/// * `alpha` - Smoothing factor (0 < alpha <= 1)
///
/// # Returns
/// Array of exponential moving averages
pub fn exponential_moving_avg(values: &[f64], alpha: f64) -> Result<Vec<f64>> {
    if alpha <= 0.0 || alpha > 1.0 {
        return Err(BlazeError::invalid_argument(
            "Alpha must be between 0 (exclusive) and 1 (inclusive)",
        ));
    }

    let mut result = Vec::with_capacity(values.len());
    let mut ema = 0.0;

    for (i, &value) in values.iter().enumerate() {
        if i == 0 {
            ema = value;
        } else {
            ema = alpha * value + (1.0 - alpha) * ema;
        }
        result.push(ema);
    }

    Ok(result)
}

/// Generate a series of timestamps.
///
/// # Arguments
/// * `start_micros` - Start timestamp in microseconds
/// * `end_micros` - End timestamp in microseconds
/// * `interval_micros` - Step interval in microseconds
///
/// # Returns
/// Vector of timestamps from start to end (inclusive of start, exclusive of end)
pub fn generate_series(
    start_micros: i64,
    end_micros: i64,
    interval_micros: i64,
) -> Result<Vec<i64>> {
    if interval_micros <= 0 {
        return Err(BlazeError::invalid_argument(
            "Interval must be positive",
        ));
    }

    if start_micros >= end_micros {
        return Ok(vec![]);
    }

    let count = ((end_micros - start_micros) / interval_micros) as usize + 1;
    if count > 1_000_000 {
        return Err(BlazeError::resource_exhausted(
            "Generated series would be too large (> 1M elements)",
        ));
    }

    let mut result = Vec::with_capacity(count);
    let mut current = start_micros;

    while current < end_micros {
        result.push(current);
        current += interval_micros;
    }

    Ok(result)
}

/// Fill gaps in time series data using various strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapFillStrategy {
    /// Fill with NULL values
    Null,
    /// Fill with previous value (forward fill)
    Previous,
    /// Fill with next value (backward fill)
    Next,
    /// Linear interpolation
    Linear,
    /// Fill with a constant value
    Constant,
}

impl GapFillStrategy {
    /// Parse a gap fill strategy from a string.
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "null" | "none" => Ok(GapFillStrategy::Null),
            "previous" | "ffill" | "forward" => Ok(GapFillStrategy::Previous),
            "next" | "bfill" | "backward" => Ok(GapFillStrategy::Next),
            "linear" | "interpolate" => Ok(GapFillStrategy::Linear),
            "constant" | "value" => Ok(GapFillStrategy::Constant),
            _ => Err(BlazeError::invalid_argument(format!(
                "Unknown gap fill strategy: {}",
                s
            ))),
        }
    }
}

/// Fill gaps in time series data.
///
/// # Arguments
/// * `timestamps` - Sorted array of timestamps in microseconds
/// * `values` - Array of values corresponding to timestamps
/// * `interval_micros` - Expected interval between timestamps
/// * `strategy` - Gap filling strategy
/// * `constant` - Constant value for Constant strategy
///
/// # Returns
/// Tuple of (filled_timestamps, filled_values)
pub fn gap_fill(
    timestamps: &[i64],
    values: &[Option<f64>],
    interval_micros: i64,
    strategy: GapFillStrategy,
    constant: Option<f64>,
) -> Result<(Vec<i64>, Vec<Option<f64>>)> {
    if timestamps.len() != values.len() {
        return Err(BlazeError::invalid_argument(
            "Timestamps and values must have the same length",
        ));
    }

    if timestamps.is_empty() {
        return Ok((vec![], vec![]));
    }

    let mut result_ts = Vec::new();
    let mut result_vals = Vec::new();

    for (i, (&ts, &val)) in timestamps.iter().zip(values.iter()).enumerate() {
        // Add the current point
        result_ts.push(ts);
        result_vals.push(val);

        // Check for gap to next point
        if i + 1 < timestamps.len() {
            let next_ts = timestamps[i + 1];
            let expected_next = ts + interval_micros;

            // Fill gaps
            let mut fill_ts = expected_next;
            while fill_ts < next_ts {
                result_ts.push(fill_ts);

                let fill_val = match strategy {
                    GapFillStrategy::Null => None,
                    GapFillStrategy::Previous => val,
                    GapFillStrategy::Next => values[i + 1],
                    GapFillStrategy::Linear => {
                        // Linear interpolation
                        match (val, values[i + 1]) {
                            (Some(v1), Some(v2)) => {
                                let fraction = (fill_ts - ts) as f64 / (next_ts - ts) as f64;
                                Some(v1 + (v2 - v1) * fraction)
                            }
                            _ => None,
                        }
                    }
                    GapFillStrategy::Constant => constant,
                };

                result_vals.push(fill_val);
                fill_ts += interval_micros;
            }
        }
    }

    Ok((result_ts, result_vals))
}

/// Resample time series data to a different frequency.
///
/// # Arguments
/// * `timestamps` - Array of timestamps in microseconds
/// * `values` - Array of values
/// * `new_interval_micros` - New interval for resampling
/// * `aggregation` - Aggregation function ("sum", "avg", "min", "max", "first", "last", "count")
///
/// # Returns
/// Tuple of (resampled_timestamps, resampled_values)
pub fn resample(
    timestamps: &[i64],
    values: &[f64],
    new_interval_micros: i64,
    aggregation: &str,
) -> Result<(Vec<i64>, Vec<f64>)> {
    if timestamps.len() != values.len() {
        return Err(BlazeError::invalid_argument(
            "Timestamps and values must have the same length",
        ));
    }

    if timestamps.is_empty() {
        return Ok((vec![], vec![]));
    }

    // Group values by bucket
    let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> = std::collections::BTreeMap::new();

    for (&ts, &val) in timestamps.iter().zip(values.iter()) {
        let bucket = time_bucket(ts, new_interval_micros, None)?;
        buckets.entry(bucket).or_default().push(val);
    }

    // Aggregate each bucket
    let mut result_ts = Vec::with_capacity(buckets.len());
    let mut result_vals = Vec::with_capacity(buckets.len());

    for (bucket, vals) in buckets {
        result_ts.push(bucket);

        let agg_val = match aggregation.to_lowercase().as_str() {
            "sum" => vals.iter().sum(),
            "avg" | "mean" => vals.iter().sum::<f64>() / vals.len() as f64,
            "min" => vals.iter().cloned().fold(f64::INFINITY, f64::min),
            "max" => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            "first" => vals.first().copied().unwrap_or(f64::NAN),
            "last" => vals.last().copied().unwrap_or(f64::NAN),
            "count" => vals.len() as f64,
            _ => {
                return Err(BlazeError::invalid_argument(format!(
                    "Unknown aggregation: {}",
                    aggregation
                )))
            }
        };

        result_vals.push(agg_val);
    }

    Ok((result_ts, result_vals))
}

// Helper functions

fn timestamp_to_datetime(micros: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_micros(micros)
        .single()
        .ok_or_else(|| BlazeError::execution("Invalid timestamp"))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
            .timestamp_micros()
    }

    #[test]
    fn test_time_unit_from_str() {
        assert_eq!(TimeUnit::from_str("hour").unwrap(), TimeUnit::Hour);
        assert_eq!(TimeUnit::from_str("HOUR").unwrap(), TimeUnit::Hour);
        assert_eq!(TimeUnit::from_str("hours").unwrap(), TimeUnit::Hour);
        assert_eq!(TimeUnit::from_str("day").unwrap(), TimeUnit::Day);
        assert_eq!(TimeUnit::from_str("week").unwrap(), TimeUnit::Week);
        assert_eq!(TimeUnit::from_str("month").unwrap(), TimeUnit::Month);
        assert!(TimeUnit::from_str("invalid").is_err());
    }

    #[test]
    fn test_date_trunc_hour() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);
        let truncated = date_trunc(ts, TimeUnit::Hour).unwrap();
        let expected = make_timestamp(2024, 6, 15, 14, 0, 0);
        assert_eq!(truncated, expected);
    }

    #[test]
    fn test_date_trunc_day() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);
        let truncated = date_trunc(ts, TimeUnit::Day).unwrap();
        let expected = make_timestamp(2024, 6, 15, 0, 0, 0);
        assert_eq!(truncated, expected);
    }

    #[test]
    fn test_date_trunc_month() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);
        let truncated = date_trunc(ts, TimeUnit::Month).unwrap();
        let expected = make_timestamp(2024, 6, 1, 0, 0, 0);
        assert_eq!(truncated, expected);
    }

    #[test]
    fn test_date_trunc_quarter() {
        let ts = make_timestamp(2024, 8, 15, 14, 30, 45);
        let truncated = date_trunc(ts, TimeUnit::Quarter).unwrap();
        let expected = make_timestamp(2024, 7, 1, 0, 0, 0);
        assert_eq!(truncated, expected);
    }

    #[test]
    fn test_date_trunc_year() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);
        let truncated = date_trunc(ts, TimeUnit::Year).unwrap();
        let expected = make_timestamp(2024, 1, 1, 0, 0, 0);
        assert_eq!(truncated, expected);
    }

    #[test]
    fn test_time_bucket() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);
        let hour_micros = 3_600_000_000i64;

        let bucket = time_bucket(ts, hour_micros, None).unwrap();
        let expected = make_timestamp(2024, 6, 15, 14, 0, 0);
        assert_eq!(bucket, expected);
    }

    #[test]
    fn test_date_diff_days() {
        let start = make_timestamp(2024, 6, 10, 0, 0, 0);
        let end = make_timestamp(2024, 6, 15, 0, 0, 0);

        let diff = date_diff(TimeUnit::Day, start, end).unwrap();
        assert_eq!(diff, 5);
    }

    #[test]
    fn test_date_diff_months() {
        let start = make_timestamp(2024, 1, 15, 0, 0, 0);
        let end = make_timestamp(2024, 6, 15, 0, 0, 0);

        let diff = date_diff(TimeUnit::Month, start, end).unwrap();
        assert_eq!(diff, 5);
    }

    #[test]
    fn test_date_add_days() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 0);
        let result = date_add(ts, 10, TimeUnit::Day).unwrap();
        let expected = make_timestamp(2024, 6, 25, 14, 30, 0);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_date_add_months() {
        let ts = make_timestamp(2024, 1, 31, 14, 30, 0);
        let result = date_add(ts, 1, TimeUnit::Month).unwrap();
        // Feb doesn't have 31 days, so it should be Feb 29 (leap year)
        let expected = make_timestamp(2024, 2, 29, 14, 30, 0);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_extract_part() {
        let ts = make_timestamp(2024, 6, 15, 14, 30, 45);

        assert_eq!(extract_part("year", ts).unwrap(), 2024);
        assert_eq!(extract_part("month", ts).unwrap(), 6);
        assert_eq!(extract_part("day", ts).unwrap(), 15);
        assert_eq!(extract_part("hour", ts).unwrap(), 14);
        assert_eq!(extract_part("minute", ts).unwrap(), 30);
        assert_eq!(extract_part("second", ts).unwrap(), 45);
        assert_eq!(extract_part("quarter", ts).unwrap(), 2);
    }

    #[test]
    fn test_moving_avg() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = moving_avg(&values, 3);

        assert_eq!(result[0], None);
        assert_eq!(result[1], None);
        assert_eq!(result[2], Some(2.0)); // (1+2+3)/3
        assert_eq!(result[3], Some(3.0)); // (2+3+4)/3
        assert_eq!(result[4], Some(4.0)); // (3+4+5)/3
    }

    #[test]
    fn test_exponential_moving_avg() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = exponential_moving_avg(&values, 0.5).unwrap();

        assert_eq!(result[0], 1.0);
        assert!((result[1] - 1.5).abs() < 0.0001); // 0.5*2 + 0.5*1
        assert!((result[2] - 2.25).abs() < 0.0001); // 0.5*3 + 0.5*1.5
    }

    #[test]
    fn test_generate_series() {
        let start = make_timestamp(2024, 6, 1, 0, 0, 0);
        let end = make_timestamp(2024, 6, 1, 3, 0, 0);
        let interval = 3_600_000_000i64; // 1 hour

        let result = generate_series(start, end, interval).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], start);
        assert_eq!(result[1], start + interval);
        assert_eq!(result[2], start + interval * 2);
    }

    #[test]
    fn test_gap_fill_previous() {
        let timestamps = vec![0, 2, 4];
        let values = vec![Some(1.0), Some(2.0), Some(3.0)];

        let (ts, vals) = gap_fill(&timestamps, &values, 1, GapFillStrategy::Previous, None).unwrap();

        assert_eq!(ts, vec![0, 1, 2, 3, 4]);
        assert_eq!(vals, vec![Some(1.0), Some(1.0), Some(2.0), Some(2.0), Some(3.0)]);
    }

    #[test]
    fn test_gap_fill_linear() {
        let timestamps = vec![0, 4];
        let values = vec![Some(0.0), Some(4.0)];

        let (ts, vals) = gap_fill(&timestamps, &values, 1, GapFillStrategy::Linear, None).unwrap();

        assert_eq!(ts, vec![0, 1, 2, 3, 4]);
        assert_eq!(vals[0], Some(0.0));
        assert_eq!(vals[1], Some(1.0));
        assert_eq!(vals[2], Some(2.0));
        assert_eq!(vals[3], Some(3.0));
        assert_eq!(vals[4], Some(4.0));
    }

    #[test]
    fn test_resample() {
        let timestamps = vec![0, 1, 2, 3, 4, 5];
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];

        let (ts, vals) = resample(&timestamps, &values, 2, "sum").unwrap();

        assert_eq!(ts, vec![0, 2, 4]);
        assert_eq!(vals, vec![3.0, 7.0, 11.0]); // 1+2, 3+4, 5+6
    }

    #[test]
    fn test_resample_avg() {
        let timestamps = vec![0, 1, 2, 3, 4, 5];
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];

        let (ts, vals) = resample(&timestamps, &values, 2, "avg").unwrap();

        assert_eq!(ts, vec![0, 2, 4]);
        assert_eq!(vals, vec![1.5, 3.5, 5.5]); // (1+2)/2, (3+4)/2, (5+6)/2
    }
}
