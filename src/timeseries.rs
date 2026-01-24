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

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

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
            Utc.timestamp_millis_opt(millis)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Second => Utc
            .with_ymd_and_hms(
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
            )
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
        TimeUnit::Minute => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), 0)
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
        TimeUnit::Hour => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), 0, 0)
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
        TimeUnit::Day => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
        TimeUnit::Week => {
            // Truncate to Monday of the week
            let weekday = dt.weekday().num_days_from_monday();
            let monday = dt - Duration::days(weekday as i64);
            Utc.with_ymd_and_hms(monday.year(), monday.month(), monday.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Month => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
        TimeUnit::Quarter => {
            let quarter_month = ((dt.month() - 1) / 3) * 3 + 1;
            Utc.with_ymd_and_hms(dt.year(), quarter_month, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?
        }
        TimeUnit::Year => Utc
            .with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0)
            .single()
            .ok_or_else(|| BlazeError::execution("Failed to create timestamp"))?,
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
        return Err(BlazeError::invalid_argument("Interval must be positive"));
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

// ---------------------------------------------------------------------------
// ASOF JOIN
// ---------------------------------------------------------------------------

/// Direction for ASOF JOIN matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofDirection {
    /// Match the nearest prior timestamp (right <= left).
    Backward,
    /// Match the nearest future timestamp (right >= left).
    Forward,
    /// Match the closest timestamp in either direction.
    Nearest,
}

/// Configuration for an ASOF JOIN.
#[derive(Debug, Clone)]
pub struct AsofJoinConfig {
    /// Maximum allowed time difference in microseconds. `None` means unlimited.
    pub tolerance: Option<i64>,
    /// Direction of the temporal match.
    pub direction: AsofDirection,
}

/// Perform an ASOF JOIN between left and right time series.
///
/// Both `left_timestamps` and `right_timestamps` **must** be sorted in ascending
/// order. For each left row the function finds the best-matching right row
/// according to `config.direction` and `config.tolerance`.
pub fn asof_join(
    left_timestamps: &[i64],
    left_values: &[f64],
    right_timestamps: &[i64],
    right_values: &[f64],
    config: &AsofJoinConfig,
) -> Result<Vec<(i64, f64, Option<f64>)>> {
    if left_timestamps.len() != left_values.len() {
        return Err(BlazeError::execution(
            "left_timestamps and left_values must have the same length",
        ));
    }
    if right_timestamps.len() != right_values.len() {
        return Err(BlazeError::execution(
            "right_timestamps and right_values must have the same length",
        ));
    }

    let mut result = Vec::with_capacity(left_timestamps.len());
    let mut right_idx: usize = 0;

    for i in 0..left_timestamps.len() {
        let lt = left_timestamps[i];

        // Advance right_idx so that right_timestamps[right_idx] is the first
        // value >= lt (or past the end).
        while right_idx < right_timestamps.len() && right_timestamps[right_idx] < lt {
            right_idx += 1;
        }

        let matched = match config.direction {
            AsofDirection::Backward => {
                // Best candidate is the largest right ts <= lt.
                let candidate_idx =
                    if right_idx < right_timestamps.len() && right_timestamps[right_idx] == lt {
                        Some(right_idx)
                    } else if right_idx > 0 {
                        Some(right_idx - 1)
                    } else {
                        None
                    };
                candidate_idx.and_then(|ci| {
                    let diff = (lt - right_timestamps[ci]).abs();
                    if config.tolerance.map_or(true, |tol| diff <= tol) {
                        Some(right_values[ci])
                    } else {
                        None
                    }
                })
            }
            AsofDirection::Forward => {
                // Best candidate is the smallest right ts >= lt.
                let candidate_idx = if right_idx < right_timestamps.len() {
                    Some(right_idx)
                } else {
                    None
                };
                candidate_idx.and_then(|ci| {
                    let diff = (right_timestamps[ci] - lt).abs();
                    if config.tolerance.map_or(true, |tol| diff <= tol) {
                        Some(right_values[ci])
                    } else {
                        None
                    }
                })
            }
            AsofDirection::Nearest => {
                let before = if right_idx > 0 {
                    Some(right_idx - 1)
                } else {
                    None
                };
                let after = if right_idx < right_timestamps.len() {
                    Some(right_idx)
                } else {
                    None
                };
                let best = match (before, after) {
                    (Some(b), Some(a)) => {
                        let db = (lt - right_timestamps[b]).abs();
                        let da = (right_timestamps[a] - lt).abs();
                        if db <= da {
                            Some((b, db))
                        } else {
                            Some((a, da))
                        }
                    }
                    (Some(b), None) => Some((b, (lt - right_timestamps[b]).abs())),
                    (None, Some(a)) => Some((a, (right_timestamps[a] - lt).abs())),
                    (None, None) => None,
                };
                best.and_then(|(ci, diff)| {
                    if config.tolerance.map_or(true, |tol| diff <= tol) {
                        Some(right_values[ci])
                    } else {
                        None
                    }
                })
            }
        };

        result.push((lt, left_values[i], matched));
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Retention Policy
// ---------------------------------------------------------------------------

/// Action to take when data exceeds the retention policy.
#[derive(Debug, Clone, PartialEq)]
pub enum RetentionAction {
    /// Delete expired data.
    Delete,
    /// Archive expired data to the given destination.
    Archive { destination: String },
    /// Downsample expired data into the given interval (microseconds).
    Downsample { target_interval: i64 },
}

/// Retention policy for time-partitioned data.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age of data to keep.
    pub max_age: Duration,
    /// Column name that contains the timestamp.
    pub check_column: String,
    /// Action to take on expired data.
    pub action: RetentionAction,
}

impl RetentionPolicy {
    /// Create a simple delete-after retention policy.
    pub fn delete_after(max_age: Duration, column: impl Into<String>) -> Self {
        Self {
            max_age,
            check_column: column.into(),
            action: RetentionAction::Delete,
        }
    }

    /// Check whether a row with the given timestamp should be retained.
    pub fn should_retain(&self, timestamp_micros: i64, now_micros: i64) -> bool {
        let age_micros = now_micros - timestamp_micros;
        let max_age_micros = self.max_age.num_microseconds().unwrap_or(i64::MAX);
        age_micros <= max_age_micros
    }

    /// Partition a slice of timestamps into retained and expired index sets.
    ///
    /// Returns `(retained_indices, expired_indices)`.
    pub fn partition_data(&self, timestamps: &[i64], now_micros: i64) -> (Vec<usize>, Vec<usize>) {
        let mut retained = Vec::new();
        let mut expired = Vec::new();
        for (i, &ts) in timestamps.iter().enumerate() {
            if self.should_retain(ts, now_micros) {
                retained.push(i);
            } else {
                expired.push(i);
            }
        }
        (retained, expired)
    }
}

// ---------------------------------------------------------------------------
// LTTB Downsampling
// ---------------------------------------------------------------------------

/// LTTB (Largest Triangle Three Buckets) downsampling for visualization.
///
/// Reduces a time series to at most `target_points` while preserving the
/// visual shape of the data.
pub fn lttb_downsample(
    timestamps: &[i64],
    values: &[f64],
    target_points: usize,
) -> Result<(Vec<i64>, Vec<f64>)> {
    let n = timestamps.len();
    if n != values.len() {
        return Err(BlazeError::execution(
            "timestamps and values must have the same length",
        ));
    }
    if target_points < 2 {
        return Err(BlazeError::execution("target_points must be at least 2"));
    }
    if n <= target_points {
        return Ok((timestamps.to_vec(), values.to_vec()));
    }

    let mut out_ts = Vec::with_capacity(target_points);
    let mut out_val = Vec::with_capacity(target_points);

    // Always keep the first point.
    out_ts.push(timestamps[0]);
    out_val.push(values[0]);

    let bucket_size = (n - 2) as f64 / (target_points - 2) as f64;

    let mut prev_selected: usize = 0;

    for bucket_idx in 0..(target_points - 2) {
        let bucket_start = ((bucket_idx as f64 * bucket_size) as usize) + 1;
        let bucket_end = ((((bucket_idx + 1) as f64) * bucket_size) as usize) + 1;
        let bucket_end = bucket_end.min(n - 1);

        // Calculate the average point of the *next* bucket (used as the third
        // vertex of the triangle).
        let next_bucket_start = bucket_end;
        let next_bucket_end = if bucket_idx + 2 < target_points - 2 {
            ((((bucket_idx + 2) as f64) * bucket_size) as usize) + 1
        } else {
            n - 1
        };
        let next_bucket_end = next_bucket_end.min(n);

        let mut avg_ts: f64 = 0.0;
        let mut avg_val: f64 = 0.0;
        let next_len = (next_bucket_end - next_bucket_start).max(1);
        for j in next_bucket_start..next_bucket_end {
            avg_ts += timestamps[j] as f64;
            avg_val += values[j];
        }
        avg_ts /= next_len as f64;
        avg_val /= next_len as f64;

        // Select the point in the current bucket that forms the largest
        // triangle with the previously selected point and the average of the
        // next bucket.
        let mut max_area: f64 = -1.0;
        let mut max_idx = bucket_start;

        let prev_ts = timestamps[prev_selected] as f64;
        let prev_val = values[prev_selected];

        for j in bucket_start..bucket_end {
            let area = ((prev_ts - avg_ts) * (values[j] - prev_val)
                - (prev_ts - timestamps[j] as f64) * (avg_val - prev_val))
                .abs()
                * 0.5;
            if area > max_area {
                max_area = area;
                max_idx = j;
            }
        }

        out_ts.push(timestamps[max_idx]);
        out_val.push(values[max_idx]);
        prev_selected = max_idx;
    }

    // Always keep the last point.
    out_ts.push(timestamps[n - 1]);
    out_val.push(values[n - 1]);

    Ok((out_ts, out_val))
}

// ---------------------------------------------------------------------------
// Rate / Delta Functions
// ---------------------------------------------------------------------------

/// Calculate the rate of change **per second** between consecutive points.
///
/// The first element is always `None` because there is no prior point.
pub fn rate(timestamps: &[i64], values: &[f64]) -> Result<Vec<Option<f64>>> {
    if timestamps.len() != values.len() {
        return Err(BlazeError::execution(
            "timestamps and values must have the same length",
        ));
    }
    let mut result = Vec::with_capacity(timestamps.len());
    result.push(None);
    for i in 1..timestamps.len() {
        let dt = (timestamps[i] - timestamps[i - 1]) as f64 / 1_000_000.0; // seconds
        if dt == 0.0 {
            result.push(None);
        } else {
            result.push(Some((values[i] - values[i - 1]) / dt));
        }
    }
    Ok(result)
}

/// Calculate the delta (difference) between consecutive points.
///
/// The first element is always `None`.
pub fn delta(timestamps: &[i64], values: &[f64]) -> Result<Vec<Option<f64>>> {
    if timestamps.len() != values.len() {
        return Err(BlazeError::execution(
            "timestamps and values must have the same length",
        ));
    }
    let mut result = Vec::with_capacity(timestamps.len());
    result.push(None);
    for i in 1..timestamps.len() {
        result.push(Some(values[i] - values[i - 1]));
    }
    Ok(result)
}

/// Calculate monotonic rate (handles counter resets).
///
/// When a value decreases (counter reset), the delta is treated as zero for
/// that interval, and the rate is reported as `None`.
pub fn monotonic_rate(timestamps: &[i64], values: &[f64]) -> Result<Vec<Option<f64>>> {
    if timestamps.len() != values.len() {
        return Err(BlazeError::execution(
            "timestamps and values must have the same length",
        ));
    }
    let mut result = Vec::with_capacity(timestamps.len());
    result.push(None);
    for i in 1..timestamps.len() {
        if values[i] < values[i - 1] {
            // Counter reset detected.
            result.push(None);
        } else {
            let dt = (timestamps[i] - timestamps[i - 1]) as f64 / 1_000_000.0;
            if dt == 0.0 {
                result.push(None);
            } else {
                result.push(Some((values[i] - values[i - 1]) / dt));
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Time Partition Config
// ---------------------------------------------------------------------------

/// Time-based partitioning configuration.
#[derive(Debug, Clone)]
pub struct TimePartitionConfig {
    /// Column name containing the timestamp.
    pub column: String,
    /// Interval granularity for each partition.
    pub interval: TimeUnit,
    /// Optional hint for the number of partitions.
    pub num_partitions_hint: Option<usize>,
}

impl TimePartitionConfig {
    /// Create a new time-partition configuration.
    pub fn new(column: impl Into<String>, interval: TimeUnit) -> Self {
        Self {
            column: column.into(),
            interval,
            num_partitions_hint: None,
        }
    }

    /// Compute the partition key for a given timestamp.
    ///
    /// The key is the truncated timestamp (start of the partition interval) in
    /// microseconds since epoch.
    pub fn partition_key(&self, timestamp_micros: i64) -> Result<i64> {
        date_trunc(timestamp_micros, self.interval)
    }

    /// Return the `[start, end)` range in microseconds for the given
    /// partition key.
    pub fn partition_range(&self, key: i64) -> Result<(i64, i64)> {
        let start = key;
        let end = date_add(key, 1, self.interval)?;
        Ok((start, end))
    }
}

// ---------------------------------------------------------------------------
// Time-Partitioned Segment Storage
// ---------------------------------------------------------------------------

/// A time-partitioned segment containing data for a specific time range.
#[derive(Debug, Clone)]
pub struct TimeSegment {
    /// Partition key (start timestamp in micros)
    pub partition_key: i64,
    /// Time range start (inclusive)
    pub range_start: i64,
    /// Time range end (exclusive)
    pub range_end: i64,
    /// Data batches in this segment
    pub batches: Vec<arrow::record_batch::RecordBatch>,
    /// Number of rows
    pub row_count: usize,
    /// Approximate size in bytes
    pub size_bytes: usize,
    /// Whether this segment has been compacted
    pub compacted: bool,
}

impl TimeSegment {
    pub fn new(range_start: i64, range_end: i64) -> Self {
        Self {
            partition_key: range_start,
            range_start,
            range_end,
            batches: Vec::new(),
            row_count: 0,
            size_bytes: 0,
            compacted: false,
        }
    }

    /// Add a batch to this segment.
    pub fn append(&mut self, batch: arrow::record_batch::RecordBatch) {
        self.row_count += batch.num_rows();
        self.size_bytes += batch.get_array_memory_size();
        self.batches.push(batch);
    }

    /// Check if a timestamp falls within this segment's range.
    pub fn contains(&self, timestamp_micros: i64) -> bool {
        timestamp_micros >= self.range_start && timestamp_micros < self.range_end
    }

    /// Check if this segment overlaps with a given time range.
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.range_start < end && self.range_end > start
    }
}

/// Configuration for time-series segment storage.
#[derive(Debug, Clone)]
pub struct SegmentStorageConfig {
    /// Time partitioning configuration
    pub partition_config: TimePartitionConfig,
    /// Target rows per compacted segment
    pub target_segment_rows: usize,
    /// Maximum number of uncompacted segments before triggering compaction
    pub compaction_threshold: usize,
    /// Maximum total segments to keep (older segments are dropped per retention)
    pub max_segments: usize,
}

impl Default for SegmentStorageConfig {
    fn default() -> Self {
        Self {
            partition_config: TimePartitionConfig::new("timestamp", TimeUnit::Hour),
            target_segment_rows: 8192,
            compaction_threshold: 10,
            max_segments: 1000,
        }
    }
}

/// Time-partitioned segment store for time-series data.
pub struct SegmentStore {
    /// Ordered map of partition_key -> segment
    segments: parking_lot::RwLock<std::collections::BTreeMap<i64, TimeSegment>>,
    /// Configuration
    config: SegmentStorageConfig,
    /// Total row count across all segments
    total_rows: std::sync::atomic::AtomicUsize,
}

impl SegmentStore {
    pub fn new(config: SegmentStorageConfig) -> Self {
        Self {
            segments: parking_lot::RwLock::new(std::collections::BTreeMap::new()),
            config,
            total_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Insert a batch into the appropriate time segment.
    pub fn insert(
        &self,
        batch: arrow::record_batch::RecordBatch,
        timestamp_micros: i64,
    ) -> Result<()> {
        let key = self
            .config
            .partition_config
            .partition_key(timestamp_micros)?;
        let (range_start, range_end) = self.config.partition_config.partition_range(key)?;

        let rows = batch.num_rows();
        let mut segments = self.segments.write();
        let segment = segments
            .entry(key)
            .or_insert_with(|| TimeSegment::new(range_start, range_end));
        segment.append(batch);
        self.total_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Query segments that overlap with the given time range.
    pub fn query_range(
        &self,
        start_micros: i64,
        end_micros: i64,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        let segments = self.segments.read();
        let mut result = Vec::new();
        for segment in segments.values() {
            if segment.overlaps(start_micros, end_micros) {
                result.extend(segment.batches.iter().cloned());
            }
        }
        result
    }

    /// Get all data from all segments.
    pub fn scan_all(&self) -> Vec<arrow::record_batch::RecordBatch> {
        let segments = self.segments.read();
        segments
            .values()
            .flat_map(|s| s.batches.iter().cloned())
            .collect()
    }

    /// Compact segments that have too many small batches.
    pub fn compact(&self) -> Result<usize> {
        let mut segments = self.segments.write();
        let mut compacted_count = 0;

        for segment in segments.values_mut() {
            if segment.batches.len() > 1 && !segment.compacted {
                if let Some(first) = segment.batches.first() {
                    let schema = first.schema();
                    match arrow::compute::concat_batches(&schema, &segment.batches) {
                        Ok(merged) => {
                            segment.batches = vec![merged];
                            segment.compacted = true;
                            compacted_count += 1;
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        Ok(compacted_count)
    }

    /// Apply retention policy: drop segments older than the cutoff.
    pub fn apply_retention(&self, cutoff_micros: i64) -> usize {
        let mut segments = self.segments.write();
        let keys_to_remove: Vec<i64> = segments
            .iter()
            .filter(|(_, seg)| seg.range_end <= cutoff_micros)
            .map(|(k, _)| *k)
            .collect();
        let removed = keys_to_remove.len();
        let removed_rows: usize = keys_to_remove
            .iter()
            .filter_map(|k| segments.get(k))
            .map(|s| s.row_count)
            .sum();
        for key in &keys_to_remove {
            segments.remove(key);
        }
        self.total_rows
            .fetch_sub(removed_rows, std::sync::atomic::Ordering::Relaxed);
        removed
    }

    /// Get the total number of rows.
    pub fn total_rows(&self) -> usize {
        self.total_rows.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Check if compaction is needed.
    pub fn needs_compaction(&self) -> bool {
        let segments = self.segments.read();
        segments
            .values()
            .any(|s| s.batches.len() > self.config.compaction_threshold && !s.compacted)
    }
}

/// Downsampling engine: creates rollup aggregations at coarser time granularity.
pub struct DownsamplingEngine;

impl DownsamplingEngine {
    /// Downsample data from one time granularity to a coarser one.
    ///
    /// Takes raw values at a fine granularity and produces aggregated values
    /// at a coarser granularity using the specified aggregation function.
    pub fn downsample(
        timestamps: &[i64],
        values: &[f64],
        target_unit: TimeUnit,
        agg: DownsampleAgg,
    ) -> Result<(Vec<i64>, Vec<f64>)> {
        if timestamps.len() != values.len() {
            return Err(BlazeError::execution(
                "Timestamps and values must have equal length",
            ));
        }
        if timestamps.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Group values by truncated timestamp
        let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> =
            std::collections::BTreeMap::new();
        for (ts, val) in timestamps.iter().zip(values.iter()) {
            let key = date_trunc(*ts, target_unit)?;
            buckets.entry(key).or_default().push(*val);
        }

        let mut out_timestamps = Vec::with_capacity(buckets.len());
        let mut out_values = Vec::with_capacity(buckets.len());

        for (ts, vals) in buckets {
            let aggregated = match agg {
                DownsampleAgg::Sum => vals.iter().sum(),
                DownsampleAgg::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
                DownsampleAgg::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
                DownsampleAgg::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                DownsampleAgg::Count => vals.len() as f64,
                DownsampleAgg::First => vals[0],
                DownsampleAgg::Last => *vals.last().unwrap(),
            };
            out_timestamps.push(ts);
            out_values.push(aggregated);
        }

        Ok((out_timestamps, out_values))
    }
}

/// Aggregation function for downsampling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownsampleAgg {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
}

/// Delta-of-delta encoder for timestamp compression.
/// Exploits temporal correlation: timestamps in time series data
/// are typically regularly spaced, so delta-of-delta values are
/// mostly zero or very small.
pub struct DeltaDeltaEncoder {
    prev_value: i64,
    prev_delta: i64,
    encoded: Vec<i64>,
}

impl DeltaDeltaEncoder {
    pub fn new() -> Self {
        Self {
            prev_value: 0,
            prev_delta: 0,
            encoded: Vec::new(),
        }
    }

    /// Encodes a value using delta-of-delta.
    pub fn encode(&mut self, value: i64) {
        if self.encoded.is_empty() {
            self.encoded.push(value);
            self.prev_value = value;
        } else if self.encoded.len() == 1 {
            let delta = value - self.prev_value;
            self.encoded.push(delta);
            self.prev_delta = delta;
            self.prev_value = value;
        } else {
            let delta = value - self.prev_value;
            let delta_of_delta = delta - self.prev_delta;
            self.encoded.push(delta_of_delta);
            self.prev_delta = delta;
            self.prev_value = value;
        }
    }

    /// Returns the encoded values.
    pub fn finish(self) -> Vec<i64> {
        self.encoded
    }

    /// Decodes back to original values.
    pub fn decode(encoded: &[i64]) -> Vec<i64> {
        if encoded.is_empty() {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(encoded.len());
        result.push(encoded[0]);
        if encoded.len() == 1 {
            return result;
        }
        let mut prev_delta = encoded[1];
        result.push(encoded[0] + prev_delta);
        for &dod in &encoded[2..] {
            let delta = prev_delta + dod;
            let value = *result.last().unwrap() + delta;
            result.push(value);
            prev_delta = delta;
        }
        result
    }

    /// Ratio of original vs encoded size.
    pub fn compression_ratio(original_len: usize, encoded: &[i64]) -> f64 {
        if encoded.is_empty() {
            return 1.0;
        }
        let orig_bytes = original_len * std::mem::size_of::<i64>();
        // Estimate encoded size: non-zero values need full i64, zeros need ~1 bit.
        // For simplicity, count zeros as 1 byte and non-zeros as 8 bytes.
        let encoded_bytes: usize = encoded
            .iter()
            .map(|&v| if v == 0 { 1 } else { std::mem::size_of::<i64>() })
            .sum();
        if encoded_bytes == 0 {
            return 1.0;
        }
        orig_bytes as f64 / encoded_bytes as f64
    }
}

/// Result of flushing an ingestion buffer.
#[derive(Debug, Clone)]
pub struct IngestionFlushResult {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub labels: Vec<String>,
    pub count: usize,
}

/// High-throughput ingestion buffer for time-series data.
/// Accepts out-of-order data, buffers until flush threshold, and
/// sorts by timestamp before flushing.
pub struct IngestionBuffer {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    labels: Vec<String>,
    flush_threshold: usize,
    total_ingested: u64,
    total_flushed: u64,
}

impl IngestionBuffer {
    pub fn new(flush_threshold: usize) -> Self {
        Self {
            timestamps: Vec::new(),
            values: Vec::new(),
            labels: Vec::new(),
            flush_threshold,
            total_ingested: 0,
            total_flushed: 0,
        }
    }

    /// Adds a single data point.
    pub fn ingest(&mut self, timestamp: i64, value: f64, label: impl Into<String>) {
        self.timestamps.push(timestamp);
        self.values.push(value);
        self.labels.push(label.into());
        self.total_ingested += 1;
    }

    /// Adds multiple data points.
    pub fn ingest_batch(&mut self, timestamps: &[i64], values: &[f64], label: impl Into<String>) {
        let label = label.into();
        for i in 0..timestamps.len().min(values.len()) {
            self.timestamps.push(timestamps[i]);
            self.values.push(values[i]);
            self.labels.push(label.clone());
        }
        self.total_ingested += timestamps.len().min(values.len()) as u64;
    }

    /// Returns true if buffer size >= flush_threshold.
    pub fn should_flush(&self) -> bool {
        self.timestamps.len() >= self.flush_threshold
    }

    /// Sorts by timestamp and returns sorted data, resets buffer.
    pub fn flush(&mut self) -> IngestionFlushResult {
        let count = self.timestamps.len();

        // Build index permutation sorted by timestamp
        let mut indices: Vec<usize> = (0..count).collect();
        indices.sort_by_key(|&i| self.timestamps[i]);

        let timestamps: Vec<i64> = indices.iter().map(|&i| self.timestamps[i]).collect();
        let values: Vec<f64> = indices.iter().map(|&i| self.values[i]).collect();
        let labels: Vec<String> = indices.iter().map(|&i| self.labels[i].clone()).collect();

        self.timestamps.clear();
        self.values.clear();
        self.labels.clear();
        self.total_flushed += count as u64;

        IngestionFlushResult {
            timestamps,
            values,
            labels,
            count,
        }
    }

    pub fn pending_count(&self) -> usize {
        self.timestamps.len()
    }

    pub fn total_ingested(&self) -> u64 {
        self.total_ingested
    }

    pub fn total_flushed(&self) -> u64 {
        self.total_flushed
    }
}

/// Runtime metrics for time-series operations.
#[derive(Debug, Clone, Default)]
pub struct TimeSeriesMetrics {
    pub total_points_ingested: u64,
    pub total_points_queried: u64,
    pub total_compactions: u64,
    pub total_retentions_applied: u64,
    pub avg_ingestion_rate_per_sec: f64,
    pub avg_query_latency_ms: f64,
    pub active_segments: usize,
    pub total_bytes_stored: u64,
}

impl TimeSeriesMetrics {
    pub fn record_ingestion(&mut self, points: u64, duration_ms: f64) {
        self.total_points_ingested += points;
        if duration_ms > 0.0 {
            let rate = (points as f64 / duration_ms) * 1000.0;
            // Exponential moving average for ingestion rate
            if self.avg_ingestion_rate_per_sec == 0.0 {
                self.avg_ingestion_rate_per_sec = rate;
            } else {
                self.avg_ingestion_rate_per_sec =
                    self.avg_ingestion_rate_per_sec * 0.8 + rate * 0.2;
            }
        }
    }

    pub fn record_query(&mut self, points: u64, latency_ms: f64) {
        self.total_points_queried += points;
        if self.avg_query_latency_ms == 0.0 {
            self.avg_query_latency_ms = latency_ms;
        } else {
            self.avg_query_latency_ms = self.avg_query_latency_ms * 0.8 + latency_ms * 0.2;
        }
    }

    /// Returns bytes per point (storage efficiency).
    pub fn storage_efficiency(&self) -> f64 {
        if self.total_points_ingested == 0 {
            return 0.0;
        }
        self.total_bytes_stored as f64 / self.total_points_ingested as f64
    }
}

/// Interpolation methods for filling missing time-series values.
pub struct InterpolationEngine;

impl InterpolationEngine {
    /// Linear interpolation between two known points.
    pub fn linear(t1: i64, v1: f64, t2: i64, v2: f64, t: i64) -> f64 {
        if t1 == t2 {
            return v1;
        }
        let ratio = (t - t1) as f64 / (t2 - t1) as f64;
        v1 + ratio * (v2 - v1)
    }

    /// Step interpolation (last observation carried forward).
    pub fn locf(values: &[Option<f64>]) -> Vec<f64> {
        let mut result = Vec::with_capacity(values.len());
        let mut last = 0.0;
        for v in values {
            match v {
                Some(val) => {
                    last = *val;
                    result.push(*val);
                }
                None => result.push(last),
            }
        }
        result
    }

    /// Next observation carried backward.
    pub fn nocb(values: &[Option<f64>]) -> Vec<f64> {
        let mut result = vec![0.0; values.len()];
        let mut next = 0.0;
        for i in (0..values.len()).rev() {
            match values[i] {
                Some(val) => {
                    next = val;
                    result[i] = val;
                }
                None => result[i] = next,
            }
        }
        result
    }

    /// Fill with a constant value.
    pub fn constant(values: &[Option<f64>], fill_value: f64) -> Vec<f64> {
        values
            .iter()
            .map(|v| v.unwrap_or(fill_value))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Phase 2: ASOF JOIN + time_bucket + GENERATE_SERIES
// ---------------------------------------------------------------------------

/// Executor for ASOF JOIN operations.
pub struct AsofJoinExecutor {
    config: AsofJoinConfig,
}

impl AsofJoinExecutor {
    pub fn new(config: AsofJoinConfig) -> Self {
        Self { config }
    }

    /// Execute an ASOF join between two sorted timestamp arrays.
    /// For each left timestamp, finds the closest matching right timestamp.
    pub fn execute(
        &self,
        left_times: &[i64],
        right_times: &[i64],
    ) -> Vec<Option<usize>> {
        left_times.iter().map(|&lt| {
            self.find_match(lt, right_times)
        }).collect()
    }

    fn find_match(&self, left_time: i64, right_times: &[i64]) -> Option<usize> {
        let tolerance = self.config.tolerance.unwrap_or(i64::MAX);
        
        match self.config.direction {
            AsofDirection::Backward => {
                // Find the latest right time <= left_time
                let pos = right_times.partition_point(|&t| t <= left_time);
                if pos > 0 {
                    let idx = pos - 1;
                    if (left_time - right_times[idx]).abs() <= tolerance {
                        return Some(idx);
                    }
                }
                None
            }
            AsofDirection::Forward => {
                // Find the earliest right time >= left_time
                let pos = right_times.partition_point(|&t| t < left_time);
                if pos < right_times.len() {
                    if (right_times[pos] - left_time).abs() <= tolerance {
                        return Some(pos);
                    }
                }
                None
            }
            AsofDirection::Nearest => {
                let pos = right_times.partition_point(|&t| t < left_time);
                let candidates = [
                    pos.checked_sub(1).map(|i| (i, (left_time - right_times[i]).abs())),
                    if pos < right_times.len() { Some((pos, (right_times[pos] - left_time).abs())) } else { None },
                ];
                candidates.iter()
                    .filter_map(|&c| c)
                    .filter(|&(_, diff)| diff <= tolerance)
                    .min_by_key(|&(_, diff)| diff)
                    .map(|(idx, _)| idx)
            }
        }
    }
}

/// Time bucket function that truncates timestamps to bucket boundaries.
pub struct TimeBucket;

impl TimeBucket {
    /// Truncate a timestamp (microseconds since epoch) to the nearest bucket boundary.
    pub fn bucket(timestamp_us: i64, interval_us: i64) -> i64 {
        if interval_us <= 0 { return timestamp_us; }
        (timestamp_us / interval_us) * interval_us
    }

    /// Bucket with an origin offset.
    pub fn bucket_with_origin(timestamp_us: i64, interval_us: i64, origin_us: i64) -> i64 {
        if interval_us <= 0 { return timestamp_us; }
        let shifted = timestamp_us - origin_us;
        (shifted / interval_us) * interval_us + origin_us
    }

    /// Common interval values in microseconds.
    pub fn interval_us(unit: &str) -> Option<i64> {
        match unit.to_lowercase().as_str() {
            "second" | "1s" => Some(1_000_000),
            "minute" | "1m" => Some(60_000_000),
            "hour" | "1h" => Some(3_600_000_000),
            "day" | "1d" => Some(86_400_000_000),
            "week" | "1w" => Some(604_800_000_000),
            _ => None,
        }
    }
}

/// Generate a series of timestamps.
pub struct GenerateSeries;

impl GenerateSeries {
    /// Generate timestamps from start to stop (inclusive) with the given step.
    pub fn generate(start_us: i64, stop_us: i64, step_us: i64) -> Vec<i64> {
        if step_us <= 0 || start_us > stop_us {
            return Vec::new();
        }
        let mut result = Vec::new();
        let mut current = start_us;
        while current <= stop_us {
            result.push(current);
            current += step_us;
        }
        result
    }

    /// Generate timestamps for a specific number of buckets.
    pub fn generate_n(start_us: i64, step_us: i64, count: usize) -> Vec<i64> {
        (0..count).map(|i| start_us + (i as i64) * step_us).collect()
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Streaming Ingestion API + Out-of-Order + Retention Enforcer
// ---------------------------------------------------------------------------

/// Streaming ingestion API with out-of-order handling.
pub struct StreamingIngestionApi {
    buffer: Vec<TimestampedRow>,
    sort_buffer_size: usize,
    watermark: i64,
    late_rows_dropped: u64,
    rows_ingested: u64,
}

/// A row with a timestamp for ingestion.
#[derive(Debug, Clone)]
pub struct TimestampedRow {
    pub timestamp_us: i64,
    pub values: Vec<f64>,
}

impl StreamingIngestionApi {
    pub fn new(sort_buffer_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(sort_buffer_size),
            sort_buffer_size,
            watermark: 0,
            late_rows_dropped: 0,
            rows_ingested: 0,
        }
    }

    /// Ingest a row. Returns true if accepted, false if late (dropped).
    pub fn ingest(&mut self, row: TimestampedRow) -> bool {
        self.rows_ingested += 1;
        if row.timestamp_us < self.watermark {
            self.late_rows_dropped += 1;
            return false;
        }
        self.buffer.push(row);
        
        // Sort and flush if buffer is full
        if self.buffer.len() >= self.sort_buffer_size {
            self.sort_buffer();
        }
        true
    }

    /// Sort the buffer by timestamp.
    fn sort_buffer(&mut self) {
        self.buffer.sort_by_key(|r| r.timestamp_us);
    }

    /// Flush sorted rows up to the given watermark.
    pub fn flush(&mut self, new_watermark: i64) -> Vec<TimestampedRow> {
        self.sort_buffer();
        self.watermark = new_watermark;
        let (flushed, remaining): (Vec<_>, Vec<_>) = self.buffer
            .drain(..)
            .partition(|r| r.timestamp_us <= new_watermark);
        self.buffer = remaining;
        flushed
    }

    pub fn buffer_size(&self) -> usize { self.buffer.len() }
    pub fn late_rows_dropped(&self) -> u64 { self.late_rows_dropped }
    pub fn rows_ingested(&self) -> u64 { self.rows_ingested }
    pub fn watermark(&self) -> i64 { self.watermark }
}

/// Enforces retention policies on time-series data.
pub struct RetentionEnforcer {
    policy: RetentionPolicy,
    last_enforced: std::time::Instant,
    rows_deleted: u64,
}

impl RetentionEnforcer {
    pub fn new(policy: RetentionPolicy) -> Self {
        Self {
            policy,
            last_enforced: std::time::Instant::now(),
            rows_deleted: 0,
        }
    }

    /// Determine which rows to delete based on the retention policy.
    /// Returns the cutoff timestamp - rows before this should be deleted.
    pub fn compute_cutoff(&self, current_time_us: i64) -> i64 {
        let max_age_us = self.policy.max_age.num_microseconds().unwrap_or(i64::MAX);
        current_time_us - max_age_us
    }

    /// Count rows that would be affected by enforcement.
    pub fn count_expired_rows(&self, timestamps: &[i64], current_time_us: i64) -> usize {
        let cutoff = self.compute_cutoff(current_time_us);
        timestamps.iter().filter(|&&t| t < cutoff).count()
    }

    /// Record that rows were deleted during enforcement.
    pub fn record_enforcement(&mut self, rows_deleted: u64) {
        self.rows_deleted += rows_deleted;
        self.last_enforced = std::time::Instant::now();
    }

    pub fn total_rows_deleted(&self) -> u64 { self.rows_deleted }
    pub fn policy(&self) -> &RetentionPolicy { &self.policy }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> i64 {
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

        let (ts, vals) =
            gap_fill(&timestamps, &values, 1, GapFillStrategy::Previous, None).unwrap();

        assert_eq!(ts, vec![0, 1, 2, 3, 4]);
        assert_eq!(
            vals,
            vec![Some(1.0), Some(1.0), Some(2.0), Some(2.0), Some(3.0)]
        );
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

    // -----------------------------------------------------------------------
    // ASOF JOIN tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_asof_join_backward() {
        // Left:  1, 3, 5
        // Right: 2, 4, 6
        let left_ts = vec![1_000_000, 3_000_000, 5_000_000];
        let left_vals = vec![10.0, 30.0, 50.0];
        let right_ts = vec![2_000_000, 4_000_000, 6_000_000];
        let right_vals = vec![20.0, 40.0, 60.0];

        let config = AsofJoinConfig {
            tolerance: None,
            direction: AsofDirection::Backward,
        };
        let result = asof_join(&left_ts, &left_vals, &right_ts, &right_vals, &config).unwrap();

        // t=1: no right ts <= 1 → None
        assert_eq!(result[0], (1_000_000, 10.0, None));
        // t=3: right ts=2 <= 3 → 20.0
        assert_eq!(result[1], (3_000_000, 30.0, Some(20.0)));
        // t=5: right ts=4 <= 5 → 40.0
        assert_eq!(result[2], (5_000_000, 50.0, Some(40.0)));
    }

    #[test]
    fn test_asof_join_with_tolerance() {
        let left_ts = vec![1_000_000, 5_000_000, 10_000_000];
        let left_vals = vec![1.0, 2.0, 3.0];
        let right_ts = vec![2_000_000, 4_000_000, 6_000_000];
        let right_vals = vec![100.0, 200.0, 300.0];

        let config = AsofJoinConfig {
            tolerance: Some(1_500_000), // 1.5 seconds
            direction: AsofDirection::Backward,
        };
        let result = asof_join(&left_ts, &left_vals, &right_ts, &right_vals, &config).unwrap();

        // t=1: no right ts <= 1 → None
        assert_eq!(result[0].2, None);
        // t=5: right ts=4 (diff=1s ≤ 1.5s) → 200.0
        assert_eq!(result[1].2, Some(200.0));
        // t=10: right ts=6 (diff=4s > 1.5s) → None
        assert_eq!(result[2].2, None);
    }

    // -----------------------------------------------------------------------
    // LTTB downsampling tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_lttb_downsample() {
        // 10 points, downsample to 5
        let timestamps: Vec<i64> = (0..10).map(|i| i * 1_000_000).collect();
        let values: Vec<f64> = vec![0.0, 5.0, 2.0, 8.0, 1.0, 7.0, 3.0, 9.0, 4.0, 6.0];

        let (ts, vals) = lttb_downsample(&timestamps, &values, 5).unwrap();

        assert_eq!(ts.len(), 5);
        assert_eq!(vals.len(), 5);
        // First and last must be preserved.
        assert_eq!(ts[0], timestamps[0]);
        assert_eq!(vals[0], values[0]);
        assert_eq!(*ts.last().unwrap(), *timestamps.last().unwrap());
        assert_eq!(*vals.last().unwrap(), *values.last().unwrap());
    }

    #[test]
    fn test_lttb_downsample_passthrough() {
        // Fewer points than target → return all.
        let timestamps = vec![0, 1_000_000, 2_000_000];
        let values = vec![1.0, 2.0, 3.0];

        let (ts, vals) = lttb_downsample(&timestamps, &values, 10).unwrap();
        assert_eq!(ts, timestamps);
        assert_eq!(vals, values);
    }

    // -----------------------------------------------------------------------
    // Rate / Delta tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rate_calculation() {
        // 1-second intervals, values increase by 10 each second → rate = 10/s
        let timestamps = vec![0, 1_000_000, 2_000_000, 3_000_000];
        let values = vec![100.0, 110.0, 120.0, 130.0];

        let r = rate(&timestamps, &values).unwrap();
        assert_eq!(r.len(), 4);
        assert!(r[0].is_none());
        assert!((r[1].unwrap() - 10.0).abs() < 1e-9);
        assert!((r[2].unwrap() - 10.0).abs() < 1e-9);
        assert!((r[3].unwrap() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_delta_calculation() {
        let timestamps = vec![0, 1_000_000, 2_000_000];
        let values = vec![5.0, 8.0, 3.0];

        let d = delta(&timestamps, &values).unwrap();
        assert_eq!(d.len(), 3);
        assert!(d[0].is_none());
        assert!((d[1].unwrap() - 3.0).abs() < 1e-9);
        assert!((d[2].unwrap() - (-5.0)).abs() < 1e-9);
    }

    #[test]
    fn test_monotonic_rate_handles_reset() {
        // Counter resets from 100 → 10 at index 2.
        let timestamps = vec![0, 1_000_000, 2_000_000, 3_000_000];
        let values = vec![50.0, 100.0, 10.0, 60.0];

        let r = monotonic_rate(&timestamps, &values).unwrap();
        assert!(r[0].is_none());
        assert!((r[1].unwrap() - 50.0).abs() < 1e-9); // 100 - 50 in 1s
        assert!(r[2].is_none()); // counter reset
        assert!((r[3].unwrap() - 50.0).abs() < 1e-9); // 60 - 10 in 1s
    }

    // -----------------------------------------------------------------------
    // Retention Policy tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_retention_policy() {
        let policy = RetentionPolicy::delete_after(Duration::hours(1), "ts");

        let now = 10_000_000_000i64; // 10 000 seconds in micros
        let one_hour_micros = 3_600_000_000i64;

        // Recent data should be retained.
        assert!(policy.should_retain(now - 1_000_000, now));
        // Data older than 1 hour should expire.
        assert!(!policy.should_retain(now - one_hour_micros - 1, now));

        let timestamps = vec![
            now - 1_000_000,             // recent → retained
            now - one_hour_micros - 1,   // expired
            now - one_hour_micros + 100, // just within → retained
        ];
        let (retained, expired) = policy.partition_data(&timestamps, now);
        assert_eq!(retained, vec![0, 2]);
        assert_eq!(expired, vec![1]);
    }

    // -----------------------------------------------------------------------
    // Time Partition Config tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_time_partition_key() {
        let cfg = TimePartitionConfig::new("ts", TimeUnit::Hour);

        // 2024-01-15 13:45:30 UTC in microseconds
        let ts = make_timestamp(2024, 1, 15, 13, 45, 30);
        let key = cfg.partition_key(ts).unwrap();

        // Key should be truncated to the start of the hour.
        let expected = make_timestamp(2024, 1, 15, 13, 0, 0);
        assert_eq!(key, expected);

        // Range should span exactly one hour.
        let (start, end) = cfg.partition_range(key).unwrap();
        assert_eq!(start, expected);
        let expected_end = make_timestamp(2024, 1, 15, 14, 0, 0);
        assert_eq!(end, expected_end);
    }

    // --- Segment Storage tests ---

    fn make_ts_batch(rows: usize) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let values: Vec<i64> = (0..rows as i64).collect();
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .unwrap()
    }

    #[test]
    fn test_segment_store_insert_and_query() {
        let config = SegmentStorageConfig::default();
        let store = SegmentStore::new(config);

        let ts1 = make_timestamp(2024, 1, 15, 10, 0, 0);
        let ts2 = make_timestamp(2024, 1, 15, 11, 0, 0);

        store.insert(make_ts_batch(5), ts1).unwrap();
        store.insert(make_ts_batch(3), ts2).unwrap();

        assert_eq!(store.total_rows(), 8);
        assert_eq!(store.segment_count(), 2);

        // Query range covering both hours
        let start = make_timestamp(2024, 1, 15, 9, 0, 0);
        let end = make_timestamp(2024, 1, 15, 12, 0, 0);
        let results = store.query_range(start, end);
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn test_segment_store_range_query() {
        let config = SegmentStorageConfig::default();
        let store = SegmentStore::new(config);

        let ts1 = make_timestamp(2024, 1, 15, 10, 30, 0);
        let ts2 = make_timestamp(2024, 1, 15, 11, 30, 0);
        let ts3 = make_timestamp(2024, 1, 15, 12, 30, 0);

        store.insert(make_ts_batch(3), ts1).unwrap();
        store.insert(make_ts_batch(4), ts2).unwrap();
        store.insert(make_ts_batch(5), ts3).unwrap();

        // Only query 11:00-12:00 range
        let start = make_timestamp(2024, 1, 15, 11, 0, 0);
        let end = make_timestamp(2024, 1, 15, 12, 0, 0);
        let results = store.query_range(start, end);
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4); // Only the 11:30 batch
    }

    #[test]
    fn test_segment_store_compact() {
        let config = SegmentStorageConfig::default();
        let store = SegmentStore::new(config);

        let ts = make_timestamp(2024, 1, 15, 10, 0, 0);
        // Insert multiple small batches into same segment
        store.insert(make_ts_batch(3), ts).unwrap();
        store.insert(make_ts_batch(4), ts).unwrap();
        store.insert(make_ts_batch(5), ts).unwrap();

        assert_eq!(store.segment_count(), 1);
        assert_eq!(store.total_rows(), 12);

        let compacted = store.compact().unwrap();
        assert_eq!(compacted, 1);

        // Data should still be there
        let all = store.scan_all();
        let total: usize = all.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 12);
    }

    #[test]
    fn test_segment_store_retention() {
        let config = SegmentStorageConfig::default();
        let store = SegmentStore::new(config);

        let ts_old = make_timestamp(2024, 1, 10, 10, 0, 0);
        let ts_new = make_timestamp(2024, 1, 15, 10, 0, 0);

        store.insert(make_ts_batch(5), ts_old).unwrap();
        store.insert(make_ts_batch(3), ts_new).unwrap();
        assert_eq!(store.segment_count(), 2);

        // Drop segments ending before Jan 15
        let cutoff = make_timestamp(2024, 1, 15, 0, 0, 0);
        let removed = store.apply_retention(cutoff);
        assert_eq!(removed, 1);
        assert_eq!(store.segment_count(), 1);
        assert_eq!(store.total_rows(), 3);
    }

    #[test]
    fn test_segment_contains_and_overlaps() {
        let start = make_timestamp(2024, 1, 15, 10, 0, 0);
        let end = make_timestamp(2024, 1, 15, 11, 0, 0);
        let segment = TimeSegment::new(start, end);

        assert!(segment.contains(make_timestamp(2024, 1, 15, 10, 30, 0)));
        assert!(!segment.contains(make_timestamp(2024, 1, 15, 11, 0, 0))); // exclusive end
        assert!(!segment.contains(make_timestamp(2024, 1, 15, 9, 0, 0)));

        assert!(segment.overlaps(start, end));
        assert!(segment.overlaps(
            make_timestamp(2024, 1, 15, 10, 30, 0),
            make_timestamp(2024, 1, 15, 12, 0, 0)
        ));
        assert!(!segment.overlaps(
            make_timestamp(2024, 1, 15, 11, 0, 0),
            make_timestamp(2024, 1, 15, 12, 0, 0)
        ));
    }

    // --- Downsampling tests ---

    #[test]
    fn test_downsample_sum() {
        let timestamps = vec![
            make_timestamp(2024, 1, 15, 10, 0, 0),
            make_timestamp(2024, 1, 15, 10, 30, 0),
            make_timestamp(2024, 1, 15, 11, 0, 0),
            make_timestamp(2024, 1, 15, 11, 30, 0),
        ];
        let values = vec![10.0, 20.0, 30.0, 40.0];

        let (ts, vals) = DownsamplingEngine::downsample(
            &timestamps,
            &values,
            TimeUnit::Hour,
            DownsampleAgg::Sum,
        )
        .unwrap();

        assert_eq!(ts.len(), 2); // Two hours
        assert_eq!(vals[0], 30.0); // 10 + 20
        assert_eq!(vals[1], 70.0); // 30 + 40
    }

    #[test]
    fn test_downsample_avg() {
        let timestamps = vec![
            make_timestamp(2024, 1, 15, 10, 0, 0),
            make_timestamp(2024, 1, 15, 10, 30, 0),
        ];
        let values = vec![10.0, 20.0];

        let (_, vals) = DownsamplingEngine::downsample(
            &timestamps,
            &values,
            TimeUnit::Hour,
            DownsampleAgg::Avg,
        )
        .unwrap();

        assert_eq!(vals[0], 15.0);
    }

    #[test]
    fn test_downsample_count() {
        let timestamps = vec![
            make_timestamp(2024, 1, 15, 10, 0, 0),
            make_timestamp(2024, 1, 15, 10, 15, 0),
            make_timestamp(2024, 1, 15, 10, 30, 0),
        ];
        let values = vec![1.0, 2.0, 3.0];

        let (_, vals) = DownsamplingEngine::downsample(
            &timestamps,
            &values,
            TimeUnit::Hour,
            DownsampleAgg::Count,
        )
        .unwrap();

        assert_eq!(vals[0], 3.0);
    }

    #[test]
    fn test_downsample_empty() {
        let (ts, vals) =
            DownsamplingEngine::downsample(&[], &[], TimeUnit::Hour, DownsampleAgg::Sum).unwrap();
        assert!(ts.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn test_downsample_mismatched_lengths() {
        let result = DownsamplingEngine::downsample(
            &[1, 2, 3],
            &[1.0, 2.0],
            TimeUnit::Hour,
            DownsampleAgg::Sum,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_delta_delta_encode_decode() {
        let values = vec![100, 205, 315, 420, 530];
        let mut encoder = DeltaDeltaEncoder::new();
        for &v in &values {
            encoder.encode(v);
        }
        let encoded = encoder.finish();
        let decoded = DeltaDeltaEncoder::decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_delta_regular_timestamps() {
        // Regularly spaced timestamps: delta-of-delta should be all zeros after first two
        let values: Vec<i64> = (0..10).map(|i| 1000 + i * 100).collect();
        let mut encoder = DeltaDeltaEncoder::new();
        for &v in &values {
            encoder.encode(v);
        }
        let encoded = encoder.finish();
        // After first value and first delta, all delta-of-deltas should be 0
        for &dod in &encoded[2..] {
            assert_eq!(dod, 0);
        }
        let decoded = DeltaDeltaEncoder::decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_delta_compression_ratio() {
        // Regular timestamps should compress well
        let values: Vec<i64> = (0..100).map(|i| 1_000_000 + i * 1000).collect();
        let mut encoder = DeltaDeltaEncoder::new();
        for &v in &values {
            encoder.encode(v);
        }
        let encoded = encoder.finish();
        let ratio = DeltaDeltaEncoder::compression_ratio(values.len(), &encoded);
        assert!(ratio > 1.0, "Regular timestamps should compress well, got ratio {ratio}");
    }

    #[test]
    fn test_ingestion_buffer_basic() {
        let mut buf = IngestionBuffer::new(100);
        buf.ingest(1000, 1.0, "cpu");
        buf.ingest(2000, 2.0, "cpu");
        assert_eq!(buf.pending_count(), 2);
        assert_eq!(buf.total_ingested(), 2);
        assert!(!buf.should_flush());
    }

    #[test]
    fn test_ingestion_buffer_flush() {
        let mut buf = IngestionBuffer::new(3);
        buf.ingest(3000, 3.0, "cpu");
        buf.ingest(1000, 1.0, "cpu");
        buf.ingest(2000, 2.0, "cpu");
        assert!(buf.should_flush());
        let result = buf.flush();
        assert_eq!(result.count, 3);
        assert_eq!(result.timestamps, vec![1000, 2000, 3000]);
        assert_eq!(result.values, vec![1.0, 2.0, 3.0]);
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(buf.total_flushed(), 3);
    }

    #[test]
    fn test_ingestion_buffer_batch() {
        let mut buf = IngestionBuffer::new(100);
        buf.ingest_batch(&[100, 200, 300], &[1.0, 2.0, 3.0], "mem");
        assert_eq!(buf.pending_count(), 3);
        assert_eq!(buf.total_ingested(), 3);
    }

    #[test]
    fn test_ingestion_buffer_out_of_order_sorted() {
        let mut buf = IngestionBuffer::new(100);
        buf.ingest(500, 5.0, "a");
        buf.ingest(100, 1.0, "b");
        buf.ingest(300, 3.0, "c");
        buf.ingest(200, 2.0, "d");
        buf.ingest(400, 4.0, "e");
        let result = buf.flush();
        assert_eq!(result.timestamps, vec![100, 200, 300, 400, 500]);
        assert_eq!(result.values, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(result.labels, vec!["b", "d", "c", "e", "a"]);
    }

    #[test]
    fn test_timeseries_metrics_recording() {
        let mut metrics = TimeSeriesMetrics::default();
        metrics.record_ingestion(1000, 100.0);
        assert_eq!(metrics.total_points_ingested, 1000);
        assert!(metrics.avg_ingestion_rate_per_sec > 0.0);

        metrics.record_query(500, 5.0);
        assert_eq!(metrics.total_points_queried, 500);
        assert!((metrics.avg_query_latency_ms - 5.0).abs() < f64::EPSILON);

        metrics.total_bytes_stored = 8000;
        let eff = metrics.storage_efficiency();
        assert!((eff - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_interpolation_linear() {
        let v = InterpolationEngine::linear(0, 0.0, 10, 100.0, 5);
        assert!((v - 50.0).abs() < f64::EPSILON);
        let v2 = InterpolationEngine::linear(0, 0.0, 10, 100.0, 0);
        assert!((v2 - 0.0).abs() < f64::EPSILON);
        let v3 = InterpolationEngine::linear(0, 0.0, 10, 100.0, 10);
        assert!((v3 - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_interpolation_locf() {
        let values = vec![Some(1.0), None, None, Some(4.0), None];
        let filled = InterpolationEngine::locf(&values);
        assert_eq!(filled, vec![1.0, 1.0, 1.0, 4.0, 4.0]);
    }

    #[test]
    fn test_interpolation_nocb() {
        let values = vec![None, None, Some(3.0), None, Some(5.0)];
        let filled = InterpolationEngine::nocb(&values);
        assert_eq!(filled, vec![3.0, 3.0, 3.0, 5.0, 5.0]);
    }

    #[test]
    fn test_interpolation_constant() {
        let values = vec![Some(1.0), None, Some(3.0), None];
        let filled = InterpolationEngine::constant(&values, -1.0);
        assert_eq!(filled, vec![1.0, -1.0, 3.0, -1.0]);
    }

    #[test]
    fn test_asof_join_executor_backward() {
        let config = AsofJoinConfig { direction: AsofDirection::Backward, tolerance: None };
        let executor = AsofJoinExecutor::new(config);
        let left = vec![10, 20, 30];
        let right = vec![5, 15, 25, 35];
        let matches = executor.execute(&left, &right);
        assert_eq!(matches[0], Some(0)); // 10 -> 5
        assert_eq!(matches[1], Some(1)); // 20 -> 15
        assert_eq!(matches[2], Some(2)); // 30 -> 25
    }

    #[test]
    fn test_asof_join_executor_forward() {
        let config = AsofJoinConfig { direction: AsofDirection::Forward, tolerance: None };
        let executor = AsofJoinExecutor::new(config);
        let left = vec![10, 20, 30];
        let right = vec![5, 15, 25, 35];
        let matches = executor.execute(&left, &right);
        assert_eq!(matches[0], Some(1)); // 10 -> 15
        assert_eq!(matches[1], Some(2)); // 20 -> 25
        assert_eq!(matches[2], Some(3)); // 30 -> 35
    }

    #[test]
    fn test_asof_join_executor_nearest() {
        let config = AsofJoinConfig { direction: AsofDirection::Nearest, tolerance: None };
        let executor = AsofJoinExecutor::new(config);
        let left = vec![12];
        let right = vec![5, 10, 15, 20];
        let matches = executor.execute(&left, &right);
        assert_eq!(matches[0], Some(1)); // 12 nearest to 10
    }

    #[test]
    fn test_asof_join_executor_tolerance() {
        let config = AsofJoinConfig { direction: AsofDirection::Backward, tolerance: Some(3) };
        let executor = AsofJoinExecutor::new(config);
        let left = vec![10, 100];
        let right = vec![8, 50];
        let matches = executor.execute(&left, &right);
        assert_eq!(matches[0], Some(0)); // 10-8=2 <= 3
        assert_eq!(matches[1], None);    // 100-50=50 > 3
    }

    #[test]
    fn test_time_bucket_truncation() {
        let hour = 3_600_000_000i64;
        let ts = hour + 1_800_000_000; // 1.5 hours
        assert_eq!(TimeBucket::bucket(ts, hour), hour);
    }

    #[test]
    fn test_time_bucket_interval_lookup() {
        assert_eq!(TimeBucket::interval_us("second"), Some(1_000_000));
        assert_eq!(TimeBucket::interval_us("hour"), Some(3_600_000_000));
        assert_eq!(TimeBucket::interval_us("unknown"), None);
    }

    #[test]
    fn test_generate_series_range() {
        let series = GenerateSeries::generate(0, 1_000_000, 250_000);
        assert_eq!(series.len(), 5); // 0, 250k, 500k, 750k, 1M
        assert_eq!(series[0], 0);
        assert_eq!(series[4], 1_000_000);
    }

    #[test]
    fn test_generate_series_n() {
        let series = GenerateSeries::generate_n(100, 10, 5);
        assert_eq!(series, vec![100, 110, 120, 130, 140]);
    }

    #[test]
    fn test_streaming_ingestion() {
        let mut api = StreamingIngestionApi::new(10);
        assert!(api.ingest(TimestampedRow { timestamp_us: 100, values: vec![1.0] }));
        assert!(api.ingest(TimestampedRow { timestamp_us: 200, values: vec![2.0] }));
        assert_eq!(api.buffer_size(), 2);
        
        let flushed = api.flush(150);
        assert_eq!(flushed.len(), 1); // Only ts=100
        assert_eq!(api.buffer_size(), 1); // ts=200 remains
    }

    #[test]
    fn test_streaming_ingestion_late_data() {
        let mut api = StreamingIngestionApi::new(100);
        api.flush(100); // Set watermark to 100
        assert!(!api.ingest(TimestampedRow { timestamp_us: 50, values: vec![1.0] }));
        assert_eq!(api.late_rows_dropped(), 1);
    }

    #[test]
    fn test_retention_enforcer() {
        let policy = RetentionPolicy::delete_after(Duration::hours(1), "ts");
        let enforcer = RetentionEnforcer::new(policy);
        let current = 7200_000_000i64; // 2 hours in microseconds
        let cutoff = enforcer.compute_cutoff(current);
        assert_eq!(cutoff, 3600_000_000); // 1 hour in microseconds
    }

    #[test]
    fn test_retention_count_expired() {
        let policy = RetentionPolicy::delete_after(Duration::seconds(100), "ts");
        let enforcer = RetentionEnforcer::new(policy);
        let timestamps = vec![50_000_000, 80_000_000, 150_000_000, 200_000_000];
        let expired = enforcer.count_expired_rows(&timestamps, 200_000_000);
        assert_eq!(expired, 2); // 50M and 80M are before cutoff (100M)
    }
}
