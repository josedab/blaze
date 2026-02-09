//! Data type definitions for Blaze.

use std::fmt;

use arrow::datatypes::DataType as ArrowDataType;

use crate::error::{BlazeError, Result};

/// SQL data types supported by Blaze.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Null type (unknown type)
    Null,
    /// Boolean type
    Boolean,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 8-bit unsigned integer
    UInt8,
    /// 16-bit unsigned integer
    UInt16,
    /// 32-bit unsigned integer
    UInt32,
    /// 64-bit unsigned integer
    UInt64,
    /// 16-bit floating point
    Float16,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// Decimal type with precision and scale
    Decimal128 { precision: u8, scale: i8 },
    /// UTF-8 encoded string
    Utf8,
    /// Large UTF-8 encoded string (64-bit offsets)
    LargeUtf8,
    /// Binary data
    Binary,
    /// Large binary data (64-bit offsets)
    LargeBinary,
    /// Date (days since epoch)
    Date32,
    /// Date (milliseconds since epoch)
    Date64,
    /// Time with second precision
    Time32Second,
    /// Time with millisecond precision
    Time32Millisecond,
    /// Time with microsecond precision
    Time64Microsecond,
    /// Time with nanosecond precision
    Time64Nanosecond,
    /// Timestamp with timezone
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    /// Duration
    Duration { unit: TimeUnit },
    /// Interval (year-month)
    IntervalYearMonth,
    /// Interval (day-time)
    IntervalDayTime,
    /// Interval (month-day-nanosecond)
    IntervalMonthDayNano,
    /// List of values
    List(Box<DataType>),
    /// Fixed-size list
    FixedSizeList {
        element_type: Box<DataType>,
        size: i32,
    },
    /// Struct type
    Struct(Vec<(String, DataType)>),
    /// Map type
    Map {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
    },
    /// JSON data (stored as UTF-8 string)
    Json,
    /// Fixed-dimension vector of Float32 values for embeddings
    Vector { dimension: usize },
}

/// Time unit for temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl DataType {
    /// Parse a SQL type string into a DataType.
    ///
    /// Supports common SQL type names like INT, VARCHAR, BOOLEAN, etc.
    pub fn from_sql_type(sql_type: &str) -> Self {
        let sql_type = sql_type.to_uppercase();
        let sql_type = sql_type.trim();

        // Handle parameterized types first
        if sql_type.starts_with("DECIMAL") || sql_type.starts_with("NUMERIC") {
            // Parse DECIMAL(p,s) or DECIMAL(p)
            if let Some(start) = sql_type.find('(') {
                if let Some(end) = sql_type.find(')') {
                    let params = &sql_type[start + 1..end];
                    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
                    let precision = parts.first().and_then(|s| s.parse().ok()).unwrap_or(38);
                    let scale = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                    return DataType::Decimal128 { precision, scale };
                }
            }
            return DataType::Decimal128 {
                precision: 38,
                scale: 0,
            };
        }

        if sql_type.starts_with("VECTOR") {
            if let Some(start) = sql_type.find('(') {
                if let Some(end) = sql_type.find(')') {
                    let dim_str = sql_type[start + 1..end].trim();
                    if let Ok(dimension) = dim_str.parse::<usize>() {
                        return DataType::Vector { dimension };
                    }
                }
            }
            // Default dimension if not specified
            return DataType::Vector { dimension: 128 };
        }

        if sql_type.starts_with("VARCHAR")
            || sql_type.starts_with("CHAR")
            || sql_type.starts_with("TEXT")
        {
            return DataType::Utf8;
        }

        if sql_type.starts_with("TIMESTAMP") {
            // Check for time zone
            if sql_type.contains("WITH TIME ZONE") || sql_type.contains("TZ") {
                return DataType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".to_string()),
                };
            }
            return DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            };
        }

        // Simple type matching
        match sql_type {
            // Boolean
            "BOOL" | "BOOLEAN" => DataType::Boolean,

            // Integers
            "TINYINT" | "INT1" => DataType::Int8,
            "SMALLINT" | "INT2" => DataType::Int16,
            "INT" | "INTEGER" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" | "LONG" => DataType::Int64,

            // Unsigned integers
            "UTINYINT" | "UINT1" => DataType::UInt8,
            "USMALLINT" | "UINT2" => DataType::UInt16,
            "UINT" | "UINTEGER" | "UINT4" => DataType::UInt32,
            "UBIGINT" | "UINT8" | "ULONG" => DataType::UInt64,

            // Floating point
            "REAL" | "FLOAT4" | "FLOAT" => DataType::Float32,
            "DOUBLE" | "FLOAT8" | "DOUBLE PRECISION" => DataType::Float64,

            // Date/Time
            "DATE" => DataType::Date32,
            "TIME" => DataType::Time64Microsecond,
            "INTERVAL" => DataType::IntervalDayTime,

            // Binary
            "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,

            // JSON
            "JSON" | "JSONB" => DataType::Json,

            // Default to Utf8 for unknown types
            _ => DataType::Utf8,
        }
    }

    /// Check if this type is numeric.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128 { .. }
        )
    }

    /// Check if this type is an integer.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }

    /// Check if this type is a floating point.
    pub fn is_floating(&self) -> bool {
        matches!(
            self,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    }

    /// Check if this type is temporal (date/time).
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32Second
                | DataType::Time32Millisecond
                | DataType::Time64Microsecond
                | DataType::Time64Nanosecond
                | DataType::Timestamp { .. }
                | DataType::Duration { .. }
                | DataType::IntervalYearMonth
                | DataType::IntervalDayTime
                | DataType::IntervalMonthDayNano
        )
    }

    /// Check if this type is a string.
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Utf8 | DataType::LargeUtf8)
    }

    /// Check if this type is binary.
    pub fn is_binary(&self) -> bool {
        matches!(self, DataType::Binary | DataType::LargeBinary)
    }

    /// Check if this type is a nested type (list, struct, map).
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            DataType::List(_)
                | DataType::FixedSizeList { .. }
                | DataType::Struct(_)
                | DataType::Map { .. }
        )
    }

    /// Check if this type is JSON.
    pub fn is_json(&self) -> bool {
        matches!(self, DataType::Json)
    }

    /// Check if this type is a vector (embedding).
    pub fn is_vector(&self) -> bool {
        matches!(self, DataType::Vector { .. })
    }

    /// Get the default value for this type (typically null-like).
    pub fn default_value(&self) -> &'static str {
        match self {
            DataType::Boolean => "false",
            DataType::Utf8 | DataType::LargeUtf8 => "''",
            _ if self.is_numeric() => "0",
            _ => "NULL",
        }
    }

    /// Convert to Arrow data type.
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::Null => ArrowDataType::Null,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float16 => ArrowDataType::Float16,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Decimal128 { precision, scale } => {
                ArrowDataType::Decimal128(*precision, *scale)
            }
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
            DataType::Binary => ArrowDataType::Binary,
            DataType::LargeBinary => ArrowDataType::LargeBinary,
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::Time32Second => ArrowDataType::Time32(arrow::datatypes::TimeUnit::Second),
            DataType::Time32Millisecond => {
                ArrowDataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
            }
            DataType::Time64Microsecond => {
                ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            }
            DataType::Time64Nanosecond => {
                ArrowDataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
            }
            DataType::Timestamp { unit, timezone } => {
                ArrowDataType::Timestamp(unit.to_arrow(), timezone.clone().map(Into::into))
            }
            DataType::Duration { unit } => ArrowDataType::Duration(unit.to_arrow()),
            DataType::IntervalYearMonth => {
                ArrowDataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)
            }
            DataType::IntervalDayTime => {
                ArrowDataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
            }
            DataType::IntervalMonthDayNano => {
                ArrowDataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
            }
            DataType::List(inner) => ArrowDataType::List(arrow::datatypes::FieldRef::new(
                arrow::datatypes::Field::new("item", inner.to_arrow(), true),
            )),
            DataType::FixedSizeList { element_type, size } => ArrowDataType::FixedSizeList(
                arrow::datatypes::FieldRef::new(arrow::datatypes::Field::new(
                    "item",
                    element_type.to_arrow(),
                    true,
                )),
                *size,
            ),
            DataType::Struct(fields) => ArrowDataType::Struct(
                fields
                    .iter()
                    .map(|(name, dtype)| arrow::datatypes::Field::new(name, dtype.to_arrow(), true))
                    .collect(),
            ),
            DataType::Map {
                key_type,
                value_type,
            } => {
                let entries_field = arrow::datatypes::Field::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            arrow::datatypes::Field::new("key", key_type.to_arrow(), false),
                            arrow::datatypes::Field::new("value", value_type.to_arrow(), true),
                        ]
                        .into(),
                    ),
                    false,
                );
                ArrowDataType::Map(arrow::datatypes::FieldRef::new(entries_field), false)
            }
            // JSON is stored as UTF-8 string in Arrow
            DataType::Json => ArrowDataType::Utf8,
            DataType::Vector { dimension } => ArrowDataType::FixedSizeList(
                arrow::datatypes::FieldRef::new(arrow::datatypes::Field::new(
                    "item",
                    ArrowDataType::Float32,
                    true,
                )),
                *dimension as i32,
            ),
        }
    }

    /// Convert from Arrow data type.
    pub fn from_arrow(arrow_type: &ArrowDataType) -> Result<Self> {
        match arrow_type {
            ArrowDataType::Null => Ok(DataType::Null),
            ArrowDataType::Boolean => Ok(DataType::Boolean),
            ArrowDataType::Int8 => Ok(DataType::Int8),
            ArrowDataType::Int16 => Ok(DataType::Int16),
            ArrowDataType::Int32 => Ok(DataType::Int32),
            ArrowDataType::Int64 => Ok(DataType::Int64),
            ArrowDataType::UInt8 => Ok(DataType::UInt8),
            ArrowDataType::UInt16 => Ok(DataType::UInt16),
            ArrowDataType::UInt32 => Ok(DataType::UInt32),
            ArrowDataType::UInt64 => Ok(DataType::UInt64),
            ArrowDataType::Float16 => Ok(DataType::Float16),
            ArrowDataType::Float32 => Ok(DataType::Float32),
            ArrowDataType::Float64 => Ok(DataType::Float64),
            ArrowDataType::Decimal128(precision, scale) => Ok(DataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            }),
            ArrowDataType::Utf8 => Ok(DataType::Utf8),
            ArrowDataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            ArrowDataType::Binary => Ok(DataType::Binary),
            ArrowDataType::LargeBinary => Ok(DataType::LargeBinary),
            ArrowDataType::Date32 => Ok(DataType::Date32),
            ArrowDataType::Date64 => Ok(DataType::Date64),
            ArrowDataType::Time32(unit) => match unit {
                arrow::datatypes::TimeUnit::Second => Ok(DataType::Time32Second),
                arrow::datatypes::TimeUnit::Millisecond => Ok(DataType::Time32Millisecond),
                _ => Err(BlazeError::type_error(format!(
                    "Unsupported Time32 unit: {:?}",
                    unit
                ))),
            },
            ArrowDataType::Time64(unit) => match unit {
                arrow::datatypes::TimeUnit::Microsecond => Ok(DataType::Time64Microsecond),
                arrow::datatypes::TimeUnit::Nanosecond => Ok(DataType::Time64Nanosecond),
                _ => Err(BlazeError::type_error(format!(
                    "Unsupported Time64 unit: {:?}",
                    unit
                ))),
            },
            ArrowDataType::Timestamp(unit, tz) => Ok(DataType::Timestamp {
                unit: TimeUnit::from_arrow(*unit),
                timezone: tz.as_ref().map(|s| s.to_string()),
            }),
            ArrowDataType::Duration(unit) => Ok(DataType::Duration {
                unit: TimeUnit::from_arrow(*unit),
            }),
            ArrowDataType::Interval(unit) => match unit {
                arrow::datatypes::IntervalUnit::YearMonth => Ok(DataType::IntervalYearMonth),
                arrow::datatypes::IntervalUnit::DayTime => Ok(DataType::IntervalDayTime),
                arrow::datatypes::IntervalUnit::MonthDayNano => Ok(DataType::IntervalMonthDayNano),
            },
            ArrowDataType::List(field) => {
                let inner = DataType::from_arrow(field.data_type())?;
                Ok(DataType::List(Box::new(inner)))
            }
            ArrowDataType::FixedSizeList(field, size) => {
                // Detect vector type: FixedSizeList of Float32
                if matches!(field.data_type(), ArrowDataType::Float32) {
                    return Ok(DataType::Vector {
                        dimension: *size as usize,
                    });
                }
                let element_type = DataType::from_arrow(field.data_type())?;
                Ok(DataType::FixedSizeList {
                    element_type: Box::new(element_type),
                    size: *size,
                })
            }
            ArrowDataType::Struct(fields) => {
                let mut result = Vec::with_capacity(fields.len());
                for field in fields.iter() {
                    result.push((
                        field.name().clone(),
                        DataType::from_arrow(field.data_type())?,
                    ));
                }
                Ok(DataType::Struct(result))
            }
            _ => Err(BlazeError::type_error(format!(
                "Unsupported Arrow type: {:?}",
                arrow_type
            ))),
        }
    }
}

impl TimeUnit {
    /// Convert to Arrow time unit.
    pub fn to_arrow(self) -> arrow::datatypes::TimeUnit {
        match self {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }

    /// Convert from Arrow time unit.
    pub fn from_arrow(unit: arrow::datatypes::TimeUnit) -> Self {
        match unit {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int8 => write!(f, "TINYINT"),
            DataType::Int16 => write!(f, "SMALLINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::UInt8 => write!(f, "UTINYINT"),
            DataType::UInt16 => write!(f, "USMALLINT"),
            DataType::UInt32 => write!(f, "UINTEGER"),
            DataType::UInt64 => write!(f, "UBIGINT"),
            DataType::Float16 => write!(f, "FLOAT16"),
            DataType::Float32 => write!(f, "REAL"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Decimal128 { precision, scale } => {
                write!(f, "DECIMAL({}, {})", precision, scale)
            }
            DataType::Utf8 | DataType::LargeUtf8 => write!(f, "VARCHAR"),
            DataType::Binary | DataType::LargeBinary => write!(f, "BLOB"),
            DataType::Date32 | DataType::Date64 => write!(f, "DATE"),
            DataType::Time32Second
            | DataType::Time32Millisecond
            | DataType::Time64Microsecond
            | DataType::Time64Nanosecond => write!(f, "TIME"),
            DataType::Timestamp { timezone, .. } => {
                if let Some(tz) = timezone {
                    write!(f, "TIMESTAMP WITH TIME ZONE ({})", tz)
                } else {
                    write!(f, "TIMESTAMP")
                }
            }
            DataType::Duration { .. } => write!(f, "INTERVAL"),
            DataType::IntervalYearMonth
            | DataType::IntervalDayTime
            | DataType::IntervalMonthDayNano => write!(f, "INTERVAL"),
            DataType::List(inner) => write!(f, "{}[]", inner),
            DataType::FixedSizeList { element_type, size } => {
                write!(f, "{}[{}]", element_type, size)
            }
            DataType::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, dtype)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", name, dtype)?;
                }
                write!(f, ")")
            }
            DataType::Map {
                key_type,
                value_type,
            } => write!(f, "MAP({}, {})", key_type, value_type),
            DataType::Json => write!(f, "JSON"),
            DataType::Vector { dimension } => write!(f, "VECTOR({})", dimension),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_predicates() {
        assert!(DataType::Int32.is_numeric());
        assert!(DataType::Int32.is_integer());
        assert!(!DataType::Int32.is_floating());

        assert!(DataType::Float64.is_numeric());
        assert!(!DataType::Float64.is_integer());
        assert!(DataType::Float64.is_floating());

        assert!(DataType::Utf8.is_string());
        assert!(DataType::Date32.is_temporal());
    }

    #[test]
    fn test_arrow_roundtrip() {
        let types = vec![
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
        ];

        for dtype in types {
            let arrow_type = dtype.to_arrow();
            let back = DataType::from_arrow(&arrow_type).unwrap();
            assert_eq!(dtype, back);
        }
    }
}
