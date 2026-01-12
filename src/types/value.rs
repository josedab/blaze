//! Scalar value representation for Blaze.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use super::{DataType, TimeUnit};
use crate::error::{BlazeError, Result};

/// A scalar value that can represent any single data value.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// Null value
    Null,
    /// Boolean value
    Boolean(Option<bool>),
    /// 8-bit signed integer
    Int8(Option<i8>),
    /// 16-bit signed integer
    Int16(Option<i16>),
    /// 32-bit signed integer
    Int32(Option<i32>),
    /// 64-bit signed integer
    Int64(Option<i64>),
    /// 8-bit unsigned integer
    UInt8(Option<u8>),
    /// 16-bit unsigned integer
    UInt16(Option<u16>),
    /// 32-bit unsigned integer
    UInt32(Option<u32>),
    /// 64-bit unsigned integer
    UInt64(Option<u64>),
    /// 32-bit floating point
    Float32(Option<f32>),
    /// 64-bit floating point
    Float64(Option<f64>),
    /// Decimal128 with precision and scale
    Decimal128(Option<i128>, u8, i8),
    /// UTF-8 string
    Utf8(Option<String>),
    /// Large UTF-8 string
    LargeUtf8(Option<String>),
    /// Binary data
    Binary(Option<Vec<u8>>),
    /// Large binary data
    LargeBinary(Option<Vec<u8>>),
    /// Date (days since epoch)
    Date32(Option<i32>),
    /// Date (milliseconds since epoch)
    Date64(Option<i64>),
    /// Timestamp with timezone
    Timestamp {
        value: Option<i64>,
        unit: TimeUnit,
        timezone: Option<String>,
    },
    /// List value
    List(Option<Vec<ScalarValue>>, Box<DataType>),
    /// Struct value
    Struct(Option<Vec<ScalarValue>>, Vec<(String, DataType)>),
    /// JSON value (stored as UTF-8 string)
    Json(Option<String>),
}

impl ScalarValue {
    /// Create a null value of a given type.
    pub fn null_of_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal128 { precision, scale } => {
                ScalarValue::Decimal128(None, *precision, *scale)
            }
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Binary => ScalarValue::Binary(None),
            DataType::LargeBinary => ScalarValue::LargeBinary(None),
            DataType::Date32 => ScalarValue::Date32(None),
            DataType::Date64 => ScalarValue::Date64(None),
            DataType::Timestamp { unit, timezone } => ScalarValue::Timestamp {
                value: None,
                unit: *unit,
                timezone: timezone.clone(),
            },
            DataType::List(inner) => ScalarValue::List(None, inner.clone()),
            DataType::Struct(fields) => ScalarValue::Struct(None, fields.clone()),
            DataType::Json => ScalarValue::Json(None),
            _ => ScalarValue::Null,
        }
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Null => true,
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::LargeUtf8(v) => v.is_none(),
            ScalarValue::Binary(v) => v.is_none(),
            ScalarValue::LargeBinary(v) => v.is_none(),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::Timestamp { value, .. } => value.is_none(),
            ScalarValue::List(v, _) => v.is_none(),
            ScalarValue::Struct(v, _) => v.is_none(),
            ScalarValue::Json(v) => v.is_none(),
        }
    }

    /// Get the data type of this value.
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Decimal128(_, precision, scale) => DataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Timestamp {
                unit, timezone, ..
            } => DataType::Timestamp {
                unit: *unit,
                timezone: timezone.clone(),
            },
            ScalarValue::List(_, inner) => DataType::List(inner.clone()),
            ScalarValue::Struct(_, fields) => DataType::Struct(fields.clone()),
            ScalarValue::Json(_) => DataType::Json,
        }
    }

    /// Try to convert to i64.
    pub fn try_as_i64(&self) -> Result<Option<i64>> {
        match self {
            ScalarValue::Int8(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int16(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int32(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::Int64(v) => Ok(*v),
            ScalarValue::UInt8(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt16(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt32(v) => Ok(v.map(|x| x as i64)),
            ScalarValue::UInt64(v) => {
                v.map(|x| {
                    if x <= i64::MAX as u64 {
                        Ok(x as i64)
                    } else {
                        Err(BlazeError::type_error("UInt64 value too large for i64"))
                    }
                })
                .transpose()
            }
            _ => Err(BlazeError::type_error(format!(
                "Cannot convert {:?} to i64",
                self.data_type()
            ))),
        }
    }

    /// Try to convert to f64.
    pub fn try_as_f64(&self) -> Result<Option<f64>> {
        match self {
            ScalarValue::Float32(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::Float64(v) => Ok(*v),
            ScalarValue::Int8(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::Int16(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::Int32(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::Int64(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::UInt8(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::UInt16(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::UInt32(v) => Ok(v.map(|x| x as f64)),
            ScalarValue::UInt64(v) => Ok(v.map(|x| x as f64)),
            _ => Err(BlazeError::type_error(format!(
                "Cannot convert {:?} to f64",
                self.data_type()
            ))),
        }
    }

    /// Try to convert to string.
    pub fn try_as_string(&self) -> Result<Option<String>> {
        match self {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => Ok(v.clone()),
            _ => Err(BlazeError::type_error(format!(
                "Cannot convert {:?} to string",
                self.data_type()
            ))),
        }
    }

    /// Try to convert to bool.
    pub fn try_as_bool(&self) -> Result<Option<bool>> {
        match self {
            ScalarValue::Boolean(v) => Ok(*v),
            _ => Err(BlazeError::type_error(format!(
                "Cannot convert {:?} to bool",
                self.data_type()
            ))),
        }
    }

    /// Convert to an Arrow array with a single element.
    pub fn to_array(&self) -> Result<ArrayRef> {
        self.to_array_of_size(1)
    }

    /// Convert to an Arrow array with multiple copies of this value.
    pub fn to_array_of_size(&self, size: usize) -> Result<ArrayRef> {
        use arrow::array::*;

        Ok(match self {
            ScalarValue::Null => std::sync::Arc::new(NullArray::new(size)),
            ScalarValue::Boolean(v) => {
                let arr: BooleanArray = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Int8(v) => {
                let arr: Int8Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Int16(v) => {
                let arr: Int16Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Int32(v) => {
                let arr: Int32Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Int64(v) => {
                let arr: Int64Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::UInt8(v) => {
                let arr: UInt8Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::UInt16(v) => {
                let arr: UInt16Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::UInt32(v) => {
                let arr: UInt32Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::UInt64(v) => {
                let arr: UInt64Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Float32(v) => {
                let arr: Float32Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Float64(v) => {
                let arr: Float64Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Utf8(v) => {
                let arr: StringArray = (0..size).map(|_| v.as_deref()).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::LargeUtf8(v) => {
                let arr: LargeStringArray = (0..size).map(|_| v.as_deref()).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Binary(v) => {
                let arr: arrow::array::BinaryArray =
                    (0..size).map(|_| v.as_deref()).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::LargeBinary(v) => {
                let arr: LargeBinaryArray = (0..size).map(|_| v.as_deref()).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Date32(v) => {
                let arr: Date32Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Date64(v) => {
                let arr: arrow::array::Date64Array = (0..size).map(|_| *v).collect();
                std::sync::Arc::new(arr)
            }
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            } => {
                let tz: Option<Arc<str>> = timezone.clone().map(|s| s.into());
                match unit {
                    TimeUnit::Second => {
                        let arr: TimestampSecondArray = (0..size).map(|_| *value).collect();
                        std::sync::Arc::new(arr.with_timezone_opt(tz))
                    }
                    TimeUnit::Millisecond => {
                        let arr: TimestampMillisecondArray = (0..size).map(|_| *value).collect();
                        std::sync::Arc::new(arr.with_timezone_opt(tz))
                    }
                    TimeUnit::Microsecond => {
                        let arr: TimestampMicrosecondArray = (0..size).map(|_| *value).collect();
                        std::sync::Arc::new(arr.with_timezone_opt(tz))
                    }
                    TimeUnit::Nanosecond => {
                        let arr: TimestampNanosecondArray = (0..size).map(|_| *value).collect();
                        std::sync::Arc::new(arr.with_timezone_opt(tz))
                    }
                }
            }
            // JSON is stored as UTF-8 string
            ScalarValue::Json(v) => {
                let arr: StringArray = (0..size).map(|_| v.as_deref()).collect();
                std::sync::Arc::new(arr)
            }
            _ => {
                return Err(BlazeError::not_implemented(format!(
                    "to_array for {:?}",
                    self.data_type()
                )))
            }
        })
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => true,
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a == b,
            (ScalarValue::Int16(a), ScalarValue::Int16(b)) => a == b,
            (ScalarValue::Int32(a), ScalarValue::Int32(b)) => a == b,
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a == b,
            (ScalarValue::UInt8(a), ScalarValue::UInt8(b)) => a == b,
            (ScalarValue::UInt16(a), ScalarValue::UInt16(b)) => a == b,
            (ScalarValue::UInt32(a), ScalarValue::UInt32(b)) => a == b,
            (ScalarValue::UInt64(a), ScalarValue::UInt64(b)) => a == b,
            (ScalarValue::Float32(a), ScalarValue::Float32(b)) => match (a, b) {
                (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                (None, None) => true,
                _ => false,
            },
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => match (a, b) {
                (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
                (None, None) => true,
                _ => false,
            },
            (ScalarValue::Decimal128(a, p1, s1), ScalarValue::Decimal128(b, p2, s2)) => {
                a == b && p1 == p2 && s1 == s2
            }
            (ScalarValue::Utf8(a), ScalarValue::Utf8(b)) => a == b,
            (ScalarValue::LargeUtf8(a), ScalarValue::LargeUtf8(b)) => a == b,
            (ScalarValue::Binary(a), ScalarValue::Binary(b)) => a == b,
            (ScalarValue::LargeBinary(a), ScalarValue::LargeBinary(b)) => a == b,
            (ScalarValue::Date32(a), ScalarValue::Date32(b)) => a == b,
            (ScalarValue::Date64(a), ScalarValue::Date64(b)) => a == b,
            (
                ScalarValue::Timestamp {
                    value: v1,
                    unit: u1,
                    timezone: t1,
                },
                ScalarValue::Timestamp {
                    value: v2,
                    unit: u2,
                    timezone: t2,
                },
            ) => v1 == v2 && u1 == u2 && t1 == t2,
            (ScalarValue::Json(a), ScalarValue::Json(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Null => {}
            ScalarValue::Boolean(v) => v.hash(state),
            ScalarValue::Int8(v) => v.hash(state),
            ScalarValue::Int16(v) => v.hash(state),
            ScalarValue::Int32(v) => v.hash(state),
            ScalarValue::Int64(v) => v.hash(state),
            ScalarValue::UInt8(v) => v.hash(state),
            ScalarValue::UInt16(v) => v.hash(state),
            ScalarValue::UInt32(v) => v.hash(state),
            ScalarValue::UInt64(v) => v.hash(state),
            ScalarValue::Float32(v) => v.map(|f| f.to_bits()).hash(state),
            ScalarValue::Float64(v) => v.map(|f| f.to_bits()).hash(state),
            ScalarValue::Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v.hash(state),
            ScalarValue::Binary(v) | ScalarValue::LargeBinary(v) => v.hash(state),
            ScalarValue::Date32(v) => v.hash(state),
            ScalarValue::Date64(v) => v.hash(state),
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            } => {
                value.hash(state);
                unit.hash(state);
                timezone.hash(state);
            }
            ScalarValue::List(v, dt) => {
                if let Some(list) = v {
                    for item in list {
                        item.hash(state);
                    }
                }
                dt.hash(state);
            }
            ScalarValue::Struct(v, fields) => {
                if let Some(list) = v {
                    for item in list {
                        item.hash(state);
                    }
                }
                for (name, dt) in fields {
                    name.hash(state);
                    dt.hash(state);
                }
            }
            ScalarValue::Json(v) => v.hash(state),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{}", v),
            ScalarValue::Boolean(None) => write!(f, "NULL"),
            ScalarValue::Int8(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int8(None) => write!(f, "NULL"),
            ScalarValue::Int16(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int16(None) => write!(f, "NULL"),
            ScalarValue::Int32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int32(None) => write!(f, "NULL"),
            ScalarValue::Int64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int64(None) => write!(f, "NULL"),
            ScalarValue::UInt8(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt8(None) => write!(f, "NULL"),
            ScalarValue::UInt16(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt16(None) => write!(f, "NULL"),
            ScalarValue::UInt32(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt32(None) => write!(f, "NULL"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt64(None) => write!(f, "NULL"),
            ScalarValue::Float32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float32(None) => write!(f, "NULL"),
            ScalarValue::Float64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float64(None) => write!(f, "NULL"),
            ScalarValue::Decimal128(Some(v), _, scale) => {
                let scale = *scale as usize;
                if scale == 0 {
                    write!(f, "{}", v)
                } else {
                    let s = format!("{:0>width$}", v.abs(), width = scale + 1);
                    let (int, frac) = s.split_at(s.len() - scale);
                    if *v < 0 {
                        write!(f, "-{}.{}", int, frac)
                    } else {
                        write!(f, "{}.{}", int, frac)
                    }
                }
            }
            ScalarValue::Decimal128(None, _, _) => write!(f, "NULL"),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => write!(f, "'{}'", v),
            ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => write!(f, "NULL"),
            ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => {
                write!(f, "X'")?;
                for byte in v {
                    write!(f, "{:02X}", byte)?;
                }
                write!(f, "'")
            }
            ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => write!(f, "NULL"),
            ScalarValue::Date32(Some(v)) => write!(f, "DATE '{}'", v),
            ScalarValue::Date32(None) => write!(f, "NULL"),
            ScalarValue::Date64(Some(v)) => write!(f, "DATE '{}'", v),
            ScalarValue::Date64(None) => write!(f, "NULL"),
            ScalarValue::Timestamp {
                value: Some(v),
                unit,
                ..
            } => {
                let unit_str = match unit {
                    TimeUnit::Second => "s",
                    TimeUnit::Millisecond => "ms",
                    TimeUnit::Microsecond => "us",
                    TimeUnit::Nanosecond => "ns",
                };
                write!(f, "TIMESTAMP '{}' {}", v, unit_str)
            }
            ScalarValue::Timestamp { value: None, .. } => write!(f, "NULL"),
            ScalarValue::List(Some(values), _) => {
                write!(f, "[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            ScalarValue::List(None, _) => write!(f, "NULL"),
            ScalarValue::Struct(Some(values), fields) => {
                write!(f, "{{")?;
                for (i, (v, (name, _))) in values.iter().zip(fields.iter()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, v)?;
                }
                write!(f, "}}")
            }
            ScalarValue::Struct(None, _) => write!(f, "NULL"),
            ScalarValue::Json(Some(v)) => write!(f, "'{}'", v),
            ScalarValue::Json(None) => write!(f, "NULL"),
        }
    }
}

// Convenience conversion implementations
impl From<bool> for ScalarValue {
    fn from(v: bool) -> Self {
        ScalarValue::Boolean(Some(v))
    }
}

impl From<i32> for ScalarValue {
    fn from(v: i32) -> Self {
        ScalarValue::Int32(Some(v))
    }
}

impl From<i64> for ScalarValue {
    fn from(v: i64) -> Self {
        ScalarValue::Int64(Some(v))
    }
}

impl From<f64> for ScalarValue {
    fn from(v: f64) -> Self {
        ScalarValue::Float64(Some(v))
    }
}

impl From<&str> for ScalarValue {
    fn from(v: &str) -> Self {
        ScalarValue::Utf8(Some(v.to_string()))
    }
}

impl From<String> for ScalarValue {
    fn from(v: String) -> Self {
        ScalarValue::Utf8(Some(v))
    }
}

impl From<Option<String>> for ScalarValue {
    fn from(v: Option<String>) -> Self {
        ScalarValue::Utf8(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_value_display() {
        assert_eq!(format!("{}", ScalarValue::Int32(Some(42))), "42");
        assert_eq!(format!("{}", ScalarValue::Utf8(Some("hello".into()))), "'hello'");
        assert_eq!(format!("{}", ScalarValue::Boolean(Some(true))), "true");
        assert_eq!(format!("{}", ScalarValue::Null), "NULL");
    }

    #[test]
    fn test_scalar_value_data_type() {
        assert_eq!(ScalarValue::Int32(Some(42)).data_type(), DataType::Int32);
        assert_eq!(
            ScalarValue::Utf8(Some("hello".into())).data_type(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_scalar_value_is_null() {
        assert!(ScalarValue::Null.is_null());
        assert!(ScalarValue::Int32(None).is_null());
        assert!(!ScalarValue::Int32(Some(42)).is_null());
    }

    #[test]
    fn test_scalar_value_conversions() {
        let v = ScalarValue::Int64(Some(42));
        assert_eq!(v.try_as_i64().unwrap(), Some(42));

        let v = ScalarValue::Float64(Some(3.14));
        assert_eq!(v.try_as_f64().unwrap(), Some(3.14));

        let v = ScalarValue::Utf8(Some("hello".into()));
        assert_eq!(v.try_as_string().unwrap(), Some("hello".to_string()));
    }
}
