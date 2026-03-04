//! Hash aggregate operator and accumulators.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};

use super::hash_join::JoinHashTable;
use super::try_downcast;

/// Legacy HashTable for backward compatibility.
pub struct HashTable {
    /// Map from hash key to row indices
    map: HashMap<u64, Vec<usize>>,
    /// The batches stored in the hash table
    batches: Vec<RecordBatch>,
}

impl HashTable {
    /// Create a new hash table from record batches.
    pub fn build(batches: Vec<RecordBatch>, key_indices: &[usize]) -> Result<Self> {
        let mut map: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut row_offset = 0;

        for batch in &batches {
            for row in 0..batch.num_rows() {
                let hash = Self::compute_hash(batch, row, key_indices)?;
                map.entry(hash).or_default().push(row_offset + row);
            }
            row_offset += batch.num_rows();
        }

        Ok(Self { map, batches })
    }

    /// Probe the hash table with a key.
    pub fn probe(&self, hash: u64) -> Option<&Vec<usize>> {
        self.map.get(&hash)
    }

    /// Get a row from the stored batches.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<ArrayRef>> {
        let mut offset = 0;
        for batch in &self.batches {
            if row_idx < offset + batch.num_rows() {
                let local_idx = row_idx - offset;
                return Some(
                    batch
                        .columns()
                        .iter()
                        .map(|col| col.slice(local_idx, 1))
                        .collect(),
                );
            }
            offset += batch.num_rows();
        }
        None
    }

    fn compute_hash(batch: &RecordBatch, row: usize, key_indices: &[usize]) -> Result<u64> {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        for &idx in key_indices {
            let col = batch.column(idx);
            Self::hash_value(col, row, &mut hasher)?;
        }

        Ok(hasher.finish())
    }

    fn hash_value<H: Hasher>(array: &ArrayRef, row: usize, hasher: &mut H) -> Result<()> {
        if array.is_null(row) {
            0u8.hash(hasher);
            return Ok(());
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = try_downcast::<arrow::array::BooleanArray>(array, "BooleanArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int8 => {
                let arr = try_downcast::<Int8Array>(array, "Int8Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int16 => {
                let arr = try_downcast::<Int16Array>(array, "Int16Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(array, "Int32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(array, "Int64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::UInt32 => {
                let arr = try_downcast::<UInt32Array>(array, "UInt32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::UInt64 => {
                let arr = try_downcast::<UInt64Array>(array, "UInt64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(array, "Float32Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Date32 => {
                let arr = try_downcast::<Date32Array>(array, "Date32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Date64 => {
                let arr = try_downcast::<Date64Array>(array, "Date64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Decimal128(_, _) => {
                let arr = try_downcast::<Decimal128Array>(array, "Decimal128Array")?;
                arr.value(row).hash(hasher);
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "GROUP BY hash for type {:?}. Supported types: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Decimal128.",
                    dt
                )));
            }
        }

        Ok(())
    }
}

/// Aggregate accumulator trait.
pub trait Accumulator: Send + Sync {
    /// Update the accumulator with new values.
    fn update(&mut self, values: &ArrayRef) -> Result<()>;

    /// Merge with another accumulator.
    fn merge(&mut self, other: &dyn Accumulator) -> Result<()>;

    /// Finalize and return the result.
    fn finalize(&self) -> Result<ArrayRef>;

    /// Reset the accumulator.
    fn reset(&mut self);

    /// Return self as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// COUNT accumulator.
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        // Count non-null values
        self.count += (values.len() - values.null_count()) as i64;
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<CountAccumulator>() {
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.count])))
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// SUM accumulator for numeric types.
pub struct SumAccumulator {
    sum: Option<f64>,
    int_sum: Option<i128>,
    is_int: bool,
    decimal_sum: Option<i128>,
    decimal_precision: u8,
    decimal_scale: i8,
    is_decimal: bool,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self {
            sum: None,
            int_sum: None,
            is_int: false,
            decimal_sum: None,
            decimal_precision: 38,
            decimal_scale: 0,
            is_decimal: false,
        }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                self.is_int = true;
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                if let Some(s) = compute::sum(arr) {
                    self.int_sum = Some(self.int_sum.unwrap_or(0) + s as i128);
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                if let Some(s) = compute::sum(arr) {
                    self.sum = Some(self.sum.unwrap_or(0.0) + s);
                }
            }
            DataType::Int32 => {
                self.is_int = true;
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                if let Some(s) = compute::sum(arr) {
                    self.int_sum = Some(self.int_sum.unwrap_or(0) + s as i128);
                }
            }
            DataType::Decimal128(p, s) => {
                self.is_decimal = true;
                self.decimal_precision = *p;
                self.decimal_scale = *s;
                let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.decimal_sum = Some(self.decimal_sum.unwrap_or(0) + arr.value(i));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<SumAccumulator>() {
            if other.is_decimal {
                self.is_decimal = true;
                self.decimal_precision = other.decimal_precision;
                self.decimal_scale = other.decimal_scale;
                if let Some(s) = other.decimal_sum {
                    self.decimal_sum = Some(self.decimal_sum.unwrap_or(0) + s);
                }
            } else if other.is_int {
                self.is_int = true;
                if let Some(s) = other.int_sum {
                    self.int_sum = Some(self.int_sum.unwrap_or(0) + s);
                }
            } else if let Some(s) = other.sum {
                self.sum = Some(self.sum.unwrap_or(0.0) + s);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.is_decimal {
            let arr = match self.decimal_sum {
                Some(v) => Decimal128Array::from(vec![Some(v)]),
                None => Decimal128Array::from(vec![Option::<i128>::None]),
            };
            Ok(Arc::new(
                arr.with_precision_and_scale(self.decimal_precision, self.decimal_scale)
                    .map_err(|e| {
                        BlazeError::execution(format!("Decimal precision error: {}", e))
                    })?,
            ))
        } else if self.is_int {
            Ok(Arc::new(Int64Array::from(vec![
                self.int_sum.map(|v| v as i64),
            ])))
        } else {
            Ok(Arc::new(Float64Array::from(vec![self.sum])))
        }
    }

    fn reset(&mut self) {
        self.sum = None;
        self.int_sum = None;
        self.is_int = false;
        self.decimal_sum = None;
        self.is_decimal = false;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Decimal-preserving SUM accumulator. Accumulates as i128 and returns Decimal128.
pub struct DecimalSumAccumulator {
    sum: Option<i128>,
    precision: u8,
    scale: i8,
}

impl DecimalSumAccumulator {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            sum: None,
            precision,
            scale,
        }
    }
}

impl Accumulator for DecimalSumAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        if let DataType::Decimal128(_, _) = values.data_type() {
            let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    self.sum = Some(self.sum.unwrap_or(0) + arr.value(i));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<DecimalSumAccumulator>() {
            if let Some(s) = other.sum {
                self.sum = Some(self.sum.unwrap_or(0) + s);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let arr = match self.sum {
            Some(v) => Decimal128Array::from(vec![Some(v)]),
            None => Decimal128Array::from(vec![Option::<i128>::None]),
        };
        Ok(Arc::new(
            arr.with_precision_and_scale(self.precision, self.scale)
                .map_err(|e| BlazeError::execution(format!("Decimal precision error: {}", e)))?,
        ))
    }

    fn reset(&mut self) {
        self.sum = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Decimal-preserving MIN accumulator.
pub struct DecimalMinAccumulator {
    min: Option<i128>,
    precision: u8,
    scale: i8,
}

impl DecimalMinAccumulator {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            min: None,
            precision,
            scale,
        }
    }
}

impl Accumulator for DecimalMinAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        if let DataType::Decimal128(_, _) = values.data_type() {
            let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    self.min = Some(self.min.map(|m| m.min(v)).unwrap_or(v));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<DecimalMinAccumulator>() {
            if let Some(m) = other.min {
                self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let arr = match self.min {
            Some(v) => Decimal128Array::from(vec![Some(v)]),
            None => Decimal128Array::from(vec![Option::<i128>::None]),
        };
        Ok(Arc::new(
            arr.with_precision_and_scale(self.precision, self.scale)
                .map_err(|e| BlazeError::execution(format!("Decimal precision error: {}", e)))?,
        ))
    }

    fn reset(&mut self) {
        self.min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Decimal-preserving MAX accumulator.
pub struct DecimalMaxAccumulator {
    max: Option<i128>,
    precision: u8,
    scale: i8,
}

impl DecimalMaxAccumulator {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            max: None,
            precision,
            scale,
        }
    }
}

impl Accumulator for DecimalMaxAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        if let DataType::Decimal128(_, _) = values.data_type() {
            let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    self.max = Some(self.max.map(|m| m.max(v)).unwrap_or(v));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<DecimalMaxAccumulator>() {
            if let Some(m) = other.max {
                self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let arr = match self.max {
            Some(v) => Decimal128Array::from(vec![Some(v)]),
            None => Decimal128Array::from(vec![Option::<i128>::None]),
        };
        Ok(Arc::new(
            arr.with_precision_and_scale(self.precision, self.scale)
                .map_err(|e| BlazeError::execution(format!("Decimal precision error: {}", e)))?,
        ))
    }

    fn reset(&mut self) {
        self.max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// AVG accumulator.
pub struct AvgAccumulator {
    sum: f64,
    count: u64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let (sum, count) = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                let sum = compute::sum(arr).unwrap_or(0) as f64;
                let count = (arr.len() - arr.null_count()) as u64;
                (sum, count)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                let sum = compute::sum(arr).unwrap_or(0.0);
                let count = (arr.len() - arr.null_count()) as u64;
                (sum, count)
            }
            DataType::Decimal128(_, _) => {
                let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
                let mut sum: f64 = 0.0;
                let mut count: u64 = 0;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sum += arr.value(i) as f64;
                        count += 1;
                    }
                }
                (sum, count)
            }
            _ => (0.0, 0),
        };

        self.sum += sum;
        self.count += count;

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<AvgAccumulator>() {
            self.sum += other.sum;
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let avg = if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        };
        Ok(Arc::new(Float64Array::from(vec![avg])))
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MIN accumulator.
pub struct MinAccumulator {
    min: Option<f64>,
    decimal_min: Option<i128>,
    decimal_precision: u8,
    decimal_scale: i8,
    is_decimal: bool,
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self {
            min: None,
            decimal_min: None,
            decimal_precision: 38,
            decimal_scale: 0,
            is_decimal: false,
        }
    }
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MinAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                if let Some(m) = compute::min(arr).map(|v| v as f64) {
                    self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                if let Some(m) = compute::min(arr) {
                    self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
                }
            }
            DataType::Decimal128(p, s) => {
                self.is_decimal = true;
                self.decimal_precision = *p;
                self.decimal_scale = *s;
                let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        self.decimal_min =
                            Some(self.decimal_min.map(|m| m.min(v)).unwrap_or(v));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MinAccumulator>() {
            if other.is_decimal {
                self.is_decimal = true;
                self.decimal_precision = other.decimal_precision;
                self.decimal_scale = other.decimal_scale;
                if let Some(m) = other.decimal_min {
                    self.decimal_min = Some(self.decimal_min.map(|c| c.min(m)).unwrap_or(m));
                }
            } else if let Some(m) = other.min {
                self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.is_decimal {
            let arr = match self.decimal_min {
                Some(v) => Decimal128Array::from(vec![Some(v)]),
                None => Decimal128Array::from(vec![Option::<i128>::None]),
            };
            Ok(Arc::new(
                arr.with_precision_and_scale(self.decimal_precision, self.decimal_scale)
                    .map_err(|e| {
                        BlazeError::execution(format!("Decimal precision error: {}", e))
                    })?,
            ))
        } else {
            Ok(Arc::new(Float64Array::from(vec![self.min])))
        }
    }

    fn reset(&mut self) {
        self.min = None;
        self.decimal_min = None;
        self.is_decimal = false;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MAX accumulator.
pub struct MaxAccumulator {
    max: Option<f64>,
    decimal_max: Option<i128>,
    decimal_precision: u8,
    decimal_scale: i8,
    is_decimal: bool,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self {
            max: None,
            decimal_max: None,
            decimal_precision: 38,
            decimal_scale: 0,
            is_decimal: false,
        }
    }
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MaxAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                if let Some(m) = compute::max(arr).map(|v| v as f64) {
                    self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                if let Some(m) = compute::max(arr) {
                    self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
                }
            }
            DataType::Decimal128(p, s) => {
                self.is_decimal = true;
                self.decimal_precision = *p;
                self.decimal_scale = *s;
                let arr = try_downcast::<Decimal128Array>(values, "Decimal128Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        self.decimal_max =
                            Some(self.decimal_max.map(|m| m.max(v)).unwrap_or(v));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MaxAccumulator>() {
            if other.is_decimal {
                self.is_decimal = true;
                self.decimal_precision = other.decimal_precision;
                self.decimal_scale = other.decimal_scale;
                if let Some(m) = other.decimal_max {
                    self.decimal_max = Some(self.decimal_max.map(|c| c.max(m)).unwrap_or(m));
                }
            } else if let Some(m) = other.max {
                self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.is_decimal {
            let arr = match self.decimal_max {
                Some(v) => Decimal128Array::from(vec![Some(v)]),
                None => Decimal128Array::from(vec![Option::<i128>::None]),
            };
            Ok(Arc::new(
                arr.with_precision_and_scale(self.decimal_precision, self.decimal_scale)
                    .map_err(|e| {
                        BlazeError::execution(format!("Decimal precision error: {}", e))
                    })?,
            ))
        } else {
            Ok(Arc::new(Float64Array::from(vec![self.max])))
        }
    }

    fn reset(&mut self) {
        self.max = None;
        self.decimal_max = None;
        self.is_decimal = false;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// COUNT(DISTINCT) accumulator using type-specific hashing for performance.
/// Uses i64 HashSet for integers, u64 bits for floats, String for others.
pub struct CountDistinctAccumulator {
    int_seen: Option<std::collections::HashSet<i64>>,
    float_seen: Option<std::collections::HashSet<u64>>,
    str_seen: Option<std::collections::HashSet<String>>,
}

impl CountDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            int_seen: None,
            float_seen: None,
            str_seen: None,
        }
    }

    fn count(&self) -> usize {
        self.int_seen.as_ref().map(|s| s.len()).unwrap_or(0)
            + self.float_seen.as_ref().map(|s| s.len()).unwrap_or(0)
            + self.str_seen.as_ref().map(|s| s.len()).unwrap_or(0)
    }
}

impl Default for CountDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for CountDistinctAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                let set = self.int_seen.get_or_insert_with(std::collections::HashSet::new);
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        set.insert(arr.value(i));
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                let set = self.int_seen.get_or_insert_with(std::collections::HashSet::new);
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        set.insert(arr.value(i) as i64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                let set = self.float_seen.get_or_insert_with(std::collections::HashSet::new);
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        set.insert(arr.value(i).to_bits());
                    }
                }
            }
            _ => {
                let set = self.str_seen.get_or_insert_with(std::collections::HashSet::new);
                for i in 0..values.len() {
                    if !values.is_null(i) {
                        let s = arrow::util::display::array_value_to_string(values, i)
                            .unwrap_or_default();
                        set.insert(s);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<CountDistinctAccumulator>() {
            if let Some(other_ints) = &other.int_seen {
                let set = self.int_seen.get_or_insert_with(std::collections::HashSet::new);
                set.extend(other_ints);
            }
            if let Some(other_floats) = &other.float_seen {
                let set = self.float_seen.get_or_insert_with(std::collections::HashSet::new);
                set.extend(other_floats);
            }
            if let Some(other_strs) = &other.str_seen {
                let set = self.str_seen.get_or_insert_with(std::collections::HashSet::new);
                set.extend(other_strs.iter().cloned());
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.count() as i64])))
    }

    fn reset(&mut self) {
        self.int_seen = None;
        self.float_seen = None;
        self.str_seen = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// STRING_AGG accumulator — concatenates string values with a separator.
pub struct StringAggAccumulator {
    values: Vec<String>,
    separator: String,
}

impl StringAggAccumulator {
    pub fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }
}

impl Accumulator for StringAggAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(arr) = values.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    self.values.push(arr.value(i).to_string());
                }
            }
        } else {
            // For non-string types, convert to string representation
            for i in 0..values.len() {
                if !values.is_null(i) {
                    let s = arrow::util::display::array_value_to_string(values, i)
                        .unwrap_or_default();
                    self.values.push(s);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<StringAggAccumulator>() {
            self.values.extend(other.values.iter().cloned());
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.values.is_empty() {
            Ok(Arc::new(StringArray::from(vec![Option::<&str>::None])))
        } else {
            let result = self.values.join(&self.separator);
            Ok(Arc::new(StringArray::from(vec![Some(result.as_str())])))
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// ARRAY_AGG accumulator — collects values into a JSON array string.
pub struct ArrayAggAccumulator {
    values: Vec<String>,
}

impl ArrayAggAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Default for ArrayAggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        for i in 0..values.len() {
            if !values.is_null(i) {
                let s = arrow::util::display::array_value_to_string(values, i)
                    .unwrap_or_default();
                self.values.push(s);
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<ArrayAggAccumulator>() {
            self.values.extend(other.values.iter().cloned());
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        use arrow::array::ListArray;
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field as ArrowField;

        // Build a ListArray with a single list element containing all values
        let values_array = StringArray::from(
            self.values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        );
        let field = Arc::new(ArrowField::new("item", arrow::datatypes::DataType::Utf8, true));
        let offsets = OffsetBuffer::from_lengths([self.values.len()]);
        let list_array = ListArray::new(field, offsets, Arc::new(values_array), None);
        Ok(Arc::new(list_array))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// APPROX_COUNT_DISTINCT accumulator using HyperLogLog.
pub struct ApproxCountDistinctAccumulator {
    hll: crate::approx::HyperLogLog,
}

impl ApproxCountDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            hll: crate::approx::HyperLogLog::default_precision(),
        }
    }
}

impl Default for ApproxCountDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for ApproxCountDistinctAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int8 => {
                let arr = try_downcast::<Int8Array>(values, "Int8Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i) as i64);
                    }
                }
            }
            DataType::Int16 => {
                let arr = try_downcast::<Int16Array>(values, "Int16Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i) as i64);
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i32(arr.value(i));
                    }
                }
            }
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i));
                    }
                }
            }
            DataType::UInt32 => {
                let arr = try_downcast::<UInt32Array>(values, "UInt32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_u64(arr.value(i) as u64);
                    }
                }
            }
            DataType::UInt64 => {
                let arr = try_downcast::<UInt64Array>(values, "UInt64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_u64(arr.value(i));
                    }
                }
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(values, "Float32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_f64(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_f64(arr.value(i));
                    }
                }
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(values, "StringArray")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_str(arr.value(i));
                    }
                }
            }
            DataType::Date32 => {
                let arr = try_downcast::<Date32Array>(values, "Date32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i) as i64);
                    }
                }
            }
            DataType::Date64 => {
                let arr = try_downcast::<Date64Array>(values, "Date64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i));
                    }
                }
            }
            _ => {
                // For any other type, hash the string representation
                for i in 0..values.len() {
                    if !values.is_null(i) {
                        self.hll.add(&i);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other
            .as_any()
            .downcast_ref::<ApproxCountDistinctAccumulator>()
        {
            self.hll.merge(&other.hll);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let estimate = self.hll.estimate() as i64;
        Ok(Arc::new(Int64Array::from(vec![estimate])))
    }

    fn reset(&mut self) {
        self.hll = crate::approx::HyperLogLog::default_precision();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// APPROX_PERCENTILE accumulator using T-Digest.
pub struct ApproxPercentileAccumulator {
    tdigest: crate::approx::TDigest,
    percentile: f64,
}

impl ApproxPercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            tdigest: crate::approx::TDigest::default_compression(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }

    pub fn median() -> Self {
        Self::new(0.5)
    }
}

impl Default for ApproxPercentileAccumulator {
    fn default() -> Self {
        Self::median()
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int8 => {
                let arr = try_downcast::<Int8Array>(values, "Int8Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Int16 => {
                let arr = try_downcast::<Int16Array>(values, "Int16Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::UInt32 => {
                let arr = try_downcast::<UInt32Array>(values, "UInt32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::UInt64 => {
                let arr = try_downcast::<UInt64Array>(values, "UInt64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(values, "Float32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i));
                    }
                }
            }
            _ => {
                return Err(BlazeError::execution(format!(
                    "APPROX_PERCENTILE does not support type {:?}",
                    values.data_type()
                )));
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<ApproxPercentileAccumulator>() {
            self.tdigest.merge(&other.tdigest);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let mut td = self.tdigest.clone();
        let result = td.quantile(self.percentile);
        Ok(Arc::new(Float64Array::from(vec![result])))
    }

    fn reset(&mut self) {
        self.tdigest = crate::approx::TDigest::default_compression();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Variance accumulator using Welford's online algorithm.
/// Supports both population and sample variance.
pub struct VarianceAccumulator {
    count: u64,
    mean: f64,
    m2: f64,
    is_population: bool,
}

impl VarianceAccumulator {
    pub fn population() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_population: true,
        }
    }

    pub fn sample() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_population: false,
        }
    }

    fn update_value(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }
}

impl Accumulator for VarianceAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.update_value(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.update_value(arr.value(i));
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.update_value(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(values, "Float32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.update_value(arr.value(i) as f64);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<VarianceAccumulator>() {
            if other.count == 0 {
                return Ok(());
            }
            if self.count == 0 {
                self.count = other.count;
                self.mean = other.mean;
                self.m2 = other.m2;
                return Ok(());
            }
            let combined_count = self.count + other.count;
            let delta = other.mean - self.mean;
            let new_mean = self.mean
                + delta * other.count as f64 / combined_count as f64;
            let new_m2 = self.m2
                + other.m2
                + delta * delta * self.count as f64 * other.count as f64
                    / combined_count as f64;
            self.count = combined_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let result = if self.count == 0 {
            None
        } else if self.is_population {
            Some(self.m2 / self.count as f64)
        } else if self.count == 1 {
            None
        } else {
            Some(self.m2 / (self.count - 1) as f64)
        };
        Ok(Arc::new(Float64Array::from(vec![result])))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Standard deviation accumulator (wraps VarianceAccumulator, takes sqrt).
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    pub fn population() -> Self {
        Self {
            variance: VarianceAccumulator::population(),
        }
    }

    pub fn sample() -> Self {
        Self {
            variance: VarianceAccumulator::sample(),
        }
    }
}

impl Accumulator for StddevAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        self.variance.update(values)
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<StddevAccumulator>() {
            self.variance.merge(&other.variance)?;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let var_arr = self.variance.finalize()?;
        let var_arr = try_downcast::<Float64Array>(&var_arr, "Float64Array")?;
        let result = if var_arr.is_null(0) {
            None
        } else {
            Some(var_arr.value(0).sqrt())
        };
        Ok(Arc::new(Float64Array::from(vec![result])))
    }

    fn reset(&mut self) {
        self.variance.reset();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// BOOL_AND aggregate: returns true if all non-null values are true.
pub struct BoolAndAccumulator {
    result: Option<bool>,
}

impl BoolAndAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Default for BoolAndAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for BoolAndAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let arr = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| BlazeError::type_error("BOOL_AND requires boolean argument"))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let val = arr.value(i);
                self.result = Some(self.result.unwrap_or(true) && val);
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<BoolAndAccumulator>() {
            if let Some(other_val) = other.result {
                self.result = Some(self.result.unwrap_or(true) && other_val);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(BooleanArray::from(vec![self.result])))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// BOOL_OR aggregate: returns true if any non-null value is true.
pub struct BoolOrAccumulator {
    result: Option<bool>,
}

impl BoolOrAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Default for BoolOrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for BoolOrAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let arr = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| BlazeError::type_error("BOOL_OR requires boolean argument"))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let val = arr.value(i);
                self.result = Some(self.result.unwrap_or(false) || val);
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<BoolOrAccumulator>() {
            if let Some(other_val) = other.result {
                self.result = Some(self.result.unwrap_or(false) || other_val);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(BooleanArray::from(vec![self.result])))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// ANY_VALUE aggregate: returns the first non-null value encountered.
pub struct AnyValueAccumulator {
    value: Option<ArrayRef>,
}

impl AnyValueAccumulator {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl Default for AnyValueAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AnyValueAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        if self.value.is_some() {
            return Ok(());
        }
        for i in 0..values.len() {
            if !values.is_null(i) {
                self.value = Some(values.slice(i, 1));
                return Ok(());
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if self.value.is_none() {
            if let Some(other) = other.as_any().downcast_ref::<AnyValueAccumulator>() {
                self.value = other.value.clone();
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        match &self.value {
            Some(v) => Ok(v.clone()),
            None => Ok(Arc::new(Int64Array::from(vec![None::<i64>]))),
        }
    }

    fn reset(&mut self) {
        self.value = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// PERCENTILE_CONT accumulator: exact continuous percentile with linear interpolation.
pub struct PercentileContAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileContAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }
}

impl Accumulator for PercentileContAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i));
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(values, "Float32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<PercentileContAccumulator>() {
            self.values.extend_from_slice(&other.values);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.values.is_empty() {
            return Ok(Arc::new(Float64Array::from(vec![None::<f64>])));
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        // Linear interpolation between adjacent values
        let idx = self.percentile * (n - 1) as f64;
        let lower = idx.floor() as usize;
        let upper = idx.ceil() as usize;
        let result = if lower == upper {
            sorted[lower]
        } else {
            let frac = idx - lower as f64;
            sorted[lower] * (1.0 - frac) + sorted[upper] * frac
        };
        Ok(Arc::new(Float64Array::from(vec![Some(result)])))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// PERCENTILE_DISC accumulator: exact discrete percentile (nearest value).
pub struct PercentileDiscAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileDiscAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }
}

impl Accumulator for PercentileDiscAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i));
                    }
                }
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(values, "Int32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(values, "Float32Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.values.push(arr.value(i) as f64);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<PercentileDiscAccumulator>() {
            self.values.extend_from_slice(&other.values);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        if self.values.is_empty() {
            return Ok(Arc::new(Float64Array::from(vec![None::<f64>])));
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // Nearest-rank: ceil(percentile * n) clamped to valid index
        let idx = (self.percentile * sorted.len() as f64).ceil() as usize;
        let idx = idx.saturating_sub(1).min(sorted.len() - 1);
        Ok(Arc::new(Float64Array::from(vec![Some(sorted[idx])])))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Hash aggregate operator for GROUP BY queries.
pub struct HashAggregateOperator;

impl HashAggregateOperator {
    /// Execute hash aggregation on input batches.
    ///
    /// Groups rows by the group_by expressions and computes aggregates for each group.
    pub fn execute(
        group_by: &[Arc<dyn crate::planner::PhysicalExpr>],
        aggr_exprs: &[crate::planner::AggregateExpr],
        output_schema: &Arc<ArrowSchema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        // Handle case with no grouping (global aggregates)
        if group_by.is_empty() {
            return Self::execute_global_aggregate(aggr_exprs, output_schema, input_batches);
        }

        // Build hash map: group_key_hash -> (group_key_values, accumulators)
        let mut groups: HashMap<u64, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)> = HashMap::new();

        for batch in &input_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Evaluate group by expressions
            let group_arrays: Vec<ArrayRef> = group_by
                .iter()
                .map(|expr| expr.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;

            // Evaluate aggregate input expressions
            let aggr_input_arrays: Vec<Vec<ArrayRef>> = aggr_exprs
                .iter()
                .map(|agg| {
                    agg.args
                        .iter()
                        .map(|arg| arg.evaluate(batch))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            // Process each row
            for row in 0..batch.num_rows() {
                let hash = JoinHashTable::compute_hash_from_arrays(&group_arrays, row)?;

                let entry = groups.entry(hash).or_insert_with(|| {
                    // Extract group key values for this row
                    let key_values: Vec<ArrayRef> =
                        group_arrays.iter().map(|arr| arr.slice(row, 1)).collect();

                    // Create accumulators for this group
                    let accumulators: Vec<Box<dyn Accumulator>> = aggr_exprs
                        .iter()
                        .map(|agg| Self::create_accumulator(agg))
                        .collect();

                    (key_values, accumulators)
                });

                // Update accumulators with values from this row
                for (i, agg_inputs) in aggr_input_arrays.iter().enumerate() {
                    if !agg_inputs.is_empty() {
                        let eval_idx = Self::get_eval_arg_index(&aggr_exprs[i].func, agg_inputs.len());
                        let value_slice = agg_inputs[eval_idx].slice(row, 1);
                        entry.1[i].update(&value_slice)?;
                    } else {
                        // COUNT(*) case - count the row
                        let one = Arc::new(Int64Array::from(vec![1i64])) as ArrayRef;
                        entry.1[i].update(&one)?;
                    }
                }
            }
        }

        // Handle empty input with no groups
        if groups.is_empty() && !input_batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
        }

        // Build output batch from groups
        Self::build_output(groups, group_by.len(), aggr_exprs.len(), output_schema)
    }

    fn execute_global_aggregate(
        aggr_exprs: &[crate::planner::AggregateExpr],
        output_schema: &Arc<ArrowSchema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        // Create single set of accumulators
        let mut accumulators: Vec<Box<dyn Accumulator>> = aggr_exprs
            .iter()
            .map(|agg| Self::create_accumulator(agg))
            .collect();

        // Process all input
        for batch in &input_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            for (i, agg) in aggr_exprs.iter().enumerate() {
                if !agg.args.is_empty() {
                    // For ordered-set aggregates with WITHIN GROUP, use the last arg (the column)
                    let eval_idx = Self::get_eval_arg_index(&agg.func, agg.args.len());
                    let values = agg.args[eval_idx].evaluate(batch)?;
                    accumulators[i].update(&values)?;
                } else {
                    // COUNT(*) - count all rows
                    let ones = Arc::new(Int64Array::from(vec![1i64; batch.num_rows()])) as ArrayRef;
                    accumulators[i].update(&ones)?;
                }
            }
        }

        // Finalize and build output
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(aggr_exprs.len());
        for acc in &accumulators {
            columns.push(acc.finalize()?);
        }

        Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
    }

    fn create_accumulator(agg: &crate::planner::AggregateExpr) -> Box<dyn Accumulator> {
        use crate::planner::AggregateFunc;

        match &agg.func {
            AggregateFunc::Count => {
                if agg.distinct {
                    Box::new(CountDistinctAccumulator::new())
                } else {
                    Box::new(CountAccumulator::new())
                }
            }
            AggregateFunc::Sum => Box::new(SumAccumulator::new()),
            AggregateFunc::Avg => Box::new(AvgAccumulator::new()),
            AggregateFunc::Min => Box::new(MinAccumulator::new()),
            AggregateFunc::Max => Box::new(MaxAccumulator::new()),
            AggregateFunc::ApproxCountDistinct => Box::new(ApproxCountDistinctAccumulator::new()),
            AggregateFunc::ApproxPercentile => Box::new(ApproxPercentileAccumulator::new(0.5)),
            AggregateFunc::ApproxMedian => Box::new(ApproxPercentileAccumulator::median()),
            AggregateFunc::VariancePop => Box::new(VarianceAccumulator::population()),
            AggregateFunc::VarianceSamp => Box::new(VarianceAccumulator::sample()),
            AggregateFunc::StddevPop => Box::new(StddevAccumulator::population()),
            AggregateFunc::StddevSamp => Box::new(StddevAccumulator::sample()),
            AggregateFunc::BoolAnd => Box::new(BoolAndAccumulator::new()),
            AggregateFunc::BoolOr => Box::new(BoolOrAccumulator::new()),
            AggregateFunc::AnyValue => Box::new(AnyValueAccumulator::new()),
            AggregateFunc::First => Box::new(AnyValueAccumulator::new()),
            AggregateFunc::Last => Box::new(AnyValueAccumulator::new()),
            AggregateFunc::PercentileCont => {
                let frac = Self::extract_percentile_fraction(agg);
                Box::new(PercentileContAccumulator::new(frac))
            }
            AggregateFunc::PercentileDisc => {
                let frac = Self::extract_percentile_fraction(agg);
                Box::new(PercentileDiscAccumulator::new(frac))
            }
            AggregateFunc::Median => Box::new(PercentileContAccumulator::new(0.5)),
            AggregateFunc::CountDistinct => Box::new(CountDistinctAccumulator::new()),
            AggregateFunc::ArrayAgg => Box::new(ArrayAggAccumulator::new()),
            AggregateFunc::StringAgg => {
                // STRING_AGG separator defaults to comma
                let sep = agg.args.get(1).and_then(|arg| {
                    use crate::planner::physical_expr::LiteralExpr;
                    arg.as_any().downcast_ref::<LiteralExpr>().and_then(|lit| {
                        if let crate::types::ScalarValue::Utf8(Some(s)) = lit.value() {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                }).unwrap_or_else(|| ",".to_string());
                Box::new(StringAggAccumulator::new(sep))
            }
        }
    }

    /// Extract percentile fraction from aggregate args.
    /// For PERCENTILE_CONT(0.75, col) the fraction is the first literal arg.
    fn extract_percentile_fraction(agg: &crate::planner::AggregateExpr) -> f64 {
        use crate::planner::physical_expr::LiteralExpr;
        if let Some(first_arg) = agg.args.first() {
            if let Some(lit) = first_arg.as_any().downcast_ref::<LiteralExpr>() {
                match lit.value() {
                    crate::types::ScalarValue::Float64(Some(v)) => return *v,
                    crate::types::ScalarValue::Int64(Some(v)) => return *v as f64,
                    _ => {}
                }
            }
        }
        0.5
    }

    /// For ordered-set aggregates (PERCENTILE_CONT, PERCENTILE_DISC), the column
    /// to aggregate is the last arg (after the fraction). For all others, use arg[0].
    fn get_eval_arg_index(func: &crate::planner::AggregateFunc, num_args: usize) -> usize {
        use crate::planner::AggregateFunc;
        if num_args > 1 {
            match func {
                AggregateFunc::PercentileCont
                | AggregateFunc::PercentileDisc => num_args - 1,
                _ => 0,
            }
        } else {
            0
        }
    }

    fn build_output(
        groups: HashMap<u64, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)>,
        num_group_cols: usize,
        num_agg_cols: usize,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }

        let _num_groups = groups.len();

        // Collect group keys and finalized aggregates
        let mut group_col_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_group_cols];
        let mut agg_col_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_agg_cols];

        for (_hash, (key_values, accumulators)) in groups {
            for (i, key_val) in key_values.into_iter().enumerate() {
                group_col_arrays[i].push(key_val);
            }
            for (i, acc) in accumulators.iter().enumerate() {
                agg_col_arrays[i].push(acc.finalize()?);
            }
        }

        // Concatenate arrays for each column
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + num_agg_cols);

        for col_arrays in group_col_arrays {
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            columns.push(compute::concat(&refs)?);
        }

        for col_arrays in agg_col_arrays {
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            columns.push(compute::concat(&refs)?);
        }

        Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int64_array(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn float64_array(values: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    // --- CountAccumulator ---

    #[test]
    fn test_count_basic() {
        let mut acc = CountAccumulator::new();
        acc.update(&int64_array(&[Some(1), Some(2), Some(3)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3);
    }

    #[test]
    fn test_count_with_nulls() {
        let mut acc = CountAccumulator::new();
        acc.update(&int64_array(&[Some(1), None, Some(3), None]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 2, "COUNT should skip NULL values");
    }

    #[test]
    fn test_count_all_nulls() {
        let mut acc = CountAccumulator::new();
        acc.update(&int64_array(&[None, None, None])).unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0, "COUNT of all NULLs should be 0");
    }

    #[test]
    fn test_count_empty() {
        let mut acc = CountAccumulator::new();
        acc.update(&int64_array(&[])).unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0);
    }

    #[test]
    fn test_count_merge() {
        let mut acc1 = CountAccumulator::new();
        acc1.update(&int64_array(&[Some(1), Some(2)])).unwrap();
        let mut acc2 = CountAccumulator::new();
        acc2.update(&int64_array(&[Some(3)])).unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3);
    }

    #[test]
    fn test_count_reset() {
        let mut acc = CountAccumulator::new();
        acc.update(&int64_array(&[Some(1), Some(2)])).unwrap();
        acc.reset();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0);
    }

    // --- SumAccumulator ---

    #[test]
    fn test_sum_int64() {
        let mut acc = SumAccumulator::new();
        acc.update(&int64_array(&[Some(10), Some(20), Some(30)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 60);
    }

    #[test]
    fn test_sum_float64() {
        let mut acc = SumAccumulator::new();
        acc.update(&float64_array(&[Some(1.5), Some(2.5), Some(3.0)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sum_with_nulls() {
        let mut acc = SumAccumulator::new();
        acc.update(&int64_array(&[Some(10), None, Some(30)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 40);
    }

    #[test]
    fn test_sum_all_nulls_returns_none() {
        let mut acc = SumAccumulator::new();
        acc.update(&int64_array(&[None, None])).unwrap();
        let result = acc.finalize().unwrap();
        // All nulls input with Int64 → returns Int64 array with null
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0), "SUM of all NULLs should be NULL");
    }

    #[test]
    fn test_sum_empty_returns_none() {
        let acc = SumAccumulator::new();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0), "SUM of empty set should be NULL");
    }

    #[test]
    fn test_sum_merge() {
        let mut acc1 = SumAccumulator::new();
        acc1.update(&int64_array(&[Some(10)])).unwrap();
        let mut acc2 = SumAccumulator::new();
        acc2.update(&int64_array(&[Some(20)])).unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 30);
    }

    // --- AvgAccumulator ---

    #[test]
    fn test_avg_basic() {
        let mut acc = AvgAccumulator::new();
        acc.update(&int64_array(&[Some(10), Some(20), Some(30)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_with_nulls() {
        let mut acc = AvgAccumulator::new();
        acc.update(&int64_array(&[Some(10), None, Some(30)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            (arr.value(0) - 20.0).abs() < f64::EPSILON,
            "AVG should skip NULLs"
        );
    }

    #[test]
    fn test_avg_empty_returns_null() {
        let acc = AvgAccumulator::new();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0), "AVG of empty set should be NULL");
    }

    #[test]
    fn test_avg_merge() {
        let mut acc1 = AvgAccumulator::new();
        acc1.update(&int64_array(&[Some(10), Some(20)])).unwrap();
        let mut acc2 = AvgAccumulator::new();
        acc2.update(&int64_array(&[Some(30)])).unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 20.0).abs() < f64::EPSILON);
    }

    // --- MinAccumulator ---

    #[test]
    fn test_min_basic() {
        let mut acc = MinAccumulator::new();
        acc.update(&int64_array(&[Some(30), Some(10), Some(20)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 10.0);
    }

    #[test]
    fn test_min_with_nulls() {
        let mut acc = MinAccumulator::new();
        acc.update(&int64_array(&[None, Some(30), None, Some(10)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 10.0, "MIN should skip NULLs");
    }

    #[test]
    fn test_min_all_nulls() {
        let mut acc = MinAccumulator::new();
        acc.update(&int64_array(&[None, None])).unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0), "MIN of all NULLs should be NULL");
    }

    // --- MaxAccumulator ---

    #[test]
    fn test_max_basic() {
        let mut acc = MaxAccumulator::new();
        acc.update(&int64_array(&[Some(10), Some(30), Some(20)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 30.0);
    }

    #[test]
    fn test_max_with_nulls() {
        let mut acc = MaxAccumulator::new();
        acc.update(&int64_array(&[None, Some(10), Some(30), None]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 30.0, "MAX should skip NULLs");
    }

    #[test]
    fn test_max_merge() {
        let mut acc1 = MaxAccumulator::new();
        acc1.update(&int64_array(&[Some(10)])).unwrap();
        let mut acc2 = MaxAccumulator::new();
        acc2.update(&int64_array(&[Some(50)])).unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 50.0);
    }

    // --- ApproxCountDistinctAccumulator (HyperLogLog) ---

    #[test]
    fn test_approx_count_distinct_basic() {
        let mut acc = ApproxCountDistinctAccumulator::new();
        acc.update(&int64_array(&[Some(1), Some(2), Some(3), Some(1), Some(2)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        let estimate = arr.value(0);
        assert!(
            estimate >= 2 && estimate <= 5,
            "Approx count distinct of [1,2,3,1,2] should be ~3, got {}",
            estimate
        );
    }

    #[test]
    fn test_approx_count_distinct_with_nulls() {
        let mut acc = ApproxCountDistinctAccumulator::new();
        acc.update(&int64_array(&[Some(1), None, Some(2), None, Some(1)]))
            .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        let estimate = arr.value(0);
        assert!(
            estimate >= 1 && estimate <= 4,
            "Approx count distinct should be ~2, got {}",
            estimate
        );
    }

    #[test]
    fn test_approx_count_distinct_strings() {
        let mut acc = ApproxCountDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            Some("c"),
            Some("b"),
        ]));
        acc.update(&arr).unwrap();
        let result = acc.finalize().unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        let estimate = result_arr.value(0);
        assert!(
            estimate >= 2 && estimate <= 5,
            "Approx count distinct of strings should be ~3, got {}",
            estimate
        );
    }

    #[test]
    fn test_approx_count_distinct_merge() {
        let mut acc1 = ApproxCountDistinctAccumulator::new();
        acc1.update(&int64_array(&[Some(1), Some(2)])).unwrap();
        let mut acc2 = ApproxCountDistinctAccumulator::new();
        acc2.update(&int64_array(&[Some(3), Some(4)])).unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        let estimate = arr.value(0);
        assert!(
            estimate >= 3 && estimate <= 6,
            "Merged approx count distinct should be ~4, got {}",
            estimate
        );
    }

    #[test]
    fn test_approx_count_distinct_reset() {
        let mut acc = ApproxCountDistinctAccumulator::new();
        acc.update(&int64_array(&[Some(1), Some(2), Some(3)]))
            .unwrap();
        acc.reset();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0, "After reset, estimate should be 0");
    }

    // --- ApproxPercentileAccumulator (T-Digest) ---

    #[test]
    fn test_approx_percentile_median() {
        let mut acc = ApproxPercentileAccumulator::median();
        acc.update(&float64_array(&[
            Some(10.0),
            Some(20.0),
            Some(30.0),
            Some(40.0),
            Some(50.0),
        ]))
        .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let median = arr.value(0);
        assert!(
            (median - 30.0).abs() < 10.0,
            "Median of [10,20,30,40,50] should be ~30, got {}",
            median
        );
    }

    #[test]
    fn test_approx_percentile_p90() {
        let mut acc = ApproxPercentileAccumulator::new(0.9);
        let values: Vec<Option<f64>> = (1..=100).map(|i| Some(i as f64)).collect();
        acc.update(&float64_array(&values)).unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let p90 = arr.value(0);
        assert!(
            p90 >= 50.0 && p90 <= 100.0,
            "P90 of 1..100 should be high, got {}",
            p90
        );
    }

    #[test]
    fn test_approx_percentile_with_nulls() {
        let mut acc = ApproxPercentileAccumulator::median();
        acc.update(&float64_array(&[
            Some(10.0),
            None,
            Some(30.0),
            None,
            Some(50.0),
        ]))
        .unwrap();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let median = arr.value(0);
        assert!(
            (median - 30.0).abs() < 15.0,
            "Median of [10,30,50] (nulls skipped) should be ~30, got {}",
            median
        );
    }

    #[test]
    fn test_approx_percentile_unsupported_type() {
        let mut acc = ApproxPercentileAccumulator::median();
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let result = acc.update(&arr);
        assert!(result.is_err(), "T-Digest should not support string type");
    }

    #[test]
    fn test_approx_percentile_merge() {
        let mut acc1 = ApproxPercentileAccumulator::median();
        acc1.update(&float64_array(&[Some(10.0), Some(20.0)]))
            .unwrap();
        let mut acc2 = ApproxPercentileAccumulator::median();
        acc2.update(&float64_array(&[Some(30.0), Some(40.0)]))
            .unwrap();
        acc1.merge(&acc2).unwrap();
        let result = acc1.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let median = arr.value(0);
        assert!(
            (median - 25.0).abs() < 15.0,
            "Merged median of [10,20,30,40] should be ~25, got {}",
            median
        );
    }

    #[test]
    fn test_approx_percentile_reset() {
        let mut acc = ApproxPercentileAccumulator::median();
        acc.update(&float64_array(&[Some(100.0)])).unwrap();
        acc.reset();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.len() == 1);
    }

    #[test]
    fn test_approx_percentile_clamped() {
        let acc = ApproxPercentileAccumulator::new(1.5);
        assert!(
            (acc.percentile - 1.0).abs() < f64::EPSILON,
            "Percentile should be clamped to 1.0"
        );
        let acc2 = ApproxPercentileAccumulator::new(-0.5);
        assert!(
            (acc2.percentile - 0.0).abs() < f64::EPSILON,
            "Percentile should be clamped to 0.0"
        );
    }
}
