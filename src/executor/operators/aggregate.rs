//! Hash aggregate operator and accumulators.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};

use super::try_downcast;
use super::hash_join::JoinHashTable;

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
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(array, "Int64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "Hash for type {:?}",
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
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let sum = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::sum(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::sum(arr)
            }
            _ => None,
        };

        if let Some(s) = sum {
            self.sum = Some(self.sum.unwrap_or(0.0) + s);
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<SumAccumulator>() {
            if let Some(s) = other.sum {
                self.sum = Some(self.sum.unwrap_or(0.0) + s);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.sum])))
    }

    fn reset(&mut self) {
        self.sum = None;
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
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MinAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let min = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::min(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::min(arr)
            }
            _ => None,
        };

        if let Some(m) = min {
            self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MinAccumulator>() {
            if let Some(m) = other.min {
                self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.min])))
    }

    fn reset(&mut self) {
        self.min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MAX accumulator.
pub struct MaxAccumulator {
    max: Option<f64>,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MaxAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let max = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::max(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::max(arr)
            }
            _ => None,
        };

        if let Some(m) = max {
            self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MaxAccumulator>() {
            if let Some(m) = other.max {
                self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.max])))
    }

    fn reset(&mut self) {
        self.max = None;
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
                        .map(|agg| Self::create_accumulator(&agg.func))
                        .collect();

                    (key_values, accumulators)
                });

                // Update accumulators with values from this row
                for (i, agg_inputs) in aggr_input_arrays.iter().enumerate() {
                    // For simplicity, use the first argument (most aggregates have one arg)
                    if !agg_inputs.is_empty() {
                        let value_slice = agg_inputs[0].slice(row, 1);
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
            .map(|agg| Self::create_accumulator(&agg.func))
            .collect();

        // Process all input
        for batch in &input_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            for (i, agg) in aggr_exprs.iter().enumerate() {
                if !agg.args.is_empty() {
                    let values = agg.args[0].evaluate(batch)?;
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

    fn create_accumulator(func: &crate::planner::AggregateFunc) -> Box<dyn Accumulator> {
        use crate::planner::AggregateFunc;

        match func {
            AggregateFunc::Count => Box::new(CountAccumulator::new()),
            AggregateFunc::Sum => Box::new(SumAccumulator::new()),
            AggregateFunc::Avg => Box::new(AvgAccumulator::new()),
            AggregateFunc::Min => Box::new(MinAccumulator::new()),
            AggregateFunc::Max => Box::new(MaxAccumulator::new()),
            AggregateFunc::ApproxCountDistinct => Box::new(ApproxCountDistinctAccumulator::new()),
            AggregateFunc::ApproxPercentile => Box::new(ApproxPercentileAccumulator::new(0.5)), // Default to median
            AggregateFunc::ApproxMedian => Box::new(ApproxPercentileAccumulator::median()),
            _ => Box::new(CountAccumulator::new()), // Fallback
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

