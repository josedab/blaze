//! GPU Compute Kernels
//!
//! This module provides GPU kernel abstractions for common query operations
//! like filtering, aggregation, and joins.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use super::memory::{GpuBuffer, GpuRecordBatch, GpuMemoryPool};

/// Trait for GPU compute kernels.
pub trait GpuKernel: Send + Sync + std::fmt::Debug {
    /// Kernel name for identification.
    fn name(&self) -> &str;

    /// Execute the kernel on GPU data.
    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        output_buffer: &GpuBuffer,
        params: &KernelParams,
    ) -> Result<KernelResult>;

    /// Estimate output size for allocation.
    fn estimate_output_size(&self, input_size: usize) -> usize;

    /// Check if this kernel can be fused with another.
    fn can_fuse_with(&self, _other: &dyn GpuKernel) -> bool {
        false
    }

    /// Get kernel metadata.
    fn metadata(&self) -> KernelMetadata {
        KernelMetadata::default()
    }
}

/// Parameters passed to kernel execution.
#[derive(Debug, Clone, Default)]
pub struct KernelParams {
    /// Filter predicate parameters
    pub filter_value: Option<i64>,
    /// Column indices to operate on
    pub column_indices: Vec<usize>,
    /// Aggregation functions
    pub aggregations: Vec<AggregationType>,
    /// Join keys (left column index, right column index)
    pub join_keys: Vec<(usize, usize)>,
    /// Sort columns and directions
    pub sort_specs: Vec<(usize, bool)>,
    /// Limit value
    pub limit: Option<usize>,
    /// Custom parameters
    pub custom: HashMap<String, String>,
}

impl KernelParams {
    /// Create new kernel parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set filter value.
    pub fn with_filter_value(mut self, value: i64) -> Self {
        self.filter_value = Some(value);
        self
    }

    /// Set column indices.
    pub fn with_columns(mut self, indices: Vec<usize>) -> Self {
        self.column_indices = indices;
        self
    }

    /// Add aggregation.
    pub fn with_aggregation(mut self, agg: AggregationType) -> Self {
        self.aggregations.push(agg);
        self
    }

    /// Set join keys.
    pub fn with_join_keys(mut self, keys: Vec<(usize, usize)>) -> Self {
        self.join_keys = keys;
        self
    }

    /// Set limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Aggregation types supported by GPU kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationType {
    /// Count rows
    Count,
    /// Sum values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average value
    Avg,
    /// Count distinct values
    CountDistinct,
}

impl AggregationType {
    /// Get the kernel name for this aggregation.
    pub fn kernel_name(&self) -> &'static str {
        match self {
            AggregationType::Count => "count",
            AggregationType::Sum => "sum",
            AggregationType::Min => "min",
            AggregationType::Max => "max",
            AggregationType::Avg => "avg",
            AggregationType::CountDistinct => "count_distinct",
        }
    }
}

/// Result of kernel execution.
#[derive(Debug)]
pub struct KernelResult {
    /// Output row count
    pub output_rows: usize,
    /// Output byte size
    pub output_bytes: usize,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Whether result is cached in GPU memory
    pub cached: bool,
}

impl KernelResult {
    /// Create a new kernel result.
    pub fn new(output_rows: usize, output_bytes: usize) -> Self {
        Self {
            output_rows,
            output_bytes,
            execution_time_us: 0,
            cached: false,
        }
    }

    /// Set execution time.
    pub fn with_time(mut self, time_us: u64) -> Self {
        self.execution_time_us = time_us;
        self
    }

    /// Set cached flag.
    pub fn with_cached(mut self, cached: bool) -> Self {
        self.cached = cached;
        self
    }
}

/// Kernel metadata for optimization.
#[derive(Debug, Clone, Default)]
pub struct KernelMetadata {
    /// Estimated FLOPS per row
    pub flops_per_row: f64,
    /// Memory bandwidth requirement (bytes per row)
    pub memory_per_row: usize,
    /// Whether kernel is memory-bound
    pub memory_bound: bool,
    /// Optimal block size
    pub optimal_block_size: u32,
    /// Whether kernel supports vectorization
    pub vectorizable: bool,
}

/// Registry for GPU kernels.
pub struct KernelRegistry {
    /// Registered kernels
    kernels: HashMap<String, Arc<dyn GpuKernel>>,
}

impl KernelRegistry {
    /// Create a new kernel registry.
    pub fn new() -> Self {
        let mut registry = Self {
            kernels: HashMap::new(),
        };

        // Register built-in kernels
        registry.register(Arc::new(FilterKernel::new()));
        registry.register(Arc::new(AggregateKernel::new()));
        registry.register(Arc::new(JoinKernel::new()));
        registry.register(Arc::new(SortKernel::new()));
        registry.register(Arc::new(ProjectKernel::new()));

        registry
    }

    /// Register a kernel.
    pub fn register(&mut self, kernel: Arc<dyn GpuKernel>) {
        self.kernels.insert(kernel.name().to_string(), kernel);
    }

    /// Get a kernel by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn GpuKernel>> {
        self.kernels.get(name).cloned()
    }

    /// List all registered kernels.
    pub fn list(&self) -> Vec<&str> {
        self.kernels.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a kernel exists.
    pub fn contains(&self, name: &str) -> bool {
        self.kernels.contains_key(name)
    }
}

impl Default for KernelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter kernel for predicate evaluation.
#[derive(Debug)]
pub struct FilterKernel {
    /// Kernel name
    name: String,
}

impl FilterKernel {
    /// Create a new filter kernel.
    pub fn new() -> Self {
        Self {
            name: "filter".to_string(),
        }
    }
}

impl Default for FilterKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl GpuKernel for FilterKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        _output_buffer: &GpuBuffer,
        params: &KernelParams,
    ) -> Result<KernelResult> {
        if inputs.is_empty() {
            return Err(BlazeError::invalid_argument("No input data"));
        }

        let input = inputs[0];
        let input_rows = input.num_rows();

        // Simulate filter execution - estimate 50% selectivity by default
        let selectivity = if params.filter_value.is_some() {
            0.5
        } else {
            1.0
        };

        let output_rows = (input_rows as f64 * selectivity) as usize;
        let output_bytes = (input.size() as f64 * selectivity) as usize;

        Ok(KernelResult::new(output_rows, output_bytes).with_time(100))
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        // Assume 50% selectivity for estimation
        input_size / 2
    }

    fn can_fuse_with(&self, other: &dyn GpuKernel) -> bool {
        // Filter can fuse with project or another filter
        other.name() == "project" || other.name() == "filter"
    }

    fn metadata(&self) -> KernelMetadata {
        KernelMetadata {
            flops_per_row: 2.0,
            memory_per_row: 8,
            memory_bound: true,
            optimal_block_size: 256,
            vectorizable: true,
        }
    }
}

/// Aggregate kernel for group-by operations.
#[derive(Debug)]
pub struct AggregateKernel {
    /// Kernel name
    name: String,
}

impl AggregateKernel {
    /// Create a new aggregate kernel.
    pub fn new() -> Self {
        Self {
            name: "aggregate".to_string(),
        }
    }
}

impl Default for AggregateKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl GpuKernel for AggregateKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        _output_buffer: &GpuBuffer,
        params: &KernelParams,
    ) -> Result<KernelResult> {
        if inputs.is_empty() {
            return Err(BlazeError::invalid_argument("No input data"));
        }

        let input = inputs[0];

        // For aggregates, output is typically much smaller
        // Estimate based on number of groups (assume 1% cardinality)
        let estimated_groups = (input.num_rows() as f64 * 0.01).max(1.0) as usize;
        let output_rows = if params.aggregations.is_empty() {
            1 // Single aggregate without GROUP BY
        } else {
            estimated_groups
        };

        let bytes_per_row = params.aggregations.len() * 8; // 8 bytes per aggregate
        let output_bytes = output_rows * bytes_per_row.max(8);

        Ok(KernelResult::new(output_rows, output_bytes).with_time(500))
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        // Aggregates typically reduce data significantly
        (input_size / 100).max(64)
    }

    fn metadata(&self) -> KernelMetadata {
        KernelMetadata {
            flops_per_row: 4.0,
            memory_per_row: 16,
            memory_bound: false, // Compute-bound for reductions
            optimal_block_size: 512,
            vectorizable: true,
        }
    }
}

/// Join kernel for hash-based joins.
#[derive(Debug)]
pub struct JoinKernel {
    /// Kernel name
    name: String,
}

impl JoinKernel {
    /// Create a new join kernel.
    pub fn new() -> Self {
        Self {
            name: "join".to_string(),
        }
    }
}

impl Default for JoinKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl GpuKernel for JoinKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        _output_buffer: &GpuBuffer,
        _params: &KernelParams,
    ) -> Result<KernelResult> {
        if inputs.len() < 2 {
            return Err(BlazeError::invalid_argument(
                "Join requires at least 2 inputs",
            ));
        }

        let left = inputs[0];
        let right = inputs[1];

        // Estimate join output (pessimistic: left * right / 100)
        let output_rows = (left.num_rows() * right.num_rows() / 100).max(left.num_rows());
        let row_width = left.size() / left.num_rows().max(1)
            + right.size() / right.num_rows().max(1);
        let output_bytes = output_rows * row_width;

        Ok(KernelResult::new(output_rows, output_bytes).with_time(1000))
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        // Join output can be larger than input
        input_size * 2
    }

    fn metadata(&self) -> KernelMetadata {
        KernelMetadata {
            flops_per_row: 10.0,
            memory_per_row: 32,
            memory_bound: true,
            optimal_block_size: 256,
            vectorizable: false, // Hash probing is irregular
        }
    }
}

/// Sort kernel for parallel sorting.
#[derive(Debug)]
pub struct SortKernel {
    /// Kernel name
    name: String,
}

impl SortKernel {
    /// Create a new sort kernel.
    pub fn new() -> Self {
        Self {
            name: "sort".to_string(),
        }
    }
}

impl Default for SortKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl GpuKernel for SortKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        _output_buffer: &GpuBuffer,
        _params: &KernelParams,
    ) -> Result<KernelResult> {
        if inputs.is_empty() {
            return Err(BlazeError::invalid_argument("No input data"));
        }

        let input = inputs[0];

        // Sort preserves row count but may reorder
        let output_rows = input.num_rows();
        let output_bytes = input.size();

        // Sort time is O(n log n)
        let time_factor = (output_rows as f64).log2().max(1.0);
        let time_us = (output_rows as f64 * time_factor / 1000.0) as u64;

        Ok(KernelResult::new(output_rows, output_bytes).with_time(time_us))
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        input_size // Sort preserves size
    }

    fn metadata(&self) -> KernelMetadata {
        KernelMetadata {
            flops_per_row: 20.0, // log n comparisons
            memory_per_row: 16,
            memory_bound: true,
            optimal_block_size: 512,
            vectorizable: true, // Bitonic sort is vectorizable
        }
    }
}

/// Project kernel for column selection/transformation.
#[derive(Debug)]
pub struct ProjectKernel {
    /// Kernel name
    name: String,
}

impl ProjectKernel {
    /// Create a new project kernel.
    pub fn new() -> Self {
        Self {
            name: "project".to_string(),
        }
    }
}

impl Default for ProjectKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl GpuKernel for ProjectKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        _output_buffer: &GpuBuffer,
        params: &KernelParams,
    ) -> Result<KernelResult> {
        if inputs.is_empty() {
            return Err(BlazeError::invalid_argument("No input data"));
        }

        let input = inputs[0];

        // Project preserves rows but may change columns
        let output_rows = input.num_rows();
        let column_ratio = if params.column_indices.is_empty() {
            1.0
        } else {
            params.column_indices.len() as f64 / input.num_columns() as f64
        };
        let output_bytes = (input.size() as f64 * column_ratio) as usize;

        Ok(KernelResult::new(output_rows, output_bytes).with_time(50))
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        input_size // Conservative estimate
    }

    fn can_fuse_with(&self, other: &dyn GpuKernel) -> bool {
        // Project can fuse with filter or another project
        other.name() == "filter" || other.name() == "project"
    }

    fn metadata(&self) -> KernelMetadata {
        KernelMetadata {
            flops_per_row: 1.0,
            memory_per_row: 8,
            memory_bound: true,
            optimal_block_size: 256,
            vectorizable: true,
        }
    }
}

/// Fused kernel that combines multiple operations.
#[derive(Debug)]
pub struct FusedKernel {
    /// Kernel name
    name: String,
    /// Component kernels
    components: Vec<Arc<dyn GpuKernel>>,
}

impl FusedKernel {
    /// Create a new fused kernel.
    pub fn new(name: String, components: Vec<Arc<dyn GpuKernel>>) -> Self {
        Self { name, components }
    }

    /// Get the component kernels.
    pub fn components(&self) -> &[Arc<dyn GpuKernel>] {
        &self.components
    }
}

impl GpuKernel for FusedKernel {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(
        &self,
        inputs: &[&GpuRecordBatch],
        output_buffer: &GpuBuffer,
        params: &KernelParams,
    ) -> Result<KernelResult> {
        // Execute components in sequence (conceptually - actual fusion would be single kernel)
        let mut result = KernelResult::new(0, 0);

        for kernel in &self.components {
            let component_result = kernel.execute(inputs, output_buffer, params)?;
            result.output_rows = component_result.output_rows;
            result.output_bytes = component_result.output_bytes;
            result.execution_time_us += component_result.execution_time_us;
        }

        Ok(result)
    }

    fn estimate_output_size(&self, input_size: usize) -> usize {
        // Use the last component's estimate
        self.components
            .last()
            .map(|k| k.estimate_output_size(input_size))
            .unwrap_or(input_size)
    }

    fn metadata(&self) -> KernelMetadata {
        // Combine metadata from components
        let mut combined = KernelMetadata::default();
        for kernel in &self.components {
            let meta = kernel.metadata();
            combined.flops_per_row += meta.flops_per_row;
            combined.memory_per_row = combined.memory_per_row.max(meta.memory_per_row);
        }
        combined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_gpu_batch(rows: usize, cols: usize) -> GpuRecordBatch {
        let fields: Vec<Field> = (0..cols)
            .map(|i| Field::new(format!("col_{}", i), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        GpuRecordBatch {
            buffer: GpuBuffer::new(1, rows * cols * 8, 0),
            num_rows: rows,
            num_columns: cols,
            schema,
        }
    }

    #[test]
    fn test_kernel_registry() {
        let registry = KernelRegistry::new();

        assert!(registry.contains("filter"));
        assert!(registry.contains("aggregate"));
        assert!(registry.contains("join"));
        assert!(registry.contains("sort"));
        assert!(registry.contains("project"));

        let kernels = registry.list();
        assert!(kernels.len() >= 5);
    }

    #[test]
    fn test_filter_kernel() {
        let kernel = FilterKernel::new();
        let batch = create_test_gpu_batch(1000, 5);
        let output_buffer = GpuBuffer::new(2, 40000, 0);
        let params = KernelParams::new().with_filter_value(42);

        let result = kernel.execute(&[&batch], &output_buffer, &params).unwrap();

        assert!(result.output_rows <= 1000);
        assert!(result.execution_time_us > 0);
    }

    #[test]
    fn test_aggregate_kernel() {
        let kernel = AggregateKernel::new();
        let batch = create_test_gpu_batch(10000, 3);
        let output_buffer = GpuBuffer::new(2, 1000, 0);
        let params = KernelParams::new()
            .with_aggregation(AggregationType::Sum)
            .with_aggregation(AggregationType::Count);

        let result = kernel.execute(&[&batch], &output_buffer, &params).unwrap();

        // Aggregate should reduce rows
        assert!(result.output_rows < 10000);
    }

    #[test]
    fn test_join_kernel() {
        let kernel = JoinKernel::new();
        let left = create_test_gpu_batch(1000, 3);
        let right = create_test_gpu_batch(500, 2);
        let output_buffer = GpuBuffer::new(3, 100000, 0);
        let params = KernelParams::new().with_join_keys(vec![(0, 0)]);

        let result = kernel
            .execute(&[&left, &right], &output_buffer, &params)
            .unwrap();

        assert!(result.output_rows > 0);
    }

    #[test]
    fn test_sort_kernel() {
        let kernel = SortKernel::new();
        let batch = create_test_gpu_batch(5000, 4);
        let output_buffer = GpuBuffer::new(2, 160000, 0);
        let params = KernelParams::new();

        let result = kernel.execute(&[&batch], &output_buffer, &params).unwrap();

        // Sort preserves row count
        assert_eq!(result.output_rows, 5000);
    }

    #[test]
    fn test_kernel_fusion() {
        let filter = FilterKernel::new();
        let project = ProjectKernel::new();

        assert!(filter.can_fuse_with(&project));
        assert!(project.can_fuse_with(&filter));
    }

    #[test]
    fn test_fused_kernel() {
        let fused = FusedKernel::new(
            "filter_project".to_string(),
            vec![
                Arc::new(FilterKernel::new()),
                Arc::new(ProjectKernel::new()),
            ],
        );

        assert_eq!(fused.components().len(), 2);

        let batch = create_test_gpu_batch(1000, 5);
        let output_buffer = GpuBuffer::new(2, 40000, 0);
        let params = KernelParams::new()
            .with_filter_value(10)
            .with_columns(vec![0, 1, 2]);

        let result = fused.execute(&[&batch], &output_buffer, &params).unwrap();
        assert!(result.output_rows <= 1000);
    }

    #[test]
    fn test_aggregation_type() {
        assert_eq!(AggregationType::Sum.kernel_name(), "sum");
        assert_eq!(AggregationType::Count.kernel_name(), "count");
        assert_eq!(AggregationType::Avg.kernel_name(), "avg");
    }

    #[test]
    fn test_kernel_params() {
        let params = KernelParams::new()
            .with_filter_value(100)
            .with_columns(vec![0, 1])
            .with_aggregation(AggregationType::Sum)
            .with_limit(1000);

        assert_eq!(params.filter_value, Some(100));
        assert_eq!(params.column_indices, vec![0, 1]);
        assert_eq!(params.aggregations, vec![AggregationType::Sum]);
        assert_eq!(params.limit, Some(1000));
    }

    #[test]
    fn test_kernel_metadata() {
        let filter = FilterKernel::new();
        let meta = filter.metadata();

        assert!(meta.flops_per_row > 0.0);
        assert!(meta.memory_per_row > 0);
        assert!(meta.optimal_block_size > 0);
    }
}
