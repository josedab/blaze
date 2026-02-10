//! GPU Acceleration Module
//!
//! This module provides GPU-accelerated query execution using CUDA (NVIDIA)
//! or Metal (Apple) backends. It includes device management, memory transfer,
//! and compute kernel abstractions.
//!
//! # Architecture
//!
//! The GPU module is organized into layers:
//! - **Device Layer**: GPU detection, capability querying, device selection
//! - **Memory Layer**: GPU memory allocation, host-device transfers
//! - **Kernel Layer**: Compute kernel abstractions for query operations
//! - **Executor Layer**: GPU-accelerated query execution engine
//!
//! # Feature Flags
//!
//! - `cuda`: Enable NVIDIA CUDA support
//! - `metal`: Enable Apple Metal support
//! - `wgpu`: Enable cross-platform WebGPU support (default fallback)

mod device;
mod executor;
mod kernels;
mod memory;

pub use device::{DeviceManager, GpuBackend, GpuCapabilities, GpuDevice};
pub use executor::{GpuExecutionPlan, GpuExecutor, GpuOperator};
pub use kernels::{AggregateKernel, FilterKernel, GpuKernel, JoinKernel, KernelRegistry};
pub use memory::{GpuBuffer, GpuMemoryPool, MemoryTransfer, TransferDirection};

use crate::error::{BlazeError, Result};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Configuration for GPU acceleration.
#[derive(Debug, Clone)]
pub struct GpuConfig {
    /// Whether GPU acceleration is enabled
    pub enabled: bool,
    /// Preferred GPU backend
    pub preferred_backend: Option<GpuBackend>,
    /// Minimum data size (in bytes) to trigger GPU execution
    pub min_gpu_batch_size: usize,
    /// Maximum GPU memory to use (in bytes)
    pub max_gpu_memory: usize,
    /// Whether to use async memory transfers
    pub async_transfers: bool,
    /// Number of CUDA streams / Metal command queues
    pub num_streams: usize,
    /// Whether to enable kernel fusion optimization
    pub kernel_fusion: bool,
}

impl Default for GpuConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            preferred_backend: None,
            min_gpu_batch_size: 64 * 1024,          // 64 KB minimum
            max_gpu_memory: 4 * 1024 * 1024 * 1024, // 4 GB
            async_transfers: true,
            num_streams: 4,
            kernel_fusion: true,
        }
    }
}

impl GpuConfig {
    /// Create a new GPU configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable GPU acceleration.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set the preferred backend.
    pub fn with_backend(mut self, backend: GpuBackend) -> Self {
        self.preferred_backend = Some(backend);
        self
    }

    /// Set minimum batch size for GPU execution.
    pub fn with_min_batch_size(mut self, size: usize) -> Self {
        self.min_gpu_batch_size = size;
        self
    }

    /// Set maximum GPU memory limit.
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_gpu_memory = bytes;
        self
    }

    /// Enable or disable async transfers.
    pub fn with_async_transfers(mut self, enabled: bool) -> Self {
        self.async_transfers = enabled;
        self
    }

    /// Set number of parallel streams.
    pub fn with_num_streams(mut self, num: usize) -> Self {
        self.num_streams = num;
        self
    }

    /// Enable or disable kernel fusion.
    pub fn with_kernel_fusion(mut self, enabled: bool) -> Self {
        self.kernel_fusion = enabled;
        self
    }
}

/// GPU acceleration context that manages devices and execution.
pub struct GpuContext {
    /// Configuration
    config: GpuConfig,
    /// Device manager
    device_manager: DeviceManager,
    /// Memory pool
    memory_pool: GpuMemoryPool,
    /// Kernel registry
    kernel_registry: KernelRegistry,
    /// GPU executor
    executor: GpuExecutor,
}

impl GpuContext {
    /// Create a new GPU context with default configuration.
    pub fn new() -> Result<Self> {
        Self::with_config(GpuConfig::default())
    }

    /// Create a new GPU context with custom configuration.
    pub fn with_config(config: GpuConfig) -> Result<Self> {
        let device_manager = DeviceManager::new(config.preferred_backend)?;
        let memory_pool = GpuMemoryPool::new(config.max_gpu_memory);
        let kernel_registry = KernelRegistry::new();
        let executor = GpuExecutor::new(config.clone());

        Ok(Self {
            config,
            device_manager,
            memory_pool,
            kernel_registry,
            executor,
        })
    }

    /// Check if GPU acceleration is available.
    pub fn is_available(&self) -> bool {
        self.config.enabled && self.device_manager.has_devices()
    }

    /// Get the active GPU device.
    pub fn device(&self) -> Option<&GpuDevice> {
        self.device_manager.active_device()
    }

    /// Get GPU capabilities.
    pub fn capabilities(&self) -> Option<&GpuCapabilities> {
        self.device_manager
            .active_device()
            .map(|d| d.capabilities())
    }

    /// Check if a batch should be processed on GPU based on size.
    pub fn should_use_gpu(&self, batch: &RecordBatch) -> bool {
        if !self.is_available() {
            return false;
        }

        let batch_size = batch.get_array_memory_size();
        batch_size >= self.config.min_gpu_batch_size
    }

    /// Execute a query operation on GPU.
    pub fn execute(
        &self,
        plan: &GpuExecutionPlan,
        input: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        if !self.is_available() {
            return Err(BlazeError::execution("GPU not available"));
        }

        self.executor.execute(plan, input, &self.memory_pool)
    }

    /// Get memory pool statistics.
    pub fn memory_stats(&self) -> MemoryStats {
        self.memory_pool.stats()
    }

    /// Get the device manager.
    pub fn device_manager(&self) -> &DeviceManager {
        &self.device_manager
    }

    /// Get the kernel registry.
    pub fn kernel_registry(&self) -> &KernelRegistry {
        &self.kernel_registry
    }
}

impl Default for GpuContext {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| {
            // Return a disabled context if initialization fails
            Self {
                config: GpuConfig::disabled(),
                device_manager: DeviceManager::empty(),
                memory_pool: GpuMemoryPool::new(0),
                kernel_registry: KernelRegistry::new(),
                executor: GpuExecutor::new(GpuConfig::disabled()),
            }
        })
    }
}

/// Memory statistics for GPU memory pool.
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Total GPU memory available
    pub total_bytes: usize,
    /// Currently allocated bytes
    pub allocated_bytes: usize,
    /// Peak allocation
    pub peak_bytes: usize,
    /// Number of active allocations
    pub num_allocations: usize,
    /// Number of allocation requests
    pub total_allocations: u64,
    /// Number of deallocations
    pub total_deallocations: u64,
}

impl MemoryStats {
    /// Get the percentage of memory used.
    pub fn usage_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.allocated_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    /// Get available memory.
    pub fn available_bytes(&self) -> usize {
        self.total_bytes.saturating_sub(self.allocated_bytes)
    }
}

/// Trait for GPU-acceleratable operations.
pub trait GpuAccelerable {
    /// Check if this operation can be accelerated on GPU.
    fn can_accelerate(&self) -> bool;

    /// Get the GPU kernel for this operation.
    fn gpu_kernel(&self) -> Option<Arc<dyn GpuKernel>>;

    /// Estimate GPU memory requirements.
    fn estimate_gpu_memory(&self, input_size: usize) -> usize;
}

// ---------------------------------------------------------------------------
// Cost-Based GPU Offloading
// ---------------------------------------------------------------------------

/// Types of operations that can be offloaded to the GPU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GpuOperationType {
    Filter,
    Join,
    Aggregate,
    Sort,
    Project,
    Window,
}

impl std::fmt::Display for GpuOperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Filter => write!(f, "Filter"),
            Self::Join => write!(f, "Join"),
            Self::Aggregate => write!(f, "Aggregate"),
            Self::Sort => write!(f, "Sort"),
            Self::Project => write!(f, "Project"),
            Self::Window => write!(f, "Window"),
        }
    }
}

/// Cost model for deciding whether to offload work to GPU.
#[derive(Debug, Clone)]
pub struct GpuCostModel {
    /// Minimum row thresholds per operation type
    filter_threshold: usize,
    join_threshold: usize,
    aggregate_threshold: usize,
    sort_threshold: usize,
}

impl Default for GpuCostModel {
    fn default() -> Self {
        Self {
            filter_threshold: 100_000,
            join_threshold: 50_000,
            aggregate_threshold: 200_000,
            sort_threshold: 500_000,
        }
    }
}

impl GpuCostModel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Estimate the GPU speedup multiplier for a given operation.
    ///
    /// Returns a value >= 1.0 when GPU is expected to be faster.
    pub fn estimate_gpu_speedup(
        &self,
        op_type: GpuOperationType,
        row_count: usize,
        column_count: usize,
    ) -> f64 {
        let base = match op_type {
            GpuOperationType::Filter => {
                if row_count < self.filter_threshold {
                    return 0.8;
                }
                2.0
            }
            GpuOperationType::Join => {
                if row_count < self.join_threshold {
                    return 0.7;
                }
                3.0
            }
            GpuOperationType::Aggregate => {
                if row_count < self.aggregate_threshold {
                    return 0.9;
                }
                2.5
            }
            GpuOperationType::Sort => {
                if row_count < self.sort_threshold {
                    return 0.6;
                }
                4.0
            }
            GpuOperationType::Project => 1.5,
            GpuOperationType::Window => 2.0,
        };

        // More columns → slightly more benefit from columnar GPU processing
        let col_factor = 1.0 + (column_count as f64 - 1.0).max(0.0) * 0.05;
        // More rows → logarithmic scaling beyond the threshold
        let row_factor = 1.0 + (row_count as f64).log10() * 0.1;

        base * col_factor * row_factor
    }

    /// Returns `true` when offloading to the GPU is expected to be faster.
    pub fn should_offload(&self, op_type: GpuOperationType, row_count: usize) -> bool {
        let threshold = match op_type {
            GpuOperationType::Filter => self.filter_threshold,
            GpuOperationType::Join => self.join_threshold,
            GpuOperationType::Aggregate => self.aggregate_threshold,
            GpuOperationType::Sort => self.sort_threshold,
            // Project and Window have no minimum threshold
            GpuOperationType::Project | GpuOperationType::Window => 0,
        };
        row_count >= threshold
    }
}

// ---------------------------------------------------------------------------
// Kernel Fusion
// ---------------------------------------------------------------------------

/// A plan describing a set of operations fused into a single GPU kernel.
#[derive(Debug, Clone)]
pub struct FusionPlan {
    /// Operations that make up this fused kernel.
    pub ops: Vec<GpuOperationType>,
}

impl FusionPlan {
    pub fn new(ops: Vec<GpuOperationType>) -> Self {
        Self { ops }
    }

    /// Human-readable description of the fused kernel.
    pub fn describe(&self) -> String {
        let names: Vec<String> = self.ops.iter().map(|o| o.to_string()).collect();
        format!("FusedKernel({})", names.join(" -> "))
    }

    /// Estimated speedup from fusing these operations vs running them separately.
    pub fn estimated_speedup(&self) -> f64 {
        if self.ops.len() <= 1 {
            return 1.0;
        }
        // Each additional fused operation saves ~20% of kernel launch overhead
        1.0 + (self.ops.len() - 1) as f64 * 0.2
    }
}

/// Optimizer that detects fusible operation sequences.
#[derive(Debug, Default)]
pub struct FusionOptimizer;

impl FusionOptimizer {
    pub fn new() -> Self {
        Self
    }

    /// Analyse a sequence of GPU operations and return fusion plans.
    ///
    /// Fusion rules:
    /// - Filter + Project → fuse
    /// - Filter + Aggregate → fuse
    /// - Consecutive Filters → fuse
    pub fn optimize(&self, ops: &[GpuOperationType]) -> Vec<FusionPlan> {
        let mut plans: Vec<FusionPlan> = Vec::new();
        let mut i = 0;

        while i < ops.len() {
            let mut fused: Vec<GpuOperationType> = vec![ops[i]];

            // Greedily extend the fusion group while the next op is fusible
            while i + 1 < ops.len() && Self::can_fuse(*fused.last().unwrap(), ops[i + 1]) {
                i += 1;
                fused.push(ops[i]);
            }

            plans.push(FusionPlan::new(fused));
            i += 1;
        }

        plans
    }

    fn can_fuse(a: GpuOperationType, b: GpuOperationType) -> bool {
        matches!(
            (a, b),
            (GpuOperationType::Filter, GpuOperationType::Project)
                | (GpuOperationType::Filter, GpuOperationType::Aggregate)
                | (GpuOperationType::Filter, GpuOperationType::Filter)
        )
    }
}

// ---------------------------------------------------------------------------
// Multi-GPU Support
// ---------------------------------------------------------------------------

/// A unit of work to be scheduled across GPU devices.
#[derive(Debug, Clone)]
pub struct GpuWorkItem {
    pub id: usize,
    pub estimated_cost: f64,
    pub row_count: usize,
}

/// Statistics about scheduler work distribution.
#[derive(Debug, Clone)]
pub struct SchedulerStats {
    pub total_items: usize,
    pub items_per_device: Vec<usize>,
    pub load_balance_ratio: f64,
}

/// Round-robin scheduler for distributing work across multiple GPUs.
#[derive(Debug)]
pub struct MultiGpuScheduler {
    num_devices: usize,
}

impl MultiGpuScheduler {
    pub fn new(num_devices: usize) -> Self {
        Self {
            num_devices: num_devices.max(1),
        }
    }

    /// Assign work items to devices using round-robin.
    ///
    /// Returns a `Vec` of `(device_id, Vec<&GpuWorkItem>)`.
    pub fn assign_work<'a>(
        &self,
        work_items: &'a [GpuWorkItem],
    ) -> Vec<(usize, Vec<&'a GpuWorkItem>)> {
        let mut buckets: Vec<Vec<&GpuWorkItem>> = vec![Vec::new(); self.num_devices];

        for (idx, item) in work_items.iter().enumerate() {
            buckets[idx % self.num_devices].push(item);
        }

        buckets.into_iter().enumerate().collect()
    }

    /// Compute distribution statistics for a given set of work items.
    pub fn stats(&self, work_items: &[GpuWorkItem]) -> SchedulerStats {
        let assignments = self.assign_work(work_items);
        let items_per_device: Vec<usize> = assignments.iter().map(|(_, items)| items.len()).collect();
        let max = *items_per_device.iter().max().unwrap_or(&0);
        let min = *items_per_device.iter().min().unwrap_or(&0);
        let load_balance_ratio = if max == 0 {
            1.0
        } else {
            min as f64 / max as f64
        };

        SchedulerStats {
            total_items: work_items.len(),
            items_per_device,
            load_balance_ratio,
        }
    }
}

// ---------------------------------------------------------------------------
// GPU Memory Manager with spill-to-host
// ---------------------------------------------------------------------------

/// Handle representing a GPU memory allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuAllocation {
    pub id: u64,
    pub bytes: usize,
}

/// Statistics for the GPU memory manager.
#[derive(Debug, Clone, Default)]
pub struct GpuMemoryStats {
    pub allocated: usize,
    pub peak: usize,
    pub spilled_to_host: usize,
    pub allocation_count: u64,
}

/// GPU memory manager that supports spilling to host memory.
#[derive(Debug)]
pub struct GpuMemoryManager {
    device_capacity: usize,
    host_spill_capacity: usize,
    allocated: usize,
    peak: usize,
    spilled_to_host: usize,
    next_id: u64,
    allocations: Vec<GpuAllocation>,
}

impl GpuMemoryManager {
    pub fn new(device_memory_bytes: usize, host_spill_bytes: usize) -> Self {
        Self {
            device_capacity: device_memory_bytes,
            host_spill_capacity: host_spill_bytes,
            allocated: 0,
            peak: 0,
            spilled_to_host: 0,
            next_id: 0,
            allocations: Vec::new(),
        }
    }

    /// Allocate `bytes` of GPU memory, returning a handle.
    pub fn allocate(&mut self, bytes: usize) -> Result<GpuAllocation> {
        if bytes == 0 {
            return Err(BlazeError::execution("Cannot allocate 0 bytes on GPU"));
        }
        if self.allocated + bytes > self.device_capacity {
            return Err(BlazeError::execution(
                "GPU memory exhausted; call spill_to_host() to free space",
            ));
        }

        let alloc = GpuAllocation {
            id: self.next_id,
            bytes,
        };
        self.next_id += 1;
        self.allocated += bytes;
        if self.allocated > self.peak {
            self.peak = self.allocated;
        }
        self.allocations.push(alloc.clone());
        Ok(alloc)
    }

    /// Deallocate a previous allocation.
    pub fn deallocate(&mut self, allocation: &GpuAllocation) {
        if let Some(pos) = self.allocations.iter().position(|a| a.id == allocation.id) {
            self.allocated = self.allocated.saturating_sub(allocation.bytes);
            self.allocations.remove(pos);
        }
    }

    /// Spill the oldest allocation to host memory, returning the number of freed bytes.
    pub fn spill_to_host(&mut self) -> usize {
        if let Some(oldest) = self.allocations.first().cloned() {
            if self.spilled_to_host + oldest.bytes <= self.host_spill_capacity {
                self.allocated = self.allocated.saturating_sub(oldest.bytes);
                self.spilled_to_host += oldest.bytes;
                self.allocations.remove(0);
                return oldest.bytes;
            }
        }
        0
    }

    /// Current memory statistics.
    pub fn stats(&self) -> GpuMemoryStats {
        GpuMemoryStats {
            allocated: self.allocated,
            peak: self.peak,
            spilled_to_host: self.spilled_to_host,
            allocation_count: self.allocations.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_config_default() {
        let config = GpuConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_gpu_batch_size, 64 * 1024);
        assert!(config.async_transfers);
    }

    #[test]
    fn test_gpu_config_builder() {
        let config = GpuConfig::new()
            .with_backend(GpuBackend::Metal)
            .with_min_batch_size(1024)
            .with_max_memory(1024 * 1024 * 1024)
            .with_num_streams(8);

        assert_eq!(config.preferred_backend, Some(GpuBackend::Metal));
        assert_eq!(config.min_gpu_batch_size, 1024);
        assert_eq!(config.max_gpu_memory, 1024 * 1024 * 1024);
        assert_eq!(config.num_streams, 8);
    }

    #[test]
    fn test_gpu_config_disabled() {
        let config = GpuConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_memory_stats() {
        let stats = MemoryStats {
            total_bytes: 1000,
            allocated_bytes: 250,
            peak_bytes: 500,
            num_allocations: 5,
            total_allocations: 100,
            total_deallocations: 95,
        };

        assert_eq!(stats.usage_percent(), 25.0);
        assert_eq!(stats.available_bytes(), 750);
    }

    #[test]
    fn test_memory_stats_zero_total() {
        let stats = MemoryStats::default();
        assert_eq!(stats.usage_percent(), 0.0);
        assert_eq!(stats.available_bytes(), 0);
    }

    #[test]
    fn test_gpu_context_default() {
        let ctx = GpuContext::default();
        // In test environment without GPU, should be disabled or have no devices
        // This test just ensures it doesn't panic
        let _ = ctx.is_available();
    }

    // -----------------------------------------------------------------------
    // Cost-Based GPU Offloading tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cost_model_should_offload_filter() {
        let model = GpuCostModel::new();
        assert!(!model.should_offload(GpuOperationType::Filter, 99_999));
        assert!(model.should_offload(GpuOperationType::Filter, 100_000));
    }

    #[test]
    fn test_cost_model_should_offload_join() {
        let model = GpuCostModel::new();
        assert!(!model.should_offload(GpuOperationType::Join, 49_999));
        assert!(model.should_offload(GpuOperationType::Join, 50_000));
    }

    #[test]
    fn test_cost_model_should_offload_aggregate_and_sort() {
        let model = GpuCostModel::new();
        assert!(!model.should_offload(GpuOperationType::Aggregate, 199_999));
        assert!(model.should_offload(GpuOperationType::Aggregate, 200_000));
        assert!(!model.should_offload(GpuOperationType::Sort, 499_999));
        assert!(model.should_offload(GpuOperationType::Sort, 500_000));
    }

    #[test]
    fn test_cost_model_speedup_below_threshold() {
        let model = GpuCostModel::new();
        // Below threshold returns < 1.0 (not worth offloading)
        let speedup = model.estimate_gpu_speedup(GpuOperationType::Filter, 1_000, 1);
        assert!(speedup < 1.0, "speedup below threshold should be < 1.0");
    }

    #[test]
    fn test_cost_model_speedup_above_threshold() {
        let model = GpuCostModel::new();
        let speedup = model.estimate_gpu_speedup(GpuOperationType::Filter, 1_000_000, 4);
        assert!(speedup > 1.0, "speedup above threshold should be > 1.0");
    }

    // -----------------------------------------------------------------------
    // Kernel Fusion tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fusion_filter_project() {
        let opt = FusionOptimizer::new();
        let plans = opt.optimize(&[GpuOperationType::Filter, GpuOperationType::Project]);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].ops.len(), 2);
        assert!(plans[0].describe().contains("Filter"));
        assert!(plans[0].describe().contains("Project"));
    }

    #[test]
    fn test_fusion_no_fusion() {
        let opt = FusionOptimizer::new();
        let plans = opt.optimize(&[GpuOperationType::Sort, GpuOperationType::Join]);
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn test_fusion_consecutive_filters() {
        let opt = FusionOptimizer::new();
        let plans = opt.optimize(&[
            GpuOperationType::Filter,
            GpuOperationType::Filter,
            GpuOperationType::Filter,
        ]);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].ops.len(), 3);
    }

    #[test]
    fn test_fusion_plan_speedup() {
        let plan = FusionPlan::new(vec![GpuOperationType::Filter, GpuOperationType::Project]);
        assert!(plan.estimated_speedup() > 1.0);
        let single = FusionPlan::new(vec![GpuOperationType::Filter]);
        assert_eq!(single.estimated_speedup(), 1.0);
    }

    // -----------------------------------------------------------------------
    // Multi-GPU Scheduler tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_gpu_round_robin() {
        let scheduler = MultiGpuScheduler::new(2);
        let items: Vec<GpuWorkItem> = (0..4)
            .map(|i| GpuWorkItem {
                id: i,
                estimated_cost: 1.0,
                row_count: 1000,
            })
            .collect();
        let assignments = scheduler.assign_work(&items);
        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].1.len(), 2);
        assert_eq!(assignments[1].1.len(), 2);
    }

    #[test]
    fn test_scheduler_stats() {
        let scheduler = MultiGpuScheduler::new(3);
        let items: Vec<GpuWorkItem> = (0..6)
            .map(|i| GpuWorkItem {
                id: i,
                estimated_cost: 1.0,
                row_count: 100,
            })
            .collect();
        let stats = scheduler.stats(&items);
        assert_eq!(stats.total_items, 6);
        assert_eq!(stats.items_per_device, vec![2, 2, 2]);
        assert_eq!(stats.load_balance_ratio, 1.0);
    }

    // -----------------------------------------------------------------------
    // GPU Memory Manager tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gpu_memory_allocate_deallocate() {
        let mut mgr = GpuMemoryManager::new(1024, 512);
        let a = mgr.allocate(256).unwrap();
        assert_eq!(mgr.stats().allocated, 256);
        mgr.deallocate(&a);
        assert_eq!(mgr.stats().allocated, 0);
    }

    #[test]
    fn test_gpu_memory_exhaustion() {
        let mut mgr = GpuMemoryManager::new(256, 128);
        let _a = mgr.allocate(256).unwrap();
        assert!(mgr.allocate(1).is_err());
    }

    #[test]
    fn test_gpu_memory_spill_to_host() {
        let mut mgr = GpuMemoryManager::new(256, 512);
        let _a = mgr.allocate(128).unwrap();
        let _b = mgr.allocate(128).unwrap();
        assert_eq!(mgr.stats().allocated, 256);

        let freed = mgr.spill_to_host();
        assert_eq!(freed, 128);
        assert_eq!(mgr.stats().allocated, 128);
        assert_eq!(mgr.stats().spilled_to_host, 128);
    }

    #[test]
    fn test_gpu_memory_peak_tracking() {
        let mut mgr = GpuMemoryManager::new(1024, 256);
        let a = mgr.allocate(512).unwrap();
        assert_eq!(mgr.stats().peak, 512);
        mgr.deallocate(&a);
        let _b = mgr.allocate(128).unwrap();
        // Peak should still reflect the historic maximum
        assert_eq!(mgr.stats().peak, 512);
    }

    #[test]
    fn test_gpu_memory_zero_alloc_error() {
        let mut mgr = GpuMemoryManager::new(1024, 256);
        assert!(mgr.allocate(0).is_err());
    }
}
