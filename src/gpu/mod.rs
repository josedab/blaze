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
}
