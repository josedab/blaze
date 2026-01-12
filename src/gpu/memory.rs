//! GPU Memory Management
//!
//! This module provides GPU memory allocation, pooling, and host-device
//! data transfer abstractions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use super::MemoryStats;

/// Direction of memory transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    /// Host (CPU) to Device (GPU)
    HostToDevice,
    /// Device (GPU) to Host (CPU)
    DeviceToHost,
    /// Device to Device (peer-to-peer or same device)
    DeviceToDevice,
}

impl TransferDirection {
    /// Get a human-readable name.
    pub fn name(&self) -> &'static str {
        match self {
            TransferDirection::HostToDevice => "H2D",
            TransferDirection::DeviceToHost => "D2H",
            TransferDirection::DeviceToDevice => "D2D",
        }
    }
}

/// Handle to GPU memory allocation.
pub struct GpuBuffer {
    /// Unique buffer ID
    id: u64,
    /// Size in bytes
    size: usize,
    /// Device index
    device_index: usize,
    /// Whether this buffer is pinned (page-locked)
    pinned: bool,
    /// Raw pointer (simulated, would be actual GPU pointer)
    ptr: usize,
    /// Reference to the memory pool for cleanup
    pool_ref: Option<Arc<GpuMemoryPoolInner>>,
}

impl std::fmt::Debug for GpuBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuBuffer")
            .field("id", &self.id)
            .field("size", &self.size)
            .field("device_index", &self.device_index)
            .field("pinned", &self.pinned)
            .field("ptr", &format_args!("0x{:x}", self.ptr))
            .finish()
    }
}

impl GpuBuffer {
    /// Create a new GPU buffer (for testing/simulation).
    pub fn new(id: u64, size: usize, device_index: usize) -> Self {
        Self {
            id,
            size,
            device_index,
            pinned: false,
            ptr: id as usize * 0x1000, // Simulated address
            pool_ref: None,
        }
    }

    /// Get the buffer ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the buffer size in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the device index.
    pub fn device_index(&self) -> usize {
        self.device_index
    }

    /// Check if this buffer is pinned.
    pub fn is_pinned(&self) -> bool {
        self.pinned
    }

    /// Get the raw pointer (for FFI).
    pub fn as_ptr(&self) -> usize {
        self.ptr
    }

    /// Set pool reference for automatic cleanup.
    fn set_pool_ref(&mut self, pool: Arc<GpuMemoryPoolInner>) {
        self.pool_ref = Some(pool);
    }
}

impl Drop for GpuBuffer {
    fn drop(&mut self) {
        // Notify the pool that this buffer is being freed
        if let Some(ref pool) = self.pool_ref {
            pool.record_deallocation(self.size);
        }
    }
}

/// Inner state for GPU memory pool.
struct GpuMemoryPoolInner {
    /// Maximum memory limit
    max_memory: usize,
    /// Currently allocated bytes
    allocated: AtomicUsize,
    /// Peak allocation
    peak: AtomicUsize,
    /// Number of current allocations
    num_allocations: AtomicUsize,
    /// Total allocation count
    total_allocations: AtomicU64,
    /// Total deallocation count
    total_deallocations: AtomicU64,
    /// Next buffer ID
    next_id: AtomicU64,
    /// Free list for buffer recycling (size -> list of buffers)
    free_list: Mutex<HashMap<usize, Vec<GpuBuffer>>>,
}

impl GpuMemoryPoolInner {
    fn record_deallocation(&self, size: usize) {
        self.allocated.fetch_sub(size, Ordering::SeqCst);
        self.num_allocations.fetch_sub(1, Ordering::SeqCst);
        self.total_deallocations.fetch_add(1, Ordering::SeqCst);
    }
}

/// GPU memory pool for efficient allocation and reuse.
pub struct GpuMemoryPool {
    /// Inner state
    inner: Arc<GpuMemoryPoolInner>,
    /// Device index
    device_index: usize,
}

impl GpuMemoryPool {
    /// Create a new memory pool with the given maximum size.
    pub fn new(max_memory: usize) -> Self {
        Self {
            inner: Arc::new(GpuMemoryPoolInner {
                max_memory,
                allocated: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                num_allocations: AtomicUsize::new(0),
                total_allocations: AtomicU64::new(0),
                total_deallocations: AtomicU64::new(0),
                next_id: AtomicU64::new(1),
                free_list: Mutex::new(HashMap::new()),
            }),
            device_index: 0,
        }
    }

    /// Allocate a GPU buffer of the given size.
    pub fn allocate(&self, size: usize) -> Result<GpuBuffer> {
        // Check if we can allocate
        let current = self.inner.allocated.load(Ordering::SeqCst);
        if current + size > self.inner.max_memory {
            return Err(BlazeError::resource_exhausted(format!(
                "GPU memory exhausted: requested {} bytes, {} available",
                size,
                self.inner.max_memory - current
            )));
        }

        // Try to recycle from free list
        if let Ok(mut free_list) = self.inner.free_list.lock() {
            // Look for exact size match
            if let Some(buffers) = free_list.get_mut(&size) {
                if let Some(buffer) = buffers.pop() {
                    // Reuse existing buffer
                    return Ok(buffer);
                }
            }

            // Look for larger buffer that could work
            let larger_sizes: Vec<usize> = free_list
                .keys()
                .filter(|&s| *s >= size && *s <= size * 2)
                .copied()
                .collect();

            for larger_size in larger_sizes {
                if let Some(buffers) = free_list.get_mut(&larger_size) {
                    if let Some(buffer) = buffers.pop() {
                        return Ok(buffer);
                    }
                }
            }
        }

        // Allocate new buffer
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let mut buffer = GpuBuffer::new(id, size, self.device_index);
        buffer.set_pool_ref(self.inner.clone());

        // Update statistics
        let new_allocated = self.inner.allocated.fetch_add(size, Ordering::SeqCst) + size;
        self.inner.num_allocations.fetch_add(1, Ordering::SeqCst);
        self.inner.total_allocations.fetch_add(1, Ordering::SeqCst);

        // Update peak
        loop {
            let current_peak = self.inner.peak.load(Ordering::SeqCst);
            if new_allocated <= current_peak {
                break;
            }
            if self.inner.peak.compare_exchange(
                current_peak,
                new_allocated,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                break;
            }
        }

        Ok(buffer)
    }

    /// Allocate a pinned (page-locked) buffer for faster transfers.
    pub fn allocate_pinned(&self, size: usize) -> Result<GpuBuffer> {
        let mut buffer = self.allocate(size)?;
        buffer.pinned = true;
        Ok(buffer)
    }

    /// Return a buffer to the pool for reuse.
    pub fn recycle(&self, buffer: GpuBuffer) {
        if let Ok(mut free_list) = self.inner.free_list.lock() {
            free_list
                .entry(buffer.size)
                .or_insert_with(Vec::new)
                .push(buffer);
        }
    }

    /// Clear the free list and release cached buffers.
    pub fn clear_cache(&self) {
        if let Ok(mut free_list) = self.inner.free_list.lock() {
            free_list.clear();
        }
    }

    /// Get memory statistics.
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            total_bytes: self.inner.max_memory,
            allocated_bytes: self.inner.allocated.load(Ordering::SeqCst),
            peak_bytes: self.inner.peak.load(Ordering::SeqCst),
            num_allocations: self.inner.num_allocations.load(Ordering::SeqCst),
            total_allocations: self.inner.total_allocations.load(Ordering::SeqCst),
            total_deallocations: self.inner.total_deallocations.load(Ordering::SeqCst),
        }
    }

    /// Get available memory.
    pub fn available(&self) -> usize {
        self.inner.max_memory - self.inner.allocated.load(Ordering::SeqCst)
    }

    /// Check if there's enough memory for an allocation.
    pub fn can_allocate(&self, size: usize) -> bool {
        self.available() >= size
    }
}

/// Handles memory transfers between host and device.
pub struct MemoryTransfer {
    /// Transfer statistics
    stats: TransferStats,
    /// Whether async transfers are enabled
    async_enabled: bool,
}

impl MemoryTransfer {
    /// Create a new memory transfer handler.
    pub fn new(async_enabled: bool) -> Self {
        Self {
            stats: TransferStats::default(),
            async_enabled,
        }
    }

    /// Transfer a RecordBatch to GPU memory.
    pub fn to_device(
        &mut self,
        batch: &RecordBatch,
        pool: &GpuMemoryPool,
    ) -> Result<GpuRecordBatch> {
        let total_size = batch.get_array_memory_size();

        // Allocate GPU buffer
        let buffer = pool.allocate(total_size)?;

        // Record transfer statistics
        self.stats.record_transfer(TransferDirection::HostToDevice, total_size);

        // In a real implementation, this would copy data to GPU
        // For now, we track the metadata

        Ok(GpuRecordBatch {
            buffer,
            num_rows: batch.num_rows(),
            num_columns: batch.num_columns(),
            schema: batch.schema(),
        })
    }

    /// Transfer an Arrow array to GPU memory.
    pub fn array_to_device(
        &mut self,
        array: &ArrayRef,
        pool: &GpuMemoryPool,
    ) -> Result<GpuArray> {
        let size = array.get_array_memory_size();

        let buffer = pool.allocate(size)?;
        self.stats.record_transfer(TransferDirection::HostToDevice, size);

        Ok(GpuArray {
            buffer,
            len: array.len(),
            null_count: array.null_count(),
        })
    }

    /// Transfer GPU data back to host.
    pub fn to_host(
        &mut self,
        gpu_batch: &GpuRecordBatch,
    ) -> Result<RecordBatch> {
        self.stats.record_transfer(
            TransferDirection::DeviceToHost,
            gpu_batch.buffer.size(),
        );

        // In a real implementation, this would copy data from GPU
        // For now, return an empty batch with the same schema
        let arrays: Vec<ArrayRef> = (0..gpu_batch.num_columns)
            .map(|_| {
                Arc::new(arrow::array::Int32Array::from(vec![0i32; gpu_batch.num_rows]))
                    as ArrayRef
            })
            .collect();

        // This is a simulation - real implementation would transfer actual data
        RecordBatch::try_new(gpu_batch.schema.clone(), arrays)
            .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))
    }

    /// Get transfer statistics.
    pub fn stats(&self) -> &TransferStats {
        &self.stats
    }

    /// Check if async transfers are enabled.
    pub fn is_async(&self) -> bool {
        self.async_enabled
    }
}

/// A RecordBatch stored in GPU memory.
pub struct GpuRecordBatch {
    /// GPU buffer containing the data
    pub buffer: GpuBuffer,
    /// Number of rows
    pub num_rows: usize,
    /// Number of columns
    pub num_columns: usize,
    /// Schema
    pub schema: Arc<arrow::datatypes::Schema>,
}

impl GpuRecordBatch {
    /// Get the GPU buffer.
    pub fn buffer(&self) -> &GpuBuffer {
        &self.buffer
    }

    /// Get the number of rows.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Get the number of columns.
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    /// Get the schema.
    pub fn schema(&self) -> &Arc<arrow::datatypes::Schema> {
        &self.schema
    }

    /// Get the size in bytes.
    pub fn size(&self) -> usize {
        self.buffer.size()
    }
}

/// An Arrow array stored in GPU memory.
pub struct GpuArray {
    /// GPU buffer containing the data
    buffer: GpuBuffer,
    /// Number of elements
    len: usize,
    /// Null count
    null_count: usize,
}

impl GpuArray {
    /// Get the GPU buffer.
    pub fn buffer(&self) -> &GpuBuffer {
        &self.buffer
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the null count.
    pub fn null_count(&self) -> usize {
        self.null_count
    }
}

/// Statistics for memory transfers.
#[derive(Debug, Clone, Default)]
pub struct TransferStats {
    /// Total bytes transferred host-to-device
    pub h2d_bytes: u64,
    /// Total bytes transferred device-to-host
    pub d2h_bytes: u64,
    /// Number of H2D transfers
    pub h2d_count: u64,
    /// Number of D2H transfers
    pub d2h_count: u64,
}

impl TransferStats {
    /// Record a transfer.
    pub fn record_transfer(&mut self, direction: TransferDirection, bytes: usize) {
        match direction {
            TransferDirection::HostToDevice => {
                self.h2d_bytes += bytes as u64;
                self.h2d_count += 1;
            }
            TransferDirection::DeviceToHost => {
                self.d2h_bytes += bytes as u64;
                self.d2h_count += 1;
            }
            TransferDirection::DeviceToDevice => {
                // D2D transfers don't count toward H2D/D2H stats
            }
        }
    }

    /// Get total bytes transferred.
    pub fn total_bytes(&self) -> u64 {
        self.h2d_bytes + self.d2h_bytes
    }

    /// Get total transfer count.
    pub fn total_count(&self) -> u64 {
        self.h2d_count + self.d2h_count
    }

    /// Get average H2D transfer size.
    pub fn avg_h2d_size(&self) -> f64 {
        if self.h2d_count == 0 {
            0.0
        } else {
            self.h2d_bytes as f64 / self.h2d_count as f64
        }
    }

    /// Get average D2H transfer size.
    pub fn avg_d2h_size(&self) -> f64 {
        if self.d2h_count == 0 {
            0.0
        } else {
            self.d2h_bytes as f64 / self.d2h_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_gpu_buffer() {
        let buffer = GpuBuffer::new(1, 1024, 0);
        assert_eq!(buffer.id(), 1);
        assert_eq!(buffer.size(), 1024);
        assert_eq!(buffer.device_index(), 0);
        assert!(!buffer.is_pinned());
    }

    #[test]
    fn test_memory_pool_allocation() {
        let pool = GpuMemoryPool::new(1024 * 1024); // 1 MB

        let buffer = pool.allocate(1024).unwrap();
        assert_eq!(buffer.size(), 1024);

        let stats = pool.stats();
        assert_eq!(stats.allocated_bytes, 1024);
        assert_eq!(stats.num_allocations, 1);
    }

    #[test]
    fn test_memory_pool_exhaustion() {
        let pool = GpuMemoryPool::new(1024); // 1 KB

        // First allocation should succeed
        let _buffer = pool.allocate(512).unwrap();

        // Second allocation that would exceed limit should fail
        let result = pool.allocate(1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_pool_recycling() {
        let pool = GpuMemoryPool::new(1024 * 1024);

        // Allocate and recycle
        let buffer = pool.allocate(1024).unwrap();
        let id1 = buffer.id();
        pool.recycle(buffer);

        // Next allocation of same size should reuse
        let buffer2 = pool.allocate(1024).unwrap();
        assert_eq!(buffer2.id(), id1);
    }

    #[test]
    fn test_transfer_stats() {
        let mut stats = TransferStats::default();

        stats.record_transfer(TransferDirection::HostToDevice, 1000);
        stats.record_transfer(TransferDirection::HostToDevice, 2000);
        stats.record_transfer(TransferDirection::DeviceToHost, 500);

        assert_eq!(stats.h2d_bytes, 3000);
        assert_eq!(stats.h2d_count, 2);
        assert_eq!(stats.d2h_bytes, 500);
        assert_eq!(stats.d2h_count, 1);
        assert_eq!(stats.total_bytes(), 3500);
        assert_eq!(stats.avg_h2d_size(), 1500.0);
    }

    #[test]
    fn test_memory_transfer() {
        let pool = GpuMemoryPool::new(1024 * 1024);
        let mut transfer = MemoryTransfer::new(true);

        // Create a test batch
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // Transfer to device
        let gpu_batch = transfer.to_device(&batch, &pool).unwrap();
        assert_eq!(gpu_batch.num_rows(), 5);
        assert_eq!(gpu_batch.num_columns(), 1);

        let stats = transfer.stats();
        assert!(stats.h2d_bytes > 0);
        assert_eq!(stats.h2d_count, 1);
    }

    #[test]
    fn test_gpu_record_batch() {
        let buffer = GpuBuffer::new(1, 1024, 0);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let gpu_batch = GpuRecordBatch {
            buffer,
            num_rows: 100,
            num_columns: 1,
            schema,
        };

        assert_eq!(gpu_batch.num_rows(), 100);
        assert_eq!(gpu_batch.num_columns(), 1);
        assert_eq!(gpu_batch.size(), 1024);
    }

    #[test]
    fn test_transfer_direction() {
        assert_eq!(TransferDirection::HostToDevice.name(), "H2D");
        assert_eq!(TransferDirection::DeviceToHost.name(), "D2H");
        assert_eq!(TransferDirection::DeviceToDevice.name(), "D2D");
    }
}
