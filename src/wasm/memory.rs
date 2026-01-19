//! WASM Memory Management
//!
//! This module provides memory management utilities for the WASM runtime,
//! including buffer allocation and tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::error::{BlazeError, Result};

/// WASM memory manager.
pub struct WasmMemoryManager {
    /// Maximum memory limit
    max_memory: usize,
    /// Currently allocated bytes
    allocated: AtomicUsize,
    /// Peak allocation
    peak: AtomicUsize,
    /// Number of active allocations
    num_allocations: AtomicUsize,
    /// Next buffer ID
    next_id: AtomicU64,
    /// Active buffers
    buffers: Mutex<HashMap<u64, BufferInfo>>,
}

/// Information about an allocated buffer.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BufferInfo {
    /// Buffer ID
    id: u64,
    /// Size in bytes
    size: usize,
    /// Description/purpose
    description: String,
}

impl WasmMemoryManager {
    /// Create a new memory manager.
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            allocated: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            num_allocations: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            buffers: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a buffer.
    pub fn allocate(&self, size: usize, description: &str) -> Result<WasmBuffer> {
        // Check memory limit
        let current = self.allocated.load(Ordering::SeqCst);
        if current + size > self.max_memory {
            return Err(BlazeError::resource_exhausted(format!(
                "WASM memory limit exceeded: requested {} bytes, {} available",
                size,
                self.max_memory - current
            )));
        }

        // Allocate
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let data = vec![0u8; size];

        // Update tracking
        let new_allocated = self.allocated.fetch_add(size, Ordering::SeqCst) + size;
        self.num_allocations.fetch_add(1, Ordering::SeqCst);

        // Update peak
        loop {
            let current_peak = self.peak.load(Ordering::SeqCst);
            if new_allocated <= current_peak {
                break;
            }
            if self
                .peak
                .compare_exchange(
                    current_peak,
                    new_allocated,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
        }

        // Track buffer
        if let Ok(mut buffers) = self.buffers.lock() {
            buffers.insert(
                id,
                BufferInfo {
                    id,
                    size,
                    description: description.to_string(),
                },
            );
        }

        Ok(WasmBuffer { id, data, size })
    }

    /// Free a buffer.
    pub fn free(&self, buffer: WasmBuffer) {
        self.allocated.fetch_sub(buffer.size, Ordering::SeqCst);
        self.num_allocations.fetch_sub(1, Ordering::SeqCst);

        if let Ok(mut buffers) = self.buffers.lock() {
            buffers.remove(&buffer.id);
        }
    }

    /// Get current memory usage.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::SeqCst)
    }

    /// Get peak memory usage.
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// Get available memory.
    pub fn available(&self) -> usize {
        self.max_memory - self.allocated.load(Ordering::SeqCst)
    }

    /// Get number of active allocations.
    pub fn num_allocations(&self) -> usize {
        self.num_allocations.load(Ordering::SeqCst)
    }

    /// Check if allocation is possible.
    pub fn can_allocate(&self, size: usize) -> bool {
        self.available() >= size
    }

    /// Reset statistics (for testing).
    pub fn reset_stats(&self) {
        self.peak
            .store(self.allocated.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Get memory statistics.
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            allocated: self.allocated.load(Ordering::SeqCst),
            peak: self.peak.load(Ordering::SeqCst),
            max_memory: self.max_memory,
            num_allocations: self.num_allocations.load(Ordering::SeqCst),
        }
    }
}

impl Default for WasmMemoryManager {
    fn default() -> Self {
        Self::new(256 * 1024 * 1024) // 256 MB default
    }
}

/// WASM buffer for data storage.
pub struct WasmBuffer {
    /// Buffer ID
    id: u64,
    /// Data
    data: Vec<u8>,
    /// Size in bytes
    size: usize,
}

impl WasmBuffer {
    /// Get the buffer ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the buffer size.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get a reference to the data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get a mutable reference to the data.
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the data as a pointer (for WASM interop).
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Get the data as a mutable pointer (for WASM interop).
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    /// Write data to the buffer.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > self.size {
            return Err(BlazeError::invalid_argument(format!(
                "Buffer overflow: offset {} + len {} > size {}",
                offset,
                data.len(),
                self.size
            )));
        }

        self.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Read data from the buffer.
    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if offset + len > self.size {
            return Err(BlazeError::invalid_argument(format!(
                "Buffer read out of bounds: offset {} + len {} > size {}",
                offset, len, self.size
            )));
        }

        Ok(&self.data[offset..offset + len])
    }

    /// Clear the buffer (zero-fill).
    pub fn clear(&mut self) {
        self.data.fill(0);
    }

    /// Create a buffer from existing data.
    pub fn from_vec(id: u64, data: Vec<u8>) -> Self {
        let size = data.len();
        Self { id, data, size }
    }
}

impl std::fmt::Debug for WasmBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmBuffer")
            .field("id", &self.id)
            .field("size", &self.size)
            .finish()
    }
}

/// Memory statistics.
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Currently allocated bytes
    pub allocated: usize,
    /// Peak allocation
    pub peak: usize,
    /// Maximum memory limit
    pub max_memory: usize,
    /// Number of active allocations
    pub num_allocations: usize,
}

impl MemoryStats {
    /// Get usage percentage.
    pub fn usage_percent(&self) -> f64 {
        if self.max_memory == 0 {
            0.0
        } else {
            (self.allocated as f64 / self.max_memory as f64) * 100.0
        }
    }

    /// Get available memory.
    pub fn available(&self) -> usize {
        self.max_memory.saturating_sub(self.allocated)
    }
}

/// Scoped buffer that automatically frees when dropped.
#[allow(dead_code)]
pub struct ScopedBuffer<'a> {
    /// The buffer
    buffer: Option<WasmBuffer>,
    /// Reference to the memory manager
    manager: &'a WasmMemoryManager,
}

#[allow(dead_code)]
impl<'a> ScopedBuffer<'a> {
    /// Create a new scoped buffer.
    pub fn new(manager: &'a WasmMemoryManager, size: usize, description: &str) -> Result<Self> {
        let buffer = manager.allocate(size, description)?;
        Ok(Self {
            buffer: Some(buffer),
            manager,
        })
    }

    /// Get a reference to the buffer.
    pub fn buffer(&self) -> Option<&WasmBuffer> {
        self.buffer.as_ref()
    }

    /// Get a mutable reference to the buffer.
    pub fn buffer_mut(&mut self) -> Option<&mut WasmBuffer> {
        self.buffer.as_mut()
    }

    /// Take ownership of the buffer (prevents auto-free).
    pub fn take(mut self) -> Option<WasmBuffer> {
        self.buffer.take()
    }
}

impl<'a> Drop for ScopedBuffer<'a> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.manager.free(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_manager() {
        let manager = WasmMemoryManager::new(1024);

        let buffer = manager.allocate(256, "test").unwrap();
        assert_eq!(buffer.size(), 256);
        assert_eq!(manager.allocated(), 256);
        assert_eq!(manager.num_allocations(), 1);

        manager.free(buffer);
        assert_eq!(manager.allocated(), 0);
        assert_eq!(manager.num_allocations(), 0);
    }

    #[test]
    fn test_memory_limit() {
        let manager = WasmMemoryManager::new(100);

        let result = manager.allocate(200, "too big");
        assert!(result.is_err());
    }

    #[test]
    fn test_wasm_buffer() {
        let mut buffer = WasmBuffer::from_vec(1, vec![0; 100]);

        assert_eq!(buffer.size(), 100);
        assert_eq!(buffer.id(), 1);

        // Write and read
        buffer.write(0, b"hello").unwrap();
        let data = buffer.read(0, 5).unwrap();
        assert_eq!(data, b"hello");

        // Clear
        buffer.clear();
        let data = buffer.read(0, 5).unwrap();
        assert_eq!(data, &[0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_buffer_overflow() {
        let mut buffer = WasmBuffer::from_vec(1, vec![0; 10]);

        let result = buffer.write(8, b"hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_stats() {
        let manager = WasmMemoryManager::new(1000);

        let b1 = manager.allocate(200, "b1").unwrap();
        let b2 = manager.allocate(300, "b2").unwrap();

        let stats = manager.stats();
        assert_eq!(stats.allocated, 500);
        assert_eq!(stats.num_allocations, 2);
        assert!((stats.usage_percent() - 50.0).abs() < 0.1);

        manager.free(b1);
        manager.free(b2);

        let stats = manager.stats();
        assert_eq!(stats.allocated, 0);
        assert_eq!(stats.peak, 500);
    }

    #[test]
    fn test_scoped_buffer() {
        let manager = WasmMemoryManager::new(1024);

        {
            let scoped = ScopedBuffer::new(&manager, 256, "scoped").unwrap();
            assert_eq!(manager.allocated(), 256);
            assert!(scoped.buffer().is_some());
        }

        // Buffer should be freed after scope
        assert_eq!(manager.allocated(), 0);
    }

    #[test]
    fn test_scoped_buffer_take() {
        let manager = WasmMemoryManager::new(1024);

        let buffer = {
            let scoped = ScopedBuffer::new(&manager, 256, "scoped").unwrap();
            scoped.take()
        };

        // Buffer should NOT be freed because we took it
        assert_eq!(manager.allocated(), 256);
        assert!(buffer.is_some());

        // Manually free
        manager.free(buffer.unwrap());
        assert_eq!(manager.allocated(), 0);
    }
}
