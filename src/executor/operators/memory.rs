//! Memory manager for tracking and limiting memory usage.

use crate::error::{BlazeError, Result};

/// Memory manager for tracking and limiting memory usage.
#[derive(Debug)]
pub struct MemoryManager {
    /// Maximum memory budget in bytes
    max_memory: usize,
    /// Current memory usage in bytes
    used_memory: std::sync::atomic::AtomicUsize,
    /// Peak memory usage in bytes (high-water mark)
    peak_memory: std::sync::atomic::AtomicUsize,
}

impl MemoryManager {
    /// Create a new memory manager with the given budget.
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used_memory: std::sync::atomic::AtomicUsize::new(0),
            peak_memory: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a memory manager with default budget (1GB).
    pub fn default_budget() -> Self {
        Self::new(1024 * 1024 * 1024) // 1GB
    }

    /// Try to reserve memory. Returns true if successful.
    pub fn try_reserve(&self, bytes: usize) -> bool {
        use std::sync::atomic::Ordering;

        let current = self.used_memory.load(Ordering::Relaxed);
        if current + bytes <= self.max_memory {
            let new_used = self.used_memory.fetch_add(bytes, Ordering::Relaxed) + bytes;
            self.peak_memory.fetch_max(new_used, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Reserve memory, returning error if over budget.
    pub fn reserve(&self, bytes: usize) -> Result<MemoryReservation<'_>> {
        if self.try_reserve(bytes) {
            Ok(MemoryReservation {
                manager: self,
                bytes,
            })
        } else {
            Err(BlazeError::resource_exhausted(format!(
                "Memory limit exceeded: requested {} bytes, {} of {} used",
                bytes,
                self.used_memory.load(std::sync::atomic::Ordering::Relaxed),
                self.max_memory
            )))
        }
    }

    /// Release memory.
    pub fn release(&self, bytes: usize) {
        use std::sync::atomic::Ordering;
        self.used_memory.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get current memory usage.
    pub fn used(&self) -> usize {
        self.used_memory.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get maximum memory budget.
    pub fn max(&self) -> usize {
        self.max_memory
    }

    /// Get available memory.
    pub fn available(&self) -> usize {
        self.max_memory.saturating_sub(self.used())
    }

    /// Get peak memory usage (high-water mark since last reset).
    pub fn peak(&self) -> usize {
        self.peak_memory
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reset peak memory counter to current usage, returning the previous peak.
    pub fn reset_peak(&self) -> usize {
        let current = self.used();
        self.peak_memory
            .swap(current, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::default_budget()
    }
}

/// A reservation of memory that is released when dropped.
pub struct MemoryReservation<'a> {
    manager: &'a MemoryManager,
    bytes: usize,
}

impl<'a> MemoryReservation<'a> {
    /// Get the number of reserved bytes.
    pub fn size(&self) -> usize {
        self.bytes
    }

    /// Grow the reservation by additional bytes.
    pub fn grow(&mut self, additional: usize) -> Result<()> {
        if self.manager.try_reserve(additional) {
            self.bytes += additional;
            Ok(())
        } else {
            Err(BlazeError::resource_exhausted("Memory limit exceeded"))
        }
    }

    /// Shrink the reservation by the given bytes.
    pub fn shrink(&mut self, bytes: usize) {
        let release = bytes.min(self.bytes);
        self.manager.release(release);
        self.bytes -= release;
    }
}

impl<'a> Drop for MemoryReservation<'a> {
    fn drop(&mut self) {
        self.manager.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_manager_reserve_and_release() {
        let mgr = MemoryManager::new(1000);
        assert_eq!(mgr.used(), 0);
        assert_eq!(mgr.available(), 1000);

        assert!(mgr.try_reserve(400));
        assert_eq!(mgr.used(), 400);
        assert_eq!(mgr.available(), 600);

        mgr.release(400);
        assert_eq!(mgr.used(), 0);
    }

    #[test]
    fn test_memory_manager_over_budget() {
        let mgr = MemoryManager::new(100);
        assert!(mgr.try_reserve(50));
        assert!(!mgr.try_reserve(60)); // 50 + 60 > 100

        let result = mgr.reserve(60);
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_reservation_releases_on_drop() {
        let mgr = MemoryManager::new(1000);
        {
            let _reservation = mgr.reserve(500).unwrap();
            assert_eq!(mgr.used(), 500);
        }
        // After drop, memory should be released
        assert_eq!(mgr.used(), 0);
    }

    #[test]
    fn test_memory_manager_peak_tracking() {
        let mgr = MemoryManager::new(1000);
        assert_eq!(mgr.peak(), 0);

        assert!(mgr.try_reserve(400));
        assert_eq!(mgr.peak(), 400);

        assert!(mgr.try_reserve(300));
        assert_eq!(mgr.peak(), 700);

        // Release some memory — peak should NOT decrease
        mgr.release(500);
        assert_eq!(mgr.used(), 200);
        assert_eq!(mgr.peak(), 700);

        // Reset peak — returns old peak, sets to current usage
        let old_peak = mgr.reset_peak();
        assert_eq!(old_peak, 700);
        assert_eq!(mgr.peak(), 200);
    }
}
