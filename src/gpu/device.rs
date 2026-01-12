//! GPU Device Management
//!
//! This module provides GPU device detection, capability querying,
//! and device selection for different backends (CUDA, Metal, WebGPU).

use std::fmt;
use crate::error::{BlazeError, Result};

/// GPU backend type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GpuBackend {
    /// NVIDIA CUDA
    Cuda,
    /// Apple Metal
    Metal,
    /// Cross-platform WebGPU
    WebGpu,
    /// OpenCL (legacy support)
    OpenCL,
    /// Software fallback (CPU simulation)
    Software,
}

impl GpuBackend {
    /// Get the display name of this backend.
    pub fn name(&self) -> &'static str {
        match self {
            GpuBackend::Cuda => "CUDA",
            GpuBackend::Metal => "Metal",
            GpuBackend::WebGpu => "WebGPU",
            GpuBackend::OpenCL => "OpenCL",
            GpuBackend::Software => "Software",
        }
    }

    /// Check if this backend is available on the current platform.
    pub fn is_available(&self) -> bool {
        match self {
            GpuBackend::Cuda => cfg!(feature = "cuda"),
            GpuBackend::Metal => cfg!(target_os = "macos") || cfg!(target_os = "ios"),
            GpuBackend::WebGpu => true, // Always available as fallback
            GpuBackend::OpenCL => cfg!(feature = "opencl"),
            GpuBackend::Software => true,
        }
    }
}

impl fmt::Display for GpuBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// GPU device capabilities.
#[derive(Debug, Clone)]
pub struct GpuCapabilities {
    /// Compute capability version (e.g., 7.5 for CUDA)
    pub compute_version: (u32, u32),
    /// Total global memory in bytes
    pub total_memory: usize,
    /// Maximum threads per block
    pub max_threads_per_block: u32,
    /// Maximum blocks per grid (x, y, z)
    pub max_grid_size: (u32, u32, u32),
    /// Maximum shared memory per block in bytes
    pub max_shared_memory: usize,
    /// Number of multiprocessors / compute units
    pub num_compute_units: u32,
    /// Clock rate in MHz
    pub clock_rate_mhz: u32,
    /// Memory bus width in bits
    pub memory_bus_width: u32,
    /// Memory bandwidth in GB/s
    pub memory_bandwidth_gbps: f64,
    /// Whether unified memory is supported
    pub unified_memory: bool,
    /// Whether async copy is supported
    pub async_copy: bool,
    /// Maximum work group size (for compute shaders)
    pub max_work_group_size: u32,
}

impl Default for GpuCapabilities {
    fn default() -> Self {
        Self {
            compute_version: (0, 0),
            total_memory: 0,
            max_threads_per_block: 256,
            max_grid_size: (65535, 65535, 65535),
            max_shared_memory: 48 * 1024, // 48 KB typical
            num_compute_units: 1,
            clock_rate_mhz: 1000,
            memory_bus_width: 128,
            memory_bandwidth_gbps: 100.0,
            unified_memory: false,
            async_copy: true,
            max_work_group_size: 256,
        }
    }
}

impl GpuCapabilities {
    /// Get theoretical TFLOPS (single precision).
    pub fn theoretical_tflops(&self) -> f64 {
        // Approximate: 2 * cores * clock (ops per core per cycle varies by arch)
        let cores_per_unit = 64; // Approximate for modern GPUs
        let total_cores = self.num_compute_units as f64 * cores_per_unit as f64;
        let clock_ghz = self.clock_rate_mhz as f64 / 1000.0;
        2.0 * total_cores * clock_ghz / 1000.0 // TFLOPS
    }

    /// Check if the GPU has sufficient memory for an operation.
    pub fn has_sufficient_memory(&self, required_bytes: usize) -> bool {
        // Leave some headroom for GPU runtime
        let available = (self.total_memory as f64 * 0.9) as usize;
        required_bytes <= available
    }
}

/// Represents a GPU device.
#[derive(Debug, Clone)]
pub struct GpuDevice {
    /// Device index
    index: usize,
    /// Device name
    name: String,
    /// Backend type
    backend: GpuBackend,
    /// Device capabilities
    capabilities: GpuCapabilities,
    /// Whether this device is active
    active: bool,
}

impl GpuDevice {
    /// Create a new GPU device.
    pub fn new(
        index: usize,
        name: String,
        backend: GpuBackend,
        capabilities: GpuCapabilities,
    ) -> Self {
        Self {
            index,
            name,
            backend,
            capabilities,
            active: false,
        }
    }

    /// Create a software fallback device.
    pub fn software_fallback() -> Self {
        Self {
            index: 0,
            name: "Software Renderer".to_string(),
            backend: GpuBackend::Software,
            capabilities: GpuCapabilities::default(),
            active: true,
        }
    }

    /// Get the device index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the device name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the backend type.
    pub fn backend(&self) -> GpuBackend {
        self.backend
    }

    /// Get device capabilities.
    pub fn capabilities(&self) -> &GpuCapabilities {
        &self.capabilities
    }

    /// Check if this device is active.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Set this device as active.
    pub fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    /// Get total memory in bytes.
    pub fn total_memory(&self) -> usize {
        self.capabilities.total_memory
    }

    /// Get number of compute units.
    pub fn compute_units(&self) -> u32 {
        self.capabilities.num_compute_units
    }
}

impl fmt::Display for GpuDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} ({}) - {} MB, {} CUs",
            self.index,
            self.name,
            self.backend,
            self.capabilities.total_memory / (1024 * 1024),
            self.capabilities.num_compute_units
        )
    }
}

/// Manages GPU devices and selection.
pub struct DeviceManager {
    /// Available devices
    devices: Vec<GpuDevice>,
    /// Active device index
    active_index: Option<usize>,
    /// Preferred backend
    preferred_backend: Option<GpuBackend>,
}

impl DeviceManager {
    /// Create a new device manager and enumerate available GPUs.
    pub fn new(preferred_backend: Option<GpuBackend>) -> Result<Self> {
        let mut manager = Self {
            devices: Vec::new(),
            active_index: None,
            preferred_backend,
        };

        manager.enumerate_devices()?;
        manager.select_best_device();

        Ok(manager)
    }

    /// Create an empty device manager (no GPU support).
    pub fn empty() -> Self {
        Self {
            devices: Vec::new(),
            active_index: None,
            preferred_backend: None,
        }
    }

    /// Enumerate available GPU devices.
    fn enumerate_devices(&mut self) -> Result<()> {
        self.devices.clear();

        // Try to detect Metal devices on macOS
        #[cfg(target_os = "macos")]
        {
            if let Some(device) = self.detect_metal_device() {
                self.devices.push(device);
            }
        }

        // Try to detect CUDA devices (if feature enabled)
        #[cfg(feature = "cuda")]
        {
            self.detect_cuda_devices()?;
        }

        // Add simulated device for testing if no real devices found
        if self.devices.is_empty() {
            self.devices.push(self.create_simulated_device());
        }

        Ok(())
    }

    /// Detect Metal device on macOS.
    #[cfg(target_os = "macos")]
    fn detect_metal_device(&self) -> Option<GpuDevice> {
        // In a real implementation, this would use the metal-rs crate
        // For now, we create a simulated Metal device
        Some(GpuDevice::new(
            0,
            "Apple GPU".to_string(),
            GpuBackend::Metal,
            GpuCapabilities {
                compute_version: (3, 0), // Metal 3
                total_memory: 8 * 1024 * 1024 * 1024, // 8 GB typical
                max_threads_per_block: 1024,
                max_grid_size: (65535, 65535, 65535),
                max_shared_memory: 32 * 1024,
                num_compute_units: 32, // Varies by chip
                clock_rate_mhz: 1500,
                memory_bus_width: 256,
                memory_bandwidth_gbps: 400.0,
                unified_memory: true,
                async_copy: true,
                max_work_group_size: 1024,
            },
        ))
    }

    /// Create a simulated GPU device for testing.
    fn create_simulated_device(&self) -> GpuDevice {
        GpuDevice::new(
            0,
            "Simulated GPU".to_string(),
            GpuBackend::Software,
            GpuCapabilities {
                compute_version: (1, 0),
                total_memory: 4 * 1024 * 1024 * 1024, // 4 GB
                max_threads_per_block: 256,
                max_grid_size: (1024, 1024, 64),
                max_shared_memory: 16 * 1024,
                num_compute_units: 8,
                clock_rate_mhz: 1000,
                memory_bus_width: 128,
                memory_bandwidth_gbps: 50.0,
                unified_memory: true,
                async_copy: false,
                max_work_group_size: 256,
            },
        )
    }

    /// Select the best available device.
    fn select_best_device(&mut self) {
        if self.devices.is_empty() {
            return;
        }

        // If preferred backend is specified, try to find matching device
        if let Some(preferred) = self.preferred_backend {
            for (idx, device) in self.devices.iter().enumerate() {
                if device.backend() == preferred {
                    self.active_index = Some(idx);
                    return;
                }
            }
        }

        // Otherwise, select device with most compute units (and prefer real GPUs)
        let best_idx = self
            .devices
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                // Prefer real GPUs over software
                let a_score = if a.backend() == GpuBackend::Software {
                    0
                } else {
                    a.compute_units() as usize * 1000
                };
                let b_score = if b.backend() == GpuBackend::Software {
                    0
                } else {
                    b.compute_units() as usize * 1000
                };
                a_score.cmp(&b_score)
            })
            .map(|(idx, _)| idx);

        self.active_index = best_idx;

        // Mark the selected device as active
        if let Some(idx) = self.active_index {
            if let Some(device) = self.devices.get_mut(idx) {
                device.set_active(true);
            }
        }
    }

    /// Check if any devices are available.
    pub fn has_devices(&self) -> bool {
        !self.devices.is_empty()
    }

    /// Get the number of available devices.
    pub fn device_count(&self) -> usize {
        self.devices.len()
    }

    /// Get all available devices.
    pub fn devices(&self) -> &[GpuDevice] {
        &self.devices
    }

    /// Get the active device.
    pub fn active_device(&self) -> Option<&GpuDevice> {
        self.active_index.and_then(|idx| self.devices.get(idx))
    }

    /// Set the active device by index.
    pub fn set_active_device(&mut self, index: usize) -> Result<()> {
        if index >= self.devices.len() {
            return Err(BlazeError::invalid_argument(format!(
                "Invalid device index: {}",
                index
            )));
        }

        // Deactivate current device
        if let Some(idx) = self.active_index {
            if let Some(device) = self.devices.get_mut(idx) {
                device.set_active(false);
            }
        }

        // Activate new device
        self.active_index = Some(index);
        if let Some(device) = self.devices.get_mut(index) {
            device.set_active(true);
        }

        Ok(())
    }

    /// Get device by index.
    pub fn device(&self, index: usize) -> Option<&GpuDevice> {
        self.devices.get(index)
    }
}

/// GPU device selector for multi-GPU configurations.
#[derive(Debug, Clone)]
pub enum DeviceSelector {
    /// Use the first available device
    First,
    /// Use device with most memory
    MostMemory,
    /// Use device with most compute units
    MostCompute,
    /// Use specific device by index
    ByIndex(usize),
    /// Use specific device by name pattern
    ByName(String),
    /// Round-robin across all devices
    RoundRobin,
}

impl DeviceSelector {
    /// Select a device from the manager.
    pub fn select<'a>(&self, manager: &'a DeviceManager) -> Option<&'a GpuDevice> {
        match self {
            DeviceSelector::First => manager.devices.first(),
            DeviceSelector::MostMemory => manager
                .devices
                .iter()
                .max_by_key(|d| d.capabilities.total_memory),
            DeviceSelector::MostCompute => manager
                .devices
                .iter()
                .max_by_key(|d| d.capabilities.num_compute_units),
            DeviceSelector::ByIndex(idx) => manager.devices.get(*idx),
            DeviceSelector::ByName(pattern) => manager
                .devices
                .iter()
                .find(|d| d.name.to_lowercase().contains(&pattern.to_lowercase())),
            DeviceSelector::RoundRobin => manager.active_device(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_backend() {
        assert_eq!(GpuBackend::Cuda.name(), "CUDA");
        assert_eq!(GpuBackend::Metal.name(), "Metal");
        assert_eq!(format!("{}", GpuBackend::WebGpu), "WebGPU");
    }

    #[test]
    fn test_gpu_capabilities() {
        let caps = GpuCapabilities {
            compute_version: (8, 0),
            total_memory: 16 * 1024 * 1024 * 1024, // 16 GB
            num_compute_units: 108,
            clock_rate_mhz: 1700,
            ..Default::default()
        };

        assert!(caps.has_sufficient_memory(1024 * 1024));
        assert!(!caps.has_sufficient_memory(20 * 1024 * 1024 * 1024));

        let tflops = caps.theoretical_tflops();
        assert!(tflops > 0.0);
    }

    #[test]
    fn test_gpu_device() {
        let device = GpuDevice::new(
            0,
            "Test GPU".to_string(),
            GpuBackend::Cuda,
            GpuCapabilities::default(),
        );

        assert_eq!(device.index(), 0);
        assert_eq!(device.name(), "Test GPU");
        assert_eq!(device.backend(), GpuBackend::Cuda);
        assert!(!device.is_active());
    }

    #[test]
    fn test_device_manager() {
        let manager = DeviceManager::new(None).unwrap();

        assert!(manager.has_devices());
        assert!(manager.device_count() > 0);
        assert!(manager.active_device().is_some());
    }

    #[test]
    fn test_device_manager_empty() {
        let manager = DeviceManager::empty();

        assert!(!manager.has_devices());
        assert_eq!(manager.device_count(), 0);
        assert!(manager.active_device().is_none());
    }

    #[test]
    fn test_device_selector() {
        let manager = DeviceManager::new(None).unwrap();

        let device = DeviceSelector::First.select(&manager);
        assert!(device.is_some());

        let device = DeviceSelector::MostMemory.select(&manager);
        assert!(device.is_some());

        let device = DeviceSelector::MostCompute.select(&manager);
        assert!(device.is_some());
    }

    #[test]
    fn test_software_fallback() {
        let device = GpuDevice::software_fallback();
        assert_eq!(device.backend(), GpuBackend::Software);
        assert!(device.is_active());
    }
}
