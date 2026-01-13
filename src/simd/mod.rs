//! SIMD Vectorization with JIT Compilation
//!
//! This module provides SIMD (Single Instruction Multiple Data) vectorization
//! capabilities with optional JIT (Just-In-Time) compilation for high-performance
//! query execution.
//!
//! # Features
//!
//! - **SIMD Primitives**: Vectorized operations for common data types
//! - **Auto-vectorization**: Automatic detection and use of best SIMD instructions
//! - **JIT Compilation**: Runtime code generation for custom operations
//! - **Platform Detection**: Support for SSE4.2, AVX2, AVX-512, NEON
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::simd::{SimdContext, SimdVector};
//!
//! let ctx = SimdContext::new();
//! let a = SimdVector::<i32>::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
//! let b = SimdVector::<i32>::from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]);
//! let result = a.add(&b);
//! ```

mod vectorize;
mod jit;
mod codegen;

pub use vectorize::{
    SimdVector, SimdVectorOps, SimdFilterOps, SimdAggregateOps,
    SimdCapabilities, detect_simd_capabilities,
};
pub use jit::{JitCompiler, JitFunction, JitConfig, CompiledKernel};
pub use codegen::{CodeGenerator, GeneratedCode, Instruction, Register};

use crate::error::{BlazeError, Result};

/// SIMD instruction set levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimdLevel {
    /// No SIMD (scalar operations)
    Scalar,
    /// SSE 4.2 (128-bit vectors)
    Sse42,
    /// AVX2 (256-bit vectors)
    Avx2,
    /// AVX-512 (512-bit vectors)
    Avx512,
    /// ARM NEON (128-bit vectors)
    Neon,
    /// WebAssembly SIMD (128-bit vectors)
    WasmSimd,
}

impl SimdLevel {
    /// Get the vector width in bytes.
    pub fn vector_width(&self) -> usize {
        match self {
            SimdLevel::Scalar => 1,
            SimdLevel::Sse42 => 16,
            SimdLevel::Avx2 => 32,
            SimdLevel::Avx512 => 64,
            SimdLevel::Neon => 16,
            SimdLevel::WasmSimd => 16,
        }
    }

    /// Get number of i32 elements per vector.
    pub fn i32_lanes(&self) -> usize {
        self.vector_width() / std::mem::size_of::<i32>()
    }

    /// Get number of i64 elements per vector.
    pub fn i64_lanes(&self) -> usize {
        self.vector_width() / std::mem::size_of::<i64>()
    }

    /// Get number of f32 elements per vector.
    pub fn f32_lanes(&self) -> usize {
        self.vector_width() / std::mem::size_of::<f32>()
    }

    /// Get number of f64 elements per vector.
    pub fn f64_lanes(&self) -> usize {
        self.vector_width() / std::mem::size_of::<f64>()
    }

    /// Check if this level is available on the current CPU.
    pub fn is_available(&self) -> bool {
        match self {
            SimdLevel::Scalar => true,
            SimdLevel::Sse42 => {
                #[cfg(target_arch = "x86_64")]
                {
                    std::is_x86_feature_detected!("sse4.2")
                }
                #[cfg(not(target_arch = "x86_64"))]
                {
                    false
                }
            }
            SimdLevel::Avx2 => {
                #[cfg(target_arch = "x86_64")]
                {
                    std::is_x86_feature_detected!("avx2")
                }
                #[cfg(not(target_arch = "x86_64"))]
                {
                    false
                }
            }
            SimdLevel::Avx512 => {
                #[cfg(target_arch = "x86_64")]
                {
                    std::is_x86_feature_detected!("avx512f")
                }
                #[cfg(not(target_arch = "x86_64"))]
                {
                    false
                }
            }
            SimdLevel::Neon => {
                #[cfg(target_arch = "aarch64")]
                {
                    true // NEON is always available on AArch64
                }
                #[cfg(not(target_arch = "aarch64"))]
                {
                    false
                }
            }
            SimdLevel::WasmSimd => {
                #[cfg(target_arch = "wasm32")]
                {
                    true // WASM SIMD is enabled at compile time
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    false
                }
            }
        }
    }

    /// Get the best available SIMD level for this CPU.
    pub fn best_available() -> Self {
        if SimdLevel::Avx512.is_available() {
            SimdLevel::Avx512
        } else if SimdLevel::Avx2.is_available() {
            SimdLevel::Avx2
        } else if SimdLevel::Sse42.is_available() {
            SimdLevel::Sse42
        } else if SimdLevel::Neon.is_available() {
            SimdLevel::Neon
        } else if SimdLevel::WasmSimd.is_available() {
            SimdLevel::WasmSimd
        } else {
            SimdLevel::Scalar
        }
    }
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimdLevel::Scalar => write!(f, "Scalar"),
            SimdLevel::Sse42 => write!(f, "SSE4.2"),
            SimdLevel::Avx2 => write!(f, "AVX2"),
            SimdLevel::Avx512 => write!(f, "AVX-512"),
            SimdLevel::Neon => write!(f, "NEON"),
            SimdLevel::WasmSimd => write!(f, "WASM SIMD"),
        }
    }
}

/// SIMD configuration.
#[derive(Debug, Clone)]
pub struct SimdConfig {
    /// Enabled SIMD level (None = auto-detect)
    pub simd_level: Option<SimdLevel>,
    /// Enable JIT compilation
    pub enable_jit: bool,
    /// JIT optimization level (0-3)
    pub jit_opt_level: u8,
    /// Cache compiled kernels
    pub cache_kernels: bool,
    /// Maximum kernel cache size
    pub max_cache_size: usize,
    /// Minimum batch size for SIMD
    pub min_batch_size: usize,
    /// Enable prefetching
    pub enable_prefetch: bool,
}

impl Default for SimdConfig {
    fn default() -> Self {
        Self {
            simd_level: None, // Auto-detect
            enable_jit: true,
            jit_opt_level: 2,
            cache_kernels: true,
            max_cache_size: 1024,
            min_batch_size: 64,
            enable_prefetch: true,
        }
    }
}

impl SimdConfig {
    /// Create a new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set SIMD level.
    pub fn with_simd_level(mut self, level: SimdLevel) -> Self {
        self.simd_level = Some(level);
        self
    }

    /// Enable or disable JIT.
    pub fn with_jit(mut self, enabled: bool) -> Self {
        self.enable_jit = enabled;
        self
    }

    /// Set JIT optimization level.
    pub fn with_jit_opt_level(mut self, level: u8) -> Self {
        self.jit_opt_level = level.min(3);
        self
    }

    /// Set minimum batch size for SIMD.
    pub fn with_min_batch_size(mut self, size: usize) -> Self {
        self.min_batch_size = size;
        self
    }
}

/// SIMD execution context.
pub struct SimdContext {
    /// Configuration
    config: SimdConfig,
    /// Detected SIMD level
    simd_level: SimdLevel,
    /// JIT compiler (if enabled)
    jit: Option<JitCompiler>,
    /// Execution statistics
    stats: SimdStats,
}

impl SimdContext {
    /// Create a new SIMD context.
    pub fn new() -> Self {
        Self::with_config(SimdConfig::default())
    }

    /// Create with custom configuration.
    pub fn with_config(config: SimdConfig) -> Self {
        let simd_level = config.simd_level.unwrap_or_else(SimdLevel::best_available);

        let jit = if config.enable_jit {
            Some(JitCompiler::new(JitConfig {
                opt_level: config.jit_opt_level,
                cache_enabled: config.cache_kernels,
                max_cache_size: config.max_cache_size,
            }))
        } else {
            None
        };

        Self {
            config,
            simd_level,
            jit,
            stats: SimdStats::default(),
        }
    }

    /// Get the current SIMD level.
    pub fn simd_level(&self) -> SimdLevel {
        self.simd_level
    }

    /// Get the JIT compiler.
    pub fn jit(&self) -> Option<&JitCompiler> {
        self.jit.as_ref()
    }

    /// Get mutable JIT compiler.
    pub fn jit_mut(&mut self) -> Option<&mut JitCompiler> {
        self.jit.as_mut()
    }

    /// Check if batch size is suitable for SIMD.
    pub fn should_use_simd(&self, batch_size: usize) -> bool {
        batch_size >= self.config.min_batch_size
    }

    /// Get execution statistics.
    pub fn stats(&self) -> &SimdStats {
        &self.stats
    }

    /// Reset statistics.
    pub fn reset_stats(&mut self) {
        self.stats = SimdStats::default();
    }

    /// Execute a vectorized addition.
    pub fn vector_add_i32(&mut self, a: &[i32], b: &[i32], result: &mut [i32]) -> Result<()> {
        if a.len() != b.len() || a.len() != result.len() {
            return Err(BlazeError::invalid_argument("Array lengths must match"));
        }

        self.stats.operations += 1;
        self.stats.elements_processed += a.len();

        // Use SIMD when appropriate
        if self.should_use_simd(a.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_add_i32(a, b, result, self.simd_level);
        } else {
            // Scalar fallback
            for i in 0..a.len() {
                result[i] = a[i].wrapping_add(b[i]);
            }
        }

        Ok(())
    }

    /// Execute a vectorized filter.
    pub fn vector_filter_gt_i32(&mut self, data: &[i32], threshold: i32) -> Result<Vec<bool>> {
        self.stats.operations += 1;
        self.stats.elements_processed += data.len();

        let result = if self.should_use_simd(data.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_filter_gt_i32(data, threshold, self.simd_level)
        } else {
            data.iter().map(|&x| x > threshold).collect()
        };

        Ok(result)
    }

    /// Execute a vectorized sum.
    pub fn vector_sum_i64(&mut self, data: &[i64]) -> Result<i64> {
        self.stats.operations += 1;
        self.stats.elements_processed += data.len();

        let result = if self.should_use_simd(data.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_sum_i64(data, self.simd_level)
        } else {
            data.iter().sum()
        };

        Ok(result)
    }

    /// Execute a vectorized sum for f64.
    pub fn vector_sum_f64(&mut self, data: &[f64]) -> Result<f64> {
        self.stats.operations += 1;
        self.stats.elements_processed += data.len();

        let result = if self.should_use_simd(data.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_sum_f64(data, self.simd_level)
        } else {
            data.iter().sum()
        };

        Ok(result)
    }

    /// Execute a vectorized min.
    pub fn vector_min_i64(&mut self, data: &[i64]) -> Result<Option<i64>> {
        if data.is_empty() {
            return Ok(None);
        }

        self.stats.operations += 1;
        self.stats.elements_processed += data.len();

        let result = if self.should_use_simd(data.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_min_i64(data, self.simd_level)
        } else {
            *data.iter().min().unwrap()
        };

        Ok(Some(result))
    }

    /// Execute a vectorized max.
    pub fn vector_max_i64(&mut self, data: &[i64]) -> Result<Option<i64>> {
        if data.is_empty() {
            return Ok(None);
        }

        self.stats.operations += 1;
        self.stats.elements_processed += data.len();

        let result = if self.should_use_simd(data.len()) {
            self.stats.simd_operations += 1;
            vectorize::simd_max_i64(data, self.simd_level)
        } else {
            *data.iter().max().unwrap()
        };

        Ok(Some(result))
    }
}

impl Default for SimdContext {
    fn default() -> Self {
        Self::new()
    }
}

/// SIMD execution statistics.
#[derive(Debug, Clone, Default)]
pub struct SimdStats {
    /// Total number of operations.
    pub operations: usize,
    /// Number of SIMD operations.
    pub simd_operations: usize,
    /// Number of JIT-compiled operations.
    pub jit_operations: usize,
    /// Total elements processed.
    pub elements_processed: usize,
    /// Cache hits for JIT kernels.
    pub jit_cache_hits: usize,
    /// Cache misses for JIT kernels.
    pub jit_cache_misses: usize,
}

impl SimdStats {
    /// Calculate SIMD utilization percentage.
    pub fn simd_utilization(&self) -> f64 {
        if self.operations == 0 {
            0.0
        } else {
            (self.simd_operations as f64 / self.operations as f64) * 100.0
        }
    }

    /// Calculate JIT cache hit rate.
    pub fn jit_cache_hit_rate(&self) -> f64 {
        let total = self.jit_cache_hits + self.jit_cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.jit_cache_hits as f64 / total as f64) * 100.0
        }
    }
}

/// Vectorized operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorOp {
    /// Addition
    Add,
    /// Subtraction
    Sub,
    /// Multiplication
    Mul,
    /// Division
    Div,
    /// Bitwise AND
    And,
    /// Bitwise OR
    Or,
    /// Bitwise XOR
    Xor,
    /// Minimum
    Min,
    /// Maximum
    Max,
    /// Sum (reduction)
    Sum,
    /// Count (reduction)
    Count,
    /// Average (reduction)
    Avg,
    /// Equality comparison
    Eq,
    /// Less than comparison
    Lt,
    /// Greater than comparison
    Gt,
    /// Less than or equal
    Le,
    /// Greater than or equal
    Ge,
}

impl VectorOp {
    /// Check if this is a reduction operation.
    pub fn is_reduction(&self) -> bool {
        matches!(self, VectorOp::Sum | VectorOp::Count | VectorOp::Avg | VectorOp::Min | VectorOp::Max)
    }

    /// Check if this is a comparison operation.
    pub fn is_comparison(&self) -> bool {
        matches!(self, VectorOp::Eq | VectorOp::Lt | VectorOp::Gt | VectorOp::Le | VectorOp::Ge)
    }
}

/// Data type for vectorized operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorDataType {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl VectorDataType {
    /// Get the size of this type in bytes.
    pub fn size(&self) -> usize {
        match self {
            VectorDataType::I8 | VectorDataType::U8 => 1,
            VectorDataType::I16 | VectorDataType::U16 => 2,
            VectorDataType::I32 | VectorDataType::U32 | VectorDataType::F32 => 4,
            VectorDataType::I64 | VectorDataType::U64 | VectorDataType::F64 => 8,
        }
    }

    /// Check if this type is signed.
    pub fn is_signed(&self) -> bool {
        matches!(self, VectorDataType::I8 | VectorDataType::I16 | VectorDataType::I32 | VectorDataType::I64)
    }

    /// Check if this is a floating point type.
    pub fn is_float(&self) -> bool {
        matches!(self, VectorDataType::F32 | VectorDataType::F64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_level_detection() {
        let level = SimdLevel::best_available();
        assert!(level >= SimdLevel::Scalar);

        // Scalar is always available
        assert!(SimdLevel::Scalar.is_available());
    }

    #[test]
    fn test_simd_level_vector_width() {
        assert_eq!(SimdLevel::Scalar.vector_width(), 1);
        assert_eq!(SimdLevel::Sse42.vector_width(), 16);
        assert_eq!(SimdLevel::Avx2.vector_width(), 32);
        assert_eq!(SimdLevel::Avx512.vector_width(), 64);
        assert_eq!(SimdLevel::Neon.vector_width(), 16);
    }

    #[test]
    fn test_simd_level_lanes() {
        assert_eq!(SimdLevel::Sse42.i32_lanes(), 4);
        assert_eq!(SimdLevel::Avx2.i32_lanes(), 8);
        assert_eq!(SimdLevel::Avx512.i32_lanes(), 16);

        assert_eq!(SimdLevel::Sse42.f64_lanes(), 2);
        assert_eq!(SimdLevel::Avx2.f64_lanes(), 4);
        assert_eq!(SimdLevel::Avx512.f64_lanes(), 8);
    }

    #[test]
    fn test_simd_config() {
        let config = SimdConfig::new()
            .with_simd_level(SimdLevel::Avx2)
            .with_jit(true)
            .with_jit_opt_level(3)
            .with_min_batch_size(128);

        assert_eq!(config.simd_level, Some(SimdLevel::Avx2));
        assert!(config.enable_jit);
        assert_eq!(config.jit_opt_level, 3);
        assert_eq!(config.min_batch_size, 128);
    }

    #[test]
    fn test_simd_context_creation() {
        let ctx = SimdContext::new();
        assert!(ctx.simd_level() >= SimdLevel::Scalar);
    }

    #[test]
    fn test_vector_add_i32() {
        let mut ctx = SimdContext::new();
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let b = vec![8, 7, 6, 5, 4, 3, 2, 1];
        let mut result = vec![0; 8];

        ctx.vector_add_i32(&a, &b, &mut result).unwrap();

        assert_eq!(result, vec![9, 9, 9, 9, 9, 9, 9, 9]);
    }

    #[test]
    fn test_vector_filter_gt() {
        let mut ctx = SimdContext::new();
        let data = vec![1, 5, 3, 7, 2, 8, 4, 6];

        let result = ctx.vector_filter_gt_i32(&data, 4).unwrap();

        assert_eq!(result, vec![false, true, false, true, false, true, false, true]);
    }

    #[test]
    fn test_vector_sum_i64() {
        let mut ctx = SimdContext::new();
        let data: Vec<i64> = (1..=100).collect();

        let result = ctx.vector_sum_i64(&data).unwrap();

        assert_eq!(result, 5050);
    }

    #[test]
    fn test_vector_sum_f64() {
        let mut ctx = SimdContext::new();
        let data: Vec<f64> = (1..=100).map(|x| x as f64).collect();

        let result = ctx.vector_sum_f64(&data).unwrap();

        assert!((result - 5050.0).abs() < 0.001);
    }

    #[test]
    fn test_vector_min_max() {
        let mut ctx = SimdContext::new();
        let data: Vec<i64> = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];

        let min = ctx.vector_min_i64(&data).unwrap();
        let max = ctx.vector_max_i64(&data).unwrap();

        assert_eq!(min, Some(1));
        assert_eq!(max, Some(9));
    }

    #[test]
    fn test_simd_stats() {
        let mut ctx = SimdContext::with_config(
            SimdConfig::new().with_min_batch_size(4)
        );

        let data: Vec<i64> = (1..=100).collect();
        let _ = ctx.vector_sum_i64(&data).unwrap();
        let _ = ctx.vector_sum_i64(&data).unwrap();

        let stats = ctx.stats();
        assert_eq!(stats.operations, 2);
        assert_eq!(stats.elements_processed, 200);
    }

    #[test]
    fn test_vector_op_properties() {
        assert!(VectorOp::Sum.is_reduction());
        assert!(VectorOp::Count.is_reduction());
        assert!(!VectorOp::Add.is_reduction());

        assert!(VectorOp::Eq.is_comparison());
        assert!(VectorOp::Lt.is_comparison());
        assert!(!VectorOp::Add.is_comparison());
    }

    #[test]
    fn test_vector_data_type() {
        assert_eq!(VectorDataType::I32.size(), 4);
        assert_eq!(VectorDataType::I64.size(), 8);
        assert_eq!(VectorDataType::F64.size(), 8);

        assert!(VectorDataType::I32.is_signed());
        assert!(!VectorDataType::U32.is_signed());
        assert!(VectorDataType::F64.is_float());
        assert!(!VectorDataType::I64.is_float());
    }
}
