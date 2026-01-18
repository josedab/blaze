//! JIT Compilation for SIMD Operations
//!
//! This module provides JIT (Just-In-Time) compilation capabilities
//! for generating optimized SIMD code at runtime.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::codegen::{CodeGenerator, GeneratedCode};
use super::{SimdLevel, VectorDataType, VectorOp};
use crate::error::{BlazeError, Result};

/// JIT compiler configuration.
#[derive(Debug, Clone)]
pub struct JitConfig {
    /// Optimization level (0-3)
    pub opt_level: u8,
    /// Enable kernel caching
    pub cache_enabled: bool,
    /// Maximum cache size
    pub max_cache_size: usize,
}

impl Default for JitConfig {
    fn default() -> Self {
        Self {
            opt_level: 2,
            cache_enabled: true,
            max_cache_size: 1024,
        }
    }
}

/// JIT compiler for SIMD operations.
pub struct JitCompiler {
    /// Configuration
    config: JitConfig,
    /// Target SIMD level
    simd_level: SimdLevel,
    /// Code generator
    codegen: CodeGenerator,
    /// Kernel cache
    cache: Arc<RwLock<KernelCache>>,
    /// Compilation statistics
    stats: JitStats,
}

impl JitCompiler {
    /// Create a new JIT compiler.
    pub fn new(config: JitConfig) -> Self {
        Self {
            simd_level: SimdLevel::best_available(),
            codegen: CodeGenerator::new(SimdLevel::best_available()),
            cache: Arc::new(RwLock::new(KernelCache::new(config.max_cache_size))),
            config,
            stats: JitStats::default(),
        }
    }

    /// Create with specific SIMD level.
    pub fn with_simd_level(mut self, level: SimdLevel) -> Self {
        self.simd_level = level;
        self.codegen = CodeGenerator::new(level);
        self
    }

    /// Get the target SIMD level.
    pub fn simd_level(&self) -> SimdLevel {
        self.simd_level
    }

    /// Get compilation statistics.
    pub fn stats(&self) -> &JitStats {
        &self.stats
    }

    /// Compile a kernel for a vector operation.
    pub fn compile_kernel(
        &mut self,
        op: VectorOp,
        data_type: VectorDataType,
    ) -> Result<CompiledKernel> {
        let key = KernelKey { op, data_type };

        // Check cache first
        if self.config.cache_enabled {
            if let Some(kernel) = self.get_cached(&key) {
                self.stats.cache_hits += 1;
                return Ok(kernel);
            }
            self.stats.cache_misses += 1;
        }

        // Generate code
        let code = self.codegen.generate_op(op, data_type)?;

        // Compile the code
        let kernel = self.compile_code(&code, op, data_type)?;

        // Cache the kernel
        if self.config.cache_enabled {
            self.cache_kernel(&key, &kernel);
        }

        self.stats.kernels_compiled += 1;
        Ok(kernel)
    }

    /// Compile a filter kernel.
    pub fn compile_filter_kernel(
        &mut self,
        data_type: VectorDataType,
        comparison: VectorOp,
    ) -> Result<CompiledKernel> {
        let key = KernelKey {
            op: comparison,
            data_type,
        };

        if self.config.cache_enabled {
            if let Some(kernel) = self.get_cached(&key) {
                self.stats.cache_hits += 1;
                return Ok(kernel);
            }
            self.stats.cache_misses += 1;
        }

        let code = self.codegen.generate_filter(comparison, data_type)?;
        let kernel = self.compile_code(&code, comparison, data_type)?;

        if self.config.cache_enabled {
            self.cache_kernel(&key, &kernel);
        }

        self.stats.kernels_compiled += 1;
        Ok(kernel)
    }

    /// Compile an aggregate kernel.
    pub fn compile_aggregate_kernel(
        &mut self,
        data_type: VectorDataType,
        agg_op: VectorOp,
    ) -> Result<CompiledKernel> {
        let key = KernelKey {
            op: agg_op,
            data_type,
        };

        if self.config.cache_enabled {
            if let Some(kernel) = self.get_cached(&key) {
                self.stats.cache_hits += 1;
                return Ok(kernel);
            }
            self.stats.cache_misses += 1;
        }

        let code = self.codegen.generate_aggregate(agg_op, data_type)?;
        let kernel = self.compile_code(&code, agg_op, data_type)?;

        if self.config.cache_enabled {
            self.cache_kernel(&key, &kernel);
        }

        self.stats.kernels_compiled += 1;
        Ok(kernel)
    }

    /// Compile a fused kernel (multiple operations).
    pub fn compile_fused_kernel(
        &mut self,
        ops: &[VectorOp],
        data_type: VectorDataType,
    ) -> Result<CompiledKernel> {
        if ops.is_empty() {
            return Err(BlazeError::invalid_argument("No operations to fuse"));
        }

        let code = self.codegen.generate_fused(ops, data_type)?;
        let kernel = CompiledKernel {
            id: self.stats.kernels_compiled as u64,
            name: format!("fused_{:?}", ops),
            code: code.bytecode.clone(),
            simd_level: self.simd_level,
            data_type,
            op: ops[0],
            execution_fn: Arc::new(|_input, _output, _params| Ok(())),
        };

        self.stats.kernels_compiled += 1;
        Ok(kernel)
    }

    /// Get a cached kernel.
    fn get_cached(&self, key: &KernelKey) -> Option<CompiledKernel> {
        let cache = self.cache.read().ok()?;
        cache.get(key).cloned()
    }

    /// Cache a kernel.
    fn cache_kernel(&self, key: &KernelKey, kernel: &CompiledKernel) {
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(key.clone(), kernel.clone());
        }
    }

    /// Compile generated code into a kernel.
    fn compile_code(
        &self,
        code: &GeneratedCode,
        op: VectorOp,
        data_type: VectorDataType,
    ) -> Result<CompiledKernel> {
        // In a real implementation, this would generate native code
        // For now, we create an interpreted kernel

        let kernel = CompiledKernel {
            id: self.stats.kernels_compiled as u64,
            name: format!("{:?}_{:?}", op, data_type),
            code: code.bytecode.clone(),
            simd_level: self.simd_level,
            data_type,
            op,
            execution_fn: create_execution_fn(op, data_type, self.simd_level),
        };

        Ok(kernel)
    }

    /// Clear the kernel cache.
    pub fn clear_cache(&mut self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Get cache size.
    pub fn cache_size(&self) -> usize {
        self.cache.read().map(|c| c.len()).unwrap_or(0)
    }
}

impl Default for JitCompiler {
    fn default() -> Self {
        Self::new(JitConfig::default())
    }
}

/// Key for kernel cache.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct KernelKey {
    op: VectorOp,
    data_type: VectorDataType,
}

/// Kernel cache.
struct KernelCache {
    cache: HashMap<KernelKey, CompiledKernel>,
    max_size: usize,
    access_order: Vec<KernelKey>,
}

impl KernelCache {
    fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            access_order: Vec::new(),
        }
    }

    fn get(&self, key: &KernelKey) -> Option<&CompiledKernel> {
        self.cache.get(key)
    }

    fn insert(&mut self, key: KernelKey, kernel: CompiledKernel) {
        // Evict if at capacity
        while self.cache.len() >= self.max_size && !self.access_order.is_empty() {
            if let Some(evict_key) = self.access_order.first().cloned() {
                self.cache.remove(&evict_key);
                self.access_order.remove(0);
            }
        }

        self.cache.insert(key.clone(), kernel);
        self.access_order.push(key);
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.access_order.clear();
    }
}

/// JIT compilation statistics.
#[derive(Debug, Clone, Default)]
pub struct JitStats {
    /// Number of kernels compiled.
    pub kernels_compiled: usize,
    /// Cache hits.
    pub cache_hits: usize,
    /// Cache misses.
    pub cache_misses: usize,
    /// Total compilation time (microseconds).
    pub compilation_time_us: u64,
    /// Total execution time (microseconds).
    pub execution_time_us: u64,
}

impl JitStats {
    /// Calculate cache hit rate.
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.cache_hits as f64 / total as f64) * 100.0
        }
    }
}

/// Compiled kernel.
pub struct CompiledKernel {
    /// Kernel ID.
    pub id: u64,
    /// Kernel name.
    pub name: String,
    /// Compiled bytecode.
    pub code: Vec<u8>,
    /// Target SIMD level.
    pub simd_level: SimdLevel,
    /// Data type.
    pub data_type: VectorDataType,
    /// Operation type.
    pub op: VectorOp,
    /// Execution function.
    execution_fn: Arc<dyn Fn(&[u8], &mut [u8], &KernelParams) -> Result<()> + Send + Sync>,
}

impl Clone for CompiledKernel {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            code: self.code.clone(),
            simd_level: self.simd_level,
            data_type: self.data_type,
            op: self.op,
            execution_fn: Arc::clone(&self.execution_fn),
        }
    }
}

impl std::fmt::Debug for CompiledKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledKernel")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("simd_level", &self.simd_level)
            .field("data_type", &self.data_type)
            .field("op", &self.op)
            .field("code_size", &self.code.len())
            .finish()
    }
}

impl CompiledKernel {
    /// Execute the kernel.
    pub fn execute(&self, input: &[u8], output: &mut [u8], params: &KernelParams) -> Result<()> {
        (self.execution_fn)(input, output, params)
    }

    /// Get code size in bytes.
    pub fn code_size(&self) -> usize {
        self.code.len()
    }
}

/// Kernel execution parameters.
#[derive(Debug, Clone, Default)]
pub struct KernelParams {
    /// Input size in elements.
    pub input_size: usize,
    /// Threshold value (for comparisons).
    pub threshold: Option<i64>,
    /// Float threshold (for f64 comparisons).
    pub threshold_f64: Option<f64>,
    /// Mask for conditional operations.
    pub mask: Option<Vec<bool>>,
}

/// JIT-compiled function.
pub struct JitFunction {
    /// Function name.
    pub name: String,
    /// Compiled kernel.
    kernel: CompiledKernel,
}

impl JitFunction {
    /// Create a new JIT function.
    pub fn new(name: impl Into<String>, kernel: CompiledKernel) -> Self {
        Self {
            name: name.into(),
            kernel,
        }
    }

    /// Execute the function.
    pub fn call(&self, input: &[u8], output: &mut [u8], params: &KernelParams) -> Result<()> {
        self.kernel.execute(input, output, params)
    }
}

impl std::fmt::Debug for JitFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JitFunction")
            .field("name", &self.name)
            .field("kernel", &self.kernel)
            .finish()
    }
}

/// Create an execution function for an operation.
fn create_execution_fn(
    op: VectorOp,
    data_type: VectorDataType,
    _simd_level: SimdLevel,
) -> Arc<dyn Fn(&[u8], &mut [u8], &KernelParams) -> Result<()> + Send + Sync> {
    match (op, data_type) {
        (VectorOp::Add, VectorDataType::I32) => {
            Arc::new(|input: &[u8], output: &mut [u8], _params: &KernelParams| {
                // Interpret input as two i32 slices concatenated
                let elem_size = std::mem::size_of::<i32>();
                let num_elems = input.len() / (2 * elem_size);

                let a_bytes = &input[..num_elems * elem_size];
                let b_bytes = &input[num_elems * elem_size..];

                for i in 0..num_elems {
                    let offset = i * elem_size;
                    let a = i32::from_le_bytes([
                        a_bytes[offset],
                        a_bytes[offset + 1],
                        a_bytes[offset + 2],
                        a_bytes[offset + 3],
                    ]);
                    let b = i32::from_le_bytes([
                        b_bytes[offset],
                        b_bytes[offset + 1],
                        b_bytes[offset + 2],
                        b_bytes[offset + 3],
                    ]);
                    let result = a.wrapping_add(b);
                    let result_bytes = result.to_le_bytes();
                    output[offset..offset + 4].copy_from_slice(&result_bytes);
                }
                Ok(())
            })
        }
        (VectorOp::Sum, VectorDataType::I64) => {
            Arc::new(|input: &[u8], output: &mut [u8], _params: &KernelParams| {
                let elem_size = std::mem::size_of::<i64>();
                let num_elems = input.len() / elem_size;
                let mut sum: i64 = 0;

                for i in 0..num_elems {
                    let offset = i * elem_size;
                    let val = i64::from_le_bytes([
                        input[offset],
                        input[offset + 1],
                        input[offset + 2],
                        input[offset + 3],
                        input[offset + 4],
                        input[offset + 5],
                        input[offset + 6],
                        input[offset + 7],
                    ]);
                    sum += val;
                }

                if output.len() >= 8 {
                    output[..8].copy_from_slice(&sum.to_le_bytes());
                }
                Ok(())
            })
        }
        (VectorOp::Gt, VectorDataType::I32) => {
            Arc::new(|input: &[u8], output: &mut [u8], params: &KernelParams| {
                let elem_size = std::mem::size_of::<i32>();
                let num_elems = input.len() / elem_size;
                let threshold = params.threshold.unwrap_or(0) as i32;

                for i in 0..num_elems.min(output.len()) {
                    let offset = i * elem_size;
                    let val = i32::from_le_bytes([
                        input[offset],
                        input[offset + 1],
                        input[offset + 2],
                        input[offset + 3],
                    ]);
                    output[i] = if val > threshold { 1 } else { 0 };
                }
                Ok(())
            })
        }
        _ => {
            // Default no-op for unsupported combinations
            Arc::new(|_input: &[u8], _output: &mut [u8], _params: &KernelParams| Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_compiler_creation() {
        let compiler = JitCompiler::new(JitConfig::default());
        assert!(compiler.simd_level() >= SimdLevel::Scalar);
    }

    #[test]
    fn test_compile_kernel() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let kernel = compiler
            .compile_kernel(VectorOp::Add, VectorDataType::I32)
            .unwrap();

        assert_eq!(kernel.op, VectorOp::Add);
        assert_eq!(kernel.data_type, VectorDataType::I32);
    }

    #[test]
    fn test_kernel_caching() {
        let mut compiler = JitCompiler::new(JitConfig {
            cache_enabled: true,
            ..Default::default()
        });

        // First compilation
        let _k1 = compiler
            .compile_kernel(VectorOp::Add, VectorDataType::I32)
            .unwrap();
        assert_eq!(compiler.stats().cache_misses, 1);
        assert_eq!(compiler.stats().cache_hits, 0);

        // Second compilation should hit cache
        let _k2 = compiler
            .compile_kernel(VectorOp::Add, VectorDataType::I32)
            .unwrap();
        assert_eq!(compiler.stats().cache_hits, 1);
    }

    #[test]
    fn test_execute_add_kernel() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let kernel = compiler
            .compile_kernel(VectorOp::Add, VectorDataType::I32)
            .unwrap();

        // Create input: two i32 arrays [1, 2, 3, 4] and [10, 20, 30, 40]
        let a: Vec<i32> = vec![1, 2, 3, 4];
        let b: Vec<i32> = vec![10, 20, 30, 40];

        let mut input: Vec<u8> = Vec::new();
        for val in &a {
            input.extend_from_slice(&val.to_le_bytes());
        }
        for val in &b {
            input.extend_from_slice(&val.to_le_bytes());
        }

        let mut output = vec![0u8; 16];
        let params = KernelParams {
            input_size: 4,
            ..Default::default()
        };

        kernel.execute(&input, &mut output, &params).unwrap();

        // Parse output
        let result: Vec<i32> = (0..4)
            .map(|i| {
                let offset = i * 4;
                i32::from_le_bytes([
                    output[offset],
                    output[offset + 1],
                    output[offset + 2],
                    output[offset + 3],
                ])
            })
            .collect();

        assert_eq!(result, vec![11, 22, 33, 44]);
    }

    #[test]
    fn test_execute_sum_kernel() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let kernel = compiler
            .compile_aggregate_kernel(VectorDataType::I64, VectorOp::Sum)
            .unwrap();

        // Create input: [1, 2, 3, 4, 5]
        let data: Vec<i64> = vec![1, 2, 3, 4, 5];
        let input: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

        let mut output = vec![0u8; 8];
        let params = KernelParams {
            input_size: 5,
            ..Default::default()
        };

        kernel.execute(&input, &mut output, &params).unwrap();

        let sum = i64::from_le_bytes([
            output[0], output[1], output[2], output[3], output[4], output[5], output[6], output[7],
        ]);

        assert_eq!(sum, 15);
    }

    #[test]
    fn test_execute_filter_kernel() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let kernel = compiler
            .compile_filter_kernel(VectorDataType::I32, VectorOp::Gt)
            .unwrap();

        // Create input: [1, 5, 3, 7, 2]
        let data: Vec<i32> = vec![1, 5, 3, 7, 2];
        let input: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

        let mut output = vec![0u8; 5];
        let params = KernelParams {
            input_size: 5,
            threshold: Some(3),
            ..Default::default()
        };

        kernel.execute(&input, &mut output, &params).unwrap();

        // Check mask
        assert_eq!(output[0], 0); // 1 > 3 = false
        assert_eq!(output[1], 1); // 5 > 3 = true
        assert_eq!(output[2], 0); // 3 > 3 = false
        assert_eq!(output[3], 1); // 7 > 3 = true
        assert_eq!(output[4], 0); // 2 > 3 = false
    }

    #[test]
    fn test_compile_fused_kernel() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let ops = vec![VectorOp::Add, VectorOp::Mul];
        let kernel = compiler
            .compile_fused_kernel(&ops, VectorDataType::I32)
            .unwrap();

        assert!(kernel.name.contains("fused"));
    }

    #[test]
    fn test_cache_eviction() {
        let config = JitConfig {
            cache_enabled: true,
            max_cache_size: 2,
            ..Default::default()
        };
        let mut compiler = JitCompiler::new(config);

        // Fill cache
        let _ = compiler.compile_kernel(VectorOp::Add, VectorDataType::I32);
        let _ = compiler.compile_kernel(VectorOp::Sub, VectorDataType::I32);
        assert_eq!(compiler.cache_size(), 2);

        // Add one more - should evict oldest
        let _ = compiler.compile_kernel(VectorOp::Mul, VectorDataType::I32);
        assert_eq!(compiler.cache_size(), 2);
    }

    #[test]
    fn test_clear_cache() {
        let mut compiler = JitCompiler::new(JitConfig::default());

        let _ = compiler.compile_kernel(VectorOp::Add, VectorDataType::I32);
        let _ = compiler.compile_kernel(VectorOp::Sub, VectorDataType::I32);
        assert!(compiler.cache_size() > 0);

        compiler.clear_cache();
        assert_eq!(compiler.cache_size(), 0);
    }

    #[test]
    fn test_jit_stats() {
        let mut compiler = JitCompiler::new(JitConfig {
            cache_enabled: true,
            ..Default::default()
        });

        let _ = compiler.compile_kernel(VectorOp::Add, VectorDataType::I32);
        let _ = compiler.compile_kernel(VectorOp::Add, VectorDataType::I32);
        let _ = compiler.compile_kernel(VectorOp::Sub, VectorDataType::I32);

        let stats = compiler.stats();
        assert_eq!(stats.kernels_compiled, 2); // Add and Sub
        assert_eq!(stats.cache_hits, 1); // Second Add call
        assert_eq!(stats.cache_misses, 2); // First Add and Sub
    }

    #[test]
    fn test_jit_function() {
        let mut compiler = JitCompiler::new(JitConfig::default());
        let kernel = compiler
            .compile_kernel(VectorOp::Add, VectorDataType::I32)
            .unwrap();
        let func = JitFunction::new("add_i32", kernel);

        assert_eq!(func.name, "add_i32");
    }
}
