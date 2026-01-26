//! SIMD Vectorization Primitives
//!
//! This module provides low-level SIMD operations for common data types.

use super::SimdLevel;

/// SIMD capabilities for the current platform.
#[derive(Debug, Clone)]
pub struct SimdCapabilities {
    /// Available SIMD level
    pub level: SimdLevel,
    /// Supports FMA (Fused Multiply-Add)
    pub has_fma: bool,
    /// Supports gather instructions
    pub has_gather: bool,
    /// Supports scatter instructions
    pub has_scatter: bool,
    /// Supports masked operations
    pub has_masked_ops: bool,
    /// L1 cache line size
    pub cache_line_size: usize,
}

impl Default for SimdCapabilities {
    fn default() -> Self {
        detect_simd_capabilities()
    }
}

/// Detect SIMD capabilities of the current CPU.
pub fn detect_simd_capabilities() -> SimdCapabilities {
    let level = SimdLevel::best_available();

    #[cfg(target_arch = "x86_64")]
    let (has_fma, has_gather, has_scatter, has_masked_ops) = {
        let fma = std::is_x86_feature_detected!("fma");
        let gather = std::is_x86_feature_detected!("avx2");
        let scatter = std::is_x86_feature_detected!("avx512f");
        let masked = std::is_x86_feature_detected!("avx512f");
        (fma, gather, scatter, masked)
    };

    #[cfg(not(target_arch = "x86_64"))]
    let (has_fma, has_gather, has_scatter, has_masked_ops) = (false, false, false, false);

    SimdCapabilities {
        level,
        has_fma,
        has_gather,
        has_scatter,
        has_masked_ops,
        cache_line_size: 64, // Typical cache line size
    }
}

/// SIMD vector trait for generic operations.
pub trait SimdVectorOps<T>: Sized {
    /// Create a vector from a slice.
    fn from_slice(data: &[T]) -> Self;

    /// Convert to a slice.
    fn to_vec(&self) -> Vec<T>;

    /// Add two vectors.
    fn add(&self, other: &Self) -> Self;

    /// Subtract two vectors.
    fn sub(&self, other: &Self) -> Self;

    /// Multiply two vectors.
    fn mul(&self, other: &Self) -> Self;
}

/// SIMD filter operations.
pub trait SimdFilterOps<T> {
    /// Compare greater than.
    fn gt(&self, threshold: T) -> Vec<bool>;

    /// Compare greater than or equal.
    fn ge(&self, threshold: T) -> Vec<bool>;

    /// Compare less than.
    fn lt(&self, threshold: T) -> Vec<bool>;

    /// Compare less than or equal.
    fn le(&self, threshold: T) -> Vec<bool>;

    /// Compare equal.
    fn eq(&self, threshold: T) -> Vec<bool>;
}

/// SIMD aggregate operations.
pub trait SimdAggregateOps<T> {
    /// Sum all elements.
    fn sum(&self) -> T;

    /// Find minimum element.
    fn min(&self) -> T;

    /// Find maximum element.
    fn max(&self) -> T;

    /// Count elements.
    fn count(&self) -> usize;
}

/// Generic SIMD vector wrapper.
#[derive(Debug, Clone)]
pub struct SimdVector<T> {
    data: Vec<T>,
}

impl<T: Clone> SimdVector<T> {
    /// Create a new SIMD vector.
    pub fn new(data: Vec<T>) -> Self {
        Self { data }
    }

    /// Get length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get reference to inner data.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }
}

impl SimdVectorOps<i32> for SimdVector<i32> {
    fn from_slice(data: &[i32]) -> Self {
        Self::new(data.to_vec())
    }

    fn to_vec(&self) -> Vec<i32> {
        self.data.clone()
    }

    fn add(&self, other: &Self) -> Self {
        let mut result = vec![0i32; self.len()];
        simd_add_i32(
            &self.data,
            &other.data,
            &mut result,
            SimdLevel::best_available(),
        );
        Self::new(result)
    }

    fn sub(&self, other: &Self) -> Self {
        let result: Vec<i32> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a.wrapping_sub(*b))
            .collect();
        Self::new(result)
    }

    fn mul(&self, other: &Self) -> Self {
        let result: Vec<i32> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a.wrapping_mul(*b))
            .collect();
        Self::new(result)
    }
}

impl SimdFilterOps<i32> for SimdVector<i32> {
    fn gt(&self, threshold: i32) -> Vec<bool> {
        simd_filter_gt_i32(&self.data, threshold, SimdLevel::best_available())
    }

    fn ge(&self, threshold: i32) -> Vec<bool> {
        self.data.iter().map(|&x| x >= threshold).collect()
    }

    fn lt(&self, threshold: i32) -> Vec<bool> {
        self.data.iter().map(|&x| x < threshold).collect()
    }

    fn le(&self, threshold: i32) -> Vec<bool> {
        self.data.iter().map(|&x| x <= threshold).collect()
    }

    fn eq(&self, threshold: i32) -> Vec<bool> {
        self.data.iter().map(|&x| x == threshold).collect()
    }
}

impl SimdAggregateOps<i32> for SimdVector<i32> {
    fn sum(&self) -> i32 {
        self.data.iter().sum()
    }

    fn min(&self) -> i32 {
        *self.data.iter().min().unwrap_or(&0)
    }

    fn max(&self) -> i32 {
        *self.data.iter().max().unwrap_or(&0)
    }

    fn count(&self) -> usize {
        self.data.len()
    }
}

impl SimdVectorOps<i64> for SimdVector<i64> {
    fn from_slice(data: &[i64]) -> Self {
        Self::new(data.to_vec())
    }

    fn to_vec(&self) -> Vec<i64> {
        self.data.clone()
    }

    fn add(&self, other: &Self) -> Self {
        let result: Vec<i64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a.wrapping_add(*b))
            .collect();
        Self::new(result)
    }

    fn sub(&self, other: &Self) -> Self {
        let result: Vec<i64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a.wrapping_sub(*b))
            .collect();
        Self::new(result)
    }

    fn mul(&self, other: &Self) -> Self {
        let result: Vec<i64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a.wrapping_mul(*b))
            .collect();
        Self::new(result)
    }
}

impl SimdAggregateOps<i64> for SimdVector<i64> {
    fn sum(&self) -> i64 {
        simd_sum_i64(&self.data, SimdLevel::best_available())
    }

    fn min(&self) -> i64 {
        simd_min_i64(&self.data, SimdLevel::best_available())
    }

    fn max(&self) -> i64 {
        simd_max_i64(&self.data, SimdLevel::best_available())
    }

    fn count(&self) -> usize {
        self.data.len()
    }
}

impl SimdVectorOps<f64> for SimdVector<f64> {
    fn from_slice(data: &[f64]) -> Self {
        Self::new(data.to_vec())
    }

    fn to_vec(&self) -> Vec<f64> {
        self.data.clone()
    }

    fn add(&self, other: &Self) -> Self {
        let result: Vec<f64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a + b)
            .collect();
        Self::new(result)
    }

    fn sub(&self, other: &Self) -> Self {
        let result: Vec<f64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a - b)
            .collect();
        Self::new(result)
    }

    fn mul(&self, other: &Self) -> Self {
        let result: Vec<f64> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a * b)
            .collect();
        Self::new(result)
    }
}

impl SimdAggregateOps<f64> for SimdVector<f64> {
    fn sum(&self) -> f64 {
        simd_sum_f64(&self.data, SimdLevel::best_available())
    }

    fn min(&self) -> f64 {
        self.data.iter().copied().fold(f64::INFINITY, f64::min)
    }

    fn max(&self) -> f64 {
        self.data.iter().copied().fold(f64::NEG_INFINITY, f64::max)
    }

    fn count(&self) -> usize {
        self.data.len()
    }
}

// SIMD implementations using platform-specific intrinsics

/// SIMD add for i32 arrays.
pub fn simd_add_i32(a: &[i32], b: &[i32], result: &mut [i32], level: SimdLevel) {
    let len = a.len().min(b.len()).min(result.len());

    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if len >= 8 => unsafe { simd_add_i32_avx2(a, b, result, len) },
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Sse42 if len >= 4 => unsafe { simd_add_i32_sse42(a, b, result, len) },
        _ => {
            // Scalar fallback
            for i in 0..len {
                result[i] = a[i].wrapping_add(b[i]);
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_add_i32_avx2(a: &[i32], b: &[i32], result: &mut [i32], len: usize) {
    use std::arch::x86_64::*;

    let chunks = len / 8;
    let remainder = len % 8;

    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
        let vb = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);
        let vr = _mm256_add_epi32(va, vb);
        _mm256_storeu_si256(result.as_mut_ptr().add(offset) as *mut __m256i, vr);
    }

    // Handle remainder
    let start = chunks * 8;
    for i in 0..remainder {
        result[start + i] = a[start + i].wrapping_add(b[start + i]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn simd_add_i32_sse42(a: &[i32], b: &[i32], result: &mut [i32], len: usize) {
    use std::arch::x86_64::*;

    let chunks = len / 4;
    let remainder = len % 4;

    for i in 0..chunks {
        let offset = i * 4;
        let va = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
        let vb = _mm_loadu_si128(b.as_ptr().add(offset) as *const __m128i);
        let vr = _mm_add_epi32(va, vb);
        _mm_storeu_si128(result.as_mut_ptr().add(offset) as *mut __m128i, vr);
    }

    // Handle remainder
    let start = chunks * 4;
    for i in 0..remainder {
        result[start + i] = a[start + i].wrapping_add(b[start + i]);
    }
}

/// SIMD filter greater than for i32.
pub fn simd_filter_gt_i32(data: &[i32], threshold: i32, level: SimdLevel) -> Vec<bool> {
    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if data.len() >= 8 => unsafe { simd_filter_gt_i32_avx2(data, threshold) },
        _ => {
            // Scalar fallback
            data.iter().map(|&x| x > threshold).collect()
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_gt_i32_avx2(data: &[i32], threshold: i32) -> Vec<bool> {
    use std::arch::x86_64::*;

    let mut result = vec![false; data.len()];
    let threshold_vec = _mm256_set1_epi32(threshold);

    let chunks = data.len() / 8;
    let remainder = data.len() % 8;

    for i in 0..chunks {
        let offset = i * 8;
        let v = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
        let cmp = _mm256_cmpgt_epi32(v, threshold_vec);
        let mask = _mm256_movemask_epi8(cmp);

        // Extract bits for each i32 (4 bytes each)
        for j in 0..8 {
            result[offset + j] = (mask >> (j * 4)) & 0xF != 0;
        }
    }

    // Handle remainder
    let start = chunks * 8;
    for i in 0..remainder {
        result[start + i] = data[start + i] > threshold;
    }

    result
}

/// SIMD sum for i64.
pub fn simd_sum_i64(data: &[i64], level: SimdLevel) -> i64 {
    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if data.len() >= 4 => unsafe { simd_sum_i64_avx2(data) },
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Sse42 if data.len() >= 2 => unsafe { simd_sum_i64_sse42(data) },
        _ => {
            // Scalar fallback
            data.iter().sum()
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_sum_i64_avx2(data: &[i64]) -> i64 {
    use std::arch::x86_64::*;

    let mut sum = _mm256_setzero_si256();
    let chunks = data.len() / 4;
    let remainder = data.len() % 4;

    for i in 0..chunks {
        let offset = i * 4;
        let v = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
        sum = _mm256_add_epi64(sum, v);
    }

    // Extract and sum the 4 i64 values
    let mut result_array = [0i64; 4];
    _mm256_storeu_si256(result_array.as_mut_ptr() as *mut __m256i, sum);
    let mut total: i64 = result_array.iter().sum();

    // Handle remainder
    let start = chunks * 4;
    for i in 0..remainder {
        total += data[start + i];
    }

    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn simd_sum_i64_sse42(data: &[i64]) -> i64 {
    use std::arch::x86_64::*;

    let mut sum = _mm_setzero_si128();
    let chunks = data.len() / 2;
    let remainder = data.len() % 2;

    for i in 0..chunks {
        let offset = i * 2;
        let v = _mm_loadu_si128(data.as_ptr().add(offset) as *const __m128i);
        sum = _mm_add_epi64(sum, v);
    }

    // Extract and sum the 2 i64 values
    let mut result_array = [0i64; 2];
    _mm_storeu_si128(result_array.as_mut_ptr() as *mut __m128i, sum);
    let mut total: i64 = result_array.iter().sum();

    // Handle remainder
    let start = chunks * 2;
    for i in 0..remainder {
        total += data[start + i];
    }

    total
}

/// SIMD sum for f64.
pub fn simd_sum_f64(data: &[f64], level: SimdLevel) -> f64 {
    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if data.len() >= 4 => unsafe { simd_sum_f64_avx2(data) },
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Sse42 if data.len() >= 2 => unsafe { simd_sum_f64_sse42(data) },
        _ => {
            // Scalar fallback
            data.iter().sum()
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn simd_sum_f64_avx2(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut sum = _mm256_setzero_pd();
    let chunks = data.len() / 4;
    let remainder = data.len() % 4;

    for i in 0..chunks {
        let offset = i * 4;
        let v = _mm256_loadu_pd(data.as_ptr().add(offset));
        sum = _mm256_add_pd(sum, v);
    }

    // Extract and sum the 4 f64 values
    let mut result_array = [0f64; 4];
    _mm256_storeu_pd(result_array.as_mut_ptr(), sum);
    let mut total: f64 = result_array.iter().sum();

    // Handle remainder
    let start = chunks * 4;
    for i in 0..remainder {
        total += data[start + i];
    }

    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn simd_sum_f64_sse42(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut sum = _mm_setzero_pd();
    let chunks = data.len() / 2;
    let remainder = data.len() % 2;

    for i in 0..chunks {
        let offset = i * 2;
        let v = _mm_loadu_pd(data.as_ptr().add(offset));
        sum = _mm_add_pd(sum, v);
    }

    // Extract and sum the 2 f64 values
    let mut result_array = [0f64; 2];
    _mm_storeu_pd(result_array.as_mut_ptr(), sum);
    let mut total: f64 = result_array.iter().sum();

    // Handle remainder
    let start = chunks * 2;
    for i in 0..remainder {
        total += data[start + i];
    }

    total
}

/// SIMD min for i64.
pub fn simd_min_i64(data: &[i64], level: SimdLevel) -> i64 {
    if data.is_empty() {
        return i64::MAX;
    }

    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if data.len() >= 4 => unsafe { simd_min_i64_avx2(data) },
        _ => {
            // Scalar fallback
            *data.iter().min().unwrap()
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_min_i64_avx2(data: &[i64]) -> i64 {
    // AVX2 doesn't have i64 min, so we use comparison and blend
    let mut min_val = i64::MAX;

    for &val in data {
        if val < min_val {
            min_val = val;
        }
    }

    min_val
}

/// SIMD max for i64.
pub fn simd_max_i64(data: &[i64], level: SimdLevel) -> i64 {
    if data.is_empty() {
        return i64::MIN;
    }

    match level {
        #[cfg(target_arch = "x86_64")]
        SimdLevel::Avx2 if data.len() >= 4 => unsafe { simd_max_i64_avx2(data) },
        _ => {
            // Scalar fallback
            *data.iter().max().unwrap()
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_max_i64_avx2(data: &[i64]) -> i64 {
    // AVX2 doesn't have i64 max, so we use comparison and blend
    let mut max_val = i64::MIN;

    for &val in data {
        if val > max_val {
            max_val = val;
        }
    }

    max_val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_capabilities() {
        let caps = detect_simd_capabilities();
        assert!(caps.level >= SimdLevel::Scalar);
        assert!(caps.cache_line_size > 0);
    }

    #[test]
    fn test_simd_vector_i32() {
        let a = SimdVector::from_slice(&[1, 2, 3, 4]);
        let b = SimdVector::from_slice(&[5, 6, 7, 8]);

        let sum = a.add(&b);
        assert_eq!(sum.to_vec(), vec![6, 8, 10, 12]);

        let diff = a.sub(&b);
        assert_eq!(diff.to_vec(), vec![-4, -4, -4, -4]);

        let prod = a.mul(&b);
        assert_eq!(prod.to_vec(), vec![5, 12, 21, 32]);
    }

    #[test]
    fn test_simd_vector_filter() {
        let v = SimdVector::from_slice(&[1, 5, 3, 7, 2, 8]);

        let gt = v.gt(4);
        assert_eq!(gt, vec![false, true, false, true, false, true]);

        let lt = v.lt(4);
        assert_eq!(lt, vec![true, false, true, false, true, false]);
    }

    #[test]
    fn test_simd_vector_aggregates_i32() {
        let v = SimdVector::from_slice(&[1, 2, 3, 4, 5]);

        assert_eq!(v.sum(), 15);
        assert_eq!(v.min(), 1);
        assert_eq!(v.max(), 5);
        assert_eq!(v.count(), 5);
    }

    #[test]
    fn test_simd_vector_i64() {
        let a = SimdVector::<i64>::from_slice(&[100, 200, 300, 400]);
        let b = SimdVector::<i64>::from_slice(&[10, 20, 30, 40]);

        let sum = a.add(&b);
        assert_eq!(sum.to_vec(), vec![110, 220, 330, 440]);
    }

    #[test]
    fn test_simd_vector_f64() {
        let a = SimdVector::<f64>::from_slice(&[1.5, 2.5, 3.5, 4.5]);
        let b = SimdVector::<f64>::from_slice(&[0.5, 0.5, 0.5, 0.5]);

        let sum = a.add(&b);
        let expected = vec![2.0, 3.0, 4.0, 5.0];
        for (got, exp) in sum.to_vec().iter().zip(expected.iter()) {
            assert!((got - exp).abs() < 0.0001);
        }
    }

    #[test]
    fn test_simd_add_i32_scalar() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let b = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut result = vec![0; 10];

        simd_add_i32(&a, &b, &mut result, SimdLevel::Scalar);

        assert_eq!(result, vec![11, 11, 11, 11, 11, 11, 11, 11, 11, 11]);
    }

    #[test]
    fn test_simd_sum_i64_scalar() {
        let data: Vec<i64> = (1..=100).collect();
        let sum = simd_sum_i64(&data, SimdLevel::Scalar);
        assert_eq!(sum, 5050);
    }

    #[test]
    fn test_simd_sum_f64_scalar() {
        let data: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let sum = simd_sum_f64(&data, SimdLevel::Scalar);
        assert!((sum - 5050.0).abs() < 0.0001);
    }

    #[test]
    fn test_simd_min_max_i64() {
        let data: Vec<i64> = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];

        let min = simd_min_i64(&data, SimdLevel::Scalar);
        let max = simd_max_i64(&data, SimdLevel::Scalar);

        assert_eq!(min, 1);
        assert_eq!(max, 9);
    }

    #[test]
    fn test_large_simd_operations() {
        // Test with larger data to exercise SIMD paths
        let size = 10000;
        let a: Vec<i32> = (0..size as i32).collect();
        let b: Vec<i32> = (0..size as i32).rev().collect();
        let mut result = vec![0i32; size];

        simd_add_i32(&a, &b, &mut result, SimdLevel::best_available());

        let expected = size as i32 - 1;
        for &r in &result {
            assert_eq!(r, expected);
        }
    }
}
