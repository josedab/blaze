//! Vectorized string processing with architecture-specific optimizations.
//!
//! Provides high-performance string operations (contains, starts_with,
//! ends_with, LIKE pattern matching) that operate on Arrow StringArrays
//! with batch processing and optional SIMD acceleration.

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use std::sync::Arc;

use crate::error::Result;

/// Vectorized string operations on Arrow StringArrays.
pub struct VectorizedStringOps;

impl VectorizedStringOps {
    /// Check if each string contains the given substring.
    /// Returns a BooleanArray with the result for each row.
    pub fn contains(array: &StringArray, pattern: &str) -> Result<ArrayRef> {
        let pattern_bytes = pattern.as_bytes();
        let pattern_len = pattern_bytes.len();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let value = array.value(i);
                    Some(fast_contains(value.as_bytes(), pattern_bytes, pattern_len))
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Check if each string starts with the given prefix.
    pub fn starts_with(array: &StringArray, prefix: &str) -> Result<ArrayRef> {
        let prefix_bytes = prefix.as_bytes();
        let prefix_len = prefix_bytes.len();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let value = array.value(i).as_bytes();
                    Some(value.len() >= prefix_len && &value[..prefix_len] == prefix_bytes)
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Check if each string ends with the given suffix.
    pub fn ends_with(array: &StringArray, suffix: &str) -> Result<ArrayRef> {
        let suffix_bytes = suffix.as_bytes();
        let suffix_len = suffix_bytes.len();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let value = array.value(i).as_bytes();
                    Some(
                        value.len() >= suffix_len
                            && &value[value.len() - suffix_len..] == suffix_bytes,
                    )
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// SQL LIKE pattern matching with `%` (any chars) and `_` (single char).
    ///
    /// Optimized paths for common patterns:
    /// - `%suffix` → ends_with
    /// - `prefix%` → starts_with
    /// - `%middle%` → contains
    /// - Complex patterns → character-by-character matching
    pub fn like(array: &StringArray, pattern: &str) -> Result<ArrayRef> {
        // Optimize common cases
        if !pattern.contains('_') {
            let parts: Vec<&str> = pattern.split('%').collect();

            // Pure prefix match: "prefix%"
            if parts.len() == 2 && parts[1].is_empty() && !parts[0].is_empty() {
                return Self::starts_with(array, parts[0]);
            }

            // Pure suffix match: "%suffix"
            if parts.len() == 2 && parts[0].is_empty() && !parts[1].is_empty() {
                return Self::ends_with(array, parts[1]);
            }

            // Contains match: "%middle%"
            if parts.len() == 3
                && parts[0].is_empty()
                && parts[2].is_empty()
                && !parts[1].is_empty()
            {
                return Self::contains(array, parts[1]);
            }

            // Exact match (no wildcards)
            if parts.len() == 1 {
                return Self::equals(array, pattern);
            }
        }

        // General LIKE matching
        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(like_match(array.value(i), pattern))
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Case-insensitive LIKE matching (ILIKE).
    pub fn ilike(array: &StringArray, pattern: &str) -> Result<ArrayRef> {
        let lower_pattern = pattern.to_lowercase();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let lower_value = array.value(i).to_lowercase();
                    Some(like_match(&lower_value, &lower_pattern))
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Exact string equality check (vectorized).
    pub fn equals(array: &StringArray, target: &str) -> Result<ArrayRef> {
        let target_bytes = target.as_bytes();
        let target_len = target_bytes.len();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let value = array.value(i).as_bytes();
                    Some(value.len() == target_len && value == target_bytes)
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Batch contains check against multiple patterns (OR semantics).
    /// Returns true if the string contains ANY of the patterns.
    pub fn contains_any(array: &StringArray, patterns: &[&str]) -> Result<ArrayRef> {
        let pattern_bytes: Vec<&[u8]> = patterns.iter().map(|p| p.as_bytes()).collect();
        let pattern_lens: Vec<usize> = pattern_bytes.iter().map(|p| p.len()).collect();

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    let value = array.value(i).as_bytes();
                    Some(
                        pattern_bytes
                            .iter()
                            .zip(pattern_lens.iter())
                            .any(|(pat, &len)| fast_contains(value, pat, len)),
                    )
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Vectorized regex match using pre-compiled regex.
    pub fn regex_match(array: &StringArray, pattern: &str) -> Result<ArrayRef> {
        let re = regex::Regex::new(pattern)
            .map_err(|e| crate::error::BlazeError::execution(format!("Invalid regex: {}", e)))?;

        let results: BooleanArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(re.is_match(array.value(i)))
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Vectorized string length computation.
    pub fn str_len(array: &StringArray) -> Result<ArrayRef> {
        let results: arrow::array::Int64Array = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i).len() as i64)
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Vectorized uppercase.
    pub fn to_upper(array: &StringArray) -> Result<ArrayRef> {
        let results: StringArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i).to_uppercase())
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Vectorized lowercase.
    pub fn to_lower(array: &StringArray) -> Result<ArrayRef> {
        let results: StringArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i).to_lowercase())
                }
            })
            .collect();

        Ok(Arc::new(results))
    }

    /// Vectorized trim.
    pub fn trim(array: &StringArray) -> Result<ArrayRef> {
        let results: StringArray = (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i).trim().to_string())
                }
            })
            .collect();

        Ok(Arc::new(results))
    }
}

/// Fast byte-level substring search using a simple two-way or memchr-style approach.
/// Falls back to the standard library's optimized contains for non-SIMD.
#[inline]
fn fast_contains(haystack: &[u8], needle: &[u8], needle_len: usize) -> bool {
    if needle_len == 0 {
        return true;
    }
    if needle_len > haystack.len() {
        return false;
    }
    if needle_len == 1 {
        return memchr_single(haystack, needle[0]);
    }
    // Use Rust's built-in optimized search for general case
    haystack.windows(needle_len).any(|window| window == needle)
}

/// Single-byte search optimized for common case.
#[inline]
fn memchr_single(haystack: &[u8], needle: u8) -> bool {
    // Process in chunks for better throughput
    const CHUNK: usize = 8;
    let chunks = haystack.len() / CHUNK;
    let remainder = haystack.len() % CHUNK;

    for i in 0..chunks {
        let base = i * CHUNK;
        // Unrolled comparison
        if haystack[base] == needle
            || haystack[base + 1] == needle
            || haystack[base + 2] == needle
            || haystack[base + 3] == needle
            || haystack[base + 4] == needle
            || haystack[base + 5] == needle
            || haystack[base + 6] == needle
            || haystack[base + 7] == needle
        {
            return true;
        }
    }

    let start = chunks * CHUNK;
    for i in 0..remainder {
        if haystack[start + i] == needle {
            return true;
        }
    }
    false
}

/// SQL LIKE pattern matching.
/// `%` matches zero or more characters, `_` matches exactly one character.
fn like_match(text: &str, pattern: &str) -> bool {
    like_match_bytes(text.as_bytes(), pattern.as_bytes())
}

fn like_match_bytes(text: &[u8], pattern: &[u8]) -> bool {
    let (tlen, plen) = (text.len(), pattern.len());

    // dp[j] = whether text[0..i] matches pattern[0..j]
    let mut prev = vec![false; plen + 1];
    let mut curr = vec![false; plen + 1];
    prev[0] = true;

    // Initialize: pattern of all % matches empty text
    for j in 1..=plen {
        if pattern[j - 1] == b'%' {
            prev[j] = prev[j - 1];
        }
    }

    for i in 1..=tlen {
        curr[0] = false;
        for j in 1..=plen {
            let pc = pattern[j - 1];
            if pc == b'%' {
                curr[j] = curr[j - 1] || prev[j];
            } else if pc == b'_' || pc == text[i - 1] {
                curr[j] = prev[j - 1];
            } else {
                curr[j] = false;
            }
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[plen]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_array(values: &[Option<&str>]) -> StringArray {
        StringArray::from(values.to_vec())
    }

    fn to_bool_vec(result: &ArrayRef) -> Vec<Option<bool>> {
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn test_contains() {
        let arr = make_string_array(&[Some("hello world"), Some("foo bar"), Some("hello"), None]);
        let result = VectorizedStringOps::contains(&arr, "hello").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(true), None]
        );
    }

    #[test]
    fn test_contains_empty_pattern() {
        let arr = make_string_array(&[Some("hello"), Some("")]);
        let result = VectorizedStringOps::contains(&arr, "").unwrap();
        assert_eq!(to_bool_vec(&result), vec![Some(true), Some(true)]);
    }

    #[test]
    fn test_starts_with() {
        let arr = make_string_array(&[Some("hello world"), Some("world hello"), Some("he"), None]);
        let result = VectorizedStringOps::starts_with(&arr, "hello").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_ends_with() {
        let arr = make_string_array(&[Some("hello world"), Some("hello"), Some("world"), None]);
        let result = VectorizedStringOps::ends_with(&arr, "world").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(true), None]
        );
    }

    #[test]
    fn test_like_prefix() {
        let arr = make_string_array(&[Some("hello world"), Some("help me"), Some("world")]);
        let result = VectorizedStringOps::like(&arr, "hel%").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn test_like_suffix() {
        let arr = make_string_array(&[Some("hello world"), Some("big world"), Some("hello")]);
        let result = VectorizedStringOps::like(&arr, "%world").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn test_like_contains() {
        let arr = make_string_array(&[Some("hello world foo"), Some("bar"), Some("world")]);
        let result = VectorizedStringOps::like(&arr, "%world%").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn test_like_underscore() {
        let arr = make_string_array(&[Some("cat"), Some("car"), Some("ca"), Some("cats")]);
        let result = VectorizedStringOps::like(&arr, "ca_").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false), Some(false)]
        );
    }

    #[test]
    fn test_like_complex() {
        let arr = make_string_array(&[
            Some("hello world"),
            Some("hello there world"),
            Some("hi world"),
        ]);
        let result = VectorizedStringOps::like(&arr, "hello%world").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn test_like_exact() {
        let arr = make_string_array(&[Some("hello"), Some("hello world"), Some("h")]);
        let result = VectorizedStringOps::like(&arr, "hello").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(false)]
        );
    }

    #[test]
    fn test_ilike() {
        let arr = make_string_array(&[Some("Hello World"), Some("HELLO"), Some("world")]);
        let result = VectorizedStringOps::ilike(&arr, "%hello%").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn test_equals() {
        let arr = make_string_array(&[Some("hello"), Some("Hello"), Some("hello"), None]);
        let result = VectorizedStringOps::equals(&arr, "hello").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(true), None]
        );
    }

    #[test]
    fn test_contains_any() {
        let arr = make_string_array(&[Some("hello world"), Some("foo bar"), Some("baz qux"), None]);
        let result = VectorizedStringOps::contains_any(&arr, &["hello", "foo"]).unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(true), Some(false), None]
        );
    }

    #[test]
    fn test_regex_match() {
        let arr = make_string_array(&[Some("hello123"), Some("world"), Some("test456"), None]);
        let result = VectorizedStringOps::regex_match(&arr, r"\d+").unwrap();
        assert_eq!(
            to_bool_vec(&result),
            vec![Some(true), Some(false), Some(true), None]
        );
    }

    #[test]
    fn test_str_len() {
        let arr = make_string_array(&[Some("hello"), Some(""), Some("hi"), None]);
        let result = VectorizedStringOps::str_len(&arr).unwrap();
        let lens = result
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(lens.value(0), 5);
        assert_eq!(lens.value(1), 0);
        assert_eq!(lens.value(2), 2);
        assert!(lens.is_null(3));
    }

    #[test]
    fn test_to_upper() {
        let arr = make_string_array(&[Some("hello"), Some("World"), None]);
        let result = VectorizedStringOps::to_upper(&arr).unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "HELLO");
        assert_eq!(strings.value(1), "WORLD");
        assert!(strings.is_null(2));
    }

    #[test]
    fn test_to_lower() {
        let arr = make_string_array(&[Some("HELLO"), Some("World"), None]);
        let result = VectorizedStringOps::to_lower(&arr).unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "hello");
        assert_eq!(strings.value(1), "world");
        assert!(strings.is_null(2));
    }

    #[test]
    fn test_trim() {
        let arr = make_string_array(&[Some("  hello  "), Some("world"), Some("  "), None]);
        let result = VectorizedStringOps::trim(&arr).unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "hello");
        assert_eq!(strings.value(1), "world");
        assert_eq!(strings.value(2), "");
        assert!(strings.is_null(3));
    }

    #[test]
    fn test_like_match_internal() {
        assert!(like_match("hello world", "hello%"));
        assert!(like_match("hello world", "%world"));
        assert!(like_match("hello world", "%llo%"));
        assert!(like_match("hello world", "h_llo%"));
        assert!(!like_match("hello", "world"));
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
        assert!(like_match("a", "_"));
        assert!(like_match("abc", "a_c"));
        assert!(!like_match("abbc", "a_c"));
    }

    #[test]
    fn test_memchr_single() {
        assert!(memchr_single(b"hello world", b'o'));
        assert!(!memchr_single(b"hello world", b'z'));
        assert!(memchr_single(b"a", b'a'));
        assert!(!memchr_single(b"", b'a'));
        // Test with > 8 bytes to exercise chunked path
        assert!(memchr_single(b"abcdefghijklmnop", b'p'));
        assert!(!memchr_single(b"abcdefghijklmnop", b'z'));
    }
}
