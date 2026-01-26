//! HTTP Range Request reader for remote Parquet files.
//!
//! Enables reading Parquet files from HTTP servers without downloading
//! the entire file. Uses range requests to fetch only the needed byte
//! ranges, with a local cache to avoid redundant fetches.
//!
//! In actual WASM builds, the fetch operations would use the browser's
//! `fetch` API via `web_sys`/`js_sys`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::error::{BlazeError, Result};

/// HTTP range request configuration.
#[derive(Debug, Clone)]
pub struct HttpReaderConfig {
    /// URL of the remote file.
    pub url: String,
    /// Size of each chunk to fetch, in bytes.
    pub chunk_size: usize,
    /// Maximum number of retry attempts per request.
    pub max_retries: usize,
    /// Timeout per request in seconds.
    pub timeout_secs: u64,
    /// Additional HTTP headers to include in requests.
    pub headers: HashMap<String, String>,
}

impl HttpReaderConfig {
    /// Create a new configuration for the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            chunk_size: 64 * 1024, // 64 KB
            max_retries: 3,
            timeout_secs: 30,
            headers: HashMap::new(),
        }
    }

    /// Set the chunk size for range requests.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Add an HTTP header to include in requests.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// A cached byte range.
#[derive(Debug, Clone)]
struct CachedRange {
    start: u64,
    end: u64,
    data: Vec<u8>,
}

/// Tracks which byte ranges have been fetched and caches them.
pub struct RangeCache {
    #[allow(dead_code)]
    url: String,
    total_size: RwLock<Option<u64>>,
    ranges: RwLock<Vec<CachedRange>>,
    total_fetched: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl RangeCache {
    /// Create a new range cache for the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            total_size: RwLock::new(None),
            ranges: RwLock::new(Vec::new()),
            total_fetched: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Set the known total size of the remote file.
    pub fn set_total_size(&self, size: u64) {
        *self.total_size.write() = Some(size);
    }

    /// Get the known total size of the remote file, if available.
    pub fn total_size(&self) -> Option<u64> {
        *self.total_size.read()
    }

    /// Add a fetched byte range to the cache.
    pub fn add_range(&self, start: u64, data: Vec<u8>) {
        let end = start + data.len() as u64;
        self.total_fetched
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let mut ranges = self.ranges.write();
        ranges.push(CachedRange { start, end, data });
    }

    /// Try to retrieve a byte range from the cache.
    ///
    /// Returns `Some(data)` if the entire requested range is cached,
    /// `None` otherwise.
    pub fn get_range(&self, start: u64, length: u64) -> Option<Vec<u8>> {
        let end = start + length;
        let ranges = self.ranges.read();

        for cached in ranges.iter() {
            if cached.start <= start && cached.end >= end {
                let offset = (start - cached.start) as usize;
                let len = length as usize;
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Some(cached.data[offset..offset + len].to_vec());
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Get the total number of bytes fetched from the network.
    pub fn total_fetched_bytes(&self) -> u64 {
        self.total_fetched.load(Ordering::Relaxed)
    }

    /// Get the cache hit rate as a ratio between 0.0 and 1.0.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Remote file reader using HTTP range requests.
///
/// In real WASM builds, `read_range` would use the browser's `fetch` API.
/// This implementation provides a simulated interface for testing and
/// non-browser environments.
pub struct HttpFileReader {
    config: HttpReaderConfig,
    cache: RangeCache,
}

impl HttpFileReader {
    /// Create a new HTTP file reader.
    pub fn new(config: HttpReaderConfig) -> Self {
        let cache = RangeCache::new(&config.url);
        Self { config, cache }
    }

    /// Get the URL being read.
    pub fn url(&self) -> &str {
        &self.config.url
    }

    /// Get a reference to the range cache.
    pub fn cache(&self) -> &RangeCache {
        &self.cache
    }

    /// Get the estimated total size of the remote file.
    pub fn estimated_total_size(&self) -> Option<u64> {
        self.cache.total_size()
    }

    /// Read a byte range from the remote file.
    ///
    /// Returns cached data if available; otherwise simulates a fetch.
    /// In real WASM builds, this would use the browser `fetch` API with
    /// a `Range` header.
    pub fn read_range(&self, start: u64, length: u64) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(data) = self.cache.get_range(start, length) {
            return Ok(data);
        }

        // Validate against known total size
        if let Some(total) = self.cache.total_size() {
            if start + length > total {
                return Err(BlazeError::execution(format!(
                    "Range {}..{} exceeds file size {}",
                    start,
                    start + length,
                    total
                )));
            }
        }

        // Simulate a fetch: in real WASM this would be an async fetch call.
        // For non-browser testing, return zeroed data.
        let data = vec![0u8; length as usize];
        self.cache.add_range(start, data.clone());
        Ok(data)
    }
}

/// Parquet metadata reader for remote files.
///
/// Wraps an [`HttpFileReader`] to provide Parquet-aware operations
/// such as reading file metadata and row group data via range requests.
pub struct RemoteParquetReader {
    reader: HttpFileReader,
}

impl RemoteParquetReader {
    /// Create a new remote Parquet reader.
    pub fn new(config: HttpReaderConfig) -> Self {
        Self {
            reader: HttpFileReader::new(config),
        }
    }

    /// Get the URL of the remote Parquet file.
    pub fn url(&self) -> &str {
        self.reader.url()
    }

    /// Get cache statistics: `(total_fetched_bytes, hit_rate)`.
    pub fn cache_stats(&self) -> (u64, f64) {
        let cache = self.reader.cache();
        (cache.total_fetched_bytes(), cache.hit_rate())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_reader_config_builder() {
        let config = HttpReaderConfig::new("https://example.com/data.parquet")
            .with_chunk_size(128 * 1024)
            .with_header("Authorization", "Bearer token123");

        assert_eq!(config.url, "https://example.com/data.parquet");
        assert_eq!(config.chunk_size, 128 * 1024);
        assert_eq!(
            config.headers.get("Authorization").unwrap(),
            "Bearer token123"
        );
    }

    #[test]
    fn test_range_cache_add_and_get() {
        let cache = RangeCache::new("https://example.com/file.parquet");
        cache.add_range(100, vec![10, 20, 30, 40, 50]);

        // Full range hit
        let data = cache.get_range(100, 5).unwrap();
        assert_eq!(data, vec![10, 20, 30, 40, 50]);

        // Sub-range hit
        let data = cache.get_range(101, 3).unwrap();
        assert_eq!(data, vec![20, 30, 40]);

        // Miss
        assert!(cache.get_range(200, 5).is_none());
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = RangeCache::new("https://example.com/file.parquet");
        assert_eq!(cache.hit_rate(), 0.0);

        cache.add_range(0, vec![1, 2, 3, 4]);

        // Hit
        cache.get_range(0, 4);
        // Miss
        cache.get_range(100, 4);

        assert!((cache.hit_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_http_file_reader_read_range() {
        let config = HttpReaderConfig::new("https://example.com/data.parquet");
        let reader = HttpFileReader::new(config);

        reader.cache().set_total_size(1000);

        // First read — simulated fetch
        let data = reader.read_range(0, 100).unwrap();
        assert_eq!(data.len(), 100);

        // Second read — should come from cache
        let data2 = reader.read_range(0, 100).unwrap();
        assert_eq!(data2.len(), 100);
        assert_eq!(reader.cache().total_fetched_bytes(), 100);

        // Out-of-bounds read
        let result = reader.read_range(900, 200);
        assert!(result.is_err());
    }

    #[test]
    fn test_remote_parquet_reader() {
        let config = HttpReaderConfig::new("https://example.com/table.parquet");
        let reader = RemoteParquetReader::new(config);

        assert_eq!(reader.url(), "https://example.com/table.parquet");
        let (fetched, hit_rate) = reader.cache_stats();
        assert_eq!(fetched, 0);
        assert_eq!(hit_rate, 0.0);
    }
}
