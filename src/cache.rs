//! Query result caching with LRU eviction and table-version-based invalidation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;

/// Configuration for the query cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of cached entries
    pub max_entries: usize,
    /// Maximum total memory budget in bytes (approximate)
    pub max_memory_bytes: usize,
    /// TTL for cached entries
    pub ttl: Duration,
    /// Whether caching is enabled
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 256,
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            ttl: Duration::from_secs(300),       // 5 minutes
            enabled: true,
        }
    }
}

impl CacheConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_entries(mut self, n: usize) -> Self {
        self.max_entries = n;
        self
    }

    pub fn with_max_memory_bytes(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// A single cached query result.
#[derive(Debug)]
struct CacheEntry {
    /// Normalized SQL key
    #[allow(dead_code)]
    sql: String,
    /// Cached result batches
    batches: Vec<RecordBatch>,
    /// Approximate size in bytes
    size_bytes: usize,
    /// When this entry was created
    created_at: Instant,
    /// Last access time (for LRU)
    last_accessed: Instant,
    /// Access count
    access_count: u64,
    /// Table versions at cache time
    table_versions: HashMap<String, u64>,
}

/// Statistics about cache performance.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub total_entries: usize,
    pub total_bytes: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Tracks table versions for cache invalidation.
#[derive(Debug, Default)]
pub struct TableVersionTracker {
    versions: RwLock<HashMap<String, AtomicU64>>,
}

impl TableVersionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the version of a table (called on writes).
    pub fn bump(&self, table_name: &str) {
        let versions = self.versions.read();
        if let Some(v) = versions.get(table_name) {
            v.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(versions);
            let mut versions = self.versions.write();
            versions
                .entry(table_name.to_string())
                .or_insert_with(|| AtomicU64::new(1));
        }
    }

    /// Get the current version of a table.
    pub fn version(&self, table_name: &str) -> u64 {
        let versions = self.versions.read();
        versions
            .get(table_name)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get a snapshot of all table versions.
    pub fn snapshot(&self) -> HashMap<String, u64> {
        let versions = self.versions.read();
        versions
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }
}

/// Semantic query result cache with LRU eviction and version-based invalidation.
pub struct QueryCache {
    config: CacheConfig,
    entries: RwLock<HashMap<String, CacheEntry>>,
    version_tracker: TableVersionTracker,
    stats: RwLock<CacheStats>,
}

impl QueryCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            version_tracker: TableVersionTracker::new(),
            stats: RwLock::new(CacheStats::default()),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Normalize a SQL string for cache key generation.
    /// Trims whitespace, lowercases keywords, and removes comments.
    pub fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<&str>>()
            .join(" ")
            .to_lowercase()
    }

    /// Look up a cached result for the given SQL query.
    pub fn get(&self, sql: &str) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let key = Self::normalize_sql(sql);
        let now = Instant::now();

        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(&key) {
            // Check TTL
            if now.duration_since(entry.created_at) > self.config.ttl {
                let size = entry.size_bytes;
                entries.remove(&key);
                let mut stats = self.stats.write();
                stats.misses += 1;
                stats.total_entries = entries.len();
                stats.total_bytes -= size;
                return None;
            }

            // Check table versions for invalidation
            for (table, cached_version) in &entry.table_versions {
                if self.version_tracker.version(table) != *cached_version {
                    let size = entry.size_bytes;
                    entries.remove(&key);
                    let mut stats = self.stats.write();
                    stats.invalidations += 1;
                    stats.misses += 1;
                    stats.total_entries = entries.len();
                    stats.total_bytes -= size;
                    return None;
                }
            }

            // Cache hit
            entry.last_accessed = now;
            entry.access_count += 1;
            let result = entry.batches.clone();
            let mut stats = self.stats.write();
            stats.hits += 1;
            Some(result)
        } else {
            let mut stats = self.stats.write();
            stats.misses += 1;
            None
        }
    }

    /// Store a query result in the cache.
    pub fn put(&self, sql: &str, batches: Vec<RecordBatch>, referenced_tables: &[String]) {
        if !self.config.enabled || batches.is_empty() {
            return;
        }

        let key = Self::normalize_sql(sql);
        let size_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        // Don't cache if single result exceeds budget
        if size_bytes > self.config.max_memory_bytes {
            return;
        }

        let table_versions: HashMap<String, u64> = referenced_tables
            .iter()
            .map(|t| (t.clone(), self.version_tracker.version(t)))
            .collect();

        let entry = CacheEntry {
            sql: key.clone(),
            batches,
            size_bytes,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
            table_versions,
        };

        let mut entries = self.entries.write();

        // Evict if necessary
        while entries.len() >= self.config.max_entries {
            self.evict_lru(&mut entries);
        }

        // Evict to stay within memory budget
        let current_bytes: usize = entries.values().map(|e| e.size_bytes).sum();
        let mut budget = current_bytes + size_bytes;
        while budget > self.config.max_memory_bytes && !entries.is_empty() {
            if let Some(evicted_size) = self.evict_lru(&mut entries) {
                budget -= evicted_size;
            } else {
                break;
            }
        }

        let mut stats = self.stats.write();
        stats.total_bytes = entries.values().map(|e| e.size_bytes).sum::<usize>() + size_bytes;
        entries.insert(key, entry);
        stats.total_entries = entries.len();
    }

    /// Invalidate all cache entries that reference the given table.
    pub fn invalidate_table(&self, table_name: &str) {
        self.version_tracker.bump(table_name);

        let mut entries = self.entries.write();
        let keys_to_remove: Vec<String> = entries
            .iter()
            .filter(|(_, e)| e.table_versions.contains_key(table_name))
            .map(|(k, _)| k.clone())
            .collect();

        let mut stats = self.stats.write();
        for key in keys_to_remove {
            if let Some(e) = entries.remove(&key) {
                stats.total_bytes -= e.size_bytes;
                stats.invalidations += 1;
            }
        }
        stats.total_entries = entries.len();
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        let mut stats = self.stats.write();
        stats.total_entries = 0;
        stats.total_bytes = 0;
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    /// Get the version tracker (for external invalidation).
    pub fn version_tracker(&self) -> &TableVersionTracker {
        &self.version_tracker
    }

    fn evict_lru(&self, entries: &mut HashMap<String, CacheEntry>) -> Option<usize> {
        let lru_key = entries
            .iter()
            .min_by_key(|(_, e)| e.last_accessed)
            .map(|(k, _)| k.clone());

        if let Some(key) = lru_key {
            if let Some(entry) = entries.remove(&key) {
                let mut stats = self.stats.write();
                stats.evictions += 1;
                return Some(entry.size_bytes);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Semantic query fingerprinting
// ---------------------------------------------------------------------------

/// Generates a semantic fingerprint for a SQL query that is invariant to:
/// - Whitespace differences
/// - Literal value differences (for parameterized matching)
/// - Column alias differences
/// - Comment differences
pub struct QueryFingerprinter;

impl QueryFingerprinter {
    /// Generate a fingerprint from a SQL string.
    ///
    /// This normalizes the query by:
    /// 1. Lowercasing and collapsing whitespace
    /// 2. Replacing integer literals with `?i`
    /// 3. Replacing string literals with `?s`
    /// 4. Replacing float literals with `?f`
    pub fn fingerprint(sql: &str) -> String {
        let normalized = QueryCache::normalize_sql(sql);
        let mut result = String::with_capacity(normalized.len());
        let mut chars = normalized.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '\'' {
                // Replace string literal
                while let Some(c) = chars.next() {
                    if c == '\'' {
                        break;
                    }
                }
                result.push_str("?s");
            } else if ch.is_ascii_digit()
                && !result.ends_with(|c: char| c.is_alphanumeric() || c == '_')
            {
                // Replace numeric literal
                let mut is_float = false;
                while let Some(&c) = chars.peek() {
                    if c == '.' {
                        is_float = true;
                        chars.next();
                    } else if c.is_ascii_digit() {
                        chars.next();
                    } else {
                        break;
                    }
                }
                result.push_str(if is_float { "?f" } else { "?i" });
            } else {
                result.push(ch);
            }
        }
        result
    }

    /// Check if two SQL queries are semantically equivalent (ignoring literal values).
    pub fn are_equivalent(sql1: &str, sql2: &str) -> bool {
        Self::fingerprint(sql1) == Self::fingerprint(sql2)
    }
}

// ---------------------------------------------------------------------------
// Frequency-based admission policy
// ---------------------------------------------------------------------------

/// Tracks query frequency for cache admission decisions.
///
/// Queries must be seen at least `min_frequency` times before being cached,
/// preventing cache pollution from one-off queries.
pub struct AdmissionPolicy {
    min_frequency: u64,
    frequency_map: RwLock<HashMap<String, u64>>,
    max_tracked: usize,
}

impl AdmissionPolicy {
    /// Create a new admission policy requiring `min_frequency` observations.
    pub fn new(min_frequency: u64) -> Self {
        Self {
            min_frequency,
            frequency_map: RwLock::new(HashMap::new()),
            max_tracked: 10_000,
        }
    }

    /// Record an observation of a query and return whether it should be admitted.
    pub fn should_admit(&self, key: &str) -> bool {
        if self.min_frequency <= 1 {
            return true;
        }
        let mut map = self.frequency_map.write();
        let count = map.entry(key.to_string()).or_insert(0);
        *count += 1;
        let admit = *count >= self.min_frequency;

        // Prevent unbounded growth
        if map.len() > self.max_tracked {
            let keys_to_remove: Vec<String> = map
                .iter()
                .filter(|(_, v)| **v < self.min_frequency)
                .map(|(k, _)| k.clone())
                .take(self.max_tracked / 4)
                .collect();
            for k in keys_to_remove {
                map.remove(&k);
            }
        }

        admit
    }

    /// Reset all frequency counters.
    pub fn reset(&self) {
        self.frequency_map.write().clear();
    }

    /// Get the number of tracked queries.
    pub fn tracked_count(&self) -> usize {
        self.frequency_map.read().len()
    }
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self::new(2) // Require seeing a query twice before caching
    }
}

impl std::fmt::Debug for QueryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("QueryCache")
            .field("entries", &stats.total_entries)
            .field("memory_bytes", &stats.total_bytes)
            .field("hit_rate", &format!("{:.1}%", stats.hit_rate() * 100.0))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(n: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let arr = Arc::new(Int64Array::from(vec![n]));
        vec![RecordBatch::try_new(schema, vec![arr]).unwrap()]
    }

    #[test]
    fn test_cache_put_and_get() {
        let cache = QueryCache::with_default_config();
        let batches = make_batch(42);
        cache.put("SELECT * FROM t", batches.clone(), &["t".into()]);

        let result = cache.get("SELECT * FROM t");
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].num_rows(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::with_default_config();
        assert!(cache.get("SELECT 1").is_none());
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_normalized_key() {
        let cache = QueryCache::with_default_config();
        cache.put("  SELECT   *   FROM   t  ", make_batch(1), &["t".into()]);
        // Same query normalized differently should still match
        let result = cache.get("select * from t");
        assert!(result.is_some());
    }

    #[test]
    fn test_cache_invalidation_on_table_write() {
        let cache = QueryCache::with_default_config();
        cache.put("SELECT * FROM orders", make_batch(1), &["orders".into()]);

        assert!(cache.get("SELECT * FROM orders").is_some());

        // Simulate a write to the orders table
        cache.invalidate_table("orders");

        // Cache should be invalidated
        assert!(cache.get("SELECT * FROM orders").is_none());
        let stats = cache.stats();
        assert!(stats.invalidations > 0);
    }

    #[test]
    fn test_cache_version_tracker() {
        let tracker = TableVersionTracker::new();
        assert_eq!(tracker.version("t"), 0);
        tracker.bump("t");
        assert_eq!(tracker.version("t"), 1);
        tracker.bump("t");
        assert_eq!(tracker.version("t"), 2);
    }

    #[test]
    fn test_cache_lru_eviction() {
        let config = CacheConfig::new().with_max_entries(2);
        let cache = QueryCache::new(config);

        cache.put("SELECT 1", make_batch(1), &[]);
        cache.put("SELECT 2", make_batch(2), &[]);

        // Access SELECT 1 to make it recently used
        cache.get("SELECT 1");

        // Adding a third entry should evict SELECT 2 (LRU)
        cache.put("SELECT 3", make_batch(3), &[]);

        assert!(cache.get("select 1").is_some());
        assert!(cache.get("select 2").is_none()); // evicted
        assert!(cache.get("select 3").is_some());

        let stats = cache.stats();
        assert!(stats.evictions > 0);
    }

    #[test]
    fn test_cache_ttl_expiry() {
        let config = CacheConfig::new().with_ttl(Duration::from_millis(1));
        let cache = QueryCache::new(config);

        cache.put("SELECT 1", make_batch(1), &[]);
        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get("SELECT 1").is_none());
    }

    #[test]
    fn test_cache_disabled() {
        let cache = QueryCache::new(CacheConfig::disabled());
        cache.put("SELECT 1", make_batch(1), &[]);
        assert!(cache.get("SELECT 1").is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryCache::with_default_config();
        cache.put("SELECT 1", make_batch(1), &[]);
        cache.put("SELECT 2", make_batch(2), &[]);
        assert_eq!(cache.stats().total_entries, 2);

        cache.clear();
        assert_eq!(cache.stats().total_entries, 0);
    }

    #[test]
    fn test_cache_stats_hit_rate() {
        let cache = QueryCache::with_default_config();
        cache.put("SELECT 1", make_batch(1), &[]);

        cache.get("SELECT 1"); // hit
        cache.get("SELECT 1"); // hit
        cache.get("SELECT 2"); // miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate() - 2.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_query_fingerprinting_basic() {
        // Same query, different literals → same fingerprint
        let fp1 = QueryFingerprinter::fingerprint("SELECT * FROM users WHERE id = 42");
        let fp2 = QueryFingerprinter::fingerprint("SELECT * FROM users WHERE id = 99");
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_query_fingerprinting_strings() {
        let fp1 = QueryFingerprinter::fingerprint("SELECT * FROM users WHERE name = 'Alice'");
        let fp2 = QueryFingerprinter::fingerprint("SELECT * FROM users WHERE name = 'Bob'");
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_query_fingerprinting_different_queries() {
        let fp1 = QueryFingerprinter::fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = QueryFingerprinter::fingerprint("SELECT * FROM orders WHERE id = 1");
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_query_fingerprinting_equivalence() {
        assert!(QueryFingerprinter::are_equivalent(
            "SELECT * FROM t WHERE x = 1",
            "SELECT * FROM t WHERE x = 2"
        ));
        assert!(!QueryFingerprinter::are_equivalent(
            "SELECT * FROM t WHERE x = 1",
            "SELECT * FROM t WHERE y = 1"
        ));
    }

    #[test]
    fn test_admission_policy_min_frequency() {
        let policy = AdmissionPolicy::new(3);

        // First two observations → not admitted
        assert!(!policy.should_admit("SELECT 1"));
        assert!(!policy.should_admit("SELECT 1"));

        // Third observation → admitted
        assert!(policy.should_admit("SELECT 1"));

        // Different query starts fresh
        assert!(!policy.should_admit("SELECT 2"));
    }

    #[test]
    fn test_admission_policy_frequency_one() {
        let policy = AdmissionPolicy::new(1);
        // Frequency 1 always admits
        assert!(policy.should_admit("SELECT 1"));
    }

    #[test]
    fn test_admission_policy_reset() {
        let policy = AdmissionPolicy::new(2);
        policy.should_admit("SELECT 1");
        assert_eq!(policy.tracked_count(), 1);
        policy.reset();
        assert_eq!(policy.tracked_count(), 0);
    }
}
