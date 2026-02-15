//! Cloud object store abstraction for Blaze.
//!
//! This module provides a unified interface for reading data from cloud
//! object stores (S3, GCS, Azure) and local filesystems. It includes:
//!
//! - [`ObjectStoreProvider`] trait for abstracting storage backends
//! - [`ObjectPath`] for parsing cloud storage URIs
//! - [`LocalFileSystemStore`] for reading from local filesystem
//! - [`ObjectStoreRegistry`] for managing multiple store backends
//! - [`ObjectStoreTable`] implementing [`TableProvider`] for query integration
//! - [`CachingObjectStore`] for LRU-cached reads

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::{TableProvider, TableStatistics, TableType};
use crate::error::{BlazeError, Result};
use crate::types::Schema;

// ---------------------------------------------------------------------------
// ObjectPath
// ---------------------------------------------------------------------------

/// A parsed object storage URI (e.g. `s3://bucket/key`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath {
    /// URI scheme (`s3`, `gs`, `az`, `file`, …)
    pub scheme: String,
    /// Bucket or container name (empty for `file://`)
    pub bucket: String,
    /// Object key / path within the bucket
    pub key: String,
}

impl ObjectPath {
    /// Parse a URI string into an [`ObjectPath`].
    ///
    /// Supported formats:
    /// - `s3://bucket/key`
    /// - `gs://bucket/key`
    /// - `az://container/blob`
    /// - `file:///absolute/path`
    pub fn parse(uri: &str) -> Result<Self> {
        let parts: Vec<&str> = uri.splitn(2, "://").collect();
        if parts.len() != 2 {
            return Err(BlazeError::invalid_argument(format!(
                "Invalid object store URI (missing '://'): {uri}"
            )));
        }

        let scheme = parts[0].to_lowercase();
        let remainder = parts[1];

        match scheme.as_str() {
            "file" => {
                // file:///path  ->  remainder = "/path"
                let key = remainder.trim_start_matches('/');
                Ok(Self {
                    scheme,
                    bucket: String::new(),
                    key: format!("/{key}"),
                })
            }
            "s3" | "gs" | "az" => {
                let (bucket, key) = match remainder.find('/') {
                    Some(pos) => (
                        remainder[..pos].to_string(),
                        remainder[pos + 1..].to_string(),
                    ),
                    None => (remainder.to_string(), String::new()),
                };
                if bucket.is_empty() {
                    return Err(BlazeError::invalid_argument(format!(
                        "Missing bucket/container in URI: {uri}"
                    )));
                }
                Ok(Self {
                    scheme,
                    bucket,
                    key,
                })
            }
            _ => Err(BlazeError::invalid_argument(format!(
                "Unsupported object store scheme: {scheme}"
            ))),
        }
    }

    /// Reconstruct the full URI string.
    pub fn full_path(&self) -> String {
        if self.scheme == "file" {
            format!("file://{}", self.key)
        } else {
            format!("{}://{}/{}", self.scheme, self.bucket, self.key)
        }
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_path())
    }
}

// ---------------------------------------------------------------------------
// ObjectMeta
// ---------------------------------------------------------------------------

/// Metadata about a stored object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Object location.
    pub path: ObjectPath,
    /// Size in bytes.
    pub size: usize,
    /// Last modification time (if available).
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    /// MIME content type (if available).
    pub content_type: Option<String>,
}

// ---------------------------------------------------------------------------
// ObjectStoreProvider trait
// ---------------------------------------------------------------------------

/// Abstraction over a cloud / local object store.
///
/// Implementations provide basic read-only operations needed by the query
/// engine to list and fetch data objects.
pub trait ObjectStoreProvider: Send + Sync + fmt::Debug {
    /// URI scheme this provider handles (e.g. `"s3"`, `"file"`).
    fn scheme(&self) -> &str;

    /// Fetch the raw bytes of an object.
    fn get(&self, path: &ObjectPath) -> Result<bytes::Bytes>;

    /// List objects under a prefix.
    fn list(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMeta>>;

    /// Get metadata for a single object without fetching its contents.
    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta>;
}

// ---------------------------------------------------------------------------
// LocalFileSystemStore
// ---------------------------------------------------------------------------

/// [`ObjectStoreProvider`] backed by the local filesystem.
#[derive(Debug)]
pub struct LocalFileSystemStore;

impl ObjectStoreProvider for LocalFileSystemStore {
    fn scheme(&self) -> &str {
        "file"
    }

    fn get(&self, path: &ObjectPath) -> Result<bytes::Bytes> {
        let data = fs::read(&path.key)?;
        Ok(bytes::Bytes::from(data))
    }

    fn list(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMeta>> {
        let dir = PathBuf::from(&prefix.key);
        if !dir.is_dir() {
            return Err(BlazeError::invalid_argument(format!(
                "Not a directory: {}",
                prefix.key
            )));
        }
        let mut results = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_file() {
                let file_path = entry.path();
                let key = file_path.to_string_lossy().to_string();
                results.push(ObjectMeta {
                    path: ObjectPath {
                        scheme: "file".into(),
                        bucket: String::new(),
                        key,
                    },
                    size: meta.len() as usize,
                    last_modified: meta
                        .modified()
                        .ok()
                        .map(chrono::DateTime::<chrono::Utc>::from),
                    content_type: None,
                });
            }
        }
        Ok(results)
    }

    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta> {
        let meta = fs::metadata(&path.key)?;
        Ok(ObjectMeta {
            path: path.clone(),
            size: meta.len() as usize,
            last_modified: meta
                .modified()
                .ok()
                .map(chrono::DateTime::<chrono::Utc>::from),
            content_type: None,
        })
    }
}

// ---------------------------------------------------------------------------
// ObjectStoreRegistry
// ---------------------------------------------------------------------------

/// Registry mapping URI schemes to their [`ObjectStoreProvider`] implementations.
///
/// A default registry pre-registers the `"file"` scheme backed by
/// [`LocalFileSystemStore`].
#[derive(Debug)]
pub struct ObjectStoreRegistry {
    stores: RwLock<HashMap<String, Arc<dyn ObjectStoreProvider>>>,
}

impl ObjectStoreRegistry {
    /// Create a new registry with the `"file"` scheme pre-registered.
    pub fn new() -> Self {
        let mut stores = HashMap::new();
        stores.insert(
            "file".to_string(),
            Arc::new(LocalFileSystemStore) as Arc<dyn ObjectStoreProvider>,
        );
        Self {
            stores: RwLock::new(stores),
        }
    }

    /// Register a provider for the given scheme (overwrites any previous one).
    pub fn register(&self, scheme: impl Into<String>, provider: Arc<dyn ObjectStoreProvider>) {
        self.stores.write().insert(scheme.into(), provider);
    }

    /// Retrieve the provider for a scheme.
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn ObjectStoreProvider>> {
        self.stores.read().get(scheme).cloned()
    }
}

impl Default for ObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CachingObjectStore
// ---------------------------------------------------------------------------

/// A wrapper around any [`ObjectStoreProvider`] that caches `get()` results
/// in an LRU cache bounded by total byte size.
#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStoreProvider>,
    cache: Mutex<LruCache<String, bytes::Bytes>>,
    /// Maximum total size of cached data in bytes.
    max_cache_bytes: usize,
    /// Current total size of cached data in bytes.
    current_bytes: Mutex<usize>,
}

impl CachingObjectStore {
    /// Create a new caching wrapper with the given capacity in bytes.
    ///
    /// `max_entries` controls the maximum number of cached objects.
    pub fn new(
        inner: Arc<dyn ObjectStoreProvider>,
        max_cache_bytes: usize,
        max_entries: usize,
    ) -> Self {
        let cap = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::new(64).unwrap());
        Self {
            inner,
            cache: Mutex::new(LruCache::new(cap)),
            max_cache_bytes,
            current_bytes: Mutex::new(0),
        }
    }
}

impl ObjectStoreProvider for CachingObjectStore {
    fn scheme(&self) -> &str {
        self.inner.scheme()
    }

    fn get(&self, path: &ObjectPath) -> Result<bytes::Bytes> {
        let cache_key = path.full_path();

        // Check cache first.
        {
            let mut cache = self.cache.lock();
            if let Some(data) = cache.get(&cache_key) {
                return Ok(data.clone());
            }
        }

        // Cache miss – fetch from the inner store.
        let data = self.inner.get(path)?;

        // Only cache if the object fits within budget.
        if data.len() <= self.max_cache_bytes {
            let mut cache = self.cache.lock();
            let mut current = self.current_bytes.lock();

            // Evict entries until we have room.
            while *current + data.len() > self.max_cache_bytes {
                if let Some((_key, evicted)) = cache.pop_lru() {
                    *current = current.saturating_sub(evicted.len());
                } else {
                    break;
                }
            }

            *current += data.len();
            cache.put(cache_key, data.clone());
        }

        Ok(data)
    }

    fn list(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta> {
        self.inner.head(path)
    }
}

// ---------------------------------------------------------------------------
// ObjectStoreTable
// ---------------------------------------------------------------------------

/// A [`TableProvider`] that reads Parquet data from an object store.
///
/// On the first `scan()` call the table fetches the Parquet file from the
/// store, decodes it into Arrow [`RecordBatch`]es, and caches the result
/// for subsequent scans.
#[derive(Debug)]
pub struct ObjectStoreTable {
    path: ObjectPath,
    schema: Schema,
    store: Arc<dyn ObjectStoreProvider>,
    cached_batches: RwLock<Option<Vec<RecordBatch>>>,
}

impl ObjectStoreTable {
    /// Create a new object-store backed table.
    pub fn new(path: ObjectPath, schema: Schema, store: Arc<dyn ObjectStoreProvider>) -> Self {
        Self {
            path,
            schema,
            store,
            cached_batches: RwLock::new(None),
        }
    }

    /// Load batches from the object store (Parquet format).
    fn load_batches(&self) -> Result<Vec<RecordBatch>> {
        let data = self.store.get(&self.path)?;
        let cursor = data;
        let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }
        Ok(batches)
    }

    /// Return cached batches, loading from the store on first access.
    fn get_batches(&self) -> Result<Vec<RecordBatch>> {
        // Fast path – already cached.
        {
            let guard = self.cached_batches.read();
            if let Some(ref batches) = *guard {
                return Ok(batches.clone());
            }
        }

        // Slow path – load and cache.
        let batches = self.load_batches()?;
        *self.cached_batches.write() = Some(batches.clone());
        Ok(batches)
    }
}

impl TableProvider for ObjectStoreTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::External
    }

    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.get_batches()?;
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in &batches {
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => batch.clone(),
            };

            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if projected.num_rows() <= remaining {
                    rows_collected += projected.num_rows();
                    result.push(projected);
                } else {
                    result.push(projected.slice(0, remaining));
                    break;
                }
            } else {
                result.push(projected);
            }
        }

        Ok(result)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Cloud storage extensions (designed for future integration)
// ---------------------------------------------------------------------------

/// A discovered partition in a partitioned dataset.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    /// Partition column name-value pairs (e.g. year=2024, month=01)
    pub columns: Vec<(String, String)>,
    /// Object paths belonging to this partition
    pub paths: Vec<ObjectPath>,
}

#[allow(dead_code)]
impl PartitionSpec {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            paths: Vec::new(),
        }
    }

    pub fn add_column(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.columns.push((name.into(), value.into()));
    }

    pub fn add_path(&mut self, path: ObjectPath) {
        self.paths.push(path);
    }

    /// Returns true if this partition has the given column with the given value.
    pub fn matches_filter(&self, column: &str, value: &str) -> bool {
        self.columns.iter().any(|(c, v)| c == column && v == value)
    }
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// PartitionDiscovery
// ---------------------------------------------------------------------------

/// Discovers partitioned datasets from path patterns.
#[allow(dead_code)]
pub struct PartitionDiscovery;

#[allow(dead_code)]
impl PartitionDiscovery {
    /// Parse partition columns from a path like `year=2024/month=01/data.parquet`.
    pub fn parse_partition_path(path: &str) -> Vec<(String, String)> {
        path.split('/')
            .filter_map(|segment| {
                if Self::is_partition_segment(segment) {
                    let mut parts = segment.splitn(2, '=');
                    let name = parts.next().unwrap().to_string();
                    let value = parts.next().unwrap().to_string();
                    Some((name, value))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a path segment is a partition key=value.
    pub fn is_partition_segment(segment: &str) -> bool {
        let parts: Vec<&str> = segment.splitn(2, '=').collect();
        parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty()
    }

    /// Prune partitions based on filter predicates.
    pub fn prune_partitions(
        partitions: &[PartitionSpec],
        column: &str,
        value: &str,
    ) -> Vec<PartitionSpec> {
        partitions
            .iter()
            .filter(|p| p.matches_filter(column, value))
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ParallelFetchConfig
// ---------------------------------------------------------------------------

/// Configuration for parallel data fetching from cloud storage.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ParallelFetchConfig {
    pub max_concurrent_fetches: usize,
    pub chunk_size_bytes: usize,
    pub retry_max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub timeout_seconds: u64,
}

impl Default for ParallelFetchConfig {
    fn default() -> Self {
        Self {
            max_concurrent_fetches: 8,
            chunk_size_bytes: 8 * 1024 * 1024, // 8 MiB
            retry_max_attempts: 3,
            retry_base_delay_ms: 100,
            timeout_seconds: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// FetchStats
// ---------------------------------------------------------------------------

/// Statistics from a cloud storage fetch operation.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct FetchStats {
    pub total_bytes_fetched: u64,
    pub total_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub total_duration_ms: u64,
    pub partitions_pruned: u64,
    pub partitions_scanned: u64,
}

#[allow(dead_code)]
impl FetchStats {
    /// Returns the cache hit rate as a value between 0.0 and 1.0.
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Returns the average duration per request in milliseconds.
    pub fn avg_request_duration_ms(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_duration_ms as f64 / self.total_requests as f64
        }
    }

    /// Returns throughput in megabytes per second.
    pub fn throughput_mbps(&self) -> f64 {
        if self.total_duration_ms == 0 {
            0.0
        } else {
            let bytes_per_ms = self.total_bytes_fetched as f64 / self.total_duration_ms as f64;
            bytes_per_ms * 1000.0 / (1024.0 * 1024.0)
        }
    }
}

// ---------------------------------------------------------------------------
// CloudStorageConfig
// ---------------------------------------------------------------------------

/// Cloud storage provider type.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudProvider {
    S3,
    Gcs,
    Azure,
    MinIO,
    R2,
    Local,
}

/// Credentials for authenticating with cloud storage.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum CloudCredentials {
    Anonymous,
    AccessKey { key_id: String, secret: String },
    ServiceAccount { path: String },
    Environment,
}

/// Configuration for connecting to cloud storage services.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CloudStorageConfig {
    pub provider: CloudProvider,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: CloudCredentials,
    pub fetch_config: ParallelFetchConfig,
}

// ---------------------------------------------------------------------------
// Cloud Provider Implementations (S3, GCS, Azure)
// ---------------------------------------------------------------------------

/// S3-compatible object store provider.
#[allow(dead_code)]
#[derive(Debug)]
pub struct S3ObjectStore {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    credentials: S3Credentials,
}

/// S3 authentication credentials.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[allow(dead_code)]
impl S3ObjectStore {
    pub fn new(bucket: &str, region: &str, credentials: S3Credentials) -> Self {
        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            endpoint: None,
            credentials,
        }
    }

    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Construct the full URL for an object.
    pub fn object_url(&self, key: &str) -> String {
        if let Some(endpoint) = &self.endpoint {
            format!("{}/{}/{}", endpoint, self.bucket, key)
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket, self.region, key
            )
        }
    }

    /// List objects with a prefix.
    pub fn list_prefix(&self, prefix: &str) -> Vec<String> {
        // Placeholder - would make actual S3 API calls
        vec![format!("{}{}", prefix, "example.parquet")]
    }
}

/// Google Cloud Storage provider.
#[allow(dead_code)]
#[derive(Debug)]
pub struct GcsObjectStore {
    bucket: String,
    project_id: String,
    credentials_json: Option<String>,
}

#[allow(dead_code)]
impl GcsObjectStore {
    pub fn new(bucket: &str, project_id: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            project_id: project_id.to_string(),
            credentials_json: None,
        }
    }

    pub fn with_credentials(mut self, json: &str) -> Self {
        self.credentials_json = Some(json.to_string());
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn object_url(&self, key: &str) -> String {
        format!("https://storage.googleapis.com/{}/{}", self.bucket, key)
    }
}

/// Azure Blob Storage provider.
#[allow(dead_code)]
#[derive(Debug)]
pub struct AzureBlobStore {
    account: String,
    container: String,
    access_key: Option<String>,
    sas_token: Option<String>,
}

#[allow(dead_code)]
impl AzureBlobStore {
    pub fn new(account: &str, container: &str) -> Self {
        Self {
            account: account.to_string(),
            container: container.to_string(),
            access_key: None,
            sas_token: None,
        }
    }

    pub fn with_access_key(mut self, key: &str) -> Self {
        self.access_key = Some(key.to_string());
        self
    }

    pub fn with_sas_token(mut self, token: &str) -> Self {
        self.sas_token = Some(token.to_string());
        self
    }

    pub fn account(&self) -> &str {
        &self.account
    }
    pub fn container(&self) -> &str {
        &self.container
    }

    pub fn blob_url(&self, key: &str) -> String {
        format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account, self.container, key
        )
    }
}

// ---------------------------------------------------------------------------
// Parallel Row-Group Fetching
// ---------------------------------------------------------------------------

/// Manages parallel fetching of Parquet row groups from remote storage.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ParallelRowGroupFetcher {
    concurrency: usize,
    chunk_size_bytes: usize,
    stats: FetchStats,
}

#[allow(dead_code)]
impl ParallelRowGroupFetcher {
    pub fn new(concurrency: usize, chunk_size_bytes: usize) -> Self {
        Self {
            concurrency,
            chunk_size_bytes,
            stats: FetchStats::default(),
        }
    }

    pub fn from_config(config: &ParallelFetchConfig) -> Self {
        Self::new(config.max_concurrent_fetches, config.chunk_size_bytes)
    }

    /// Plan fetch operations for a set of row groups.
    pub fn plan_fetches(&self, row_groups: &[RowGroupMeta]) -> Vec<FetchPlan> {
        let mut plans = Vec::new();
        for (i, rg) in row_groups.iter().enumerate() {
            let num_chunks = rg.size_bytes.div_ceil(self.chunk_size_bytes);
            for chunk in 0..num_chunks {
                let offset = chunk * self.chunk_size_bytes;
                let len = self.chunk_size_bytes.min(rg.size_bytes - offset);
                plans.push(FetchPlan {
                    row_group_index: i,
                    offset: rg.offset + offset,
                    length: len,
                    priority: i,
                });
            }
        }
        plans
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
    pub fn stats(&self) -> &FetchStats {
        &self.stats
    }
}

/// Metadata for a Parquet row group.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub index: usize,
    pub offset: usize,
    pub size_bytes: usize,
    pub num_rows: usize,
}

/// A planned fetch operation.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FetchPlan {
    pub row_group_index: usize,
    pub offset: usize,
    pub length: usize,
    pub priority: usize,
}

/// Column-level fetch optimizer that only retrieves needed columns.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ColumnProjectionFetcher {
    projected_columns: Vec<usize>,
}

#[allow(dead_code)]
impl ColumnProjectionFetcher {
    pub fn new(projected_columns: Vec<usize>) -> Self {
        Self { projected_columns }
    }

    /// Filter row group columns to only those needed.
    pub fn filter_columns<'a, T>(&self, all_columns: &'a [T]) -> Vec<&'a T> {
        self.projected_columns
            .iter()
            .filter_map(|&i| all_columns.get(i))
            .collect()
    }

    pub fn num_projected(&self) -> usize {
        self.projected_columns.len()
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Partition Pruning + Metadata Cache
// ---------------------------------------------------------------------------

/// Evaluates predicates against partition values to prune unnecessary partitions.
#[allow(dead_code)]
#[derive(Debug)]
pub struct PartitionPruner {
    partition_columns: Vec<String>,
}

#[allow(dead_code)]
impl PartitionPruner {
    pub fn new(partition_columns: Vec<String>) -> Self {
        Self { partition_columns }
    }

    /// Prune partitions that don't match the given filter predicates.
    /// Returns indices of partitions to scan.
    pub fn prune(
        &self,
        partitions: &[PartitionValue],
        predicates: &[PartitionPredicate],
    ) -> Vec<usize> {
        partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| predicates.iter().all(|pred| pred.matches(p)))
            .map(|(i, _)| i)
            .collect()
    }
}

/// A partition value (key=value pair).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionValue {
    pub column: String,
    pub value: String,
}

/// A predicate for partition pruning.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum PartitionPredicate {
    Eq(String, String),
    NotEq(String, String),
    In(String, Vec<String>),
    Gt(String, String),
    Lt(String, String),
}

#[allow(dead_code)]
impl PartitionPredicate {
    fn matches(&self, pv: &PartitionValue) -> bool {
        match self {
            Self::Eq(col, val) => pv.column != *col || pv.value == *val,
            Self::NotEq(col, val) => pv.column != *col || pv.value != *val,
            Self::In(col, vals) => pv.column != *col || vals.contains(&pv.value),
            Self::Gt(col, val) => pv.column != *col || pv.value > *val,
            Self::Lt(col, val) => pv.column != *col || pv.value < *val,
        }
    }
}

/// Metadata cache with TTL-based expiration.
#[allow(dead_code)]
pub struct MetadataCache {
    entries: HashMap<String, CacheEntry>,
    ttl_secs: u64,
    max_entries: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct CacheEntry {
    data: CachedMetadata,
    inserted_at: std::time::Instant,
}

/// Cached file/partition metadata.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CachedMetadata {
    pub file_path: String,
    pub size_bytes: u64,
    pub num_row_groups: usize,
    pub num_rows: u64,
    pub schema_fingerprint: String,
}

#[allow(dead_code)]
impl MetadataCache {
    pub fn new(ttl_secs: u64, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_secs,
            max_entries,
        }
    }

    pub fn get(&self, key: &str) -> Option<&CachedMetadata> {
        self.entries.get(key).and_then(|e| {
            if e.inserted_at.elapsed().as_secs() < self.ttl_secs {
                Some(&e.data)
            } else {
                None
            }
        })
    }

    pub fn put(&mut self, key: String, metadata: CachedMetadata) {
        if self.entries.len() >= self.max_entries {
            // Evict oldest entry
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest_key);
            }
        }
        self.entries.insert(
            key,
            CacheEntry {
                data: metadata,
                inserted_at: std::time::Instant::now(),
            },
        );
    }

    pub fn invalidate(&mut self, key: &str) {
        self.entries.remove(key);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove expired entries.
    pub fn evict_expired(&mut self) {
        let ttl = self.ttl_secs;
        self.entries
            .retain(|_, e| e.inserted_at.elapsed().as_secs() < ttl);
    }
}

/// Multi-region failover configuration.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MultiRegionConfig {
    pub primary_region: String,
    pub failover_regions: Vec<String>,
    pub health_check_interval_secs: u64,
}

#[allow(dead_code)]
impl MultiRegionConfig {
    pub fn new(primary: &str) -> Self {
        Self {
            primary_region: primary.to_string(),
            failover_regions: Vec::new(),
            health_check_interval_secs: 30,
        }
    }

    pub fn add_failover(mut self, region: &str) -> Self {
        self.failover_regions.push(region.to_string());
        self
    }

    /// Get the list of regions in priority order.
    pub fn regions_in_order(&self) -> Vec<&str> {
        let mut regions = vec![self.primary_region.as_str()];
        regions.extend(self.failover_regions.iter().map(|r| r.as_str()));
        regions
    }
}

// ---------------------------------------------------------------------------
// PrefetchStats / PrefetchManager
// ---------------------------------------------------------------------------

/// Statistics for the prefetch manager.
#[derive(Debug, Clone, Default)]
pub struct PrefetchStats {
    pub scheduled: usize,
    pub completed: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

/// Parallel prefetch manager that queues object paths for background fetching
/// and serves previously fetched data from an in-memory cache.
#[allow(dead_code)]
#[derive(Debug)]
pub struct PrefetchManager {
    max_concurrent: usize,
    buffer_size: usize,
    queue: Mutex<Vec<String>>,
    cache: Mutex<HashMap<String, Vec<u8>>>,
    stats: Mutex<PrefetchStats>,
}

#[allow(dead_code)]
impl PrefetchManager {
    pub fn new(max_concurrent: usize, buffer_size: usize) -> Self {
        Self {
            max_concurrent,
            buffer_size,
            queue: Mutex::new(Vec::new()),
            cache: Mutex::new(HashMap::new()),
            stats: Mutex::new(PrefetchStats::default()),
        }
    }

    /// Queue paths for prefetching.
    pub fn schedule_prefetch(&self, paths: &[String]) {
        let mut queue = self.queue.lock();
        let mut stats = self.stats.lock();
        for p in paths {
            if queue.len() < self.buffer_size {
                queue.push(p.clone());
                stats.scheduled += 1;
            }
        }
    }

    /// Retrieve prefetched data for the given path.
    pub fn get_prefetched(&self, path: &str) -> Option<Vec<u8>> {
        let mut stats = self.stats.lock();
        let cache = self.cache.lock();
        if let Some(data) = cache.get(path) {
            stats.cache_hits += 1;
            Some(data.clone())
        } else {
            stats.cache_misses += 1;
            None
        }
    }

    /// Simulate completing the prefetch for a given path (stores data in cache).
    pub fn complete_prefetch(&self, path: &str, data: Vec<u8>) {
        let mut cache = self.cache.lock();
        let mut stats = self.stats.lock();
        cache.insert(path.to_string(), data);
        stats.completed += 1;
    }

    /// Cancel all pending prefetch operations.
    pub fn cancel_all(&self) {
        self.queue.lock().clear();
    }

    /// Return a snapshot of prefetch statistics.
    pub fn stats(&self) -> PrefetchStats {
        self.stats.lock().clone()
    }
}

// ---------------------------------------------------------------------------
// PartitionInfo / PartitionCache
// ---------------------------------------------------------------------------

/// Metadata about a single partition.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionInfo {
    pub path: String,
    pub row_count: usize,
    pub size_bytes: u64,
    pub partition_values: HashMap<String, String>,
}

/// LRU cache for partition metadata keyed by table name.
#[allow(dead_code)]
#[derive(Debug)]
pub struct PartitionCache {
    entries: Mutex<HashMap<String, Vec<PartitionInfo>>>,
    max_entries: usize,
}

#[allow(dead_code)]
impl PartitionCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries,
        }
    }

    pub fn put(&self, table: &str, partitions: Vec<PartitionInfo>) {
        let mut entries = self.entries.lock();
        if entries.len() >= self.max_entries && !entries.contains_key(table) {
            // Evict an arbitrary entry to stay within bounds.
            if let Some(key) = entries.keys().next().cloned() {
                entries.remove(&key);
            }
        }
        entries.insert(table.to_string(), partitions);
    }

    pub fn get(&self, table: &str) -> Option<Vec<PartitionInfo>> {
        self.entries.lock().get(table).cloned()
    }

    pub fn invalidate(&self, table: &str) {
        self.entries.lock().remove(table);
    }

    pub fn invalidate_all(&self) {
        self.entries.lock().clear();
    }
}

// ---------------------------------------------------------------------------
// CredentialChain / ResolvedCredential
// ---------------------------------------------------------------------------

/// Credential provider variants.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum CredentialProvider {
    Environment,
    Static { key: String, secret: String },
    IamRole { role_arn: String },
    ServiceAccount,
    Anonymous,
}

/// A resolved credential ready for use with a cloud API.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ResolvedCredential {
    pub access_key: String,
    pub secret_key: String,
    pub token: Option<String>,
    pub expiry: Option<u64>,
}

/// Builder-style credential chain that tries providers in order.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CredentialChain {
    providers: Vec<CredentialProvider>,
}

#[allow(dead_code)]
impl CredentialChain {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn with_environment(mut self) -> Self {
        self.providers.push(CredentialProvider::Environment);
        self
    }

    pub fn with_static(mut self, key: &str, secret: &str) -> Self {
        self.providers.push(CredentialProvider::Static {
            key: key.to_string(),
            secret: secret.to_string(),
        });
        self
    }

    pub fn with_iam_role(mut self, role_arn: &str) -> Self {
        self.providers.push(CredentialProvider::IamRole {
            role_arn: role_arn.to_string(),
        });
        self
    }

    pub fn with_anonymous(mut self) -> Self {
        self.providers.push(CredentialProvider::Anonymous);
        self
    }

    /// Walk the provider chain and return the first successfully resolved
    /// credential.
    pub fn resolve(&self) -> Result<ResolvedCredential> {
        for provider in &self.providers {
            match provider {
                CredentialProvider::Static { key, secret } => {
                    return Ok(ResolvedCredential {
                        access_key: key.clone(),
                        secret_key: secret.clone(),
                        token: None,
                        expiry: None,
                    });
                }
                CredentialProvider::Anonymous => {
                    return Ok(ResolvedCredential {
                        access_key: String::new(),
                        secret_key: String::new(),
                        token: None,
                        expiry: None,
                    });
                }
                CredentialProvider::Environment => {
                    if let (Ok(ak), Ok(sk)) = (
                        std::env::var("BLAZE_ACCESS_KEY"),
                        std::env::var("BLAZE_SECRET_KEY"),
                    ) {
                        return Ok(ResolvedCredential {
                            access_key: ak,
                            secret_key: sk,
                            token: std::env::var("BLAZE_SESSION_TOKEN").ok(),
                            expiry: None,
                        });
                    }
                }
                CredentialProvider::IamRole { .. } | CredentialProvider::ServiceAccount => {
                    // Real IAM / SA resolution requires network calls;
                    // intentionally not implemented in the embedded engine.
                }
            }
        }
        Err(BlazeError::invalid_argument(
            "No credential provider in the chain could resolve credentials",
        ))
    }

    pub fn providers(&self) -> &[CredentialProvider] {
        &self.providers
    }
}

// ---------------------------------------------------------------------------
// CloudRouter / CloudEndpoint
// ---------------------------------------------------------------------------

/// Describes a routable cloud endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct CloudEndpoint {
    pub cloud: CloudProvider,
    pub region: String,
    pub endpoint: String,
}

/// Region entry stored inside the router.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RegionEntry {
    name: String,
    cloud: CloudProvider,
    endpoint: String,
}

/// Routes queries to the best cloud region based on table-to-region mappings.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CloudRouter {
    regions: Vec<RegionEntry>,
    table_map: HashMap<String, String>,
}

#[allow(dead_code)]
impl CloudRouter {
    pub fn new() -> Self {
        Self {
            regions: Vec::new(),
            table_map: HashMap::new(),
        }
    }

    pub fn add_region(mut self, name: &str, cloud: CloudProvider, endpoint: &str) -> Self {
        self.regions.push(RegionEntry {
            name: name.to_string(),
            cloud,
            endpoint: endpoint.to_string(),
        });
        self
    }

    /// Bind a table to a specific region name.
    pub fn bind_table(&mut self, table: &str, region: &str) {
        self.table_map.insert(table.to_string(), region.to_string());
    }

    /// Route a query for the given table to the best endpoint.
    pub fn route_query(&self, table: &str) -> Result<CloudEndpoint> {
        // If there is an explicit binding, use it.
        if let Some(region_name) = self.table_map.get(table) {
            if let Some(entry) = self.regions.iter().find(|r| r.name == *region_name) {
                return Ok(CloudEndpoint {
                    cloud: entry.cloud.clone(),
                    region: entry.name.clone(),
                    endpoint: entry.endpoint.clone(),
                });
            }
        }
        // Fall back to the first registered region.
        self.regions
            .first()
            .map(|entry| CloudEndpoint {
                cloud: entry.cloud.clone(),
                region: entry.name.clone(),
                endpoint: entry.endpoint.clone(),
            })
            .ok_or_else(|| {
                BlazeError::invalid_argument(format!(
                    "No region available to route query for table '{table}'"
                ))
            })
    }

    /// Estimate the data transfer cost (USD) between two regions.
    pub fn estimate_transfer_cost(&self, from: &str, to: &str, bytes: u64) -> f64 {
        if from == to {
            return 0.0;
        }
        let from_cloud = self.regions.iter().find(|r| r.name == from);
        let to_cloud = self.regions.iter().find(|r| r.name == to);
        let cross_cloud = match (from_cloud, to_cloud) {
            (Some(a), Some(b)) => a.cloud != b.cloud,
            _ => true,
        };
        let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if cross_cloud {
            gb * 0.09 // cross-cloud rate
        } else {
            gb * 0.02 // same-cloud cross-region rate
        }
    }
}

// ---------------------------------------------------------------------------
// HivePartitionDiscovery
// ---------------------------------------------------------------------------

/// Enhanced Hive-style partition discovery with recursive listing and schema inference.
#[allow(dead_code)]
#[derive(Debug)]
pub struct HivePartitionDiscovery {
    store: Arc<dyn ObjectStoreProvider>,
    partition_columns: Vec<String>,
    file_extension: String,
}

#[allow(dead_code)]
impl HivePartitionDiscovery {
    pub fn new(store: Arc<dyn ObjectStoreProvider>, file_extension: &str) -> Self {
        Self {
            store,
            partition_columns: Vec::new(),
            file_extension: file_extension.to_string(),
        }
    }

    /// Discover partitions by listing the base path and parsing Hive-style dirs.
    pub fn discover(&self, base_path: &ObjectPath) -> Result<Vec<DiscoveredPartition>> {
        let entries = self.store.list(base_path)?;
        let mut partitions = Vec::new();

        for entry in &entries {
            let relative = entry.path.key.trim_start_matches('/');
            let parts = PartitionDiscovery::parse_partition_path(relative);
            if !parts.is_empty() || relative.ends_with(&self.file_extension) {
                partitions.push(DiscoveredPartition {
                    path: ObjectPath {
                        scheme: base_path.scheme.clone(),
                        bucket: base_path.bucket.clone(),
                        key: if base_path.key.is_empty() {
                            relative.to_string()
                        } else {
                            format!("{}/{}", base_path.key, relative)
                        },
                    },
                    partition_values: parts.into_iter().collect(),
                    size_bytes: entry.size as u64,
                });
            }
        }

        Ok(partitions)
    }

    /// Discover partitions and infer the partition schema from discovered values.
    pub fn discover_with_schema(
        &self,
        base_path: &ObjectPath,
    ) -> Result<(Vec<DiscoveredPartition>, PartitionSchema)> {
        let partitions = self.discover(base_path)?;
        let schema = PartitionSchema::infer_from_partitions(&partitions);
        Ok((partitions, schema))
    }
}

/// A discovered partition with its path and extracted key=value pairs.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DiscoveredPartition {
    pub path: ObjectPath,
    pub partition_values: HashMap<String, String>,
    pub size_bytes: u64,
}

/// Schema inferred from partition column values.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionSchema {
    pub columns: Vec<PartitionColumnInfo>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionColumnInfo {
    pub name: String,
    pub distinct_values: Vec<String>,
    pub inferred_type: PartitionDataType,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionDataType {
    Integer,
    Date,
    String,
}

#[allow(dead_code)]
impl PartitionSchema {
    pub fn infer_from_partitions(partitions: &[DiscoveredPartition]) -> Self {
        let mut column_values: HashMap<String, Vec<String>> = HashMap::new();

        for partition in partitions {
            for (col, val) in &partition.partition_values {
                column_values
                    .entry(col.clone())
                    .or_default()
                    .push(val.clone());
            }
        }

        let columns = column_values
            .into_iter()
            .map(|(name, mut values)| {
                values.sort();
                values.dedup();
                let inferred_type = Self::infer_type(&values);
                PartitionColumnInfo {
                    name,
                    distinct_values: values,
                    inferred_type,
                }
            })
            .collect();

        Self { columns }
    }

    fn infer_type(values: &[String]) -> PartitionDataType {
        if values.iter().all(|v| v.parse::<i64>().is_ok()) {
            PartitionDataType::Integer
        } else if values.iter().all(|v| {
            v.len() == 10 && v.chars().nth(4) == Some('-') && v.chars().nth(7) == Some('-')
        }) {
            PartitionDataType::Date
        } else {
            PartitionDataType::String
        }
    }
}

// ---------------------------------------------------------------------------
// RangeReader
// ---------------------------------------------------------------------------

/// Reads byte ranges from cloud storage for efficient large file access.
#[allow(dead_code)]
#[derive(Debug)]
pub struct RangeReader {
    store: Arc<dyn ObjectStoreProvider>,
    path: ObjectPath,
    file_size: u64,
    range_size: usize,
}

#[allow(dead_code)]
impl RangeReader {
    pub fn new(
        store: Arc<dyn ObjectStoreProvider>,
        path: ObjectPath,
        range_size: usize,
    ) -> Result<Self> {
        let meta = store.head(&path)?;
        Ok(Self {
            store,
            path,
            file_size: meta.size as u64,
            range_size,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Calculate the ranges needed to read the entire file.
    pub fn compute_ranges(&self) -> Vec<ByteRange> {
        let mut ranges = Vec::new();
        let mut offset = 0u64;
        while offset < self.file_size {
            let length = std::cmp::min(self.range_size as u64, self.file_size - offset);
            ranges.push(ByteRange { offset, length });
            offset += length;
        }
        ranges
    }

    /// Read a specific byte range from the file.
    pub fn read_range(&self, range: &ByteRange) -> Result<Vec<u8>> {
        let full = self.store.get(&self.path)?;
        let start = range.offset as usize;
        let end = std::cmp::min(start + range.length as usize, full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }

    /// Read all ranges and concatenate into a single buffer.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        let data = self.store.get(&self.path)?;
        Ok(data.to_vec())
    }
}

/// A byte range within a file.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

// ---------------------------------------------------------------------------
// CredentialRefreshManager
// ---------------------------------------------------------------------------

/// Manages credential lifecycle with expiry tracking and auto-refresh.
#[allow(dead_code)]
#[derive(Debug)]
pub struct CredentialRefreshManager {
    chain: CredentialChain,
    current: Mutex<Option<CachedCredential>>,
    refresh_before_expiry_secs: u64,
}

#[derive(Debug, Clone)]
struct CachedCredential {
    credential: ResolvedCredential,
    resolved_at: std::time::Instant,
}

#[allow(dead_code)]
impl CredentialRefreshManager {
    pub fn new(chain: CredentialChain) -> Self {
        Self {
            chain,
            current: Mutex::new(None),
            refresh_before_expiry_secs: 300, // refresh 5 min before expiry
        }
    }

    pub fn with_refresh_buffer(mut self, secs: u64) -> Self {
        self.refresh_before_expiry_secs = secs;
        self
    }

    /// Get a valid credential, refreshing if expired or about to expire.
    pub fn get_credential(&self) -> Result<ResolvedCredential> {
        let mut cached = self.current.lock();

        if let Some(ref c) = *cached {
            if !self.is_expired(c) {
                return Ok(c.credential.clone());
            }
        }

        // Refresh
        let credential = self.chain.resolve()?;
        *cached = Some(CachedCredential {
            credential: credential.clone(),
            resolved_at: std::time::Instant::now(),
        });

        Ok(credential)
    }

    fn is_expired(&self, cached: &CachedCredential) -> bool {
        match cached.credential.expiry {
            Some(expiry_epoch) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                now + self.refresh_before_expiry_secs >= expiry_epoch
            }
            None => {
                // No expiry set - refresh every hour as a safety measure
                cached.resolved_at.elapsed().as_secs() > 3600
            }
        }
    }

    /// Force a credential refresh.
    pub fn force_refresh(&self) -> Result<ResolvedCredential> {
        let credential = self.chain.resolve()?;
        let mut cached = self.current.lock();
        *cached = Some(CachedCredential {
            credential: credential.clone(),
            resolved_at: std::time::Instant::now(),
        });
        Ok(credential)
    }
}

// ---------------------------------------------------------------------------
// CloudStorageTableFactory
// ---------------------------------------------------------------------------

/// Factory for creating ObjectStoreTables from cloud storage URIs.
/// Handles URI parsing, credential resolution, and schema inference.
#[allow(dead_code)]
pub struct CloudStorageTableFactory {
    registry: Arc<ObjectStoreRegistry>,
    metadata_cache: Arc<Mutex<MetadataCache>>,
    credential_manager: Option<Arc<CredentialRefreshManager>>,
}

impl fmt::Debug for CloudStorageTableFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudStorageTableFactory")
            .field("registry", &self.registry)
            .field("credential_manager", &self.credential_manager)
            .finish()
    }
}

#[allow(dead_code)]
impl CloudStorageTableFactory {
    pub fn new(registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            registry,
            metadata_cache: Arc::new(Mutex::new(MetadataCache::new(300, 10_000))),
            credential_manager: None,
        }
    }

    pub fn with_credentials(mut self, manager: Arc<CredentialRefreshManager>) -> Self {
        self.credential_manager = Some(manager);
        self
    }

    /// Create a table from a cloud URI, inferring schema from the Parquet file.
    pub fn create_table(&self, uri: &str) -> Result<ObjectStoreTable> {
        let path = ObjectPath::parse(uri)?;
        let store = self.registry.get(&path.scheme).ok_or_else(|| {
            BlazeError::invalid_argument(format!(
                "No object store registered for scheme '{}'",
                path.scheme
            ))
        })?;

        // Read the file to infer schema from Parquet metadata
        let data = store.get(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| BlazeError::execution(format!("Failed to read Parquet schema: {e}")))?;

        let arrow_schema = reader.schema().clone();
        let fields: Vec<crate::types::Field> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let dt = crate::types::DataType::from_arrow(f.data_type())
                    .unwrap_or(crate::types::DataType::Utf8);
                crate::types::Field::new(f.name(), dt, f.is_nullable())
            })
            .collect();
        let schema = Schema::new(fields);

        Ok(ObjectStoreTable::new(path, schema, store))
    }

    /// Create a partitioned table from a cloud URI prefix.
    pub fn create_partitioned_table(
        &self,
        base_uri: &str,
        file_extension: &str,
    ) -> Result<PartitionedObjectStoreTable> {
        let base_path = ObjectPath::parse(base_uri)?;
        let store = self.registry.get(&base_path.scheme).ok_or_else(|| {
            BlazeError::invalid_argument(format!(
                "No object store registered for scheme '{}'",
                base_path.scheme
            ))
        })?;

        let discovery = HivePartitionDiscovery::new(store.clone(), file_extension);
        let (partitions, partition_schema) = discovery.discover_with_schema(&base_path)?;

        Ok(PartitionedObjectStoreTable {
            base_path,
            store,
            partitions,
            partition_schema,
            data_schema: None,
        })
    }
}

/// A table backed by partitioned files in cloud storage.
#[allow(dead_code)]
#[derive(Debug)]
pub struct PartitionedObjectStoreTable {
    base_path: ObjectPath,
    store: Arc<dyn ObjectStoreProvider>,
    partitions: Vec<DiscoveredPartition>,
    partition_schema: PartitionSchema,
    data_schema: Option<Schema>,
}

#[allow(dead_code)]
impl PartitionedObjectStoreTable {
    pub fn partitions(&self) -> &[DiscoveredPartition] {
        &self.partitions
    }

    pub fn partition_schema(&self) -> &PartitionSchema {
        &self.partition_schema
    }

    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Total data size across all partitions.
    pub fn total_size_bytes(&self) -> u64 {
        self.partitions.iter().map(|p| p.size_bytes).sum()
    }

    /// Prune partitions based on predicates, returning only matching ones.
    pub fn prune_partitions(&self, predicates: &[PartitionPredicate]) -> Vec<&DiscoveredPartition> {
        self.partitions
            .iter()
            .filter(|partition| {
                predicates.iter().all(|pred| match pred {
                    PartitionPredicate::Eq(col, val) => {
                        partition.partition_values.get(col).is_none_or(|v| v == val)
                    }
                    PartitionPredicate::NotEq(col, val) => {
                        partition.partition_values.get(col) != Some(val)
                    }
                    PartitionPredicate::In(col, vals) => partition
                        .partition_values
                        .get(col)
                        .is_none_or(|v| vals.contains(v)),
                    PartitionPredicate::Gt(col, val) => partition
                        .partition_values
                        .get(col)
                        .is_none_or(|v| v.as_str() > val.as_str()),
                    PartitionPredicate::Lt(col, val) => partition
                        .partition_values
                        .get(col)
                        .is_none_or(|v| v.as_str() < val.as_str()),
                })
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};
    use arrow::array::Int64Array;
    use arrow::datatypes::Schema as ArrowSchema;
    use parquet::arrow::ArrowWriter;
    use std::io::Write;
    use tempfile::TempDir;

    /// Helper: create a two-column test RecordBatch.
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
        ]));
        let id = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let value = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        RecordBatch::try_new(schema, vec![id, value]).unwrap()
    }

    /// Helper: write a Parquet file into the given directory and return its path.
    fn write_test_parquet(dir: &std::path::Path) -> PathBuf {
        let file_path = dir.join("test.parquet");
        let batch = create_test_batch();
        let file = fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file_path
    }

    #[test]
    fn test_object_path_parse_s3() {
        let path = ObjectPath::parse("s3://my-bucket/data/file.parquet").unwrap();
        assert_eq!(path.scheme, "s3");
        assert_eq!(path.bucket, "my-bucket");
        assert_eq!(path.key, "data/file.parquet");
        assert_eq!(path.full_path(), "s3://my-bucket/data/file.parquet");
    }

    #[test]
    fn test_object_path_parse_file() {
        let path = ObjectPath::parse("file:///tmp/data.parquet").unwrap();
        assert_eq!(path.scheme, "file");
        assert_eq!(path.bucket, "");
        assert_eq!(path.key, "/tmp/data.parquet");
        assert_eq!(path.full_path(), "file:///tmp/data.parquet");
    }

    #[test]
    fn test_object_path_parse_gs() {
        let path = ObjectPath::parse("gs://bucket/prefix/obj").unwrap();
        assert_eq!(path.scheme, "gs");
        assert_eq!(path.bucket, "bucket");
        assert_eq!(path.key, "prefix/obj");
    }

    #[test]
    fn test_object_path_parse_invalid() {
        assert!(ObjectPath::parse("no-scheme").is_err());
        assert!(ObjectPath::parse("s3://").is_err());
        assert!(ObjectPath::parse("unknown://bucket/key").is_err());
    }

    #[test]
    fn test_local_filesystem_store() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("hello.txt");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"hello world").unwrap();
        }

        let store = LocalFileSystemStore;
        let obj_path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: file_path.to_string_lossy().to_string(),
        };

        // get
        let data = store.get(&obj_path).unwrap();
        assert_eq!(&data[..], b"hello world");

        // head
        let meta = store.head(&obj_path).unwrap();
        assert_eq!(meta.size, 11);

        // list
        let dir_path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: dir.path().to_string_lossy().to_string(),
        };
        let entries = store.list(&dir_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].size, 11);
    }

    #[test]
    fn test_object_store_registry() {
        let registry = ObjectStoreRegistry::new();

        // The "file" scheme is pre-registered.
        assert!(registry.get("file").is_some());
        assert!(registry.get("s3").is_none());

        // Register a custom provider.
        registry.register("s3", Arc::new(LocalFileSystemStore));
        assert!(registry.get("s3").is_some());
    }

    #[test]
    fn test_caching_store() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("cached.txt");
        fs::write(&file_path, b"cached data").unwrap();

        let inner: Arc<dyn ObjectStoreProvider> = Arc::new(LocalFileSystemStore);
        let caching = CachingObjectStore::new(inner, 1024, 16);

        let obj_path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: file_path.to_string_lossy().to_string(),
        };

        // First call – cache miss.
        let data1 = caching.get(&obj_path).unwrap();
        assert_eq!(&data1[..], b"cached data");

        // Overwrite the file on disk so we can verify the cache is used.
        fs::write(&file_path, b"new data!!!").unwrap();

        // Second call – should return cached (old) data.
        let data2 = caching.get(&obj_path).unwrap();
        assert_eq!(&data2[..], b"cached data");
    }

    #[test]
    fn test_caching_store_eviction() {
        let dir = TempDir::new().unwrap();

        // Create two files, each 10 bytes.
        let path_a = dir.path().join("a.txt");
        let path_b = dir.path().join("b.txt");
        fs::write(&path_a, b"aaaaaaaaaa").unwrap();
        fs::write(&path_b, b"bbbbbbbbbb").unwrap();

        // Cache can only hold 15 bytes total – one file fits, two don't.
        let inner: Arc<dyn ObjectStoreProvider> = Arc::new(LocalFileSystemStore);
        let caching = CachingObjectStore::new(inner, 15, 16);

        let obj_a = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: path_a.to_string_lossy().to_string(),
        };
        let obj_b = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: path_b.to_string_lossy().to_string(),
        };

        // Load A into cache.
        let _ = caching.get(&obj_a).unwrap();
        // Load B – should evict A.
        let _ = caching.get(&obj_b).unwrap();

        // Overwrite A on disk.
        fs::write(&path_a, b"AAAAAAAAAA").unwrap();

        // Reading A again should see the new content (cache was evicted).
        let data = caching.get(&obj_a).unwrap();
        assert_eq!(&data[..], b"AAAAAAAAAA");
    }

    #[test]
    fn test_object_store_table_scan() {
        let dir = TempDir::new().unwrap();
        let parquet_path = write_test_parquet(dir.path());

        let store: Arc<dyn ObjectStoreProvider> = Arc::new(LocalFileSystemStore);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let obj_path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: parquet_path.to_string_lossy().to_string(),
        };

        let table = ObjectStoreTable::new(obj_path, schema, store);

        // Full scan.
        let batches = table.scan(None, &[], None).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        // Projection pushdown – only "id" column.
        let batches = table.scan(Some(&[0]), &[], None).unwrap();
        assert_eq!(batches[0].num_columns(), 1);

        // Limit pushdown.
        let batches = table.scan(None, &[], Some(3)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_object_store_table_caches_batches() {
        let dir = TempDir::new().unwrap();
        let parquet_path = write_test_parquet(dir.path());

        let store: Arc<dyn ObjectStoreProvider> = Arc::new(LocalFileSystemStore);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let obj_path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: parquet_path.to_string_lossy().to_string(),
        };

        let table = ObjectStoreTable::new(obj_path, schema, store);

        // First scan loads data.
        let b1 = table.scan(None, &[], None).unwrap();
        // Delete the file to verify cache is used on second scan.
        fs::remove_file(&parquet_path).unwrap();
        let b2 = table.scan(None, &[], None).unwrap();

        assert_eq!(b1.len(), b2.len());
        assert_eq!(b1[0].num_rows(), b2[0].num_rows());
    }

    // -----------------------------------------------------------------------
    // PartitionSpec tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_spec_basic() {
        let mut spec = PartitionSpec::new();
        assert!(spec.columns.is_empty());
        assert!(spec.paths.is_empty());

        spec.add_column("year", "2024");
        spec.add_column("month", "01");
        assert_eq!(spec.columns.len(), 2);
        assert_eq!(spec.columns[0], ("year".to_string(), "2024".to_string()));
        assert_eq!(spec.columns[1], ("month".to_string(), "01".to_string()));

        let path = ObjectPath::parse("s3://bucket/year=2024/month=01/data.parquet").unwrap();
        spec.add_path(path.clone());
        assert_eq!(spec.paths.len(), 1);
        assert_eq!(spec.paths[0], path);
    }

    #[test]
    fn test_partition_spec_matches_filter() {
        let mut spec = PartitionSpec::new();
        spec.add_column("year", "2024");
        spec.add_column("month", "01");

        assert!(spec.matches_filter("year", "2024"));
        assert!(spec.matches_filter("month", "01"));
        assert!(!spec.matches_filter("year", "2023"));
        assert!(!spec.matches_filter("day", "15"));
    }

    // -----------------------------------------------------------------------
    // PartitionDiscovery tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_partition_path() {
        let cols = PartitionDiscovery::parse_partition_path("year=2024/month=01/data.parquet");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ("year".to_string(), "2024".to_string()));
        assert_eq!(cols[1], ("month".to_string(), "01".to_string()));

        // Path with no partitions.
        let cols = PartitionDiscovery::parse_partition_path("data/file.parquet");
        assert!(cols.is_empty());

        // Mixed segments.
        let cols = PartitionDiscovery::parse_partition_path("data/region=us-east/file.parquet");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0], ("region".to_string(), "us-east".to_string()));
    }

    #[test]
    fn test_is_partition_segment() {
        assert!(PartitionDiscovery::is_partition_segment("year=2024"));
        assert!(PartitionDiscovery::is_partition_segment("region=us-east"));
        assert!(!PartitionDiscovery::is_partition_segment("data.parquet"));
        assert!(!PartitionDiscovery::is_partition_segment("=value"));
        assert!(!PartitionDiscovery::is_partition_segment("key="));
        assert!(!PartitionDiscovery::is_partition_segment("noequalssign"));
    }

    #[test]
    fn test_prune_partitions() {
        let mut p1 = PartitionSpec::new();
        p1.add_column("year", "2024");
        p1.add_column("month", "01");

        let mut p2 = PartitionSpec::new();
        p2.add_column("year", "2024");
        p2.add_column("month", "02");

        let mut p3 = PartitionSpec::new();
        p3.add_column("year", "2023");
        p3.add_column("month", "01");

        let partitions = vec![p1.clone(), p2.clone(), p3.clone()];

        let pruned = PartitionDiscovery::prune_partitions(&partitions, "year", "2024");
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0], p1);
        assert_eq!(pruned[1], p2);

        let pruned = PartitionDiscovery::prune_partitions(&partitions, "month", "01");
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0], p1);
        assert_eq!(pruned[1], p3);

        let pruned = PartitionDiscovery::prune_partitions(&partitions, "year", "2025");
        assert!(pruned.is_empty());
    }

    // -----------------------------------------------------------------------
    // ParallelFetchConfig tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parallel_fetch_config_default() {
        let config = ParallelFetchConfig::default();
        assert_eq!(config.max_concurrent_fetches, 8);
        assert_eq!(config.chunk_size_bytes, 8 * 1024 * 1024);
        assert_eq!(config.retry_max_attempts, 3);
        assert_eq!(config.retry_base_delay_ms, 100);
        assert_eq!(config.timeout_seconds, 30);
    }

    // -----------------------------------------------------------------------
    // FetchStats tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_stats_cache_hit_rate() {
        let mut stats = FetchStats::default();
        // Zero requests → 0.0
        assert_eq!(stats.cache_hit_rate(), 0.0);

        stats.cache_hits = 3;
        stats.cache_misses = 1;
        assert!((stats.cache_hit_rate() - 0.75).abs() < f64::EPSILON);

        stats.cache_hits = 0;
        stats.cache_misses = 5;
        assert_eq!(stats.cache_hit_rate(), 0.0);
    }

    #[test]
    fn test_fetch_stats_throughput() {
        let mut stats = FetchStats::default();
        // Zero duration → 0.0
        assert_eq!(stats.throughput_mbps(), 0.0);
        assert_eq!(stats.avg_request_duration_ms(), 0.0);

        stats.total_bytes_fetched = 10 * 1024 * 1024; // 10 MiB
        stats.total_duration_ms = 1000; // 1 second
        stats.total_requests = 5;

        // 10 MiB / 1s = 10 MB/s
        assert!((stats.throughput_mbps() - 10.0).abs() < f64::EPSILON);
        // 1000ms / 5 requests = 200ms average
        assert!((stats.avg_request_duration_ms() - 200.0).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // CloudStorageConfig tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cloud_storage_config_creation() {
        let config = CloudStorageConfig {
            provider: CloudProvider::S3,
            region: Some("us-east-1".to_string()),
            endpoint: None,
            credentials: CloudCredentials::AccessKey {
                key_id: "AKID".to_string(),
                secret: "SECRET".to_string(),
            },
            fetch_config: ParallelFetchConfig::default(),
        };

        assert_eq!(config.provider, CloudProvider::S3);
        assert_eq!(config.region, Some("us-east-1".to_string()));
        assert!(config.endpoint.is_none());
        assert_eq!(config.fetch_config.max_concurrent_fetches, 8);

        // Test other providers.
        let gcs = CloudStorageConfig {
            provider: CloudProvider::Gcs,
            region: None,
            endpoint: Some("https://storage.googleapis.com".to_string()),
            credentials: CloudCredentials::ServiceAccount {
                path: "/path/to/sa.json".to_string(),
            },
            fetch_config: ParallelFetchConfig::default(),
        };
        assert_eq!(gcs.provider, CloudProvider::Gcs);

        let local = CloudStorageConfig {
            provider: CloudProvider::Local,
            region: None,
            endpoint: None,
            credentials: CloudCredentials::Anonymous,
            fetch_config: ParallelFetchConfig::default(),
        };
        assert_eq!(local.provider, CloudProvider::Local);

        let env = CloudStorageConfig {
            provider: CloudProvider::Azure,
            region: Some("westus2".to_string()),
            endpoint: None,
            credentials: CloudCredentials::Environment,
            fetch_config: ParallelFetchConfig::default(),
        };
        assert_eq!(env.provider, CloudProvider::Azure);
    }

    // -----------------------------------------------------------------------
    // S3/GCS/Azure provider tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_s3_object_url() {
        let creds = S3Credentials {
            access_key_id: "AKID".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        };
        let store = S3ObjectStore::new("my-bucket", "us-east-1", creds);
        let url = store.object_url("data/file.parquet");
        assert!(url.contains("my-bucket"));
        assert!(url.contains("us-east-1"));
        assert!(url.contains("data/file.parquet"));
    }

    #[test]
    fn test_s3_custom_endpoint() {
        let creds = S3Credentials {
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        };
        let store =
            S3ObjectStore::new("bucket", "us-east-1", creds).with_endpoint("http://localhost:9000");
        let url = store.object_url("key.parquet");
        assert!(url.starts_with("http://localhost:9000"));
    }

    #[test]
    fn test_gcs_object_url() {
        let store = GcsObjectStore::new("my-bucket", "my-project");
        let url = store.object_url("data/file.parquet");
        assert!(url.contains("storage.googleapis.com"));
        assert!(url.contains("my-bucket"));
    }

    #[test]
    fn test_azure_blob_url() {
        let store = AzureBlobStore::new("myaccount", "mycontainer");
        let url = store.blob_url("file.parquet");
        assert!(url.contains("myaccount"));
        assert!(url.contains("mycontainer"));
        assert!(url.contains("blob.core.windows.net"));
    }

    // -----------------------------------------------------------------------
    // Parallel fetch tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parallel_row_group_fetcher() {
        let fetcher = ParallelRowGroupFetcher::new(4, 1024);
        let row_groups = vec![
            RowGroupMeta {
                index: 0,
                offset: 0,
                size_bytes: 3000,
                num_rows: 100,
            },
            RowGroupMeta {
                index: 1,
                offset: 3000,
                size_bytes: 500,
                num_rows: 50,
            },
        ];
        let plans = fetcher.plan_fetches(&row_groups);
        assert_eq!(plans.len(), 4); // 3 chunks for first RG + 1 for second
        assert_eq!(plans[0].row_group_index, 0);
    }

    #[test]
    fn test_column_projection_fetcher() {
        let fetcher = ColumnProjectionFetcher::new(vec![0, 2, 4]);
        let columns = vec!["a", "b", "c", "d", "e"];
        let projected = fetcher.filter_columns(&columns);
        assert_eq!(projected.len(), 3);
        assert_eq!(*projected[0], "a");
        assert_eq!(*projected[1], "c");
        assert_eq!(*projected[2], "e");
    }

    // -----------------------------------------------------------------------
    // Partition pruning tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_pruner_eq() {
        let pruner = PartitionPruner::new(vec!["date".to_string()]);
        let partitions = vec![
            PartitionValue {
                column: "date".to_string(),
                value: "2024-01".to_string(),
            },
            PartitionValue {
                column: "date".to_string(),
                value: "2024-02".to_string(),
            },
            PartitionValue {
                column: "date".to_string(),
                value: "2024-03".to_string(),
            },
        ];
        let predicates = vec![PartitionPredicate::Eq(
            "date".to_string(),
            "2024-02".to_string(),
        )];
        let result = pruner.prune(&partitions, &predicates);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_partition_pruner_in() {
        let pruner = PartitionPruner::new(vec!["region".to_string()]);
        let partitions = vec![
            PartitionValue {
                column: "region".to_string(),
                value: "us".to_string(),
            },
            PartitionValue {
                column: "region".to_string(),
                value: "eu".to_string(),
            },
            PartitionValue {
                column: "region".to_string(),
                value: "ap".to_string(),
            },
        ];
        let predicates = vec![PartitionPredicate::In(
            "region".to_string(),
            vec!["us".to_string(), "eu".to_string()],
        )];
        let result = pruner.prune(&partitions, &predicates);
        assert_eq!(result, vec![0, 1]);
    }

    // -----------------------------------------------------------------------
    // Metadata cache tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_metadata_cache_put_get() {
        let mut cache = MetadataCache::new(60, 100);
        cache.put(
            "file1.parquet".to_string(),
            CachedMetadata {
                file_path: "file1.parquet".to_string(),
                size_bytes: 1024,
                num_row_groups: 2,
                num_rows: 1000,
                schema_fingerprint: "abc123".to_string(),
            },
        );
        assert!(cache.get("file1.parquet").is_some());
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_metadata_cache_invalidate() {
        let mut cache = MetadataCache::new(60, 100);
        cache.put(
            "file1.parquet".to_string(),
            CachedMetadata {
                file_path: "file1.parquet".to_string(),
                size_bytes: 1024,
                num_row_groups: 1,
                num_rows: 500,
                schema_fingerprint: "def".to_string(),
            },
        );
        cache.invalidate("file1.parquet");
        assert!(cache.get("file1.parquet").is_none());
    }

    // -----------------------------------------------------------------------
    // Multi-region tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_region_config() {
        let config = MultiRegionConfig::new("us-east-1")
            .add_failover("us-west-2")
            .add_failover("eu-west-1");
        let regions = config.regions_in_order();
        assert_eq!(regions.len(), 3);
        assert_eq!(regions[0], "us-east-1");
        assert_eq!(regions[1], "us-west-2");
    }

    // -----------------------------------------------------------------------
    // PrefetchManager tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_prefetch_schedule_and_stats() {
        let pm = PrefetchManager::new(4, 100);
        pm.schedule_prefetch(&[
            "s3://bucket/a.parquet".into(),
            "s3://bucket/b.parquet".into(),
        ]);
        let s = pm.stats();
        assert_eq!(s.scheduled, 2);
        assert_eq!(s.completed, 0);
    }

    #[test]
    fn test_prefetch_get_miss_then_hit() {
        let pm = PrefetchManager::new(4, 100);
        assert!(pm.get_prefetched("s3://bucket/a.parquet").is_none());
        pm.complete_prefetch("s3://bucket/a.parquet", vec![1, 2, 3]);
        assert_eq!(
            pm.get_prefetched("s3://bucket/a.parquet"),
            Some(vec![1, 2, 3])
        );
        let s = pm.stats();
        assert_eq!(s.cache_misses, 1);
        assert_eq!(s.cache_hits, 1);
        assert_eq!(s.completed, 1);
    }

    #[test]
    fn test_prefetch_cancel_all() {
        let pm = PrefetchManager::new(4, 100);
        pm.schedule_prefetch(&["a".into(), "b".into(), "c".into()]);
        pm.cancel_all();
        // Queue is empty; stats retain history.
        assert_eq!(pm.stats().scheduled, 3);
    }

    // -----------------------------------------------------------------------
    // PartitionCache tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_cache_put_get() {
        let cache = PartitionCache::new(10);
        let info = PartitionInfo {
            path: "s3://b/year=2024/part0.parquet".into(),
            row_count: 1000,
            size_bytes: 4096,
            partition_values: HashMap::from([("year".into(), "2024".into())]),
        };
        cache.put("events", vec![info.clone()]);
        let got = cache.get("events").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], info);
    }

    #[test]
    fn test_partition_cache_invalidate() {
        let cache = PartitionCache::new(10);
        cache.put("t1", vec![]);
        cache.put("t2", vec![]);
        cache.invalidate("t1");
        assert!(cache.get("t1").is_none());
        assert!(cache.get("t2").is_some());
    }

    #[test]
    fn test_partition_cache_invalidate_all() {
        let cache = PartitionCache::new(10);
        cache.put("t1", vec![]);
        cache.put("t2", vec![]);
        cache.invalidate_all();
        assert!(cache.get("t1").is_none());
        assert!(cache.get("t2").is_none());
    }

    // -----------------------------------------------------------------------
    // CredentialChain tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_credential_chain_static() {
        let cred = CredentialChain::new()
            .with_static("AKID", "SECRET")
            .resolve()
            .unwrap();
        assert_eq!(cred.access_key, "AKID");
        assert_eq!(cred.secret_key, "SECRET");
        assert!(cred.token.is_none());
    }

    #[test]
    fn test_credential_chain_anonymous_fallback() {
        let cred = CredentialChain::new().with_anonymous().resolve().unwrap();
        assert!(cred.access_key.is_empty());
    }

    #[test]
    fn test_credential_chain_empty_fails() {
        let res = CredentialChain::new().resolve();
        assert!(res.is_err());
    }

    #[test]
    fn test_credential_chain_order() {
        let chain = CredentialChain::new()
            .with_environment()
            .with_static("K", "S")
            .with_iam_role("arn:aws:iam::123:role/r");
        assert_eq!(chain.providers().len(), 3);
        assert_eq!(chain.providers()[0], CredentialProvider::Environment);
    }

    // -----------------------------------------------------------------------
    // CloudRouter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cloud_router_route_default() {
        let router = CloudRouter::new().add_region(
            "us-east-1",
            CloudProvider::S3,
            "https://s3.us-east-1.amazonaws.com",
        );
        let ep = router.route_query("any_table").unwrap();
        assert_eq!(ep.region, "us-east-1");
        assert_eq!(ep.cloud, CloudProvider::S3);
    }

    #[test]
    fn test_cloud_router_route_bound_table() {
        let mut router = CloudRouter::new()
            .add_region(
                "us-east-1",
                CloudProvider::S3,
                "https://s3.us-east-1.amazonaws.com",
            )
            .add_region(
                "eu-west-1",
                CloudProvider::S3,
                "https://s3.eu-west-1.amazonaws.com",
            );
        router.bind_table("eu_events", "eu-west-1");
        let ep = router.route_query("eu_events").unwrap();
        assert_eq!(ep.region, "eu-west-1");
    }

    #[test]
    fn test_cloud_router_no_regions_error() {
        let router = CloudRouter::new();
        assert!(router.route_query("t").is_err());
    }

    #[test]
    fn test_cloud_router_transfer_cost_same_region() {
        let router = CloudRouter::new().add_region(
            "us-east-1",
            CloudProvider::S3,
            "https://s3.amazonaws.com",
        );
        let cost = router.estimate_transfer_cost("us-east-1", "us-east-1", 1_073_741_824);
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn test_cloud_router_transfer_cost_cross_cloud() {
        let router = CloudRouter::new()
            .add_region("us-east-1", CloudProvider::S3, "https://s3.amazonaws.com")
            .add_region(
                "us-central1",
                CloudProvider::Gcs,
                "https://storage.googleapis.com",
            );
        let cost = router.estimate_transfer_cost("us-east-1", "us-central1", 1_073_741_824);
        // 1 GB cross-cloud at $0.09/GB
        assert!((cost - 0.09).abs() < 1e-6);
    }

    #[test]
    fn test_partition_schema_inference() {
        let partitions = vec![
            DiscoveredPartition {
                path: ObjectPath::parse("s3://bucket/year=2024/month=01/data.parquet").unwrap(),
                partition_values: HashMap::from([
                    ("year".to_string(), "2024".to_string()),
                    ("month".to_string(), "01".to_string()),
                ]),
                size_bytes: 1024,
            },
            DiscoveredPartition {
                path: ObjectPath::parse("s3://bucket/year=2024/month=02/data.parquet").unwrap(),
                partition_values: HashMap::from([
                    ("year".to_string(), "2024".to_string()),
                    ("month".to_string(), "02".to_string()),
                ]),
                size_bytes: 2048,
            },
            DiscoveredPartition {
                path: ObjectPath::parse("s3://bucket/year=2023/month=12/data.parquet").unwrap(),
                partition_values: HashMap::from([
                    ("year".to_string(), "2023".to_string()),
                    ("month".to_string(), "12".to_string()),
                ]),
                size_bytes: 512,
            },
        ];

        let schema = PartitionSchema::infer_from_partitions(&partitions);
        assert_eq!(schema.columns.len(), 2);

        let year_col = schema.columns.iter().find(|c| c.name == "year").unwrap();
        assert_eq!(year_col.inferred_type, PartitionDataType::Integer);
        assert!(year_col.distinct_values.contains(&"2023".to_string()));
        assert!(year_col.distinct_values.contains(&"2024".to_string()));

        let month_col = schema.columns.iter().find(|c| c.name == "month").unwrap();
        assert_eq!(month_col.inferred_type, PartitionDataType::Integer);
    }

    #[test]
    fn test_byte_range_computation() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("range_test.txt");
        fs::write(&file_path, "a]".repeat(50)).unwrap(); // 100 bytes

        let store: Arc<dyn ObjectStoreProvider> = Arc::new(LocalFileSystemStore);
        let path = ObjectPath {
            scheme: "file".into(),
            bucket: String::new(),
            key: file_path.to_string_lossy().to_string(),
        };

        let reader = RangeReader::new(store, path, 30).unwrap();
        assert_eq!(reader.file_size(), 100);

        let ranges = reader.compute_ranges();
        assert_eq!(ranges.len(), 4); // 30 + 30 + 30 + 10
        assert_eq!(ranges[0].offset, 0);
        assert_eq!(ranges[0].length, 30);
        assert_eq!(ranges[3].offset, 90);
        assert_eq!(ranges[3].length, 10);
    }

    #[test]
    fn test_credential_refresh_manager() {
        let chain = CredentialChain::new().with_static("test-key", "test-secret");
        let manager = CredentialRefreshManager::new(chain);

        let cred = manager.get_credential().unwrap();
        assert_eq!(cred.access_key, "test-key");
        assert_eq!(cred.secret_key, "test-secret");

        // Should return cached credential
        let cred2 = manager.get_credential().unwrap();
        assert_eq!(cred2.access_key, "test-key");

        // Force refresh should also work
        let cred3 = manager.force_refresh().unwrap();
        assert_eq!(cred3.access_key, "test-key");
    }

    #[test]
    fn test_cloud_storage_table_factory() {
        let dir = TempDir::new().unwrap();
        let parquet_path = write_test_parquet(dir.path());

        let registry = Arc::new(ObjectStoreRegistry::new());
        let factory = CloudStorageTableFactory::new(registry);

        let uri = format!("file://{}", parquet_path.to_string_lossy());
        let table = factory.create_table(&uri).unwrap();

        let batches = table.scan(None, &[], None).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_partitioned_table_pruning() {
        let partitions = vec![
            DiscoveredPartition {
                path: ObjectPath::parse("s3://b/year=2024/data.parquet").unwrap(),
                partition_values: HashMap::from([("year".to_string(), "2024".to_string())]),
                size_bytes: 100,
            },
            DiscoveredPartition {
                path: ObjectPath::parse("s3://b/year=2023/data.parquet").unwrap(),
                partition_values: HashMap::from([("year".to_string(), "2023".to_string())]),
                size_bytes: 200,
            },
        ];

        let table = PartitionedObjectStoreTable {
            base_path: ObjectPath::parse("s3://b/").unwrap(),
            store: Arc::new(LocalFileSystemStore),
            partitions,
            partition_schema: PartitionSchema { columns: vec![] },
            data_schema: None,
        };

        let pruned =
            table.prune_partitions(&[PartitionPredicate::Eq("year".into(), "2024".into())]);
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].partition_values.get("year").unwrap(), "2024");

        assert_eq!(table.total_size_bytes(), 300);
        assert_eq!(table.num_partitions(), 2);
    }
}
