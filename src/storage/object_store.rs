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
                        .map(|t| chrono::DateTime::<chrono::Utc>::from(t)),
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
                .map(|t| chrono::DateTime::<chrono::Utc>::from(t)),
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
        let cursor = bytes::Bytes::from(data);
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
}
