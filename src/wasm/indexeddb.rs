//! IndexedDB persistence layer for browser-based Blaze.
//!
//! Provides persistent storage using the browser's IndexedDB API.
//! In non-browser environments, this module provides a simulated in-memory
//! implementation that mirrors the IndexedDB interface.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{BlazeError, Result};

/// IndexedDB store configuration.
#[derive(Debug, Clone)]
pub struct IndexedDbConfig {
    /// Name of the IndexedDB database.
    pub database_name: String,
    /// Name of the object store within the database.
    pub store_name: String,
    /// Schema version for the database.
    pub version: u32,
    /// Maximum total size in bytes.
    pub max_size_bytes: usize,
}

impl Default for IndexedDbConfig {
    fn default() -> Self {
        Self {
            database_name: "blaze_db".to_string(),
            store_name: "blaze_store".to_string(),
            version: 1,
            max_size_bytes: 128 * 1024 * 1024, // 128 MB
        }
    }
}

/// Metadata for a stored item.
#[derive(Debug, Clone)]
pub struct ItemMetadata {
    /// Size in bytes.
    pub size: usize,
    /// Timestamp when the item was stored (epoch seconds).
    pub stored_at: u64,
    /// MIME or logical content type.
    pub content_type: String,
    /// Optional table name association.
    pub table_name: Option<String>,
}

/// Internal stored item representation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct StoredItem {
    key: String,
    value: Vec<u8>,
    metadata: ItemMetadata,
}

/// Simulated IndexedDB store for non-browser environments.
/// In actual WASM builds, this would use `web_sys` to access real IndexedDB.
pub struct IndexedDbStore {
    config: IndexedDbConfig,
    data: RwLock<HashMap<String, StoredItem>>,
    total_bytes: AtomicUsize,
}

impl IndexedDbStore {
    /// Create a new IndexedDB store with the given configuration.
    pub fn new(config: IndexedDbConfig) -> Self {
        Self {
            config,
            data: RwLock::new(HashMap::new()),
            total_bytes: AtomicUsize::new(0),
        }
    }

    /// Store a value with the given key and metadata.
    pub fn put(&self, key: &str, value: Vec<u8>, metadata: ItemMetadata) -> Result<()> {
        let value_len = value.len();

        let existing_size = {
            let data = self.data.read();
            data.get(key).map(|item| item.value.len()).unwrap_or(0)
        };

        let new_total = self.total_bytes.load(Ordering::Relaxed) - existing_size + value_len;
        if new_total > self.config.max_size_bytes {
            return Err(BlazeError::resource_exhausted(format!(
                "IndexedDB storage limit exceeded: {} bytes required, {} bytes max",
                new_total, self.config.max_size_bytes
            )));
        }

        let item = StoredItem {
            key: key.to_string(),
            value,
            metadata,
        };

        let mut data = self.data.write();
        data.insert(key.to_string(), item);
        self.total_bytes.store(new_total, Ordering::Relaxed);
        Ok(())
    }

    /// Retrieve a value by key.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key).map(|item| item.value.clone()))
    }

    /// Delete a value by key.
    pub fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.write();
        if let Some(item) = data.remove(key) {
            self.total_bytes
                .fetch_sub(item.value.len(), Ordering::Relaxed);
        }
        Ok(())
    }

    /// List keys, optionally filtered by prefix.
    pub fn list_keys(&self, prefix: Option<&str>) -> Vec<String> {
        let data = self.data.read();
        match prefix {
            Some(p) => data.keys().filter(|k| k.starts_with(p)).cloned().collect(),
            None => data.keys().cloned().collect(),
        }
    }

    /// Get total bytes stored.
    pub fn total_size(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Clear all stored data.
    pub fn clear(&self) -> Result<()> {
        let mut data = self.data.write();
        data.clear();
        self.total_bytes.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.read().contains_key(key)
    }

    /// Get the number of stored items.
    pub fn count(&self) -> usize {
        self.data.read().len()
    }
}

/// Table persistence manager using IndexedDB.
///
/// Provides a higher-level API for persisting and loading table data,
/// building on top of [`IndexedDbStore`].
pub struct TablePersistence {
    store: Arc<IndexedDbStore>,
}

impl TablePersistence {
    /// Create a new table persistence manager.
    pub fn new(store: Arc<IndexedDbStore>) -> Self {
        Self { store }
    }

    /// Save table data under the given table name.
    pub fn save_table(&self, table_name: &str, data: &[u8]) -> Result<()> {
        let key = format!("table:{}", table_name);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let metadata = ItemMetadata {
            size: data.len(),
            stored_at: now,
            content_type: "application/x-blaze-table".to_string(),
            table_name: Some(table_name.to_string()),
        };

        self.store.put(&key, data.to_vec(), metadata)
    }

    /// Load table data by name.
    pub fn load_table(&self, table_name: &str) -> Result<Option<Vec<u8>>> {
        let key = format!("table:{}", table_name);
        self.store.get(&key)
    }

    /// Delete table data by name.
    pub fn delete_table(&self, table_name: &str) -> Result<()> {
        let key = format!("table:{}", table_name);
        self.store.delete(&key)
    }

    /// List all persisted table names.
    pub fn list_tables(&self) -> Vec<String> {
        let keys = self.store.list_keys(Some("table:"));
        keys.into_iter()
            .filter_map(|k| k.strip_prefix("table:").map(|s| s.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> IndexedDbStore {
        IndexedDbStore::new(IndexedDbConfig::default())
    }

    #[test]
    fn test_put_and_get() {
        let store = test_store();
        let meta = ItemMetadata {
            size: 5,
            stored_at: 0,
            content_type: "text/csv".to_string(),
            table_name: None,
        };
        store.put("key1", vec![1, 2, 3, 4, 5], meta).unwrap();

        let val = store.get("key1").unwrap();
        assert_eq!(val, Some(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_delete_and_contains() {
        let store = test_store();
        let meta = ItemMetadata {
            size: 3,
            stored_at: 0,
            content_type: "text/csv".to_string(),
            table_name: None,
        };
        store.put("k", vec![1, 2, 3], meta).unwrap();
        assert!(store.contains_key("k"));

        store.delete("k").unwrap();
        assert!(!store.contains_key("k"));
        assert_eq!(store.get("k").unwrap(), None);
    }

    #[test]
    fn test_list_keys_and_count() {
        let store = test_store();
        let meta = || ItemMetadata {
            size: 1,
            stored_at: 0,
            content_type: "text/csv".to_string(),
            table_name: None,
        };
        store.put("table:a", vec![1], meta()).unwrap();
        store.put("table:b", vec![2], meta()).unwrap();
        store.put("other:c", vec![3], meta()).unwrap();

        assert_eq!(store.count(), 3);

        let mut table_keys = store.list_keys(Some("table:"));
        table_keys.sort();
        assert_eq!(table_keys, vec!["table:a", "table:b"]);

        let all_keys = store.list_keys(None);
        assert_eq!(all_keys.len(), 3);
    }

    #[test]
    fn test_storage_limit() {
        let config = IndexedDbConfig {
            max_size_bytes: 10,
            ..Default::default()
        };
        let store = IndexedDbStore::new(config);
        let meta = ItemMetadata {
            size: 8,
            stored_at: 0,
            content_type: "bin".to_string(),
            table_name: None,
        };
        store.put("a", vec![0; 8], meta.clone()).unwrap();
        let result = store.put("b", vec![0; 8], meta);
        assert!(result.is_err());
    }

    #[test]
    fn test_clear() {
        let store = test_store();
        let meta = ItemMetadata {
            size: 3,
            stored_at: 0,
            content_type: "bin".to_string(),
            table_name: None,
        };
        store.put("x", vec![1, 2, 3], meta).unwrap();
        assert_eq!(store.total_size(), 3);

        store.clear().unwrap();
        assert_eq!(store.count(), 0);
        assert_eq!(store.total_size(), 0);
    }

    #[test]
    fn test_table_persistence() {
        let store = Arc::new(test_store());
        let tp = TablePersistence::new(store);

        tp.save_table("users", b"user_data_bytes").unwrap();
        tp.save_table("orders", b"order_data_bytes").unwrap();

        let data = tp.load_table("users").unwrap();
        assert_eq!(data, Some(b"user_data_bytes".to_vec()));

        let mut tables = tp.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["orders", "users"]);

        tp.delete_table("users").unwrap();
        assert_eq!(tp.load_table("users").unwrap(), None);
    }
}
