//! Virtual Filesystem for browser-based Blaze.
//!
//! Provides a unified filesystem abstraction that can back onto:
//! - In-memory storage (default)
//! - IndexedDB persistence
//! - HTTP remote files
//! - File System Access API

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::error::{BlazeError, Result};

/// File entry in the virtual filesystem.
#[derive(Debug, Clone)]
pub struct VfsEntry {
    pub path: String,
    pub data: Vec<u8>,
    pub metadata: VfsMetadata,
}

/// Metadata for a virtual filesystem entry.
#[derive(Debug, Clone)]
pub struct VfsMetadata {
    pub size: usize,
    pub created_at: u64,
    pub modified_at: u64,
    pub content_type: VfsContentType,
    pub is_directory: bool,
}

/// Content type for virtual filesystem entries.
#[derive(Debug, Clone, PartialEq)]
pub enum VfsContentType {
    Csv,
    Parquet,
    Json,
    ArrowIpc,
    Unknown,
}

impl VfsContentType {
    /// Determine content type from a file extension.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_lowercase().as_str() {
            "csv" => VfsContentType::Csv,
            "parquet" | "pqt" => VfsContentType::Parquet,
            "json" | "jsonl" | "ndjson" => VfsContentType::Json,
            "arrow" | "ipc" => VfsContentType::ArrowIpc,
            _ => VfsContentType::Unknown,
        }
    }
}

/// Virtual filesystem for browser environments.
pub struct VirtualFileSystem {
    files: RwLock<HashMap<String, VfsEntry>>,
    total_bytes: AtomicUsize,
    max_bytes: usize,
}

impl VirtualFileSystem {
    /// Create a new virtual filesystem with the given maximum size in bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
            total_bytes: AtomicUsize::new(0),
            max_bytes,
        }
    }

    /// Write a file to the virtual filesystem.
    pub fn write_file(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let data_len = data.len();

        // Check if we're replacing an existing file
        let existing_size = {
            let files = self.files.read();
            files.get(path).map(|e| e.data.len()).unwrap_or(0)
        };

        let new_total = self.total_bytes.load(Ordering::Relaxed) - existing_size + data_len;
        if new_total > self.max_bytes {
            return Err(BlazeError::resource_exhausted(format!(
                "VFS storage limit exceeded: {} bytes required, {} bytes available",
                data_len,
                self.available_space() + existing_size
            )));
        }

        let ext = path.rsplit('.').next().unwrap_or("");
        let content_type = VfsContentType::from_extension(ext);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = VfsEntry {
            path: path.to_string(),
            data,
            metadata: VfsMetadata {
                size: data_len,
                created_at: now,
                modified_at: now,
                content_type,
                is_directory: false,
            },
        };

        let mut files = self.files.write();
        files.insert(path.to_string(), entry);
        self.total_bytes.store(new_total, Ordering::Relaxed);
        Ok(())
    }

    /// Read a file from the virtual filesystem.
    pub fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let files = self.files.read();
        files
            .get(path)
            .map(|e| e.data.clone())
            .ok_or_else(|| BlazeError::execution(format!("File not found in VFS: {}", path)))
    }

    /// Check if a file exists in the virtual filesystem.
    pub fn exists(&self, path: &str) -> bool {
        self.files.read().contains_key(path)
    }

    /// Delete a file from the virtual filesystem.
    pub fn delete_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.write();
        let entry = files
            .remove(path)
            .ok_or_else(|| BlazeError::execution(format!("File not found in VFS: {}", path)))?;
        self.total_bytes
            .fetch_sub(entry.data.len(), Ordering::Relaxed);
        Ok(())
    }

    /// List files matching the given prefix.
    pub fn list_files(&self, prefix: &str) -> Vec<String> {
        let files = self.files.read();
        files
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Get metadata for a file.
    pub fn file_metadata(&self, path: &str) -> Result<VfsMetadata> {
        let files = self.files.read();
        files
            .get(path)
            .map(|e| e.metadata.clone())
            .ok_or_else(|| BlazeError::execution(format!("File not found in VFS: {}", path)))
    }

    /// Get the total size of all files in the virtual filesystem.
    pub fn total_size(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get the available space in the virtual filesystem.
    pub fn available_space(&self) -> usize {
        let used = self.total_bytes.load(Ordering::Relaxed);
        self.max_bytes.saturating_sub(used)
    }

    /// Remove all files from the virtual filesystem.
    pub fn clear(&self) {
        let mut files = self.files.write();
        files.clear();
        self.total_bytes.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_type_from_extension() {
        assert_eq!(VfsContentType::from_extension("csv"), VfsContentType::Csv);
        assert_eq!(
            VfsContentType::from_extension("parquet"),
            VfsContentType::Parquet
        );
        assert_eq!(VfsContentType::from_extension("json"), VfsContentType::Json);
        assert_eq!(
            VfsContentType::from_extension("arrow"),
            VfsContentType::ArrowIpc
        );
        assert_eq!(
            VfsContentType::from_extension("txt"),
            VfsContentType::Unknown
        );
        assert_eq!(VfsContentType::from_extension("CSV"), VfsContentType::Csv);
    }

    #[test]
    fn test_write_and_read_file() {
        let vfs = VirtualFileSystem::new(1024 * 1024);
        let data = b"hello,world\n1,2\n".to_vec();
        vfs.write_file("/data/test.csv", data.clone()).unwrap();

        let read = vfs.read_file("/data/test.csv").unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_exists_and_delete() {
        let vfs = VirtualFileSystem::new(1024 * 1024);
        assert!(!vfs.exists("/foo.csv"));

        vfs.write_file("/foo.csv", vec![1, 2, 3]).unwrap();
        assert!(vfs.exists("/foo.csv"));

        vfs.delete_file("/foo.csv").unwrap();
        assert!(!vfs.exists("/foo.csv"));
    }

    #[test]
    fn test_storage_limit() {
        let vfs = VirtualFileSystem::new(10);
        vfs.write_file("/a.csv", vec![0; 8]).unwrap();
        let result = vfs.write_file("/b.csv", vec![0; 8]);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_files_with_prefix() {
        let vfs = VirtualFileSystem::new(1024 * 1024);
        vfs.write_file("/data/a.csv", vec![1]).unwrap();
        vfs.write_file("/data/b.csv", vec![2]).unwrap();
        vfs.write_file("/other/c.csv", vec![3]).unwrap();

        let mut listed = vfs.list_files("/data/");
        listed.sort();
        assert_eq!(listed, vec!["/data/a.csv", "/data/b.csv"]);
    }

    #[test]
    fn test_total_size_and_available_space() {
        let vfs = VirtualFileSystem::new(100);
        assert_eq!(vfs.total_size(), 0);
        assert_eq!(vfs.available_space(), 100);

        vfs.write_file("/a.bin", vec![0; 30]).unwrap();
        assert_eq!(vfs.total_size(), 30);
        assert_eq!(vfs.available_space(), 70);

        vfs.delete_file("/a.bin").unwrap();
        assert_eq!(vfs.total_size(), 0);
        assert_eq!(vfs.available_space(), 100);
    }

    #[test]
    fn test_file_metadata() {
        let vfs = VirtualFileSystem::new(1024 * 1024);
        vfs.write_file("/report.parquet", vec![0; 50]).unwrap();

        let meta = vfs.file_metadata("/report.parquet").unwrap();
        assert_eq!(meta.size, 50);
        assert_eq!(meta.content_type, VfsContentType::Parquet);
        assert!(!meta.is_directory);
    }

    #[test]
    fn test_clear() {
        let vfs = VirtualFileSystem::new(1024 * 1024);
        vfs.write_file("/a.csv", vec![1, 2, 3]).unwrap();
        vfs.write_file("/b.csv", vec![4, 5, 6]).unwrap();
        assert_eq!(vfs.total_size(), 6);

        vfs.clear();
        assert_eq!(vfs.total_size(), 0);
        assert!(!vfs.exists("/a.csv"));
    }

    #[test]
    fn test_overwrite_existing_file() {
        let vfs = VirtualFileSystem::new(100);
        vfs.write_file("/data.csv", vec![0; 50]).unwrap();
        assert_eq!(vfs.total_size(), 50);

        vfs.write_file("/data.csv", vec![0; 30]).unwrap();
        assert_eq!(vfs.total_size(), 30);
    }
}
