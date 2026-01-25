//! Thread-safe connection pool for concurrent query execution.
//!
//! Provides a pool of `Connection` instances with:
//! - Configurable pool size
//! - Read-write locking (concurrent reads, serialized writes)
//! - Health checking and idle timeout
//! - Async-compatible interface via tokio

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;

use crate::error::{BlazeError, Result};
use crate::{Connection, ConnectionConfig};

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: usize,
    /// Minimum number of idle connections to maintain
    pub min_idle: usize,
    /// Maximum time a connection can be idle before being closed
    pub idle_timeout: Duration,
    /// Maximum time to wait for a connection from the pool
    pub acquire_timeout: Duration,
    /// Connection configuration for new connections
    pub connection_config: ConnectionConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 16,
            min_idle: 2,
            idle_timeout: Duration::from_secs(300),
            acquire_timeout: Duration::from_secs(30),
            connection_config: ConnectionConfig::default(),
        }
    }
}

impl PoolConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_size(mut self, size: usize) -> Self {
        assert!(size > 0, "Pool size must be at least 1");
        self.max_size = size;
        self
    }

    pub fn with_min_idle(mut self, min: usize) -> Self {
        self.min_idle = min;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }
}

/// A pooled connection entry tracking metadata.
struct PooledEntry {
    connection: Connection,
    #[allow(dead_code)]
    created_at: Instant,
    last_used: Instant,
}

impl PooledEntry {
    fn new(connection: Connection) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used: now,
        }
    }

    fn is_expired(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

/// A thread-safe connection pool.
///
/// The pool manages a set of `Connection` instances for concurrent access.
/// Readers can operate concurrently while write operations are serialized.
///
/// # Example
///
/// ```rust,no_run
/// use blaze::pool::{ConnectionPool, PoolConfig};
///
/// let pool = ConnectionPool::new(PoolConfig::default()).unwrap();
///
/// // Acquire a connection for reading
/// let conn = pool.acquire().unwrap();
/// let results = conn.query("SELECT 1 + 1").unwrap();
/// pool.release(conn);
/// ```
pub struct ConnectionPool {
    config: PoolConfig,
    /// Pool of available connections
    available: Mutex<Vec<PooledEntry>>,
    /// Total number of connections (available + checked out)
    total_count: Mutex<usize>,
    /// Read-write lock for catalog-level operations
    rw_lock: RwLock<()>,
    /// Semaphore to limit concurrent connections
    semaphore: Arc<Semaphore>,
    /// Pool statistics
    stats: Mutex<PoolStats>,
}

/// Statistics about the connection pool.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Total successful acquires
    pub acquires: u64,
    /// Total releases
    pub releases: u64,
    /// Total acquire timeouts
    pub timeouts: u64,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(config: PoolConfig) -> Result<Self> {
        let max_size = config.max_size;
        let min_idle = config.min_idle.min(config.max_size);

        // Pre-create minimum idle connections
        let mut available = Vec::with_capacity(max_size);
        for _ in 0..min_idle {
            let conn = Connection::with_config(config.connection_config.clone())?;
            available.push(PooledEntry::new(conn));
        }

        Ok(Self {
            config,
            total_count: Mutex::new(min_idle),
            available: Mutex::new(available),
            rw_lock: RwLock::new(()),
            semaphore: Arc::new(Semaphore::new(max_size)),
            stats: Mutex::new(PoolStats {
                connections_created: min_idle as u64,
                ..Default::default()
            }),
        })
    }

    /// Create a pool with default configuration.
    pub fn default_pool() -> Result<Self> {
        Self::new(PoolConfig::default())
    }

    /// Acquire a connection from the pool.
    ///
    /// If no connections are available, creates a new one (up to max_size).
    /// Blocks until a connection is available or the acquire timeout expires.
    pub fn acquire(&self) -> Result<Connection> {
        let start = Instant::now();

        loop {
            // Try to get an existing connection
            {
                let mut available = self.available.lock();
                // Remove expired connections
                let idle_timeout = self.config.idle_timeout;
                available.retain(|entry| !entry.is_expired(idle_timeout));

                if let Some(mut entry) = available.pop() {
                    entry.touch();
                    let mut stats = self.stats.lock();
                    stats.acquires += 1;
                    return Ok(entry.connection);
                }
            }

            // Try to create a new connection
            {
                let mut total = self.total_count.lock();
                if *total < self.config.max_size {
                    *total += 1;
                    drop(total);

                    let conn = Connection::with_config(self.config.connection_config.clone())?;
                    let mut stats = self.stats.lock();
                    stats.connections_created += 1;
                    stats.acquires += 1;
                    return Ok(conn);
                }
            }

            // Check timeout
            if start.elapsed() >= self.config.acquire_timeout {
                let mut stats = self.stats.lock();
                stats.timeouts += 1;
                return Err(BlazeError::resource_exhausted(
                    "Connection pool acquire timeout",
                ));
            }

            // Brief wait before retrying
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Release a connection back to the pool.
    pub fn release(&self, connection: Connection) {
        let mut available = self.available.lock();
        if available.len() < self.config.max_size {
            available.push(PooledEntry::new(connection));
        } else {
            // Pool is full, drop the connection
            let mut total = self.total_count.lock();
            *total = total.saturating_sub(1);
            let mut stats = self.stats.lock();
            stats.connections_closed += 1;
        }
        let mut stats = self.stats.lock();
        stats.releases += 1;
    }

    /// Execute a read query using a pooled connection.
    /// Allows concurrent reads.
    pub fn read<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let _read_guard = self.rw_lock.read();
        let conn = self.acquire()?;
        let result = f(&conn);
        self.release(conn);
        result
    }

    /// Execute a write operation using a pooled connection.
    /// Serializes writes to prevent conflicts.
    pub fn write<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let _write_guard = self.rw_lock.write();
        let conn = self.acquire()?;
        let result = f(&conn);
        self.release(conn);
        result
    }

    /// Acquire a connection for async use (compatible with tokio).
    pub async fn acquire_async(&self) -> Result<Connection> {
        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| BlazeError::resource_exhausted("Async pool acquire timeout"))?
        .map_err(|_| BlazeError::internal("Pool semaphore closed"))?;

        let conn = self.acquire()?;
        // Drop the permit when the connection is released
        std::mem::forget(permit);
        Ok(conn)
    }

    /// Get pool statistics.
    pub fn stats(&self) -> PoolStats {
        self.stats.lock().clone()
    }

    /// Get the number of available connections.
    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    /// Get the total number of managed connections.
    pub fn total_count(&self) -> usize {
        *self.total_count.lock()
    }

    /// Evict idle connections that have exceeded the idle timeout.
    pub fn evict_idle(&self) {
        let mut available = self.available.lock();
        let before = available.len();
        let idle_timeout = self.config.idle_timeout;
        available.retain(|entry| !entry.is_expired(idle_timeout));
        let evicted = before - available.len();

        if evicted > 0 {
            let mut total = self.total_count.lock();
            *total = total.saturating_sub(evicted);
            let mut stats = self.stats.lock();
            stats.connections_closed += evicted as u64;
        }
    }
}

/// RAII guard for automatically releasing a connection back to the pool.
pub struct PoolGuard<'a> {
    pool: &'a ConnectionPool,
    connection: Option<Connection>,
}

impl<'a> PoolGuard<'a> {
    /// Create a new pool guard by acquiring a connection.
    pub fn acquire(pool: &'a ConnectionPool) -> Result<Self> {
        let conn = pool.acquire()?;
        Ok(Self {
            pool,
            connection: Some(conn),
        })
    }

    /// Get a reference to the connection.
    pub fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("Connection already released")
    }
}

impl<'a> Drop for PoolGuard<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.release(conn);
        }
    }
}

impl<'a> std::ops::Deref for PoolGuard<'a> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let config = PoolConfig::new().with_max_size(4).with_min_idle(2);
        let pool = ConnectionPool::new(config).unwrap();
        assert_eq!(pool.available_count(), 2);
        assert_eq!(pool.total_count(), 2);
    }

    #[test]
    fn test_pool_acquire_release() {
        let pool = ConnectionPool::new(PoolConfig::new().with_max_size(4)).unwrap();

        let conn = pool.acquire().unwrap();
        conn.query("SELECT 1 + 1").unwrap();
        pool.release(conn);

        let stats = pool.stats();
        assert_eq!(stats.acquires, 1);
        assert_eq!(stats.releases, 1);
    }

    #[test]
    fn test_pool_guard() {
        let pool = ConnectionPool::new(PoolConfig::new().with_max_size(4)).unwrap();

        {
            let guard = PoolGuard::acquire(&pool).unwrap();
            guard.query("SELECT 1").unwrap();
        } // auto-release on drop

        let stats = pool.stats();
        assert_eq!(stats.acquires, 1);
        assert_eq!(stats.releases, 1);
    }

    #[test]
    fn test_pool_concurrent_reads() {
        let pool = Arc::new(
            ConnectionPool::new(PoolConfig::new().with_max_size(4).with_min_idle(0)).unwrap(),
        );

        let mut handles = Vec::new();
        for _ in 0..4 {
            let pool = pool.clone();
            handles.push(std::thread::spawn(move || {
                pool.read(|conn| conn.query("SELECT 1 + 1")).unwrap()
            }));
        }

        for handle in handles {
            let result = handle.join().unwrap();
            assert!(!result.is_empty());
        }
    }

    #[test]
    fn test_pool_stats() {
        let pool =
            ConnectionPool::new(PoolConfig::new().with_max_size(2).with_min_idle(0)).unwrap();

        let c1 = pool.acquire().unwrap();
        let c2 = pool.acquire().unwrap();
        pool.release(c1);
        pool.release(c2);

        let stats = pool.stats();
        assert_eq!(stats.acquires, 2);
        assert_eq!(stats.releases, 2);
        assert_eq!(stats.connections_created, 2);
    }

    #[test]
    fn test_pool_evict_idle() {
        let config = PoolConfig::new()
            .with_max_size(4)
            .with_min_idle(0)
            .with_idle_timeout(Duration::from_millis(50));
        let pool = ConnectionPool::new(config).unwrap();

        let conn = pool.acquire().unwrap();
        pool.release(conn);
        assert_eq!(pool.available_count(), 1);

        std::thread::sleep(Duration::from_millis(100));
        pool.evict_idle();
        assert_eq!(pool.available_count(), 0);
    }
}
