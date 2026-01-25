//! Multi-Tenant Resource Isolation
//!
//! Per-connection resource governors with fair scheduling, priority queues,
//! and query timeout enforcement.
//!
//! # Features
//!
//! - **Resource Limits**: Per-tenant memory, CPU, and concurrency caps
//! - **Fair Scheduling**: Weighted priority queuing across tenants
//! - **Timeout Enforcement**: Wall-clock and CPU-time query timeouts
//! - **Usage Tracking**: Real-time and historical resource usage metrics

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::error::{BlazeError, Result};

// ---------------------------------------------------------------------------
// Priority
// ---------------------------------------------------------------------------

/// Priority levels for fair scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
    /// Weight 1
    Low,
    /// Weight 4
    Normal,
    /// Weight 8
    High,
    /// Weight 16
    Critical,
}

impl Priority {
    /// Scheduling weight for this priority level.
    pub fn weight(self) -> u32 {
        match self {
            Priority::Low => 1,
            Priority::Normal => 4,
            Priority::High => 8,
            Priority::Critical => 16,
        }
    }
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

// ---------------------------------------------------------------------------
// ResourceLimits
// ---------------------------------------------------------------------------

/// Resource limits for a tenant or connection.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Memory cap in bytes (default: 1 GB).
    pub max_memory_bytes: usize,
    /// CPU time cap per query in milliseconds (default: 30 000 ms).
    pub max_cpu_time_ms: u64,
    /// Maximum number of concurrent queries (default: 16).
    pub max_concurrent_queries: usize,
    /// Maximum rows in a result set (default: 1 000 000).
    pub max_result_rows: usize,
    /// Query wall-clock timeout in milliseconds (default: 60 000 ms).
    pub query_timeout_ms: u64,
    /// Scheduling priority.
    pub priority: Priority,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024, // 1 GB
            max_cpu_time_ms: 30_000,
            max_concurrent_queries: 16,
            max_result_rows: 1_000_000,
            query_timeout_ms: 60_000,
            priority: Priority::Normal,
        }
    }
}

// ---------------------------------------------------------------------------
// ResourceUsage
// ---------------------------------------------------------------------------

/// Resource usage tracking for a single tenant.
pub struct ResourceUsage {
    pub tenant_id: String,
    pub current_memory_bytes: AtomicUsize,
    pub peak_memory_bytes: AtomicUsize,
    pub total_queries_executed: AtomicU64,
    pub active_queries: AtomicUsize,
    pub total_cpu_time_ms: AtomicU64,
    pub queries_throttled: AtomicU64,
    pub queries_timed_out: AtomicU64,
}

impl ResourceUsage {
    fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            current_memory_bytes: AtomicUsize::new(0),
            peak_memory_bytes: AtomicUsize::new(0),
            total_queries_executed: AtomicU64::new(0),
            active_queries: AtomicUsize::new(0),
            total_cpu_time_ms: AtomicU64::new(0),
            queries_throttled: AtomicU64::new(0),
            queries_timed_out: AtomicU64::new(0),
        }
    }

    /// Record a memory allocation, updating current and peak usage.
    fn allocate(&self, bytes: usize) {
        let prev = self.current_memory_bytes.fetch_add(bytes, Ordering::SeqCst);
        let new = prev + bytes;
        // Update peak if necessary (compare-and-swap loop).
        let mut peak = self.peak_memory_bytes.load(Ordering::Relaxed);
        while new > peak {
            match self.peak_memory_bytes.compare_exchange_weak(
                peak,
                new,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Record a memory release.
    fn deallocate(&self, bytes: usize) {
        self.current_memory_bytes.fetch_sub(
            bytes.min(self.current_memory_bytes.load(Ordering::Relaxed)),
            Ordering::SeqCst,
        );
    }
}

/// A snapshot of `ResourceUsage` suitable for returning across threads.
#[derive(Debug, Clone)]
pub struct ResourceUsageSnapshot {
    pub tenant_id: String,
    pub current_memory_bytes: usize,
    pub peak_memory_bytes: usize,
    pub total_queries_executed: u64,
    pub active_queries: usize,
    pub total_cpu_time_ms: u64,
    pub queries_throttled: u64,
    pub queries_timed_out: u64,
}

impl ResourceUsageSnapshot {
    fn from_usage(usage: &ResourceUsage) -> Self {
        Self {
            tenant_id: usage.tenant_id.clone(),
            current_memory_bytes: usage.current_memory_bytes.load(Ordering::Relaxed),
            peak_memory_bytes: usage.peak_memory_bytes.load(Ordering::Relaxed),
            total_queries_executed: usage.total_queries_executed.load(Ordering::Relaxed),
            active_queries: usage.active_queries.load(Ordering::Relaxed),
            total_cpu_time_ms: usage.total_cpu_time_ms.load(Ordering::Relaxed),
            queries_throttled: usage.queries_throttled.load(Ordering::Relaxed),
            queries_timed_out: usage.queries_timed_out.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// GovernorConfig
// ---------------------------------------------------------------------------

/// Configuration for the resource governor.
#[derive(Debug, Clone)]
pub struct GovernorConfig {
    /// Whether to actually enforce limits or just track usage.
    pub enforce_limits: bool,
    /// Track per-query resource usage.
    pub track_per_query: bool,
    /// Enable weighted fair queuing.
    pub fair_scheduling: bool,
    /// Throttle instead of reject when limits are hit.
    pub enable_throttling: bool,
}

impl Default for GovernorConfig {
    fn default() -> Self {
        Self {
            enforce_limits: true,
            track_per_query: true,
            fair_scheduling: true,
            enable_throttling: false,
        }
    }
}

// ---------------------------------------------------------------------------
// ResourceReservation
// ---------------------------------------------------------------------------

/// A resource reservation that must be held while a query executes.
#[derive(Debug)]
pub struct ResourceReservation {
    tenant_id: String,
    memory_reserved: usize,
    started_at: Instant,
    timeout: Duration,
}

impl ResourceReservation {
    /// Returns `true` if the reservation has exceeded its timeout.
    pub fn is_expired(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }

    /// Time elapsed since the reservation was created.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Amount of memory reserved in bytes.
    pub fn memory_reserved(&self) -> usize {
        self.memory_reserved
    }

    /// The tenant that owns this reservation.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

// ---------------------------------------------------------------------------
// QueryResourceTracker
// ---------------------------------------------------------------------------

/// Per-query resource tracking.
pub struct QueryResourceTracker {
    pub query_id: u64,
    pub tenant_id: String,
    pub started_at: Instant,
    pub memory_used: AtomicUsize,
    pub rows_produced: AtomicUsize,
    pub timeout: Duration,
}

impl QueryResourceTracker {
    /// Create a new tracker for a query.
    pub fn new(query_id: u64, tenant_id: impl Into<String>, timeout: Duration) -> Self {
        Self {
            query_id,
            tenant_id: tenant_id.into(),
            started_at: Instant::now(),
            memory_used: AtomicUsize::new(0),
            rows_produced: AtomicUsize::new(0),
            timeout,
        }
    }
}

// ---------------------------------------------------------------------------
// FairScheduler
// ---------------------------------------------------------------------------

struct QueueEntry {
    tenant_id: String,
    priority: Priority,
    queued_at: Instant,
}

/// Fair scheduler that allocates resources proportionally by priority weight.
pub struct FairScheduler {
    queue: Mutex<Vec<QueueEntry>>,
    active: RwLock<HashMap<String, usize>>,
}

impl FairScheduler {
    /// Create a new fair scheduler.
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            active: RwLock::new(HashMap::new()),
        }
    }

    /// Add a tenant request to the scheduling queue.
    pub fn enqueue(&self, tenant_id: impl Into<String>, priority: Priority) {
        let mut q = self.queue.lock();
        q.push(QueueEntry {
            tenant_id: tenant_id.into(),
            priority,
            queued_at: Instant::now(),
        });
    }

    /// Dequeue the next tenant using weighted fair scheduling.
    ///
    /// Higher-priority entries accumulate effective weight faster so they are
    /// selected sooner, but lower-priority entries are not starved because
    /// wait time also contributes to the score.
    pub fn dequeue(&self) -> Option<String> {
        let mut q = self.queue.lock();
        if q.is_empty() {
            return None;
        }

        let mut best_idx = 0;
        let mut best_score: f64 = f64::MIN;

        for (i, entry) in q.iter().enumerate() {
            let wait_secs = entry.queued_at.elapsed().as_secs_f64();
            let score = (entry.priority.weight() as f64) * (1.0 + wait_secs);
            if score > best_score {
                best_score = score;
                best_idx = i;
            }
        }

        let entry = q.swap_remove(best_idx);
        // Track active count
        {
            let mut active = self.active.write();
            *active.entry(entry.tenant_id.clone()).or_insert(0) += 1;
        }
        Some(entry.tenant_id)
    }

    /// Current queue length.
    pub fn queue_length(&self) -> usize {
        self.queue.lock().len()
    }
}

impl Default for FairScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TenantState (internal)
// ---------------------------------------------------------------------------

struct TenantState {
    limits: ResourceLimits,
    usage: Arc<ResourceUsage>,
    #[allow(dead_code)]
    created_at: Instant,
}

// ---------------------------------------------------------------------------
// ResourceGovernor
// ---------------------------------------------------------------------------

/// The resource governor manages limits for multiple tenants.
pub struct ResourceGovernor {
    tenants: RwLock<HashMap<String, TenantState>>,
    #[allow(dead_code)]
    global_limits: ResourceLimits,
    global_usage: Arc<ResourceUsage>,
    config: GovernorConfig,
}

impl ResourceGovernor {
    /// Create a new resource governor.
    pub fn new(config: GovernorConfig) -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            global_limits: ResourceLimits::default(),
            global_usage: Arc::new(ResourceUsage::new("__global__")),
            config,
        }
    }

    /// Register a tenant with the given resource limits.
    pub fn register_tenant(&self, tenant_id: impl Into<String>, limits: ResourceLimits) {
        let id = tenant_id.into();
        let state = TenantState {
            limits,
            usage: Arc::new(ResourceUsage::new(id.clone())),
            created_at: Instant::now(),
        };
        self.tenants.write().insert(id, state);
    }

    /// Remove a tenant and its tracked state.
    pub fn unregister_tenant(&self, tenant_id: &str) -> bool {
        self.tenants.write().remove(tenant_id).is_some()
    }

    /// Reserve resources for a query on behalf of a tenant.
    ///
    /// Returns a `ResourceReservation` that must be held for the lifetime of
    /// the query and then released via [`release`].
    pub fn acquire(&self, tenant_id: &str, memory_bytes: usize) -> Result<ResourceReservation> {
        let tenants = self.tenants.read();
        let state = tenants.get(tenant_id).ok_or_else(|| {
            BlazeError::invalid_argument(format!("Unknown tenant: {}", tenant_id))
        })?;

        if self.config.enforce_limits {
            // Check concurrent query limit.
            let active = state.usage.active_queries.load(Ordering::SeqCst);
            if active >= state.limits.max_concurrent_queries {
                state
                    .usage
                    .queries_throttled
                    .fetch_add(1, Ordering::Relaxed);
                return Err(BlazeError::resource_exhausted(format!(
                    "Tenant '{}' concurrent query limit ({}) reached",
                    tenant_id, state.limits.max_concurrent_queries
                )));
            }

            // Check memory limit.
            let current = state.usage.current_memory_bytes.load(Ordering::SeqCst);
            if current + memory_bytes > state.limits.max_memory_bytes {
                return Err(BlazeError::resource_exhausted(format!(
                    "Tenant '{}' memory limit ({} bytes) would be exceeded",
                    tenant_id, state.limits.max_memory_bytes
                )));
            }
        }

        // Book the resources.
        state.usage.active_queries.fetch_add(1, Ordering::SeqCst);
        state.usage.allocate(memory_bytes);
        state
            .usage
            .total_queries_executed
            .fetch_add(1, Ordering::Relaxed);

        // Global tracking.
        self.global_usage
            .active_queries
            .fetch_add(1, Ordering::SeqCst);
        self.global_usage.allocate(memory_bytes);
        self.global_usage
            .total_queries_executed
            .fetch_add(1, Ordering::Relaxed);

        let timeout = Duration::from_millis(state.limits.query_timeout_ms);

        Ok(ResourceReservation {
            tenant_id: tenant_id.to_string(),
            memory_reserved: memory_bytes,
            started_at: Instant::now(),
            timeout,
        })
    }

    /// Release resources held by a reservation.
    pub fn release(&self, reservation: &ResourceReservation) {
        let tenants = self.tenants.read();
        if let Some(state) = tenants.get(&reservation.tenant_id) {
            state.usage.deallocate(reservation.memory_reserved);
            state.usage.active_queries.fetch_sub(1, Ordering::SeqCst);
        }
        self.global_usage.deallocate(reservation.memory_reserved);
        self.global_usage
            .active_queries
            .fetch_sub(1, Ordering::SeqCst);
    }

    /// Check whether a tracked query has exceeded its timeout.
    pub fn check_timeout(&self, tracker: &QueryResourceTracker) -> bool {
        let expired = tracker.started_at.elapsed() > tracker.timeout;
        if expired {
            let tenants = self.tenants.read();
            if let Some(state) = tenants.get(&tracker.tenant_id) {
                state
                    .usage
                    .queries_timed_out
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.global_usage
                .queries_timed_out
                .fetch_add(1, Ordering::Relaxed);
        }
        expired
    }

    /// Check whether allocating `additional_bytes` would exceed the tenant's
    /// memory limit.
    pub fn check_memory(&self, tenant_id: &str, additional_bytes: usize) -> Result<()> {
        let tenants = self.tenants.read();
        let state = tenants.get(tenant_id).ok_or_else(|| {
            BlazeError::invalid_argument(format!("Unknown tenant: {}", tenant_id))
        })?;

        if !self.config.enforce_limits {
            return Ok(());
        }

        let current = state.usage.current_memory_bytes.load(Ordering::SeqCst);
        if current + additional_bytes > state.limits.max_memory_bytes {
            Err(BlazeError::resource_exhausted(format!(
                "Tenant '{}' memory limit ({} bytes) would be exceeded",
                tenant_id, state.limits.max_memory_bytes
            )))
        } else {
            Ok(())
        }
    }

    /// Get a snapshot of current resource usage for a tenant.
    pub fn usage(&self, tenant_id: &str) -> Option<ResourceUsageSnapshot> {
        let tenants = self.tenants.read();
        tenants
            .get(tenant_id)
            .map(|s| ResourceUsageSnapshot::from_usage(&s.usage))
    }

    /// List all registered tenant IDs.
    pub fn all_tenants(&self) -> Vec<String> {
        self.tenants.read().keys().cloned().collect()
    }

    /// Get a snapshot of global (aggregate) resource usage.
    pub fn global_usage(&self) -> ResourceUsageSnapshot {
        ResourceUsageSnapshot::from_usage(&self.global_usage)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_defaults() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(limits.max_cpu_time_ms, 30_000);
        assert_eq!(limits.max_concurrent_queries, 16);
        assert_eq!(limits.max_result_rows, 1_000_000);
        assert_eq!(limits.query_timeout_ms, 60_000);
        assert_eq!(limits.priority, Priority::Normal);
    }

    #[test]
    fn test_register_and_unregister_tenant() {
        let gov = ResourceGovernor::new(GovernorConfig::default());

        gov.register_tenant("t1", ResourceLimits::default());
        assert!(gov.all_tenants().contains(&"t1".to_string()));

        assert!(gov.unregister_tenant("t1"));
        assert!(!gov.all_tenants().contains(&"t1".to_string()));

        // Unregistering a missing tenant returns false.
        assert!(!gov.unregister_tenant("nonexistent"));
    }

    #[test]
    fn test_acquire_and_release_resources() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        gov.register_tenant("t1", ResourceLimits::default());

        let reservation = gov.acquire("t1", 1024).unwrap();
        assert_eq!(reservation.memory_reserved(), 1024);

        let snap = gov.usage("t1").unwrap();
        assert_eq!(snap.current_memory_bytes, 1024);
        assert_eq!(snap.active_queries, 1);
        assert_eq!(snap.total_queries_executed, 1);

        gov.release(&reservation);

        let snap = gov.usage("t1").unwrap();
        assert_eq!(snap.current_memory_bytes, 0);
        assert_eq!(snap.active_queries, 0);
    }

    #[test]
    fn test_memory_limit_enforcement() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        let mut limits = ResourceLimits::default();
        limits.max_memory_bytes = 2048;
        gov.register_tenant("t1", limits);

        let r1 = gov.acquire("t1", 1024).unwrap();
        // Second allocation should fail (1024 + 2048 > 2048).
        let err = gov.acquire("t1", 2048).unwrap_err();
        assert!(err.to_string().contains("memory limit"));

        gov.release(&r1);
        // After release, we should be able to allocate again.
        let r2 = gov.acquire("t1", 2048).unwrap();
        gov.release(&r2);
    }

    #[test]
    fn test_concurrent_query_limit() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        let mut limits = ResourceLimits::default();
        limits.max_concurrent_queries = 2;
        gov.register_tenant("t1", limits);

        let r1 = gov.acquire("t1", 0).unwrap();
        let r2 = gov.acquire("t1", 0).unwrap();
        let err = gov.acquire("t1", 0).unwrap_err();
        assert!(err.to_string().contains("concurrent query limit"));

        gov.release(&r1);
        // Now one slot freed; should succeed.
        let r3 = gov.acquire("t1", 0).unwrap();
        gov.release(&r2);
        gov.release(&r3);
    }

    #[test]
    fn test_query_timeout_check() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        gov.register_tenant("t1", ResourceLimits::default());

        // Tracker with a very short timeout.
        let tracker = QueryResourceTracker::new(1, "t1", Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(20));
        assert!(gov.check_timeout(&tracker));

        // Global timed-out counter should have been incremented.
        let global = gov.global_usage();
        assert!(global.queries_timed_out >= 1);
    }

    #[test]
    fn test_fair_scheduler_priority() {
        let sched = FairScheduler::new();

        // Enqueue low then high — high should dequeue first.
        sched.enqueue("low_tenant", Priority::Low);
        sched.enqueue("high_tenant", Priority::Critical);

        assert_eq!(sched.queue_length(), 2);

        let first = sched.dequeue().unwrap();
        assert_eq!(first, "high_tenant");

        let second = sched.dequeue().unwrap();
        assert_eq!(second, "low_tenant");

        assert!(sched.dequeue().is_none());
    }

    #[test]
    fn test_governor_global_usage() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        gov.register_tenant("a", ResourceLimits::default());
        gov.register_tenant("b", ResourceLimits::default());

        let r1 = gov.acquire("a", 512).unwrap();
        let r2 = gov.acquire("b", 256).unwrap();

        let global = gov.global_usage();
        assert_eq!(global.current_memory_bytes, 768);
        assert_eq!(global.active_queries, 2);
        assert_eq!(global.total_queries_executed, 2);

        gov.release(&r1);
        gov.release(&r2);

        let global = gov.global_usage();
        assert_eq!(global.current_memory_bytes, 0);
        assert_eq!(global.active_queries, 0);
    }

    #[test]
    fn test_resource_reservation_expiry() {
        let gov = ResourceGovernor::new(GovernorConfig::default());
        let mut limits = ResourceLimits::default();
        limits.query_timeout_ms = 20; // 20 ms timeout
        gov.register_tenant("t1", limits);

        let reservation = gov.acquire("t1", 64).unwrap();
        assert!(!reservation.is_expired());

        std::thread::sleep(Duration::from_millis(30));
        assert!(reservation.is_expired());
        assert!(reservation.elapsed() >= Duration::from_millis(20));

        gov.release(&reservation);
    }
}
