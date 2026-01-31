//! Materialized views with incremental refresh.
//!
//! Provides materialized views that cache query results and can be
//! incrementally refreshed when source data changes.
//!
//! # Features
//!
//! - **Materialized Views**: Cache query results as in-memory tables
//! - **Change Tracking**: Track inserts on source tables for incremental refresh
//! - **Push Notifications**: Callback-based notification when views are refreshed
//! - **Manual/Auto Refresh**: Support both on-demand and automatic refresh

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parking_lot::{Mutex, RwLock};

use crate::catalog::CatalogList;
use crate::error::{BlazeError, Result};
use crate::executor::ExecutionContext;
use crate::planner::{Binder, Optimizer, PhysicalPlanner};
use crate::sql::parser::Parser;
use crate::storage::MemoryTable;
use crate::types::Schema;

/// Version counter for change tracking.
static GLOBAL_VERSION: AtomicU64 = AtomicU64::new(1);

fn next_version() -> u64 {
    GLOBAL_VERSION.fetch_add(1, Ordering::SeqCst)
}

/// Type of change recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

/// A recorded change with type information.
#[derive(Debug, Clone)]
pub struct Change {
    pub change_type: ChangeType,
    pub data: RecordBatch,
}

/// Tracks changes to a source table.
#[derive(Debug)]
pub struct ChangeTracker {
    /// Table name being tracked
    table_name: String,
    /// Current version of the table
    version: AtomicU64,
    /// Batches inserted since last checkpoint
    pending_changes: Mutex<Vec<RecordBatch>>,
    /// Typed changes since last checkpoint
    typed_changes: Mutex<Vec<Change>>,
    /// Version at last checkpoint
    checkpoint_version: AtomicU64,
    /// Total insert count for statistics
    total_inserts: AtomicU64,
    /// Total update count for statistics
    total_updates: AtomicU64,
    /// Total delete count for statistics
    total_deletes: AtomicU64,
}

impl ChangeTracker {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            version: AtomicU64::new(0),
            pending_changes: Mutex::new(Vec::new()),
            typed_changes: Mutex::new(Vec::new()),
            checkpoint_version: AtomicU64::new(0),
            total_inserts: AtomicU64::new(0),
            total_updates: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
        }
    }

    /// Record that new data was inserted.
    pub fn record_insert(&self, batches: &[RecordBatch]) {
        let mut pending = self.pending_changes.lock();
        pending.extend(batches.iter().cloned());
        let mut typed = self.typed_changes.lock();
        for batch in batches {
            typed.push(Change {
                change_type: ChangeType::Insert,
                data: batch.clone(),
            });
        }
        self.total_inserts
            .fetch_add(batches.len() as u64, Ordering::Relaxed);
        self.version.store(next_version(), Ordering::SeqCst);
    }

    /// Record that data was updated.
    pub fn record_update(&self, batches: &[RecordBatch]) {
        let mut typed = self.typed_changes.lock();
        for batch in batches {
            typed.push(Change {
                change_type: ChangeType::Update,
                data: batch.clone(),
            });
        }
        self.total_updates
            .fetch_add(batches.len() as u64, Ordering::Relaxed);
        self.version.store(next_version(), Ordering::SeqCst);
    }

    /// Record that data was deleted.
    pub fn record_delete(&self, batches: &[RecordBatch]) {
        let mut typed = self.typed_changes.lock();
        for batch in batches {
            typed.push(Change {
                change_type: ChangeType::Delete,
                data: batch.clone(),
            });
        }
        self.total_deletes
            .fetch_add(batches.len() as u64, Ordering::Relaxed);
        self.version.store(next_version(), Ordering::SeqCst);
    }

    /// Get the current version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Check if there are pending changes since last checkpoint.
    pub fn has_pending_changes(&self) -> bool {
        self.version.load(Ordering::SeqCst) != self.checkpoint_version.load(Ordering::SeqCst)
    }

    /// Take all pending insert changes and update checkpoint.
    pub fn take_pending_changes(&self) -> Vec<RecordBatch> {
        let changes = std::mem::take(&mut *self.pending_changes.lock());
        self.checkpoint_version
            .store(self.version.load(Ordering::SeqCst), Ordering::SeqCst);
        changes
    }

    /// Take all typed changes since last checkpoint.
    pub fn take_typed_changes(&self) -> Vec<Change> {
        let changes = std::mem::take(&mut *self.typed_changes.lock());
        changes
    }

    /// Check if only inserts occurred (safe for append-only incremental).
    pub fn is_insert_only(&self) -> bool {
        let typed = self.typed_changes.lock();
        typed.iter().all(|c| c.change_type == ChangeType::Insert)
    }

    /// Get change statistics.
    pub fn change_stats(&self) -> ChangeStats {
        ChangeStats {
            total_inserts: self.total_inserts.load(Ordering::Relaxed),
            total_updates: self.total_updates.load(Ordering::Relaxed),
            total_deletes: self.total_deletes.load(Ordering::Relaxed),
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

/// Statistics about changes tracked for a table.
#[derive(Debug, Clone)]
pub struct ChangeStats {
    pub total_inserts: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
}

impl ChangeStats {
    pub fn total_changes(&self) -> u64 {
        self.total_inserts + self.total_updates + self.total_deletes
    }

    pub fn has_mutations(&self) -> bool {
        self.total_updates > 0 || self.total_deletes > 0
    }
}

/// Dependency graph for cascading view refreshes.
#[derive(Debug, Default)]
pub struct ViewDependencyGraph {
    /// Map: view_name → set of views that depend on it
    dependents: HashMap<String, Vec<String>>,
    /// Map: view_name → set of source tables
    sources: HashMap<String, Vec<String>>,
}

impl ViewDependencyGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a view's dependencies.
    pub fn add_view(&mut self, view_name: &str, source_tables: &[String]) {
        self.sources
            .insert(view_name.to_string(), source_tables.to_vec());
        for src in source_tables {
            self.dependents
                .entry(src.clone())
                .or_default()
                .push(view_name.to_string());
        }
    }

    /// Remove a view from the graph.
    pub fn remove_view(&mut self, view_name: &str) {
        if let Some(sources) = self.sources.remove(view_name) {
            for src in &sources {
                if let Some(deps) = self.dependents.get_mut(src) {
                    deps.retain(|d| d != view_name);
                }
            }
        }
    }

    /// Get all views that should be refreshed when a table changes.
    /// Returns views in topological order (dependencies first).
    pub fn affected_views(&self, table_name: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        self.collect_affected(table_name, &mut result, &mut visited);
        result
    }

    fn collect_affected(
        &self,
        name: &str,
        result: &mut Vec<String>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if let Some(deps) = self.dependents.get(name) {
            for dep in deps {
                if visited.insert(dep.clone()) {
                    result.push(dep.clone());
                    // Recursively collect views that depend on this view
                    self.collect_affected(dep, result, visited);
                }
            }
        }
    }
}

/// Refresh strategy for materialized views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshStrategy {
    /// Full refresh: re-execute the entire query
    Full,
    /// Incremental: only process new data (when possible)
    Incremental,
}

/// A materialized view definition.
pub struct MaterializedView {
    /// View name
    name: String,
    /// SQL query that defines the view
    query: String,
    /// Source tables this view depends on
    source_tables: Vec<String>,
    /// Cached result data
    cached_data: RwLock<Vec<RecordBatch>>,
    /// Cached schema
    schema: RwLock<Option<Schema>>,
    /// Last refresh version per source table
    last_refresh_versions: Mutex<HashMap<String, u64>>,
    /// Refresh strategy
    refresh_strategy: RefreshStrategy,
    /// Refresh count
    refresh_count: AtomicU64,
}

impl MaterializedView {
    pub fn new(
        name: impl Into<String>,
        query: impl Into<String>,
        source_tables: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            source_tables,
            cached_data: RwLock::new(Vec::new()),
            schema: RwLock::new(None),
            last_refresh_versions: Mutex::new(HashMap::new()),
            refresh_strategy: RefreshStrategy::Full,
            refresh_count: AtomicU64::new(0),
        }
    }

    pub fn with_refresh_strategy(mut self, strategy: RefreshStrategy) -> Self {
        self.refresh_strategy = strategy;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn source_tables(&self) -> &[String] {
        &self.source_tables
    }

    pub fn refresh_count(&self) -> u64 {
        self.refresh_count.load(Ordering::SeqCst)
    }

    /// Get the cached data.
    pub fn data(&self) -> Vec<RecordBatch> {
        self.cached_data.read().clone()
    }

    /// Get the schema (available after first refresh).
    pub fn schema(&self) -> Option<Schema> {
        self.schema.read().clone()
    }

    /// Check if this view needs a refresh based on source table versions.
    pub fn needs_refresh(&self, trackers: &HashMap<String, Arc<ChangeTracker>>) -> bool {
        let versions = self.last_refresh_versions.lock();
        for table_name in &self.source_tables {
            if let Some(tracker) = trackers.get(table_name) {
                let current = tracker.version();
                let last = versions.get(table_name).copied().unwrap_or(0);
                if current != last {
                    return true;
                }
            }
        }
        false
    }

    /// Refresh the view by re-executing the query.
    fn refresh_full(
        &self,
        catalog_list: &Arc<CatalogList>,
        execution_context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>> {
        let statements = Parser::parse(&self.query)?;
        let statement = statements
            .into_iter()
            .next()
            .ok_or_else(|| BlazeError::analysis("Empty materialized view query"))?;

        let binder = Binder::new(catalog_list.clone());
        let logical_plan = binder.bind(statement)?;

        let optimizer = Optimizer::default();
        let optimized = optimizer.optimize(&logical_plan)?;

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&optimized)?;

        execution_context.execute(&physical_plan)
    }

    /// Update cached data and record the refresh.
    fn update_cache(&self, data: Vec<RecordBatch>, trackers: &HashMap<String, Arc<ChangeTracker>>) {
        // Update schema from first batch
        if let Some(batch) = data.first() {
            if let Ok(schema) = Schema::from_arrow(&batch.schema()) {
                *self.schema.write() = Some(schema);
            }
        }

        *self.cached_data.write() = data;
        self.refresh_count.fetch_add(1, Ordering::SeqCst);

        // Record current versions
        let mut versions = self.last_refresh_versions.lock();
        for table_name in &self.source_tables {
            if let Some(tracker) = trackers.get(table_name) {
                versions.insert(table_name.clone(), tracker.version());
            }
        }
    }
}

/// Notification callback type for view refresh events.
pub type RefreshCallback = Box<dyn Fn(&str, &[RecordBatch]) + Send + Sync>;

/// Manager for materialized views with change tracking.
pub struct MaterializedViewManager {
    /// Registered materialized views
    views: RwLock<HashMap<String, Arc<MaterializedView>>>,
    /// Change trackers for source tables
    trackers: RwLock<HashMap<String, Arc<ChangeTracker>>>,
    /// Catalog list for query execution
    catalog_list: Arc<CatalogList>,
    /// Execution context
    execution_context: ExecutionContext,
    /// Refresh callbacks
    callbacks: RwLock<Vec<RefreshCallback>>,
}

impl MaterializedViewManager {
    pub fn new(catalog_list: Arc<CatalogList>, execution_context: ExecutionContext) -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
            trackers: RwLock::new(HashMap::new()),
            catalog_list,
            execution_context,
            callbacks: RwLock::new(Vec::new()),
        }
    }

    /// Register a change tracker for a source table.
    pub fn track_table(&self, table_name: impl Into<String>) -> Arc<ChangeTracker> {
        let name = table_name.into();
        let mut trackers = self.trackers.write();
        let tracker = trackers
            .entry(name.clone())
            .or_insert_with(|| Arc::new(ChangeTracker::new(name)));
        tracker.clone()
    }

    /// Create a materialized view.
    pub fn create_view(
        &self,
        name: impl Into<String>,
        query: impl Into<String>,
        source_tables: Vec<String>,
    ) -> Result<()> {
        let name = name.into();
        let view = Arc::new(MaterializedView::new(
            name.clone(),
            query,
            source_tables.clone(),
        ));

        // Ensure trackers exist for all source tables
        for table in &source_tables {
            self.track_table(table.clone());
        }

        // Perform initial refresh
        let trackers = self.trackers.read();
        let data = view.refresh_full(&self.catalog_list, &self.execution_context)?;
        view.update_cache(data.clone(), &trackers);

        // Register as a table in the catalog
        if let Some(schema) = view.schema() {
            let table = MemoryTable::new(schema, data.clone());
            let default_catalog = self
                .catalog_list
                .catalog("default")
                .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
            let main_schema = default_catalog
                .schema("main")
                .ok_or_else(|| BlazeError::catalog("Main schema not found"))?;
            main_schema.register_table(&name, Arc::new(table))?;
        }

        self.views.write().insert(name.clone(), view);

        // Notify callbacks
        self.notify_callbacks(&name, &data);
        Ok(())
    }

    /// Drop a materialized view.
    pub fn drop_view(&self, name: &str) -> Result<()> {
        self.views.write().remove(name);
        // Also deregister the table
        let default_catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let main_schema = default_catalog
            .schema("main")
            .ok_or_else(|| BlazeError::catalog("Main schema not found"))?;
        let _ = main_schema.deregister_table(name);
        Ok(())
    }

    /// Refresh a specific materialized view.
    pub fn refresh_view(&self, name: &str) -> Result<Vec<RecordBatch>> {
        let views = self.views.read();
        let view = views
            .get(name)
            .ok_or_else(|| BlazeError::catalog(format!("Materialized view '{}' not found", name)))?
            .clone();
        drop(views);

        let trackers = self.trackers.read().clone();
        let data = view.refresh_full(&self.catalog_list, &self.execution_context)?;
        view.update_cache(data.clone(), &trackers);

        // Update the registered table
        let default_catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let main_schema = default_catalog
            .schema("main")
            .ok_or_else(|| BlazeError::catalog("Main schema not found"))?;

        if let Some(table) = main_schema.table(name) {
            if let Some(mem_table) = table.as_any().downcast_ref::<MemoryTable>() {
                mem_table.replace(data.clone());
            }
        }

        self.notify_callbacks(name, &data);
        Ok(data)
    }

    /// Refresh all views that have stale source data.
    pub fn refresh_stale_views(&self) -> Result<Vec<String>> {
        let views: Vec<(String, Arc<MaterializedView>)> = {
            let views = self.views.read();
            views.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let trackers = self.trackers.read().clone();

        let mut refreshed = Vec::new();
        for (name, view) in &views {
            if view.needs_refresh(&trackers) {
                self.refresh_view(name)?;
                refreshed.push(name.clone());
            }
        }
        Ok(refreshed)
    }

    /// Record that data was inserted into a source table.
    pub fn notify_insert(&self, table_name: &str, batches: &[RecordBatch]) {
        let trackers = self.trackers.read();
        if let Some(tracker) = trackers.get(table_name) {
            tracker.record_insert(batches);
        }
    }

    /// Register a callback for refresh notifications.
    pub fn on_refresh(&self, callback: RefreshCallback) {
        self.callbacks.write().push(callback);
    }

    /// List all materialized views.
    pub fn list_views(&self) -> Vec<String> {
        self.views.read().keys().cloned().collect()
    }

    /// Get a materialized view by name.
    pub fn get_view(&self, name: &str) -> Option<Arc<MaterializedView>> {
        self.views.read().get(name).cloned()
    }

    fn notify_callbacks(&self, name: &str, data: &[RecordBatch]) {
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb(name, data);
        }
    }
}

/// Refresh policy for materialized views.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshSchedule {
    /// Refresh only when explicitly requested.
    OnDemand,
    /// Refresh after every commit to source tables.
    OnCommit,
    /// Refresh at a fixed interval.
    Periodic { interval_secs: u64 },
    /// Refresh when staleness exceeds a threshold.
    WhenStale { max_staleness_secs: u64 },
}

/// Internal entry for staleness tracking.
struct StalenessEntry {
    last_refresh: std::time::Instant,
    changes_since_refresh: u64,
    refresh_count: u64,
    avg_refresh_duration: std::time::Duration,
}

/// Tracks staleness of materialized views.
pub struct StalenessTracker {
    entries: RwLock<HashMap<String, StalenessEntry>>,
}

impl Default for StalenessTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl StalenessTracker {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Record that a view was refreshed, updating timing statistics.
    pub fn record_refresh(&self, view_name: &str, duration: std::time::Duration) {
        let mut entries = self.entries.write();
        let entry = entries
            .entry(view_name.to_string())
            .or_insert_with(|| StalenessEntry {
                last_refresh: std::time::Instant::now(),
                changes_since_refresh: 0,
                refresh_count: 0,
                avg_refresh_duration: std::time::Duration::ZERO,
            });
        entry.refresh_count += 1;
        // Compute running average of refresh duration
        let n = entry.refresh_count;
        entry.avg_refresh_duration = entry
            .avg_refresh_duration
            .mul_f64((n - 1) as f64 / n as f64)
            + duration.mul_f64(1.0 / n as f64);
        entry.changes_since_refresh = 0;
        entry.last_refresh = std::time::Instant::now();
    }

    /// Record that a source table changed, incrementing the pending change count.
    pub fn record_source_change(&self, view_name: &str) {
        let mut entries = self.entries.write();
        let entry = entries
            .entry(view_name.to_string())
            .or_insert_with(|| StalenessEntry {
                last_refresh: std::time::Instant::now(),
                changes_since_refresh: 0,
                refresh_count: 0,
                avg_refresh_duration: std::time::Duration::ZERO,
            });
        entry.changes_since_refresh += 1;
    }

    /// Get the staleness in seconds since last refresh.
    pub fn staleness_secs(&self, view_name: &str) -> Option<f64> {
        let entries = self.entries.read();
        entries
            .get(view_name)
            .map(|e| e.last_refresh.elapsed().as_secs_f64())
    }

    /// Get the number of source changes since last refresh.
    pub fn changes_since_refresh(&self, view_name: &str) -> u64 {
        let entries = self.entries.read();
        entries
            .get(view_name)
            .map(|e| e.changes_since_refresh)
            .unwrap_or(0)
    }

    /// Determine if a view needs refresh based on its schedule policy.
    pub fn needs_refresh(&self, view_name: &str, schedule: &RefreshSchedule) -> bool {
        match schedule {
            RefreshSchedule::OnDemand => false,
            RefreshSchedule::OnCommit => self.changes_since_refresh(view_name) > 0,
            RefreshSchedule::Periodic { interval_secs } => self
                .staleness_secs(view_name)
                .map(|s| s >= *interval_secs as f64)
                .unwrap_or(true),
            RefreshSchedule::WhenStale { max_staleness_secs } => self
                .staleness_secs(view_name)
                .map(|s| s >= *max_staleness_secs as f64)
                .unwrap_or(true),
        }
    }
}

/// Incremental refresh engine for append-only workloads.
pub struct IncrementalRefreshEngine {
    staleness: Arc<StalenessTracker>,
}

impl Default for IncrementalRefreshEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl IncrementalRefreshEngine {
    pub fn new() -> Self {
        Self {
            staleness: Arc::new(StalenessTracker::new()),
        }
    }

    /// Compute an incremental delta from pending insert-only changes.
    ///
    /// Returns `Ok(Some(batches))` if the tracker has insert-only changes,
    /// `Ok(None)` if there are mutations (updates/deletes) requiring a full refresh,
    /// or an error if the tracker has no pending changes.
    pub fn compute_incremental_delta(
        &self,
        tracker: &ChangeTracker,
    ) -> Result<Option<Vec<RecordBatch>>> {
        if !tracker.has_pending_changes() {
            return Err(BlazeError::execution("No pending changes to compute delta"));
        }
        if !tracker.is_insert_only() {
            // Cannot do incremental refresh with updates/deletes
            return Ok(None);
        }
        let changes = tracker.take_pending_changes();
        Ok(Some(changes))
    }

    /// Append delta batches to existing materialized data.
    pub fn apply_delta(
        existing: &[RecordBatch],
        delta: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        if delta.is_empty() {
            return Ok(existing.to_vec());
        }
        // Validate schema compatibility
        if let (Some(existing_batch), Some(delta_batch)) = (existing.first(), delta.first()) {
            if existing_batch.schema() != delta_batch.schema() {
                return Err(BlazeError::execution(
                    "Schema mismatch between existing data and delta",
                ));
            }
        }
        let mut result = existing.to_vec();
        result.extend(delta.iter().cloned());
        Ok(result)
    }

    /// Access the staleness tracker.
    pub fn staleness_tracker(&self) -> &StalenessTracker {
        &self.staleness
    }
}

/// Cost estimator for deciding between incremental vs full refresh.
pub struct RefreshCostEstimator;

impl RefreshCostEstimator {
    /// Estimate cost of a full refresh based on total rows and join count.
    /// Cost grows linearly with rows and exponentially with joins.
    pub fn estimate_full_refresh_cost(total_rows: usize, num_joins: usize) -> f64 {
        let base = total_rows as f64;
        let join_factor = 2.0_f64.powi(num_joins as i32);
        base * join_factor
    }

    /// Estimate cost of an incremental refresh based on delta and existing rows.
    /// Incremental cost is proportional to delta size with a small scan overhead.
    pub fn estimate_incremental_cost(delta_rows: usize, existing_rows: usize) -> f64 {
        let delta = delta_rows as f64;
        let scan_overhead = (existing_rows as f64).log2().max(1.0);
        delta * scan_overhead
    }

    /// Determine whether incremental refresh is cheaper than full refresh.
    pub fn should_use_incremental(
        delta_rows: usize,
        existing_rows: usize,
        num_joins: usize,
    ) -> bool {
        let full = Self::estimate_full_refresh_cost(existing_rows + delta_rows, num_joins);
        let incr = Self::estimate_incremental_cost(delta_rows, existing_rows);
        incr < full
    }
}

// ---------------------------------------------------------------------------
// Aggregate delta computation
// ---------------------------------------------------------------------------

/// Represents a recognized aggregate operation for incremental maintenance.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaAggregateOp {
    /// COUNT(*) or COUNT(col) — maintain running count
    Count,
    /// SUM(col) — maintain running sum
    Sum,
    /// MIN(col) — maintain running minimum (insert-only safe)
    Min,
    /// MAX(col) — maintain running maximum (insert-only safe)
    Max,
}

/// Descriptor for a single aggregate column in a materialized view.
#[derive(Debug, Clone)]
pub struct AggregateColumnDescriptor {
    /// The aggregate operation
    pub op: DeltaAggregateOp,
    /// Index of this column in the output schema
    pub output_index: usize,
    /// Name of the aggregate column
    pub name: String,
}

/// Computes incremental deltas for aggregate-only views.
///
/// For insert-only workloads on views like `SELECT COUNT(*), SUM(amount) FROM t`,
/// this engine can update the existing materialized result without re-executing
/// the full query.
pub struct AggregateDeltaComputer;

impl AggregateDeltaComputer {
    /// Merge a new delta batch into the existing aggregate result for COUNT.
    ///
    /// Returns the new count = old_count + number of non-null rows in delta.
    pub fn delta_count(existing_count: i64, delta_batch: &RecordBatch) -> i64 {
        existing_count + delta_batch.num_rows() as i64
    }

    /// Merge a new delta batch into the existing aggregate result for SUM.
    ///
    /// `col_index` is the column in the delta batch to sum.
    pub fn delta_sum_i64(
        existing_sum: i64,
        delta_batch: &RecordBatch,
        col_index: usize,
    ) -> Result<i64> {
        use arrow::array::Int64Array;

        if col_index >= delta_batch.num_columns() {
            return Err(BlazeError::execution(format!(
                "Column index {} out of range for delta batch with {} columns",
                col_index,
                delta_batch.num_columns()
            )));
        }

        let arr = delta_batch
            .column(col_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                BlazeError::type_error("Expected Int64Array for SUM delta computation")
            })?;

        let delta_sum: i64 = arr.iter().flatten().sum();
        Ok(existing_sum + delta_sum)
    }

    /// Merge a new delta batch into the existing aggregate result for SUM (f64).
    pub fn delta_sum_f64(
        existing_sum: f64,
        delta_batch: &RecordBatch,
        col_index: usize,
    ) -> Result<f64> {
        use arrow::array::Float64Array;

        if col_index >= delta_batch.num_columns() {
            return Err(BlazeError::execution(format!(
                "Column index {} out of range for delta batch with {} columns",
                col_index,
                delta_batch.num_columns()
            )));
        }

        let arr = delta_batch
            .column(col_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                BlazeError::type_error("Expected Float64Array for SUM delta computation")
            })?;

        let delta_sum: f64 = arr.iter().flatten().sum();
        Ok(existing_sum + delta_sum)
    }

    /// Merge a new delta batch into the existing MIN (i64).
    /// Only valid for insert-only workloads (deletes can invalidate a MIN).
    pub fn delta_min_i64(
        existing_min: Option<i64>,
        delta_batch: &RecordBatch,
        col_index: usize,
    ) -> Result<Option<i64>> {
        use arrow::array::Int64Array;

        let arr = delta_batch
            .column(col_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                BlazeError::type_error("Expected Int64Array for MIN delta computation")
            })?;

        let delta_min = arrow::compute::min(arr);
        Ok(match (existing_min, delta_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        })
    }

    /// Merge a new delta batch into the existing MAX (i64).
    pub fn delta_max_i64(
        existing_max: Option<i64>,
        delta_batch: &RecordBatch,
        col_index: usize,
    ) -> Result<Option<i64>> {
        use arrow::array::Int64Array;

        let arr = delta_batch
            .column(col_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                BlazeError::type_error("Expected Int64Array for MAX delta computation")
            })?;

        let delta_max = arrow::compute::max(arr);
        Ok(match (existing_max, delta_max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        })
    }
}

// ---------------------------------------------------------------------------
// Auto-refresh scheduler
// ---------------------------------------------------------------------------

/// Configuration for the auto-refresh scheduler.
#[derive(Debug, Clone)]
pub struct AutoRefreshConfig {
    /// How often to check for stale views (in milliseconds).
    pub check_interval_ms: u64,
    /// Maximum number of concurrent refreshes.
    pub max_concurrent_refreshes: usize,
    /// Whether the scheduler is enabled.
    pub enabled: bool,
}

impl Default for AutoRefreshConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,
            max_concurrent_refreshes: 4,
            enabled: false,
        }
    }
}

/// Manages automatic refresh of materialized views based on their schedules.
pub struct AutoRefreshScheduler {
    config: AutoRefreshConfig,
    view_schedules: RwLock<HashMap<String, RefreshSchedule>>,
    staleness_tracker: Arc<StalenessTracker>,
    refresh_history: RwLock<Vec<RefreshEvent>>,
}

/// A recorded refresh event for auditing.
#[derive(Debug, Clone)]
pub struct RefreshEvent {
    pub view_name: String,
    pub triggered_at: std::time::Instant,
    pub duration: std::time::Duration,
    pub rows_refreshed: usize,
    pub was_incremental: bool,
}

impl AutoRefreshScheduler {
    /// Create a new auto-refresh scheduler.
    pub fn new(config: AutoRefreshConfig) -> Self {
        Self {
            config,
            view_schedules: RwLock::new(HashMap::new()),
            staleness_tracker: Arc::new(StalenessTracker::new()),
            refresh_history: RwLock::new(Vec::new()),
        }
    }

    /// Register a view with a refresh schedule.
    pub fn register_view(&self, view_name: impl Into<String>, schedule: RefreshSchedule) {
        self.view_schedules
            .write()
            .insert(view_name.into(), schedule);
    }

    /// Remove a view from the scheduler.
    pub fn unregister_view(&self, view_name: &str) {
        self.view_schedules.write().remove(view_name);
    }

    /// Check all views and return the names of those that need refresh.
    pub fn views_needing_refresh(&self) -> Vec<String> {
        let schedules = self.view_schedules.read();
        schedules
            .iter()
            .filter(|(name, schedule)| self.staleness_tracker.needs_refresh(name, schedule))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Record a refresh event.
    pub fn record_refresh(
        &self,
        view_name: &str,
        duration: std::time::Duration,
        rows: usize,
        was_incremental: bool,
    ) {
        self.staleness_tracker.record_refresh(view_name, duration);
        self.refresh_history.write().push(RefreshEvent {
            view_name: view_name.to_string(),
            triggered_at: std::time::Instant::now(),
            duration,
            rows_refreshed: rows,
            was_incremental,
        });
    }

    /// Notify the scheduler that a source table changed.
    pub fn notify_source_change(&self, table_name: &str, affected_views: &[String]) {
        for view in affected_views {
            self.staleness_tracker.record_source_change(view);
        }
        let _ = table_name; // Used for logging in production
    }

    /// Get the refresh history.
    pub fn refresh_history(&self) -> Vec<RefreshEvent> {
        self.refresh_history.read().clone()
    }

    /// Check if the scheduler is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the staleness tracker.
    pub fn staleness_tracker(&self) -> &Arc<StalenessTracker> {
        &self.staleness_tracker
    }
}

impl Default for AutoRefreshScheduler {
    fn default() -> Self {
        Self::new(AutoRefreshConfig::default())
    }
}

/// EXPLAIN MATERIALIZED VIEW output.
#[derive(Debug, Clone)]
pub struct ViewExplainInfo {
    pub view_name: String,
    pub query: String,
    pub source_tables: Vec<String>,
    pub is_incremental: bool,
    pub last_refresh: Option<String>,
    pub staleness_secs: Option<f64>,
    pub refresh_count: u64,
    pub row_count: usize,
    pub schedule: RefreshSchedule,
}

impl ViewExplainInfo {
    /// Format the explain info as a human-readable table string.
    pub fn format_table(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Materialized View: {}", self.view_name));
        lines.push(format!("  Query: {}", self.query));
        lines.push(format!(
            "  Source Tables: {}",
            self.source_tables.join(", ")
        ));
        lines.push(format!("  Incremental: {}", self.is_incremental));
        lines.push(format!(
            "  Last Refresh: {}",
            self.last_refresh.as_deref().unwrap_or("never")
        ));
        lines.push(format!(
            "  Staleness: {}",
            self.staleness_secs
                .map(|s| format!("{:.2}s", s))
                .unwrap_or_else(|| "unknown".to_string())
        ));
        lines.push(format!("  Refresh Count: {}", self.refresh_count));
        lines.push(format!("  Row Count: {}", self.row_count));
        lines.push(format!("  Schedule: {:?}", self.schedule));
        lines.join("\n")
    }
}

// ---------------------------------------------------------------------------
// Incremental Delta Propagation Pipeline
// ---------------------------------------------------------------------------

/// Describes how a delta should be transformed as it propagates through the view query.
#[derive(Debug, Clone)]
pub enum DeltaTransform {
    /// Pass-through: delta rows go directly to the view (simple SELECT * FROM t).
    Identity,
    /// Filter: apply predicate to delta rows before appending.
    Filter { predicate: String },
    /// Project: select specific columns from delta rows.
    Project { columns: Vec<String> },
    /// Filter + Project combined.
    FilterProject {
        predicate: String,
        columns: Vec<String>,
    },
    /// Join delta with another table (for views involving joins).
    JoinDelta {
        join_table: String,
        join_condition: String,
    },
    /// Requires full recomputation (complex queries, updates, deletes).
    FullRecompute,
}

/// Analyzes a view's SQL query to determine the best delta propagation strategy.
pub struct DeltaPropagationAnalyzer;

impl DeltaPropagationAnalyzer {
    /// Analyze the view query and determine the delta transform for a given source table.
    ///
    /// Simple heuristics:
    /// - SELECT * FROM table → Identity
    /// - SELECT cols FROM table → Project
    /// - SELECT ... FROM table WHERE ... → FilterProject or Filter
    /// - SELECT ... FROM table JOIN ... → JoinDelta
    /// - Anything with GROUP BY, aggregates → FullRecompute
    pub fn analyze(query: &str, _source_table: &str) -> DeltaTransform {
        let upper = query.to_uppercase();

        // Aggregates or GROUP BY require full recompute
        if upper.contains("GROUP BY")
            || upper.contains("SUM(")
            || upper.contains("COUNT(")
            || upper.contains("AVG(")
            || upper.contains("MIN(")
            || upper.contains("MAX(")
            || upper.contains("HAVING")
            || upper.contains("DISTINCT")
        {
            return DeltaTransform::FullRecompute;
        }

        let has_join = upper.contains(" JOIN ");
        let has_where = upper.contains(" WHERE ");

        if has_join {
            // Extract join table and condition (simplified parsing)
            if let Some(join_info) = Self::extract_join_info(query) {
                return DeltaTransform::JoinDelta {
                    join_table: join_info.0,
                    join_condition: join_info.1,
                };
            }
            return DeltaTransform::FullRecompute;
        }

        // Check if it's a simple SELECT * (identity)
        let is_star = upper.contains("SELECT *") || upper.contains("SELECT  *");

        match (is_star, has_where) {
            (true, false) => DeltaTransform::Identity,
            (true, true) => {
                if let Some(pred) = Self::extract_where_clause(query) {
                    DeltaTransform::Filter { predicate: pred }
                } else {
                    DeltaTransform::FullRecompute
                }
            }
            (false, false) => {
                if let Some(cols) = Self::extract_select_columns(query) {
                    DeltaTransform::Project { columns: cols }
                } else {
                    DeltaTransform::FullRecompute
                }
            }
            (false, true) => {
                let columns = Self::extract_select_columns(query);
                let predicate = Self::extract_where_clause(query);
                match (columns, predicate) {
                    (Some(cols), Some(pred)) => DeltaTransform::FilterProject {
                        predicate: pred,
                        columns: cols,
                    },
                    _ => DeltaTransform::FullRecompute,
                }
            }
        }
    }

    fn extract_where_clause(query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        let where_idx = upper.find(" WHERE ")?;
        let after_where = &query[where_idx + 7..];
        // Take everything until ORDER BY, LIMIT, GROUP BY, or end
        let end_idx = ["ORDER BY", "LIMIT", "GROUP BY", "HAVING"]
            .iter()
            .filter_map(|kw| after_where.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_where.len());
        Some(after_where[..end_idx].trim().to_string())
    }

    fn extract_select_columns(query: &str) -> Option<Vec<String>> {
        let upper = query.to_uppercase();
        let select_idx = upper.find("SELECT ")?;
        let from_idx = upper.find(" FROM ")?;
        let cols_str = &query[select_idx + 7..from_idx];
        let cols: Vec<String> = cols_str
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if cols.is_empty() {
            None
        } else {
            Some(cols)
        }
    }

    fn extract_join_info(query: &str) -> Option<(String, String)> {
        let upper = query.to_uppercase();
        let join_idx = upper.find(" JOIN ")?;
        let after_join = &query[join_idx + 6..];
        let on_idx = after_join.to_uppercase().find(" ON ")?;
        let table = after_join[..on_idx].trim().to_string();
        let condition_start = on_idx + 4;
        let after_on = &after_join[condition_start..];
        let end_idx = ["WHERE", "ORDER BY", "LIMIT", "GROUP BY"]
            .iter()
            .filter_map(|kw| after_on.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_on.len());
        let condition = after_on[..end_idx].trim().to_string();
        Some((table, condition))
    }
}

/// Pipeline that orchestrates end-to-end incremental view maintenance.
pub struct IncrementalPipeline {
    engine: IncrementalRefreshEngine,
    dependency_graph: parking_lot::RwLock<ViewDependencyGraph>,
    transforms: parking_lot::RwLock<HashMap<String, DeltaTransform>>,
}

impl IncrementalPipeline {
    pub fn new() -> Self {
        Self {
            engine: IncrementalRefreshEngine::new(),
            dependency_graph: parking_lot::RwLock::new(ViewDependencyGraph::new()),
            transforms: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Register a view and analyze its query for delta propagation.
    pub fn register_view(&self, view_name: &str, query: &str, source_tables: &[String]) {
        let mut graph = self.dependency_graph.write();
        graph.add_view(view_name, source_tables);

        let mut transforms = self.transforms.write();
        // Analyze for each source table; use the first one for simplicity
        if let Some(table) = source_tables.first() {
            let transform = DeltaPropagationAnalyzer::analyze(query, table);
            transforms.insert(view_name.to_string(), transform);
        }
    }

    /// Remove a view from the pipeline.
    pub fn unregister_view(&self, view_name: &str) {
        self.dependency_graph.write().remove_view(view_name);
        self.transforms.write().remove(view_name);
    }

    /// Get views affected by a change to a source table.
    pub fn affected_views(&self, table_name: &str) -> Vec<String> {
        self.dependency_graph.read().affected_views(table_name)
    }

    /// Get the delta transform strategy for a view.
    pub fn transform_for(&self, view_name: &str) -> Option<DeltaTransform> {
        self.transforms.read().get(view_name).cloned()
    }

    /// Determine whether a view can use incremental refresh.
    pub fn can_refresh_incrementally(&self, view_name: &str) -> bool {
        self.transforms
            .read()
            .get(view_name)
            .map(|t| !matches!(t, DeltaTransform::FullRecompute))
            .unwrap_or(false)
    }

    /// Process a table change and determine which views need refresh and how.
    pub fn process_change(
        &self,
        table_name: &str,
        tracker: &ChangeTracker,
    ) -> Vec<(String, RefreshDecision)> {
        let affected = self.affected_views(table_name);
        let transforms = self.transforms.read();
        let has_mutations = !tracker.is_insert_only();

        affected
            .into_iter()
            .map(|view_name| {
                let decision = if has_mutations {
                    RefreshDecision::FullRefresh
                } else {
                    match transforms.get(&view_name) {
                        Some(DeltaTransform::FullRecompute) => RefreshDecision::FullRefresh,
                        Some(_) => {
                            let pending = tracker.take_pending_changes();
                            let delta_rows: usize = pending.iter().map(|b| b.num_rows()).sum();
                            // Put changes back (we just peeked)
                            tracker.record_insert(&pending);
                            if RefreshCostEstimator::should_use_incremental(delta_rows, 1000, 0) {
                                RefreshDecision::Incremental
                            } else {
                                RefreshDecision::FullRefresh
                            }
                        }
                        None => RefreshDecision::FullRefresh,
                    }
                };
                (view_name, decision)
            })
            .collect()
    }

    /// Get the underlying engine.
    pub fn engine(&self) -> &IncrementalRefreshEngine {
        &self.engine
    }
}

impl Default for IncrementalPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Decision on how to refresh a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshDecision {
    /// Recompute the entire view from scratch.
    FullRefresh,
    /// Apply only the delta changes.
    Incremental,
    /// No refresh needed.
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use arrow::array::ArrayRef;
    use std::sync::atomic::AtomicBool;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[test]
    fn test_change_tracker() {
        let tracker = ChangeTracker::new("users");
        assert!(!tracker.has_pending_changes());
        assert_eq!(tracker.version(), 0);

        // Simulate insert
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        tracker.record_insert(&[batch]);
        assert!(tracker.has_pending_changes());
        assert!(tracker.version() > 0);

        let changes = tracker.take_pending_changes();
        assert_eq!(changes.len(), 1);
        assert!(!tracker.has_pending_changes());
    }

    #[test]
    fn test_materialized_view_basic() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE src (id BIGINT, val BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 20)").unwrap();

        let mgr = MaterializedViewManager::new(
            conn.catalog_list().clone(),
            ExecutionContext::new().with_catalog_list(conn.catalog_list().clone()),
        );

        mgr.create_view(
            "mv_sum",
            "SELECT SUM(val) as total FROM src",
            vec!["src".to_string()],
        )
        .unwrap();

        let view = mgr.get_view("mv_sum").unwrap();
        assert_eq!(view.refresh_count(), 1);
        let data = view.data();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_materialized_view_refresh() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE orders (id BIGINT, amount BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 100)").unwrap();

        let mgr = MaterializedViewManager::new(
            conn.catalog_list().clone(),
            ExecutionContext::new().with_catalog_list(conn.catalog_list().clone()),
        );

        mgr.create_view(
            "mv_total",
            "SELECT SUM(amount) as total FROM orders",
            vec!["orders".to_string()],
        )
        .unwrap();

        // Insert more data
        conn.execute("INSERT INTO orders VALUES (2, 200)").unwrap();
        mgr.notify_insert("orders", &[]);

        // Refresh
        let data = mgr.refresh_view("mv_total").unwrap();
        assert!(!data.is_empty());

        let view = mgr.get_view("mv_total").unwrap();
        assert_eq!(view.refresh_count(), 2);
    }

    #[test]
    fn test_materialized_view_callback() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE events (id BIGINT)").unwrap();
        conn.execute("INSERT INTO events VALUES (1)").unwrap();

        let mgr = MaterializedViewManager::new(
            conn.catalog_list().clone(),
            ExecutionContext::new().with_catalog_list(conn.catalog_list().clone()),
        );

        let was_called = Arc::new(AtomicBool::new(false));
        let was_called_clone = was_called.clone();
        mgr.on_refresh(Box::new(move |name, _data| {
            assert_eq!(name, "mv_events");
            was_called_clone.store(true, Ordering::SeqCst);
        }));

        mgr.create_view(
            "mv_events",
            "SELECT COUNT(*) as cnt FROM events",
            vec!["events".to_string()],
        )
        .unwrap();

        assert!(was_called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_list_and_drop_views() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t1 (id BIGINT)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1)").unwrap();

        let mgr = MaterializedViewManager::new(
            conn.catalog_list().clone(),
            ExecutionContext::new().with_catalog_list(conn.catalog_list().clone()),
        );

        mgr.create_view("v1", "SELECT * FROM t1", vec!["t1".to_string()])
            .unwrap();
        assert_eq!(mgr.list_views().len(), 1);

        mgr.drop_view("v1").unwrap();
        assert_eq!(mgr.list_views().len(), 0);
    }

    #[test]
    fn test_change_tracker_update_delete() {
        let tracker = ChangeTracker::new("users");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();

        tracker.record_insert(&[batch.clone()]);
        tracker.record_update(&[batch.clone()]);
        tracker.record_delete(&[batch.clone()]);

        assert!(tracker.has_pending_changes());

        let stats = tracker.change_stats();
        assert_eq!(stats.total_inserts, 1);
        assert_eq!(stats.total_updates, 1);
        assert_eq!(stats.total_deletes, 1);
        assert_eq!(stats.total_changes(), 3);
        assert!(stats.has_mutations());
    }

    #[test]
    fn test_change_tracker_insert_only() {
        let tracker = ChangeTracker::new("logs");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("msg", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec!["hello"])) as ArrayRef],
        )
        .unwrap();

        tracker.record_insert(&[batch]);
        assert!(tracker.is_insert_only());

        let typed = tracker.take_typed_changes();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].change_type, ChangeType::Insert);
    }

    #[test]
    fn test_dependency_graph() {
        let mut graph = ViewDependencyGraph::new();
        graph.add_view("mv_orders", &["orders".into()]);
        graph.add_view("mv_summary", &["mv_orders".into(), "products".into()]);

        // Changing "orders" should trigger mv_orders, then mv_summary
        let affected = graph.affected_views("orders");
        assert!(affected.contains(&"mv_orders".to_string()));
        assert!(affected.contains(&"mv_summary".to_string()));
        // mv_summary depends on mv_orders, so mv_orders should come first
        let idx_orders = affected.iter().position(|v| v == "mv_orders").unwrap();
        let idx_summary = affected.iter().position(|v| v == "mv_summary").unwrap();
        assert!(idx_orders < idx_summary);

        graph.remove_view("mv_summary");
        let affected2 = graph.affected_views("orders");
        assert_eq!(affected2.len(), 1);
        assert_eq!(affected2[0], "mv_orders");
    }

    #[test]
    fn test_staleness_tracker() {
        let tracker = StalenessTracker::new();

        // No entry yet
        assert_eq!(tracker.staleness_secs("mv1"), None);
        assert_eq!(tracker.changes_since_refresh("mv1"), 0);

        // Record a refresh
        tracker.record_refresh("mv1", std::time::Duration::from_millis(50));
        assert!(tracker.staleness_secs("mv1").is_some());
        assert_eq!(tracker.changes_since_refresh("mv1"), 0);

        // Record source changes
        tracker.record_source_change("mv1");
        tracker.record_source_change("mv1");
        assert_eq!(tracker.changes_since_refresh("mv1"), 2);

        // After another refresh, changes reset
        tracker.record_refresh("mv1", std::time::Duration::from_millis(30));
        assert_eq!(tracker.changes_since_refresh("mv1"), 0);
    }

    #[test]
    fn test_incremental_delta_computation() {
        let engine = IncrementalRefreshEngine::new();
        let tracker = ChangeTracker::new("orders");

        // No pending changes → error
        assert!(engine.compute_incremental_delta(&tracker).is_err());

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![3, 4])) as ArrayRef],
        )
        .unwrap();

        // Insert-only → returns delta
        tracker.record_insert(&[batch1.clone(), batch2.clone()]);
        let delta = engine.compute_incremental_delta(&tracker).unwrap();
        assert!(delta.is_some());
        let delta = delta.unwrap();
        assert_eq!(delta.len(), 2);

        // Apply delta to existing data
        let existing = vec![batch1.clone()];
        let merged = IncrementalRefreshEngine::apply_delta(&existing, &delta).unwrap();
        assert_eq!(merged.len(), 3);

        // With mutations → returns None (needs full refresh)
        tracker.record_insert(&[batch1.clone()]);
        tracker.record_update(&[batch2.clone()]);
        let delta = engine.compute_incremental_delta(&tracker).unwrap();
        assert!(delta.is_none());
    }

    #[test]
    fn test_refresh_cost_estimation() {
        // Full refresh cost grows with joins
        let cost_no_join = RefreshCostEstimator::estimate_full_refresh_cost(1000, 0);
        let cost_one_join = RefreshCostEstimator::estimate_full_refresh_cost(1000, 1);
        let cost_two_joins = RefreshCostEstimator::estimate_full_refresh_cost(1000, 2);
        assert!(cost_one_join > cost_no_join);
        assert!(cost_two_joins > cost_one_join);

        // Incremental cost is much smaller for small deltas
        let incr = RefreshCostEstimator::estimate_incremental_cost(10, 10_000);
        let full = RefreshCostEstimator::estimate_full_refresh_cost(10_010, 0);
        assert!(incr < full);

        // Small delta on large table → should use incremental
        assert!(RefreshCostEstimator::should_use_incremental(10, 100_000, 1));
        // Delta nearly as large as table → full may be better
        assert!(!RefreshCostEstimator::should_use_incremental(
            100_000, 100, 0
        ));
    }

    #[test]
    fn test_view_explain_info() {
        let info = ViewExplainInfo {
            view_name: "mv_sales".to_string(),
            query: "SELECT SUM(amount) FROM sales".to_string(),
            source_tables: vec!["sales".to_string(), "products".to_string()],
            is_incremental: true,
            last_refresh: Some("2024-01-01T00:00:00Z".to_string()),
            staleness_secs: Some(42.5),
            refresh_count: 7,
            row_count: 1000,
            schedule: RefreshSchedule::Periodic { interval_secs: 300 },
        };

        let output = info.format_table();
        assert!(output.contains("mv_sales"));
        assert!(output.contains("SELECT SUM(amount) FROM sales"));
        assert!(output.contains("sales, products"));
        assert!(output.contains("Incremental: true"));
        assert!(output.contains("42.50s"));
        assert!(output.contains("Refresh Count: 7"));
        assert!(output.contains("Row Count: 1000"));
        assert!(output.contains("Periodic"));
    }

    #[test]
    fn test_refresh_schedule_logic() {
        let tracker = StalenessTracker::new();

        // OnDemand never needs automatic refresh
        assert!(!tracker.needs_refresh("mv1", &RefreshSchedule::OnDemand));

        // OnCommit needs refresh when there are pending changes
        tracker.record_refresh("mv1", std::time::Duration::from_millis(10));
        assert!(!tracker.needs_refresh("mv1", &RefreshSchedule::OnCommit));
        tracker.record_source_change("mv1");
        assert!(tracker.needs_refresh("mv1", &RefreshSchedule::OnCommit));

        // Periodic: freshly refreshed view should not need refresh
        tracker.record_refresh("mv2", std::time::Duration::from_millis(10));
        assert!(!tracker.needs_refresh("mv2", &RefreshSchedule::Periodic { interval_secs: 60 }));

        // WhenStale with 0 threshold: always stale immediately
        // (the elapsed time will be > 0 after record_refresh)
        tracker.record_refresh("mv3", std::time::Duration::from_millis(1));
        // A threshold of 0 means any elapsed time triggers a refresh
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(tracker.needs_refresh(
            "mv3",
            &RefreshSchedule::WhenStale {
                max_staleness_secs: 0
            }
        ));

        // Unknown view with Periodic → needs refresh (returns true)
        assert!(tracker.needs_refresh(
            "unknown_view",
            &RefreshSchedule::Periodic { interval_secs: 10 }
        ));
    }

    #[test]
    fn test_aggregate_delta_count() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();

        let new_count = AggregateDeltaComputer::delta_count(10, &batch);
        assert_eq!(new_count, 13);
    }

    #[test]
    fn test_aggregate_delta_sum_i64() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("amount", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .unwrap();

        let new_sum = AggregateDeltaComputer::delta_sum_i64(100, &batch, 0).unwrap();
        assert_eq!(new_sum, 160);
    }

    #[test]
    fn test_aggregate_delta_sum_f64() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5])) as ArrayRef],
        )
        .unwrap();

        let new_sum = AggregateDeltaComputer::delta_sum_f64(10.0, &batch, 0).unwrap();
        assert!((new_sum - 14.0).abs() < 1e-10);
    }

    #[test]
    fn test_aggregate_delta_min_max() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("val", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![5, 3, 8])) as ArrayRef],
        )
        .unwrap();

        // MIN: existing 4, delta has 3 → new min 3
        let new_min = AggregateDeltaComputer::delta_min_i64(Some(4), &batch, 0).unwrap();
        assert_eq!(new_min, Some(3));

        // MAX: existing 6, delta has 8 → new max 8
        let new_max = AggregateDeltaComputer::delta_max_i64(Some(6), &batch, 0).unwrap();
        assert_eq!(new_max, Some(8));

        // MIN from None
        let new_min = AggregateDeltaComputer::delta_min_i64(None, &batch, 0).unwrap();
        assert_eq!(new_min, Some(3));
    }

    #[test]
    fn test_auto_refresh_scheduler() {
        let scheduler = AutoRefreshScheduler::new(AutoRefreshConfig {
            check_interval_ms: 100,
            max_concurrent_refreshes: 2,
            enabled: true,
        });

        scheduler.register_view("mv1", RefreshSchedule::OnCommit);
        scheduler.register_view("mv2", RefreshSchedule::OnDemand);

        // No changes yet — only OnDemand never needs refresh
        assert!(!scheduler
            .views_needing_refresh()
            .contains(&"mv2".to_string()));

        // Simulate source change
        scheduler.notify_source_change("orders", &["mv1".to_string()]);

        // Now mv1 needs refresh (OnCommit schedule)
        // Need to prime the staleness tracker first
        scheduler
            .staleness_tracker()
            .record_refresh("mv1", std::time::Duration::from_millis(1));
        scheduler.notify_source_change("orders", &["mv1".to_string()]);
        let needing = scheduler.views_needing_refresh();
        assert!(needing.contains(&"mv1".to_string()));

        // Record refresh and verify it resets
        scheduler.record_refresh("mv1", std::time::Duration::from_millis(5), 100, false);
        let history = scheduler.refresh_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].view_name, "mv1");
        assert_eq!(history[0].rows_refreshed, 100);
    }

    #[test]
    fn test_auto_refresh_unregister() {
        let scheduler = AutoRefreshScheduler::default();
        scheduler.register_view("mv1", RefreshSchedule::OnCommit);
        scheduler.unregister_view("mv1");
        assert!(scheduler.views_needing_refresh().is_empty());
    }

    // --- Incremental Delta Propagation tests ---

    #[test]
    fn test_delta_analyzer_identity() {
        let transform = DeltaPropagationAnalyzer::analyze("SELECT * FROM users", "users");
        assert!(matches!(transform, DeltaTransform::Identity));
    }

    #[test]
    fn test_delta_analyzer_filter() {
        let transform =
            DeltaPropagationAnalyzer::analyze("SELECT * FROM users WHERE age > 21", "users");
        match transform {
            DeltaTransform::Filter { predicate } => {
                assert_eq!(predicate, "age > 21");
            }
            _ => panic!("Expected Filter, got {:?}", transform),
        }
    }

    #[test]
    fn test_delta_analyzer_project() {
        let transform = DeltaPropagationAnalyzer::analyze("SELECT name, age FROM users", "users");
        match transform {
            DeltaTransform::Project { columns } => {
                assert_eq!(columns, vec!["name", "age"]);
            }
            _ => panic!("Expected Project, got {:?}", transform),
        }
    }

    #[test]
    fn test_delta_analyzer_aggregate_requires_full() {
        let transform = DeltaPropagationAnalyzer::analyze(
            "SELECT COUNT(*) FROM users GROUP BY department",
            "users",
        );
        assert!(matches!(transform, DeltaTransform::FullRecompute));
    }

    #[test]
    fn test_delta_analyzer_join() {
        let transform = DeltaPropagationAnalyzer::analyze(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            "users",
        );
        match transform {
            DeltaTransform::JoinDelta {
                join_table,
                join_condition,
            } => {
                assert_eq!(join_table, "orders");
                assert!(join_condition.contains("users.id = orders.user_id"));
            }
            _ => panic!("Expected JoinDelta, got {:?}", transform),
        }
    }

    #[test]
    fn test_incremental_pipeline_register() {
        let pipeline = IncrementalPipeline::new();
        pipeline.register_view(
            "active_users",
            "SELECT * FROM users WHERE active = true",
            &["users".to_string()],
        );

        assert!(pipeline.can_refresh_incrementally("active_users"));
        assert!(!pipeline.can_refresh_incrementally("nonexistent"));

        let affected = pipeline.affected_views("users");
        assert_eq!(affected, vec!["active_users"]);
    }

    #[test]
    fn test_incremental_pipeline_unregister() {
        let pipeline = IncrementalPipeline::new();
        pipeline.register_view("v1", "SELECT * FROM t1", &["t1".to_string()]);
        pipeline.unregister_view("v1");
        assert!(pipeline.affected_views("t1").is_empty());
    }

    #[test]
    fn test_incremental_pipeline_aggregate_not_incremental() {
        let pipeline = IncrementalPipeline::new();
        pipeline.register_view(
            "user_counts",
            "SELECT department, COUNT(*) FROM users GROUP BY department",
            &["users".to_string()],
        );
        assert!(!pipeline.can_refresh_incrementally("user_counts"));
    }

    #[test]
    fn test_refresh_decision_with_mutations() {
        let pipeline = IncrementalPipeline::new();
        pipeline.register_view("v1", "SELECT * FROM t1", &["t1".to_string()]);

        let tracker = ChangeTracker::new("t1");
        let batch = make_test_batch();
        tracker.record_update(&[batch]);

        let decisions = pipeline.process_change("t1", &tracker);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].1, RefreshDecision::FullRefresh);
    }

    #[test]
    fn test_delta_analyzer_filter_project() {
        let transform = DeltaPropagationAnalyzer::analyze(
            "SELECT name, age FROM users WHERE age > 18",
            "users",
        );
        match transform {
            DeltaTransform::FilterProject { predicate, columns } => {
                assert_eq!(predicate, "age > 18");
                assert_eq!(columns, vec!["name", "age"]);
            }
            _ => panic!("Expected FilterProject, got {:?}", transform),
        }
    }
}
