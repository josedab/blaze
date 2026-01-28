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

    /// Apply incremental delta transform to pending changes for a view.
    ///
    /// Takes the pending changes from the tracker, applies the view's
    /// delta transform, and returns the transformed delta batches.
    /// Returns None if full recompute is needed.
    pub fn apply_incremental(
        &self,
        view_name: &str,
        delta_batches: &[RecordBatch],
        catalog_list: Option<&CatalogList>,
    ) -> Result<Option<Vec<RecordBatch>>> {
        let transforms = self.transforms.read();
        let transform = transforms.get(view_name).ok_or_else(|| {
            BlazeError::execution(format!("No transform registered for view '{}'", view_name))
        })?;

        IncrementalExecutor::apply_transform(transform, delta_batches, catalog_list)
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

// ---------------------------------------------------------------------------
// Version vectors for multi-table change coordination
// ---------------------------------------------------------------------------

/// A version vector tracking per-table versions for consistent snapshots.
#[derive(Debug, Clone, Default)]
pub struct VersionVector {
    versions: HashMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a new version for a table.
    pub fn advance(&mut self, table: &str, version: u64) {
        let entry = self.versions.entry(table.to_string()).or_insert(0);
        if version > *entry {
            *entry = version;
        }
    }

    /// Get the version for a table.
    pub fn version_of(&self, table: &str) -> u64 {
        self.versions.get(table).copied().unwrap_or(0)
    }

    /// Check if this vector dominates another (all versions >=).
    pub fn dominates(&self, other: &VersionVector) -> bool {
        for (table, &ver) in &other.versions {
            if self.version_of(table) < ver {
                return false;
            }
        }
        true
    }

    /// Merge with another vector, taking the max of each component.
    pub fn merge(&mut self, other: &VersionVector) {
        for (table, &ver) in &other.versions {
            self.advance(table, ver);
        }
    }

    /// Tables tracked by this vector.
    pub fn tables(&self) -> Vec<String> {
        self.versions.keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Incremental executor — applies delta transforms to batches
// ---------------------------------------------------------------------------

/// Executes incremental delta transforms on RecordBatch data.
///
/// Given a `DeltaTransform` and incoming delta batches, this executor
/// applies the appropriate filter/project operations to produce
/// the delta output for a materialized view.
pub struct IncrementalExecutor;

impl IncrementalExecutor {
    /// Apply a delta transform to incoming batches.
    ///
    /// - `Identity`: pass through unchanged
    /// - `Filter`: evaluate predicate and keep matching rows
    /// - `Project`: select specified columns
    /// - `FilterProject`: filter then project
    /// - `JoinDelta`: returns None (requires full recompute currently)
    /// - `FullRecompute`: returns None (caller must do full refresh)
    pub fn apply_transform(
        transform: &DeltaTransform,
        delta: &[RecordBatch],
        _catalog_list: Option<&CatalogList>,
    ) -> Result<Option<Vec<RecordBatch>>> {
        match transform {
            DeltaTransform::Identity => Ok(Some(delta.to_vec())),

            DeltaTransform::Filter { predicate } => {
                let mut result = Vec::new();
                for batch in delta {
                    if let Some(filtered) = Self::apply_filter(batch, predicate)? {
                        if filtered.num_rows() > 0 {
                            result.push(filtered);
                        }
                    }
                }
                Ok(Some(result))
            }

            DeltaTransform::Project { columns } => {
                let mut result = Vec::new();
                for batch in delta {
                    result.push(Self::apply_projection(batch, columns)?);
                }
                Ok(Some(result))
            }

            DeltaTransform::FilterProject { predicate, columns } => {
                let mut result = Vec::new();
                for batch in delta {
                    if let Some(filtered) = Self::apply_filter(batch, predicate)? {
                        if filtered.num_rows() > 0 {
                            result.push(Self::apply_projection(&filtered, columns)?);
                        }
                    }
                }
                Ok(Some(result))
            }

            // Join deltas require full query execution — fall back to full recompute
            DeltaTransform::JoinDelta { .. } | DeltaTransform::FullRecompute => Ok(None),
        }
    }

    /// Apply a simple comparison filter to a batch.
    /// Supports: col > val, col < val, col = val, col >= val, col <= val
    fn apply_filter(batch: &RecordBatch, predicate: &str) -> Result<Option<RecordBatch>> {
        use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::compute::filter_record_batch;

        let parts: Vec<&str> = predicate.splitn(3, |c: char| c.is_whitespace()).collect();
        if parts.len() < 3 {
            // Try equality format: "col=value"
            if predicate.contains('=') {
                let eq_parts: Vec<&str> = predicate.split('=').collect();
                if eq_parts.len() == 2 {
                    let col_name = eq_parts[0].trim();
                    let value = eq_parts[1].trim();
                    return Self::apply_equality_filter(batch, col_name, value);
                }
            }
            return Ok(Some(batch.clone()));
        }

        let col_name = parts[0].trim();
        let op = parts[1].trim();
        let value_str = parts[2].trim();

        let schema = batch.schema();
        let col_idx = match schema.fields().iter().position(|f| f.name() == col_name) {
            Some(idx) => idx,
            None => return Ok(Some(batch.clone())),
        };

        let col = batch.column(col_idx);

        if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
            if let Ok(threshold) = value_str.parse::<i64>() {
                let mask: BooleanArray = match op {
                    ">" => int_arr.iter().map(|v| v.map(|x| x > threshold)).collect(),
                    ">=" => int_arr.iter().map(|v| v.map(|x| x >= threshold)).collect(),
                    "<" => int_arr.iter().map(|v| v.map(|x| x < threshold)).collect(),
                    "<=" => int_arr.iter().map(|v| v.map(|x| x <= threshold)).collect(),
                    "=" | "==" => int_arr.iter().map(|v| v.map(|x| x == threshold)).collect(),
                    "!=" | "<>" => int_arr.iter().map(|v| v.map(|x| x != threshold)).collect(),
                    _ => return Ok(Some(batch.clone())),
                };
                return Ok(Some(filter_record_batch(batch, &mask)?));
            }
        }

        if let Some(f64_arr) = col.as_any().downcast_ref::<Float64Array>() {
            if let Ok(threshold) = value_str.parse::<f64>() {
                let mask: BooleanArray = match op {
                    ">" => f64_arr.iter().map(|v| v.map(|x| x > threshold)).collect(),
                    ">=" => f64_arr.iter().map(|v| v.map(|x| x >= threshold)).collect(),
                    "<" => f64_arr.iter().map(|v| v.map(|x| x < threshold)).collect(),
                    "<=" => f64_arr.iter().map(|v| v.map(|x| x <= threshold)).collect(),
                    "=" | "==" => f64_arr
                        .iter()
                        .map(|v| v.map(|x| (x - threshold).abs() < f64::EPSILON))
                        .collect(),
                    _ => return Ok(Some(batch.clone())),
                };
                return Ok(Some(filter_record_batch(batch, &mask)?));
            }
        }

        if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
            let mask: BooleanArray = match op {
                "=" | "==" => str_arr.iter().map(|v| v.map(|x| x == value_str)).collect(),
                "!=" | "<>" => str_arr.iter().map(|v| v.map(|x| x != value_str)).collect(),
                _ => return Ok(Some(batch.clone())),
            };
            return Ok(Some(filter_record_batch(batch, &mask)?));
        }

        Ok(Some(batch.clone()))
    }

    fn apply_equality_filter(
        batch: &RecordBatch,
        col_name: &str,
        value: &str,
    ) -> Result<Option<RecordBatch>> {
        use arrow::array::{BooleanArray, Int64Array, StringArray};
        use arrow::compute::filter_record_batch;

        let schema = batch.schema();
        let col_idx = match schema.fields().iter().position(|f| f.name() == col_name) {
            Some(idx) => idx,
            None => return Ok(Some(batch.clone())),
        };

        let col = batch.column(col_idx);

        if value == "true" || value == "false" {
            let target = value == "true";
            if let Some(bool_arr) = col.as_any().downcast_ref::<BooleanArray>() {
                let mask: BooleanArray = bool_arr.iter().map(|v| v.map(|x| x == target)).collect();
                return Ok(Some(filter_record_batch(batch, &mask)?));
            }
        }

        if let Ok(int_val) = value.parse::<i64>() {
            if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                let mask: BooleanArray = int_arr.iter().map(|v| v.map(|x| x == int_val)).collect();
                return Ok(Some(filter_record_batch(batch, &mask)?));
            }
        }

        if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
            let mask: BooleanArray = str_arr.iter().map(|v| v.map(|x| x == value)).collect();
            return Ok(Some(filter_record_batch(batch, &mask)?));
        }

        Ok(Some(batch.clone()))
    }

    /// Apply column projection to a batch.
    fn apply_projection(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut indices = Vec::new();
        for col_name in columns {
            let trimmed = col_name.trim();
            if let Some(idx) = schema.fields().iter().position(|f| f.name() == trimmed) {
                indices.push(idx);
            }
        }

        if indices.is_empty() {
            return Err(BlazeError::execution(
                "No matching columns found for projection",
            ));
        }

        let projected_columns: Vec<_> = indices.iter().map(|&i| batch.column(i).clone()).collect();
        let projected_fields: Vec<_> = indices
            .iter()
            .map(|&i| Arc::clone(&schema.fields()[i]))
            .collect();
        let new_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));

        RecordBatch::try_new(new_schema, projected_columns)
            .map_err(|e| BlazeError::execution(format!("Projection failed: {}", e)))
    }
}

// ---------------------------------------------------------------------------
// Auto-trigger refresh and background scheduling
// ---------------------------------------------------------------------------

/// Configuration for automatic view refresh triggers.
#[derive(Debug, Clone)]
pub enum RefreshTrigger {
    /// Refresh on every commit (ON COMMIT)
    OnCommit,
    /// Refresh after N changes to source tables
    AfterChanges { threshold: u64 },
    /// Refresh on periodic interval
    Periodic { interval_secs: u64 },
    /// Manual refresh only
    Manual,
}

impl RefreshTrigger {
    pub fn should_trigger(&self, changes_since_refresh: u64, secs_since_refresh: f64) -> bool {
        match self {
            RefreshTrigger::OnCommit => true,
            RefreshTrigger::AfterChanges { threshold } => changes_since_refresh >= *threshold,
            RefreshTrigger::Periodic { interval_secs } => {
                secs_since_refresh >= *interval_secs as f64
            }
            RefreshTrigger::Manual => false,
        }
    }
}

/// Priority level for queued refreshes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RefreshPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// A single entry in the refresh queue.
#[derive(Debug, Clone)]
pub struct RefreshQueueEntry {
    pub view_name: String,
    pub priority: RefreshPriority,
    pub enqueued_at: std::time::Instant,
    pub trigger_reason: String,
}

/// Priority queue for pending materialized view refreshes.
#[derive(Default)]
pub struct RefreshQueue {
    queue: Mutex<Vec<RefreshQueueEntry>>,
}

impl RefreshQueue {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
        }
    }

    pub fn enqueue(
        &self,
        view_name: impl Into<String>,
        priority: RefreshPriority,
        reason: impl Into<String>,
    ) {
        let entry = RefreshQueueEntry {
            view_name: view_name.into(),
            priority,
            enqueued_at: std::time::Instant::now(),
            trigger_reason: reason.into(),
        };
        self.queue.lock().push(entry);
    }

    /// Dequeue the highest-priority, oldest entry.
    pub fn dequeue(&self) -> Option<RefreshQueueEntry> {
        let mut q = self.queue.lock();
        if q.is_empty() {
            return None;
        }
        // Find index of best candidate: highest priority, then earliest enqueued_at.
        let mut best = 0;
        for i in 1..q.len() {
            if q[i].priority > q[best].priority
                || (q[i].priority == q[best].priority && q[i].enqueued_at < q[best].enqueued_at)
            {
                best = i;
            }
        }
        Some(q.remove(best))
    }

    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    pub fn pending_views(&self) -> Vec<String> {
        self.queue
            .lock()
            .iter()
            .map(|e| e.view_name.clone())
            .collect()
    }
}

/// Metrics tracking for materialized view refresh operations.
#[derive(Debug, Clone, Default)]
pub struct RefreshMetrics {
    pub total_refreshes: u64,
    pub total_incremental_refreshes: u64,
    pub total_full_refreshes: u64,
    pub total_refresh_time_ms: u64,
    pub total_rows_processed: u64,
    pub failed_refreshes: u64,
    pub skipped_refreshes: u64,
}

impl RefreshMetrics {
    pub fn record_refresh(&mut self, incremental: bool, duration_ms: u64, rows: u64) {
        self.total_refreshes += 1;
        if incremental {
            self.total_incremental_refreshes += 1;
        } else {
            self.total_full_refreshes += 1;
        }
        self.total_refresh_time_ms += duration_ms;
        self.total_rows_processed += rows;
    }

    pub fn record_failure(&mut self) {
        self.failed_refreshes += 1;
    }

    pub fn record_skip(&mut self) {
        self.skipped_refreshes += 1;
    }

    /// Fraction of incremental refreshes vs total.
    pub fn incremental_ratio(&self) -> f64 {
        if self.total_refreshes == 0 {
            return 0.0;
        }
        self.total_incremental_refreshes as f64 / self.total_refreshes as f64
    }

    pub fn avg_refresh_time_ms(&self) -> f64 {
        if self.total_refreshes == 0 {
            return 0.0;
        }
        self.total_refresh_time_ms as f64 / self.total_refreshes as f64
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.total_refreshes + self.failed_refreshes;
        if total == 0 {
            return 1.0;
        }
        self.total_refreshes as f64 / total as f64
    }
}

/// Result of a consistency check on a materialized view.
#[derive(Debug, Clone)]
pub struct ConsistencyReport {
    pub view_name: String,
    pub is_consistent: bool,
    pub row_count_match: bool,
    pub view_row_count: usize,
    pub expected_row_count: usize,
    pub staleness_secs: f64,
}

/// Checks consistency between a materialized view and its source data.
pub struct ViewConsistencyChecker;

impl ViewConsistencyChecker {
    pub fn check_row_count(
        view_name: &str,
        view_rows: usize,
        source_rows: usize,
        staleness_secs: f64,
    ) -> ConsistencyReport {
        let row_count_match = view_rows == source_rows;
        ConsistencyReport {
            view_name: view_name.to_string(),
            is_consistent: row_count_match && staleness_secs < 60.0,
            row_count_match,
            view_row_count: view_rows,
            expected_row_count: source_rows,
            staleness_secs,
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Join Delta Computer + Streaming Delta Pipeline
// ---------------------------------------------------------------------------

/// Computes incremental deltas for join operations.
pub struct JoinDeltaComputer {
    join_type: JoinDeltaType,
}

/// Type of join for delta computation.
#[derive(Debug, Clone, Copy)]
pub enum JoinDeltaType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl JoinDeltaComputer {
    pub fn new(join_type: JoinDeltaType) -> Self {
        Self { join_type }
    }

    /// Given changes to the left table, compute the resulting changes to the join output.
    /// Returns: (new rows to add, rows to remove) based on join with right table.
    pub fn compute_left_delta(
        &self,
        left_inserts: usize,
        left_deletes: usize,
        right_row_count: usize,
    ) -> (usize, usize) {
        match self.join_type {
            JoinDeltaType::Inner => {
                // Each left insert could match all right rows (worst case)
                (
                    left_inserts * right_row_count / 10_usize.max(1),
                    left_deletes,
                )
            }
            JoinDeltaType::LeftOuter => (left_inserts, left_deletes),
            JoinDeltaType::RightOuter => (
                left_inserts * right_row_count / 10_usize.max(1),
                left_deletes,
            ),
            JoinDeltaType::FullOuter => (left_inserts, left_deletes),
        }
    }

    /// Estimate the cost of incremental vs full recomputation.
    pub fn should_use_incremental(
        &self,
        delta_size: usize,
        total_left: usize,
        total_right: usize,
    ) -> bool {
        let full_cost = total_left * total_right;
        let incremental_cost = delta_size * total_right;
        incremental_cost < full_cost / 2
    }
}

/// Streaming delta pipeline for continuous materialized view maintenance.
pub struct StreamingDeltaPipeline {
    stages: Vec<DeltaStage>,
    processed_deltas: u64,
    total_rows_propagated: u64,
}

/// A stage in the delta propagation pipeline.
#[derive(Debug, Clone)]
pub struct DeltaStage {
    pub name: String,
    pub stage_type: DeltaStageType,
    pub processed: u64,
}

/// Types of delta processing stages.
#[derive(Debug, Clone)]
pub enum DeltaStageType {
    Filter,
    Project,
    Aggregate,
    Join,
}

impl StreamingDeltaPipeline {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            processed_deltas: 0,
            total_rows_propagated: 0,
        }
    }

    /// Add a stage to the pipeline.
    pub fn add_stage(&mut self, name: &str, stage_type: DeltaStageType) {
        self.stages.push(DeltaStage {
            name: name.to_string(),
            stage_type,
            processed: 0,
        });
    }

    /// Process a delta through the pipeline.
    pub fn process_delta(&mut self, input_rows: usize) -> usize {
        self.processed_deltas += 1;
        let mut current_rows = input_rows;
        for stage in &mut self.stages {
            current_rows = match stage.stage_type {
                DeltaStageType::Filter => current_rows * 7 / 10, // ~70% selectivity
                DeltaStageType::Project => current_rows,
                DeltaStageType::Aggregate => (current_rows / 10).max(1),
                DeltaStageType::Join => current_rows * 3 / 2, // ~1.5x expansion
            };
            stage.processed += current_rows as u64;
        }
        self.total_rows_propagated += current_rows as u64;
        current_rows
    }

    pub fn stages(&self) -> &[DeltaStage] {
        &self.stages
    }
    pub fn total_deltas(&self) -> u64 {
        self.processed_deltas
    }
    pub fn total_rows(&self) -> u64 {
        self.total_rows_propagated
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Trigger-based Auto-Refresh + Async Background Scheduler
// ---------------------------------------------------------------------------

/// Trigger that fires when data changes to initiate auto-refresh.
#[derive(Debug, Clone)]
pub struct RefreshTriggerRule {
    pub view_name: String,
    pub source_table: String,
    pub trigger_type: TriggerType,
    pub min_delta_rows: usize,
    pub max_staleness_secs: u64,
}

/// When a trigger should fire.
#[derive(Debug, Clone)]
pub enum TriggerType {
    /// Refresh immediately on any change.
    Immediate,
    /// Refresh after accumulating N delta rows.
    Threshold(usize),
    /// Refresh on a time interval.
    Periodic(u64),
    /// Refresh when staleness exceeds a limit.
    Staleness(u64),
}

/// Manager for refresh triggers.
pub struct TriggerManager {
    rules: Vec<RefreshTriggerRule>,
    pending_deltas: std::collections::HashMap<String, usize>,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            pending_deltas: std::collections::HashMap::new(),
        }
    }

    /// Register a trigger rule.
    pub fn register(&mut self, rule: RefreshTriggerRule) {
        self.rules.push(rule);
    }

    /// Record that rows were changed in a source table.
    pub fn record_change(&mut self, table: &str, row_count: usize) {
        *self.pending_deltas.entry(table.to_string()).or_insert(0) += row_count;
    }

    /// Check which views need refresh based on current state.
    pub fn views_needing_refresh(&self) -> Vec<&str> {
        self.rules
            .iter()
            .filter(|rule| {
                let delta = self
                    .pending_deltas
                    .get(&rule.source_table)
                    .copied()
                    .unwrap_or(0);
                match rule.trigger_type {
                    TriggerType::Immediate => delta > 0,
                    TriggerType::Threshold(n) => delta >= n,
                    TriggerType::Periodic(_) => false, // Time-based, not delta-based
                    TriggerType::Staleness(_) => false, // Time-based
                }
            })
            .map(|rule| rule.view_name.as_str())
            .collect()
    }

    /// Clear pending deltas for a table after refresh.
    pub fn clear_deltas(&mut self, table: &str) {
        self.pending_deltas.remove(table);
    }

    pub fn num_rules(&self) -> usize {
        self.rules.len()
    }
}

/// Background refresh scheduler with priority queue.
pub struct BackgroundRefreshScheduler {
    queue: Vec<ScheduledRefresh>,
    max_concurrent: usize,
    active_count: usize,
    completed_count: u64,
}

/// A scheduled refresh task.
#[derive(Debug, Clone)]
pub struct ScheduledRefresh {
    pub view_name: String,
    pub priority: u32,
    pub scheduled_at: std::time::Instant,
    pub status: RefreshJobStatus,
}

/// Status of a refresh job.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshJobStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}

impl BackgroundRefreshScheduler {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            queue: Vec::new(),
            max_concurrent,
            active_count: 0,
            completed_count: 0,
        }
    }

    /// Schedule a view for refresh.
    pub fn schedule(&mut self, view_name: &str, priority: u32) {
        // Don't schedule if already pending
        if self
            .queue
            .iter()
            .any(|r| r.view_name == view_name && r.status == RefreshJobStatus::Pending)
        {
            return;
        }
        self.queue.push(ScheduledRefresh {
            view_name: view_name.to_string(),
            priority,
            scheduled_at: std::time::Instant::now(),
            status: RefreshJobStatus::Pending,
        });
        self.queue.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Get the next job to execute (if capacity allows).
    pub fn next_job(&mut self) -> Option<&str> {
        if self.active_count >= self.max_concurrent {
            return None;
        }
        if let Some(job) = self
            .queue
            .iter_mut()
            .find(|r| r.status == RefreshJobStatus::Pending)
        {
            job.status = RefreshJobStatus::Running;
            self.active_count += 1;
            Some(&job.view_name)
        } else {
            None
        }
    }

    /// Mark a job as completed.
    pub fn complete(&mut self, view_name: &str) {
        if let Some(job) = self
            .queue
            .iter_mut()
            .find(|r| r.view_name == view_name && r.status == RefreshJobStatus::Running)
        {
            job.status = RefreshJobStatus::Completed;
            self.active_count = self.active_count.saturating_sub(1);
            self.completed_count += 1;
        }
    }

    /// Mark a job as failed.
    pub fn fail(&mut self, view_name: &str, error: &str) {
        if let Some(job) = self
            .queue
            .iter_mut()
            .find(|r| r.view_name == view_name && r.status == RefreshJobStatus::Running)
        {
            job.status = RefreshJobStatus::Failed(error.to_string());
            self.active_count = self.active_count.saturating_sub(1);
        }
    }

    pub fn pending_count(&self) -> usize {
        self.queue
            .iter()
            .filter(|r| r.status == RefreshJobStatus::Pending)
            .count()
    }
    pub fn active_count(&self) -> usize {
        self.active_count
    }
    pub fn completed_count(&self) -> u64 {
        self.completed_count
    }
}

// ---------------------------------------------------------------------------
// Query Rewriting for Materialized View Routing
// ---------------------------------------------------------------------------

/// Analyzes queries and rewrites them to use materialized views when beneficial.
#[derive(Debug)]
pub struct MaterializedViewRewriter {
    views: HashMap<String, MaterializedViewSignature>,
}

/// A signature describing what a materialized view computes.
#[derive(Debug, Clone)]
pub struct MaterializedViewSignature {
    pub view_name: String,
    pub source_tables: Vec<String>,
    pub group_by_columns: Vec<String>,
    pub aggregate_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub is_fresh: bool,
}

/// Result of query rewrite analysis.
#[derive(Debug)]
pub struct RewriteResult {
    pub rewritten: bool,
    pub original_tables: Vec<String>,
    pub rewrite_target: Option<String>,
    pub estimated_speedup: f64,
}

impl MaterializedViewRewriter {
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    /// Register a materialized view for query rewriting.
    pub fn register_view(&mut self, sig: MaterializedViewSignature) {
        self.views.insert(sig.view_name.clone(), sig);
    }

    /// Remove a view from the rewriter.
    pub fn deregister_view(&mut self, name: &str) {
        self.views.remove(name);
    }

    /// Attempt to rewrite a query by matching it against registered materialized views.
    /// Returns the rewrite result with the best matching view (if any).
    pub fn try_rewrite(
        &self,
        source_tables: &[String],
        group_by_columns: &[String],
        aggregate_columns: &[String],
    ) -> RewriteResult {
        let mut best_match: Option<(&str, f64)> = None;

        for (name, sig) in &self.views {
            if !sig.is_fresh {
                continue;
            }

            // Check if the view covers all required source tables
            let tables_covered = source_tables.iter().all(|t| sig.source_tables.contains(t));
            if !tables_covered {
                continue;
            }

            // Check if the view has the required group-by columns
            let groups_covered = group_by_columns
                .iter()
                .all(|g| sig.group_by_columns.contains(g));
            if !groups_covered {
                continue;
            }

            // Check if the view has the required aggregate columns
            let aggs_covered = aggregate_columns
                .iter()
                .all(|a| sig.aggregate_columns.contains(a));
            if !aggs_covered {
                continue;
            }

            // Score: higher is better (exact match preferred)
            let score = (sig.source_tables.len() as f64)
                + (sig.group_by_columns.len() as f64) * 0.5
                + (sig.aggregate_columns.len() as f64) * 0.3;

            if best_match.is_none() || score > best_match.unwrap().1 {
                best_match = Some((name.as_str(), score));
            }
        }

        match best_match {
            Some((name, score)) => RewriteResult {
                rewritten: true,
                original_tables: source_tables.to_vec(),
                rewrite_target: Some(name.to_string()),
                estimated_speedup: score * 10.0,
            },
            None => RewriteResult {
                rewritten: false,
                original_tables: source_tables.to_vec(),
                rewrite_target: None,
                estimated_speedup: 1.0,
            },
        }
    }

    pub fn registered_views(&self) -> Vec<&str> {
        self.views.keys().map(|s| s.as_str()).collect()
    }
}

// ---------------------------------------------------------------------------
// Cascade Refresh Executor
// ---------------------------------------------------------------------------

/// Executes cascade refreshes through the dependency graph.
#[derive(Debug)]
pub struct CascadeRefreshExecutor {
    refresh_order: Vec<String>,
    stats: CascadeRefreshStats,
}

/// Statistics from a cascade refresh.
#[derive(Debug, Clone, Default)]
pub struct CascadeRefreshStats {
    pub views_refreshed: usize,
    pub views_skipped: usize,
    pub total_rows_refreshed: u64,
    pub errors: Vec<String>,
}

impl CascadeRefreshExecutor {
    /// Build a cascade refresh plan from the dependency graph.
    pub fn plan(graph: &ViewDependencyGraph, root_table: &str) -> Self {
        let affected = graph.affected_views(root_table);

        Self {
            refresh_order: affected,
            stats: CascadeRefreshStats::default(),
        }
    }

    /// Get the planned refresh order.
    pub fn refresh_order(&self) -> &[String] {
        &self.refresh_order
    }

    /// Record a successful refresh.
    pub fn record_refresh(&mut self, _view_name: &str, rows: u64) {
        self.stats.views_refreshed += 1;
        self.stats.total_rows_refreshed += rows;
    }

    /// Record a skipped view.
    pub fn record_skip(&mut self, _view_name: &str) {
        self.stats.views_skipped += 1;
    }

    /// Record an error during refresh.
    pub fn record_error(&mut self, view_name: &str, error: &str) {
        self.stats.errors.push(format!("{}: {}", view_name, error));
    }

    /// Get cascade refresh statistics.
    pub fn stats(&self) -> &CascadeRefreshStats {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// Filter delta computation
// ---------------------------------------------------------------------------

/// The kind of row-level change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOperation {
    Insert,
    Delete,
    Update,
}

/// A single row-level delta entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaRow {
    pub operation: DeltaOperation,
    pub row_index: usize,
}

/// Computes filter deltas for incremental maintenance.
pub struct FilterDeltaComputer;

impl FilterDeltaComputer {
    /// Given the number of original rows, a list of changed row indices,
    /// and a predicate function, compute the delta rows.
    ///
    /// For each changed index the predicate is evaluated:
    /// - If the index is new (>= original_rows) and passes → Insert
    /// - If the index is new and fails → skip
    /// - If existing and passes → Update
    /// - If existing and fails → Delete
    pub fn compute_filter_delta<F>(
        original_rows: usize,
        changes: &[usize],
        predicate_fn: F,
    ) -> Vec<DeltaRow>
    where
        F: Fn(usize) -> bool,
    {
        let mut deltas = Vec::new();
        for &idx in changes {
            let passes = predicate_fn(idx);
            if idx >= original_rows {
                if passes {
                    deltas.push(DeltaRow {
                        operation: DeltaOperation::Insert,
                        row_index: idx,
                    });
                }
            } else if passes {
                deltas.push(DeltaRow {
                    operation: DeltaOperation::Update,
                    row_index: idx,
                });
            } else {
                deltas.push(DeltaRow {
                    operation: DeltaOperation::Delete,
                    row_index: idx,
                });
            }
        }
        deltas
    }
}

// ---------------------------------------------------------------------------
// Multi-table join delta
// ---------------------------------------------------------------------------

/// Result of computing a join delta.
#[derive(Debug, Clone)]
pub struct JoinDeltaResult {
    pub rows_to_add: usize,
    pub rows_to_remove: usize,
    pub estimated_cost: f64,
}

/// Estimates the delta impact of changes on a join.
pub struct MultiTableJoinDelta {
    pub join_columns: Vec<(String, String)>,
}

impl MultiTableJoinDelta {
    pub fn new(join_columns: Vec<(String, String)>) -> Self {
        Self { join_columns }
    }

    /// Estimate the join delta given change counts on each side.
    ///
    /// `left_changes` – number of changed rows on the left side
    /// `right_table_size` – total rows on the right side
    /// `right_changes` – number of changed rows on the right side
    /// `left_table_size` – total rows on the left side
    pub fn compute_join_delta(
        &self,
        left_changes: usize,
        right_table_size: usize,
        right_changes: usize,
        left_table_size: usize,
    ) -> JoinDeltaResult {
        // Heuristic: each changed row on one side may match a fraction of the other.
        let selectivity = if self.join_columns.is_empty() {
            1.0
        } else {
            1.0 / self.join_columns.len() as f64
        };
        let rows_to_add =
            ((left_changes as f64 * right_table_size as f64 * selectivity)
                + (right_changes as f64 * left_table_size as f64 * selectivity))
                as usize;
        let rows_to_remove = (rows_to_add as f64 * 0.1) as usize; // estimate 10% removals
        let estimated_cost = (left_changes + right_changes) as f64
            * (left_table_size + right_table_size) as f64
            * selectivity;

        JoinDeltaResult {
            rows_to_add,
            rows_to_remove,
            estimated_cost,
        }
    }
}

// ---------------------------------------------------------------------------
// Staleness monitor
// ---------------------------------------------------------------------------

/// Staleness information for a single view.
pub struct ViewStaleness {
    pub view_name: String,
    pub last_refresh: u64,
    pub changes_since_refresh: u64,
    pub is_stale: bool,
}

/// Monitors view staleness and prioritizes refreshes.
pub struct StalenessMonitor {
    views: HashMap<String, ViewStaleness>,
    threshold_ms: u64,
}

impl StalenessMonitor {
    pub fn new(threshold_ms: u64) -> Self {
        Self {
            views: HashMap::new(),
            threshold_ms,
        }
    }

    /// Mark a view as freshly refreshed.
    pub fn record_refresh(&mut self, view_name: &str) {
        let entry = self.views.entry(view_name.to_string()).or_insert_with(|| {
            ViewStaleness {
                view_name: view_name.to_string(),
                last_refresh: 0,
                changes_since_refresh: 0,
                is_stale: false,
            }
        });
        entry.last_refresh = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        entry.changes_since_refresh = 0;
        entry.is_stale = false;
    }

    /// Increment the change counter for a view.
    pub fn record_change(&mut self, view_name: &str) {
        let entry = self.views.entry(view_name.to_string()).or_insert_with(|| {
            ViewStaleness {
                view_name: view_name.to_string(),
                last_refresh: 0,
                changes_since_refresh: 0,
                is_stale: false,
            }
        });
        entry.changes_since_refresh += 1;
    }

    /// Return references to views that are stale (not refreshed within threshold).
    pub fn check_staleness(&mut self, current_time: u64) -> Vec<&ViewStaleness> {
        for v in self.views.values_mut() {
            v.is_stale = current_time.saturating_sub(v.last_refresh) > self.threshold_ms;
        }
        self.views.values().filter(|v| v.is_stale).collect()
    }

    /// The view with the highest change count that is stale.
    pub fn most_stale_view(&self) -> Option<&ViewStaleness> {
        self.views
            .values()
            .filter(|v| v.is_stale || v.changes_since_refresh > 0)
            .max_by_key(|v| v.changes_since_refresh)
    }
}

// ---------------------------------------------------------------------------
// View Refresh Cost Prediction
// ---------------------------------------------------------------------------

/// Predicts refresh costs based on historical observations to decide between
/// incremental vs. full refresh.
#[derive(Debug)]
pub struct RefreshCostPredictor {
    /// Historical observations: (change_count, full_refresh_ms, incremental_refresh_ms)
    observations: Vec<(usize, f64, f64)>,
    /// Crossover point: above this change count, full refresh is cheaper.
    crossover_threshold: Option<usize>,
}

impl RefreshCostPredictor {
    pub fn new() -> Self {
        Self {
            observations: Vec::new(),
            crossover_threshold: None,
        }
    }

    /// Record an observation of refresh performance.
    pub fn record(&mut self, change_count: usize, full_ms: f64, incremental_ms: f64) {
        self.observations.push((change_count, full_ms, incremental_ms));
        self.recompute_threshold();
    }

    /// Recompute the crossover threshold from observations.
    fn recompute_threshold(&mut self) {
        // Find the change count where incremental cost exceeds full cost
        let mut crossover = None;
        for (changes, full, incr) in &self.observations {
            if incr > full {
                match crossover {
                    None => crossover = Some(*changes),
                    Some(existing) if *changes < existing => crossover = Some(*changes),
                    _ => {}
                }
            }
        }
        self.crossover_threshold = crossover;
    }

    /// Recommend whether to use incremental or full refresh.
    pub fn recommend(&self, pending_changes: usize) -> RefreshDecision {
        match self.crossover_threshold {
            Some(threshold) if pending_changes >= threshold => RefreshDecision::FullRefresh,
            _ => RefreshDecision::Incremental,
        }
    }

    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }

    pub fn crossover_threshold(&self) -> Option<usize> {
        self.crossover_threshold
    }
}

impl Default for RefreshCostPredictor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Cascading View Refresh Coordinator
// ---------------------------------------------------------------------------

/// Coordinates refresh of materialized views that depend on each other,
/// ensuring correct ordering (topological sort).
#[derive(Debug)]
pub struct CascadeRefreshCoordinator {
    /// view_name -> list of views that depend on it
    dependents: HashMap<String, Vec<String>>,
    /// view_name -> list of views it depends on
    dependencies: HashMap<String, Vec<String>>,
}

impl CascadeRefreshCoordinator {
    pub fn new() -> Self {
        Self {
            dependents: HashMap::new(),
            dependencies: HashMap::new(),
        }
    }

    /// Register that `dependent` depends on `base`.
    pub fn add_dependency(&mut self, dependent: &str, base: &str) {
        self.dependents
            .entry(base.to_string())
            .or_default()
            .push(dependent.to_string());
        self.dependencies
            .entry(dependent.to_string())
            .or_default()
            .push(base.to_string());
    }

    /// Compute refresh order using topological sort.
    /// Returns view names in the order they should be refreshed.
    pub fn refresh_order(&self, changed_view: &str) -> Vec<String> {
        let mut order = Vec::new();
        let mut visited = std::collections::HashSet::new();
        self.topo_visit(changed_view, &mut visited, &mut order);
        order
    }

    fn topo_visit(
        &self,
        view: &str,
        visited: &mut std::collections::HashSet<String>,
        order: &mut Vec<String>,
    ) {
        if visited.contains(view) {
            return;
        }
        visited.insert(view.to_string());
        order.push(view.to_string());

        if let Some(deps) = self.dependents.get(view) {
            for dep in deps {
                self.topo_visit(dep, visited, order);
            }
        }
    }

    /// Check if adding a dependency would create a cycle.
    pub fn would_create_cycle(&self, dependent: &str, base: &str) -> bool {
        // If base transitively depends on dependent, adding this edge creates a cycle
        let mut visited = std::collections::HashSet::new();
        self.has_path(dependent, base, &mut visited)
    }

    fn has_path(
        &self,
        from: &str,
        to: &str,
        visited: &mut std::collections::HashSet<String>,
    ) -> bool {
        if from == to {
            return true;
        }
        if visited.contains(from) {
            return false;
        }
        visited.insert(from.to_string());
        if let Some(deps) = self.dependents.get(from) {
            for dep in deps {
                if self.has_path(dep, to, visited) {
                    return true;
                }
            }
        }
        false
    }
}

impl Default for CascadeRefreshCoordinator {
    fn default() -> Self {
        Self::new()
    }
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

    // --- Version Vector tests ---

    #[test]
    fn test_version_vector_basic() {
        let mut vv = VersionVector::new();
        assert_eq!(vv.version_of("t1"), 0);

        vv.advance("t1", 5);
        assert_eq!(vv.version_of("t1"), 5);

        // Advance only moves forward
        vv.advance("t1", 3);
        assert_eq!(vv.version_of("t1"), 5);

        vv.advance("t1", 10);
        assert_eq!(vv.version_of("t1"), 10);
    }

    #[test]
    fn test_version_vector_dominates() {
        let mut a = VersionVector::new();
        a.advance("t1", 5);
        a.advance("t2", 3);

        let mut b = VersionVector::new();
        b.advance("t1", 4);
        b.advance("t2", 2);

        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));

        // Equal vectors dominate each other
        let c = a.clone();
        assert!(a.dominates(&c));
        assert!(c.dominates(&a));
    }

    #[test]
    fn test_version_vector_merge() {
        let mut a = VersionVector::new();
        a.advance("t1", 5);
        a.advance("t2", 1);

        let mut b = VersionVector::new();
        b.advance("t1", 3);
        b.advance("t2", 7);
        b.advance("t3", 2);

        a.merge(&b);
        assert_eq!(a.version_of("t1"), 5); // max(5,3)
        assert_eq!(a.version_of("t2"), 7); // max(1,7)
        assert_eq!(a.version_of("t3"), 2); // new from b
    }

    // --- IncrementalExecutor tests ---

    #[test]
    fn test_executor_identity_transform() {
        let batch = make_test_batch();
        let result =
            IncrementalExecutor::apply_transform(&DeltaTransform::Identity, &[batch.clone()], None)
                .unwrap();
        let output = result.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 3);
    }

    #[test]
    fn test_executor_filter_transform() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![10, 25, 30, 15, 40])) as ArrayRef],
        )
        .unwrap();

        let result = IncrementalExecutor::apply_transform(
            &DeltaTransform::Filter {
                predicate: "age > 20".to_string(),
            },
            &[batch],
            None,
        )
        .unwrap();

        let output = result.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 3); // 25, 30, 40
    }

    #[test]
    fn test_executor_project_transform() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("city", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![30, 25])) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(vec!["NYC", "LA"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = IncrementalExecutor::apply_transform(
            &DeltaTransform::Project {
                columns: vec!["name".to_string(), "age".to_string()],
            },
            &[batch],
            None,
        )
        .unwrap();

        let output = result.unwrap();
        assert_eq!(output[0].num_columns(), 2);
        assert_eq!(output[0].schema().field(0).name(), "name");
        assert_eq!(output[0].schema().field(1).name(), "age");
    }

    #[test]
    fn test_executor_filter_project_transform() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["Alice", "Bob", "Eve"])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![30, 15, 25])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = IncrementalExecutor::apply_transform(
            &DeltaTransform::FilterProject {
                predicate: "age > 20".to_string(),
                columns: vec!["name".to_string()],
            },
            &[batch],
            None,
        )
        .unwrap();

        let output = result.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2); // Alice(30) and Eve(25)
        assert_eq!(output[0].num_columns(), 1); // only name
    }

    #[test]
    fn test_executor_full_recompute_returns_none() {
        let batch = make_test_batch();
        let result =
            IncrementalExecutor::apply_transform(&DeltaTransform::FullRecompute, &[batch], None)
                .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_pipeline_apply_incremental() {
        let pipeline = IncrementalPipeline::new();
        pipeline.register_view(
            "young_users",
            "SELECT name FROM users WHERE age > 18",
            &["users".to_string()],
        );

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![25, 15])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = pipeline
            .apply_incremental("young_users", &[batch], None)
            .unwrap();
        let output = result.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1); // Only Alice (age 25 > 18)
        assert_eq!(output[0].num_columns(), 1); // Only name column
    }

    // --- RefreshTrigger tests ---

    #[test]
    fn test_refresh_trigger_on_commit() {
        let trigger = RefreshTrigger::OnCommit;
        assert!(trigger.should_trigger(0, 0.0));
        assert!(trigger.should_trigger(100, 999.0));
    }

    #[test]
    fn test_refresh_trigger_after_changes() {
        let trigger = RefreshTrigger::AfterChanges { threshold: 10 };
        assert!(!trigger.should_trigger(5, 0.0));
        assert!(!trigger.should_trigger(9, 100.0));
        assert!(trigger.should_trigger(10, 0.0));
        assert!(trigger.should_trigger(20, 0.0));
    }

    #[test]
    fn test_refresh_trigger_periodic() {
        let trigger = RefreshTrigger::Periodic { interval_secs: 60 };
        assert!(!trigger.should_trigger(100, 30.0));
        assert!(trigger.should_trigger(0, 60.0));
        assert!(trigger.should_trigger(0, 120.0));

        let manual = RefreshTrigger::Manual;
        assert!(!manual.should_trigger(1000, 9999.0));
    }

    // --- RefreshQueue tests ---

    #[test]
    fn test_refresh_queue_priority_ordering() {
        let queue = RefreshQueue::new();
        assert!(queue.is_empty());

        queue.enqueue("view_low", RefreshPriority::Low, "low priority");
        queue.enqueue("view_high", RefreshPriority::High, "high priority");
        queue.enqueue("view_normal", RefreshPriority::Normal, "normal priority");

        assert_eq!(queue.len(), 3);

        let first = queue.dequeue().unwrap();
        assert_eq!(first.view_name, "view_high");

        let second = queue.dequeue().unwrap();
        assert_eq!(second.view_name, "view_normal");

        let third = queue.dequeue().unwrap();
        assert_eq!(third.view_name, "view_low");

        assert!(queue.is_empty());
    }

    #[test]
    fn test_refresh_queue_dequeue() {
        let queue = RefreshQueue::new();
        assert!(queue.dequeue().is_none());

        queue.enqueue("v1", RefreshPriority::Normal, "reason1");
        queue.enqueue("v2", RefreshPriority::Critical, "reason2");

        let views = queue.pending_views();
        assert_eq!(views.len(), 2);
        assert!(views.contains(&"v1".to_string()));
        assert!(views.contains(&"v2".to_string()));

        let entry = queue.dequeue().unwrap();
        assert_eq!(entry.view_name, "v2");
        assert_eq!(entry.priority, RefreshPriority::Critical);
        assert_eq!(entry.trigger_reason, "reason2");

        assert_eq!(queue.len(), 1);
    }

    // --- RefreshMetrics tests ---

    #[test]
    fn test_refresh_metrics_recording() {
        let mut metrics = RefreshMetrics::default();
        assert_eq!(metrics.total_refreshes, 0);

        metrics.record_refresh(true, 100, 500);
        assert_eq!(metrics.total_refreshes, 1);
        assert_eq!(metrics.total_incremental_refreshes, 1);
        assert_eq!(metrics.total_full_refreshes, 0);
        assert_eq!(metrics.total_refresh_time_ms, 100);
        assert_eq!(metrics.total_rows_processed, 500);

        metrics.record_refresh(false, 200, 1000);
        assert_eq!(metrics.total_refreshes, 2);
        assert_eq!(metrics.total_incremental_refreshes, 1);
        assert_eq!(metrics.total_full_refreshes, 1);

        metrics.record_failure();
        assert_eq!(metrics.failed_refreshes, 1);

        metrics.record_skip();
        assert_eq!(metrics.skipped_refreshes, 1);
    }

    #[test]
    fn test_refresh_metrics_ratios() {
        let mut metrics = RefreshMetrics::default();

        // Edge case: no refreshes yet
        assert_eq!(metrics.incremental_ratio(), 0.0);
        assert_eq!(metrics.avg_refresh_time_ms(), 0.0);
        assert_eq!(metrics.success_rate(), 1.0);

        metrics.record_refresh(true, 100, 50);
        metrics.record_refresh(true, 200, 100);
        metrics.record_refresh(false, 300, 200);

        // 2 incremental out of 3 total
        let ratio = metrics.incremental_ratio();
        assert!((ratio - 2.0 / 3.0).abs() < 1e-9);

        // avg time = (100 + 200 + 300) / 3 = 200
        assert!((metrics.avg_refresh_time_ms() - 200.0).abs() < 1e-9);

        // 3 successes, 0 failures
        assert!((metrics.success_rate() - 1.0).abs() < 1e-9);

        metrics.record_failure();
        // 3 successes out of 4 total (3 + 1 failure)
        assert!((metrics.success_rate() - 0.75).abs() < 1e-9);
    }

    // --- ViewConsistencyChecker tests ---

    #[test]
    fn test_consistency_checker() {
        // Consistent: rows match and staleness low
        let report = ViewConsistencyChecker::check_row_count("mv_sales", 100, 100, 5.0);
        assert_eq!(report.view_name, "mv_sales");
        assert!(report.is_consistent);
        assert!(report.row_count_match);
        assert_eq!(report.view_row_count, 100);
        assert_eq!(report.expected_row_count, 100);

        // Inconsistent: row count mismatch
        let report = ViewConsistencyChecker::check_row_count("mv_sales", 90, 100, 5.0);
        assert!(!report.is_consistent);
        assert!(!report.row_count_match);

        // Inconsistent: staleness too high even if rows match
        let report = ViewConsistencyChecker::check_row_count("mv_sales", 100, 100, 120.0);
        assert!(!report.is_consistent);
        assert!(report.row_count_match);
    }

    #[test]
    fn test_join_delta_inner() {
        let computer = JoinDeltaComputer::new(JoinDeltaType::Inner);
        let (adds, dels) = computer.compute_left_delta(10, 2, 1000);
        assert!(adds > 0);
        assert_eq!(dels, 2);
    }

    #[test]
    fn test_join_delta_should_incremental() {
        let computer = JoinDeltaComputer::new(JoinDeltaType::Inner);
        assert!(computer.should_use_incremental(10, 10000, 10000));
        assert!(!computer.should_use_incremental(8000, 10000, 10000));
    }

    #[test]
    fn test_streaming_delta_pipeline() {
        let mut pipeline = StreamingDeltaPipeline::new();
        pipeline.add_stage("filter", DeltaStageType::Filter);
        pipeline.add_stage("aggregate", DeltaStageType::Aggregate);
        let output = pipeline.process_delta(1000);
        assert!(output < 1000); // Should reduce
        assert_eq!(pipeline.total_deltas(), 1);
    }

    #[test]
    fn test_trigger_manager() {
        let mut mgr = TriggerManager::new();
        mgr.register(RefreshTriggerRule {
            view_name: "mv_sales".to_string(),
            source_table: "sales".to_string(),
            trigger_type: TriggerType::Threshold(100),
            min_delta_rows: 0,
            max_staleness_secs: 0,
        });
        mgr.record_change("sales", 50);
        assert!(mgr.views_needing_refresh().is_empty());
        mgr.record_change("sales", 60);
        assert!(mgr.views_needing_refresh().contains(&"mv_sales"));
    }

    #[test]
    fn test_trigger_manager_immediate() {
        let mut mgr = TriggerManager::new();
        mgr.register(RefreshTriggerRule {
            view_name: "mv_users".to_string(),
            source_table: "users".to_string(),
            trigger_type: TriggerType::Immediate,
            min_delta_rows: 0,
            max_staleness_secs: 0,
        });
        mgr.record_change("users", 1);
        assert_eq!(mgr.views_needing_refresh(), vec!["mv_users"]);
    }

    #[test]
    fn test_background_scheduler() {
        let mut scheduler = BackgroundRefreshScheduler::new(2);
        scheduler.schedule("view_a", 1);
        scheduler.schedule("view_b", 10);
        assert_eq!(scheduler.pending_count(), 2);

        // Higher priority first
        let next = scheduler.next_job().map(|s| s.to_string());
        assert_eq!(next.as_deref(), Some("view_b"));
        assert_eq!(scheduler.active_count(), 1);
    }

    #[test]
    fn test_background_scheduler_complete() {
        let mut scheduler = BackgroundRefreshScheduler::new(2);
        scheduler.schedule("view_a", 1);
        let _ = scheduler.next_job().map(|s| s.to_string());
        scheduler.complete("view_a");
        assert_eq!(scheduler.completed_count(), 1);
        assert_eq!(scheduler.active_count(), 0);
    }

    // -----------------------------------------------------------------------
    // MaterializedViewRewriter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rewriter_exact_match() {
        let mut rewriter = MaterializedViewRewriter::new();
        rewriter.register_view(MaterializedViewSignature {
            view_name: "sales_summary".into(),
            source_tables: vec!["sales".into()],
            group_by_columns: vec!["region".into()],
            aggregate_columns: vec!["total_amount".into()],
            filter_columns: vec![],
            is_fresh: true,
        });

        let result = rewriter.try_rewrite(
            &["sales".into()],
            &["region".into()],
            &["total_amount".into()],
        );
        assert!(result.rewritten);
        assert_eq!(result.rewrite_target, Some("sales_summary".into()));
        assert!(result.estimated_speedup > 1.0);
    }

    #[test]
    fn test_rewriter_stale_view_skipped() {
        let mut rewriter = MaterializedViewRewriter::new();
        rewriter.register_view(MaterializedViewSignature {
            view_name: "stale_view".into(),
            source_tables: vec!["orders".into()],
            group_by_columns: vec!["status".into()],
            aggregate_columns: vec!["count".into()],
            filter_columns: vec![],
            is_fresh: false,
        });

        let result =
            rewriter.try_rewrite(&["orders".into()], &["status".into()], &["count".into()]);
        assert!(!result.rewritten);
    }

    #[test]
    fn test_rewriter_no_match() {
        let rewriter = MaterializedViewRewriter::new();
        let result =
            rewriter.try_rewrite(&["unknown_table".into()], &["col".into()], &["agg".into()]);
        assert!(!result.rewritten);
        assert_eq!(result.estimated_speedup, 1.0);
    }

    // -----------------------------------------------------------------------
    // CascadeRefreshExecutor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cascade_refresh_plan() {
        let mut graph = ViewDependencyGraph::new();
        graph.add_view("view_a", &["base_table".into()]);
        graph.add_view("view_b", &["view_a".into()]);
        graph.add_view("view_c", &["view_b".into()]);

        let executor = CascadeRefreshExecutor::plan(&graph, "base_table");
        let order = executor.refresh_order();
        // view_a depends on base_table, view_b on view_a, view_c on view_b
        assert!(!order.is_empty());
        assert!(order.contains(&"view_a".to_string()));
    }

    #[test]
    fn test_cascade_refresh_stats() {
        let graph = ViewDependencyGraph::new();
        let mut executor = CascadeRefreshExecutor::plan(&graph, "table_x");

        executor.record_refresh("v1", 1000);
        executor.record_refresh("v2", 500);
        executor.record_skip("v3");
        executor.record_error("v4", "timeout");

        let stats = executor.stats();
        assert_eq!(stats.views_refreshed, 2);
        assert_eq!(stats.total_rows_refreshed, 1500);
        assert_eq!(stats.views_skipped, 1);
        assert_eq!(stats.errors.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Auto-refresh and query rewrite tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_refresh_trigger() {
        let trigger1 = RefreshTrigger::OnCommit;
        let trigger2 = RefreshTrigger::Manual;
        let trigger3 = RefreshTrigger::AfterChanges { threshold: 1000 };

        assert!(matches!(trigger1, RefreshTrigger::OnCommit));
        assert!(matches!(trigger2, RefreshTrigger::Manual));
        assert!(matches!(
            trigger3,
            RefreshTrigger::AfterChanges { threshold: 1000 }
        ));
    }

    #[test]
    fn test_auto_refresh_manager() {
        let mut manager = AutoRefreshManager::new();

        manager.register_view(
            "sales_view",
            "SELECT * FROM sales",
            RefreshTrigger::OnCommit,
            vec!["sales"],
        );

        assert_eq!(manager.view_count(), 1);

        // Notify table change
        let views_to_refresh = manager.notify_table_change("sales");
        assert_eq!(views_to_refresh.len(), 1);
        assert_eq!(views_to_refresh[0], "sales_view");
    }

    #[test]
    fn test_auto_refresh_threshold() {
        let mut manager = AutoRefreshManager::new();

        manager.register_view(
            "summary_view",
            "SELECT COUNT(*) FROM orders",
            RefreshTrigger::AfterChanges { threshold: 100 },
            vec!["orders"],
        );

        // Accumulate changes - should not trigger yet
        for _ in 0..99 {
            let views = manager.notify_table_change("orders");
            assert!(views.is_empty()); // Should not trigger yet
        }
        let needs_refresh = manager.check_thresholds();
        assert!(needs_refresh.is_empty());

        // One more change should trigger during notify
        let views = manager.notify_table_change("orders");
        assert_eq!(views.len(), 1); // Should trigger immediately in notify_table_change
        assert_eq!(views[0], "summary_view");
    }

    #[test]
    fn test_query_rewriter() {
        let mut rewriter = QueryRewriter::new();

        let view_info = MaterializedViewInfo {
            name: "user_summary".to_string(),
            source_query: "SELECT user_id, COUNT(*) FROM events GROUP BY user_id".to_string(),
            source_tables: vec!["events".to_string()],
            last_refreshed: std::time::SystemTime::now(),
        };

        rewriter.add_view(view_info);

        // Exact match should rewrite
        let sql = "SELECT user_id, COUNT(*) FROM events GROUP BY user_id";
        let rewritten = rewriter.rewrite_with_materialized_views(sql, &rewriter.available_views());
        assert!(rewritten.is_some());
        assert!(rewritten.unwrap().contains("user_summary"));
    }

    #[test]
    fn test_query_rewriter_no_match() {
        let rewriter = QueryRewriter::new();
        let sql = "SELECT * FROM completely_different_table";
        let rewritten = rewriter.rewrite_with_materialized_views(sql, &rewriter.available_views());
        assert!(rewritten.is_none());
    }

    // -----------------------------------------------------------------------
    // FilterDeltaComputer tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_delta_inserts() {
        // 5 original rows, changes at indices 5 and 6 (new rows)
        let deltas = FilterDeltaComputer::compute_filter_delta(5, &[5, 6], |idx| idx % 2 == 0);
        // idx 5 fails predicate (odd), idx 6 passes → Insert
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(deltas[0].row_index, 6);
    }

    #[test]
    fn test_filter_delta_updates_and_deletes() {
        // 10 original rows, changes at indices 2 and 3
        let deltas = FilterDeltaComputer::compute_filter_delta(10, &[2, 3], |idx| idx == 2);
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].operation, DeltaOperation::Update); // idx 2 passes
        assert_eq!(deltas[1].operation, DeltaOperation::Delete); // idx 3 fails
    }

    // -----------------------------------------------------------------------
    // MultiTableJoinDelta tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_delta_basic() {
        let join = MultiTableJoinDelta::new(vec![("id".into(), "user_id".into())]);
        let result = join.compute_join_delta(10, 1000, 5, 500);
        assert!(result.rows_to_add > 0);
        assert!(result.estimated_cost > 0.0);
    }

    #[test]
    fn test_join_delta_no_changes() {
        let join = MultiTableJoinDelta::new(vec![("id".into(), "id".into())]);
        let result = join.compute_join_delta(0, 1000, 0, 1000);
        assert_eq!(result.rows_to_add, 0);
        assert_eq!(result.rows_to_remove, 0);
    }

    // -----------------------------------------------------------------------
    // StalenessMonitor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_staleness_monitor_basic() {
        let mut monitor = StalenessMonitor::new(1000); // 1 sec threshold
        monitor.record_refresh("mv1");
        monitor.record_change("mv1");
        monitor.record_change("mv1");

        // With current_time far in the future, the view should be stale
        let stale = monitor.check_staleness(u64::MAX);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].view_name, "mv1");
        assert_eq!(stale[0].changes_since_refresh, 2);
    }

    #[test]
    fn test_staleness_monitor_most_stale() {
        let mut monitor = StalenessMonitor::new(0);
        monitor.record_change("mv1");
        monitor.record_change("mv2");
        monitor.record_change("mv2");
        monitor.record_change("mv2");

        let most = monitor.most_stale_view();
        assert!(most.is_some());
        assert_eq!(most.unwrap().view_name, "mv2");
    }

    // -----------------------------------------------------------------------
    // Refresh Cost Predictor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_refresh_cost_predictor_incremental() {
        let mut predictor = RefreshCostPredictor::new();
        // Small changes: incremental is cheaper
        predictor.record(10, 100.0, 20.0);
        predictor.record(50, 100.0, 80.0);

        let decision = predictor.recommend(30);
        assert!(matches!(decision, RefreshDecision::Incremental));
    }

    #[test]
    fn test_refresh_cost_predictor_full() {
        let mut predictor = RefreshCostPredictor::new();
        // At 100 changes, incremental becomes more expensive
        predictor.record(50, 100.0, 80.0);
        predictor.record(100, 100.0, 150.0);

        let decision = predictor.recommend(200);
        assert!(matches!(decision, RefreshDecision::FullRefresh));
    }

    #[test]
    fn test_refresh_cost_predictor_crossover() {
        let mut predictor = RefreshCostPredictor::new();
        predictor.record(10, 500.0, 50.0);
        predictor.record(1000, 500.0, 600.0);
        assert_eq!(predictor.crossover_threshold(), Some(1000));
    }

    // -----------------------------------------------------------------------
    // Cascade Refresh Coordinator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cascade_refresh_order() {
        let mut coord = CascadeRefreshCoordinator::new();
        coord.add_dependency("mv_summary", "mv_daily");
        coord.add_dependency("mv_daily", "base_table");

        let order = coord.refresh_order("base_table");
        assert_eq!(order[0], "base_table");
        // mv_daily should come before mv_summary
        let daily_pos = order.iter().position(|v| v == "mv_daily");
        let summary_pos = order.iter().position(|v| v == "mv_summary");
        assert!(daily_pos < summary_pos);
    }

    #[test]
    fn test_cascade_cycle_detection() {
        let mut coord = CascadeRefreshCoordinator::new();
        coord.add_dependency("b", "a");
        coord.add_dependency("c", "b");
        // Adding a->c would create a cycle: a->b->c->a
        assert!(coord.would_create_cycle("a", "c"));
        assert!(!coord.would_create_cycle("d", "a"));
    }
}

// ---------------------------------------------------------------------------
// Auto-refresh and query rewrite implementation
// ---------------------------------------------------------------------------

/// Auto-refresh manager for materialized views.
#[derive(Debug)]
pub struct AutoRefreshManager {
    views: RwLock<HashMap<String, ViewRefreshConfig>>,
    change_counts: RwLock<HashMap<String, usize>>,
}

#[derive(Debug, Clone)]
struct ViewRefreshConfig {
    name: String,
    query: String,
    trigger: RefreshTrigger,
    source_tables: Vec<String>,
}

impl AutoRefreshManager {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
            change_counts: RwLock::new(HashMap::new()),
        }
    }

    /// Register a materialized view with auto-refresh.
    pub fn register_view(
        &mut self,
        name: impl Into<String>,
        query: impl Into<String>,
        trigger: RefreshTrigger,
        source_tables: Vec<impl Into<String>>,
    ) {
        let name = name.into();
        let config = ViewRefreshConfig {
            name: name.clone(),
            query: query.into(),
            trigger,
            source_tables: source_tables.into_iter().map(|s| s.into()).collect(),
        };

        self.views.write().insert(name.clone(), config);
        self.change_counts.write().insert(name, 0);
    }

    /// Notify that a table has changed, triggering refresh for dependent views.
    pub fn notify_table_change(&self, table_name: &str) -> Vec<String> {
        let views = self.views.read();
        let mut views_to_refresh = Vec::new();

        for (view_name, config) in views.iter() {
            if config.source_tables.iter().any(|t| t == table_name) {
                match &config.trigger {
                    RefreshTrigger::OnCommit => {
                        views_to_refresh.push(view_name.clone());
                    }
                    RefreshTrigger::AfterChanges { threshold } => {
                        let mut counts = self.change_counts.write();
                        let count = counts.entry(view_name.clone()).or_insert(0);
                        *count += 1;
                        if *count >= *threshold as usize {
                            views_to_refresh.push(view_name.clone());
                            *count = 0; // Reset after refresh
                        }
                    }
                    _ => {}
                }
            }
        }

        views_to_refresh
    }

    /// Check for views that need refresh based on threshold.
    pub fn check_thresholds(&self) -> Vec<String> {
        let views = self.views.read();
        let change_counts = self.change_counts.read();
        let mut needs_refresh = Vec::new();

        for (view_name, config) in views.iter() {
            if let RefreshTrigger::AfterChanges { threshold } = &config.trigger {
                if let Some(count) = change_counts.get(view_name) {
                    if *count >= *threshold as usize {
                        needs_refresh.push(view_name.clone());
                    }
                }
            }
        }

        needs_refresh
    }

    /// Get the number of registered views.
    pub fn view_count(&self) -> usize {
        self.views.read().len()
    }

    /// Get all registered view names.
    pub fn view_names(&self) -> Vec<String> {
        self.views.read().keys().cloned().collect()
    }

    /// Unregister a view.
    pub fn unregister_view(&mut self, name: &str) -> bool {
        let removed = self.views.write().remove(name).is_some();
        if removed {
            self.change_counts.write().remove(name);
        }
        removed
    }
}

impl Default for AutoRefreshManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a materialized view for query rewriting.
#[derive(Debug, Clone)]
pub struct MaterializedViewInfo {
    pub name: String,
    pub source_query: String,
    pub source_tables: Vec<String>,
    pub last_refreshed: std::time::SystemTime,
}

/// Query rewriter that substitutes materialized views.
#[derive(Debug)]
pub struct QueryRewriter {
    views: Vec<MaterializedViewInfo>,
}

impl QueryRewriter {
    pub fn new() -> Self {
        Self { views: Vec::new() }
    }

    /// Add a materialized view to the rewriter.
    pub fn add_view(&mut self, view: MaterializedViewInfo) {
        self.views.push(view);
    }

    /// Get all available views.
    pub fn available_views(&self) -> Vec<MaterializedViewInfo> {
        self.views.clone()
    }

    /// Try to rewrite a query using materialized views.
    pub fn rewrite_with_materialized_views(
        &self,
        sql: &str,
        available_views: &[MaterializedViewInfo],
    ) -> Option<String> {
        let normalized_sql = sql.trim().to_lowercase();

        for view in available_views {
            let normalized_view_query = view.source_query.trim().to_lowercase();

            // Exact match - substitute the entire query
            if normalized_sql == normalized_view_query {
                return Some(format!("SELECT * FROM {}", view.name));
            }

            // Check if query is a subset (same tables with extra WHERE clause)
            if normalized_sql.contains(&normalized_view_query.split("where").next().unwrap_or("")) {
                // This is a simplification - a full implementation would parse the SQL
                // and check if the query can be answered from the materialized view
                if normalized_sql.contains("where") && !normalized_view_query.contains("where") {
                    // Query has WHERE clause, view doesn't - potentially can use view with filter
                    let where_clause = normalized_sql.split("where").nth(1).unwrap_or("");
                    return Some(format!(
                        "SELECT * FROM {} WHERE {}",
                        view.name, where_clause
                    ));
                }
            }
        }

        None
    }
}

impl Default for QueryRewriter {
    fn default() -> Self {
        Self::new()
    }
}
