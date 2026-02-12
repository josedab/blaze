//! Distributed Query Execution
//!
//! This module provides distributed query execution capabilities, allowing Blaze
//! to split query plans into fragments and coordinate their execution across a
//! cluster of nodes.
//!
//! # Architecture
//!
//! - **DistributedCoordinator**: Central coordinator that manages cluster topology,
//!   splits plans into fragments, and collects results
//! - **ClusterNode**: Represents a node in the cluster with its endpoint and status
//! - **DistributedPlan**: A query plan split into fragments for distributed execution
//! - **PlanFragment**: An individual unit of work targeting one or more nodes
//!
//! # Features
//!
//! - **Plan Splitting**: Automatically split SQL queries into fragments based on
//!   table locations across the cluster
//! - **Node Management**: Register, deregister, and monitor cluster nodes
//! - **Result Merging**: Gather and merge results from remote fragment executions
//! - **Failure Handling**: Retry failed fragments on healthy nodes
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::distributed::{DistributedCoordinator, DistributedConfig, ClusterNode, NodeStatus};
//! use blaze::flight::FlightEndpoint;
//!
//! let config = DistributedConfig::new()
//!     .with_max_fragment_size(128 * 1024 * 1024)
//!     .with_retry_count(3);
//! let coordinator = DistributedCoordinator::new(config);
//!
//! coordinator.add_node(ClusterNode::new("node-1", FlightEndpoint::new("10.0.0.1", 8815)));
//! coordinator.add_node(ClusterNode::new("node-2", FlightEndpoint::new("10.0.0.2", 8815)));
//!
//! let plan = coordinator.plan_query("SELECT * FROM orders JOIN customers ON orders.cid = customers.id")?;
//! let results = coordinator.execute(plan)?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::{BlazeError, Result};
use crate::flight::FlightEndpoint;

// ---------------------------------------------------------------------------
// Node management
// ---------------------------------------------------------------------------

/// Status of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and accepting work.
    Active,
    /// Node is draining — it will finish current work but accept no new fragments.
    Draining,
    /// Node is unreachable or has failed.
    Down,
}

/// Capabilities advertised by a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Maximum number of concurrent fragments the node can execute.
    pub max_concurrent_fragments: usize,
    /// Available memory in bytes.
    pub available_memory_bytes: usize,
    /// Whether the node supports spilling to disk.
    pub supports_spill: bool,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            max_concurrent_fragments: 8,
            available_memory_bytes: 1024 * 1024 * 1024, // 1 GB
            supports_spill: false,
        }
    }
}

/// A node in the distributed cluster.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique identifier for this node.
    pub id: String,
    /// Flight endpoint used to communicate with this node.
    pub endpoint: FlightEndpoint,
    /// Current status of the node.
    pub status: NodeStatus,
    /// Capabilities reported by the node.
    pub capabilities: NodeCapabilities,
}

impl ClusterNode {
    /// Create a new active cluster node with default capabilities.
    pub fn new(id: impl Into<String>, endpoint: FlightEndpoint) -> Self {
        Self {
            id: id.into(),
            endpoint,
            status: NodeStatus::Active,
            capabilities: NodeCapabilities::default(),
        }
    }

    /// Set the node capabilities.
    pub fn with_capabilities(mut self, capabilities: NodeCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Set the node status.
    pub fn with_status(mut self, status: NodeStatus) -> Self {
        self.status = status;
        self
    }

    /// Returns `true` if the node can accept new work.
    pub fn is_available(&self) -> bool {
        self.status == NodeStatus::Active
    }
}

// ---------------------------------------------------------------------------
// Exchange strategies
// ---------------------------------------------------------------------------

/// Strategy used to redistribute data between plan fragments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExchangeStrategy {
    /// No data exchange needed — fragment is self-contained.
    None,
    /// Hash-partition rows on the given key columns and shuffle across nodes.
    Shuffle { keys: Vec<String> },
    /// Broadcast the full result set to every downstream node.
    Broadcast,
    /// Gather all results to the coordinator node.
    Gather,
}

// ---------------------------------------------------------------------------
// Distributed plan representation
// ---------------------------------------------------------------------------

/// Execution status of a single plan fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentStatus {
    /// Waiting to be scheduled.
    Pending,
    /// Currently executing on a node.
    Running,
    /// Completed successfully.
    Completed,
    /// Failed (may be retried).
    Failed,
}

/// A fragment of a distributed query plan.
#[derive(Debug, Clone)]
pub struct PlanFragment {
    /// Unique fragment identifier within a distributed plan.
    pub id: usize,
    /// SQL representation of the fragment.
    pub sql: String,
    /// Node IDs this fragment should execute on.
    pub target_nodes: Vec<String>,
    /// Fragment IDs that must complete before this fragment can start.
    pub dependencies: Vec<usize>,
    /// Exchange strategy used to deliver results downstream.
    pub exchange_type: ExchangeStrategy,
    /// Current execution status.
    pub status: FragmentStatus,
    /// Number of retry attempts remaining.
    pub retries_remaining: usize,
}

impl PlanFragment {
    /// Create a new pending fragment.
    pub fn new(id: usize, sql: impl Into<String>) -> Self {
        Self {
            id,
            sql: sql.into(),
            target_nodes: Vec::new(),
            dependencies: Vec::new(),
            exchange_type: ExchangeStrategy::None,
            status: FragmentStatus::Pending,
            retries_remaining: 0,
        }
    }

    /// Set the target nodes.
    pub fn with_target_nodes(mut self, nodes: Vec<String>) -> Self {
        self.target_nodes = nodes;
        self
    }

    /// Set the dependencies.
    pub fn with_dependencies(mut self, deps: Vec<usize>) -> Self {
        self.dependencies = deps;
        self
    }

    /// Set the exchange strategy.
    pub fn with_exchange(mut self, exchange: ExchangeStrategy) -> Self {
        self.exchange_type = exchange;
        self
    }

    /// Set the number of retries.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries_remaining = retries;
        self
    }

    /// Returns `true` if all dependencies are in the given completed set.
    pub fn dependencies_satisfied(&self, completed: &[usize]) -> bool {
        self.dependencies.iter().all(|dep| completed.contains(dep))
    }
}

/// A distributed query plan consisting of multiple fragments.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// The individual fragments that make up this plan.
    pub fragments: Vec<PlanFragment>,
    /// Index of the root (final gather) fragment.
    pub root_fragment_id: usize,
}

impl DistributedPlan {
    /// Create a new distributed plan.
    pub fn new(fragments: Vec<PlanFragment>, root_fragment_id: usize) -> Self {
        Self {
            fragments,
            root_fragment_id,
        }
    }

    /// Return the total number of fragments.
    pub fn fragment_count(&self) -> usize {
        self.fragments.len()
    }

    /// Return a topological ordering of fragment IDs for execution.
    ///
    /// Fragments with no dependencies come first; the root fragment comes last.
    pub fn execution_order(&self) -> Result<Vec<usize>> {
        let mut order = Vec::with_capacity(self.fragments.len());
        let mut completed: Vec<usize> = Vec::new();
        let mut remaining: Vec<&PlanFragment> = self.fragments.iter().collect();

        while !remaining.is_empty() {
            let before = remaining.len();
            let mut next_remaining = Vec::new();

            for frag in remaining {
                if frag.dependencies_satisfied(&completed) {
                    order.push(frag.id);
                    completed.push(frag.id);
                } else {
                    next_remaining.push(frag);
                }
            }

            remaining = next_remaining;

            if remaining.len() == before {
                return Err(BlazeError::execution(
                    "Cycle detected in distributed plan fragment dependencies",
                ));
            }
        }

        Ok(order)
    }
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Serializable representation of a distributed plan for wire transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedPlan {
    /// JSON-encoded fragment descriptors.
    pub fragments_json: String,
    /// Root fragment identifier.
    pub root_fragment_id: usize,
}

/// Serializable representation of a single fragment (used inside [`SerializedPlan`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedFragment {
    id: usize,
    sql: String,
    target_nodes: Vec<String>,
    dependencies: Vec<usize>,
    exchange_type: ExchangeStrategy,
}

/// Serialize a [`DistributedPlan`] to bytes for network transport.
pub fn serialize_plan(plan: &DistributedPlan) -> Result<Vec<u8>> {
    let frags: Vec<SerializedFragment> = plan
        .fragments
        .iter()
        .map(|f| SerializedFragment {
            id: f.id,
            sql: f.sql.clone(),
            target_nodes: f.target_nodes.clone(),
            dependencies: f.dependencies.clone(),
            exchange_type: f.exchange_type.clone(),
        })
        .collect();

    let fragments_json = serde_json::to_string(&frags)
        .map_err(|e| BlazeError::execution(format!("Plan serialization error: {}", e)))?;

    let serialized = SerializedPlan {
        fragments_json,
        root_fragment_id: plan.root_fragment_id,
    };

    serde_json::to_vec(&serialized)
        .map_err(|e| BlazeError::execution(format!("Plan serialization error: {}", e)))
}

/// Deserialize a [`DistributedPlan`] from bytes.
pub fn deserialize_plan(data: &[u8]) -> Result<DistributedPlan> {
    let serialized: SerializedPlan = serde_json::from_slice(data)
        .map_err(|e| BlazeError::execution(format!("Plan deserialization error: {}", e)))?;

    let frags: Vec<SerializedFragment> = serde_json::from_str(&serialized.fragments_json)
        .map_err(|e| BlazeError::execution(format!("Plan deserialization error: {}", e)))?;

    let fragments = frags
        .into_iter()
        .map(|f| {
            PlanFragment::new(f.id, f.sql)
                .with_target_nodes(f.target_nodes)
                .with_dependencies(f.dependencies)
                .with_exchange(f.exchange_type)
        })
        .collect();

    Ok(DistributedPlan::new(fragments, serialized.root_fragment_id))
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for distributed query execution.
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    /// Queries touching less than this many bytes will run locally.
    pub local_threshold_bytes: usize,
    /// Maximum byte size of a single fragment's input.
    pub max_fragment_size: usize,
    /// Number of times to retry a failed fragment before giving up.
    pub retry_count: usize,
    /// Fragment execution timeout in milliseconds.
    pub timeout_ms: u64,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            local_threshold_bytes: 64 * 1024 * 1024, // 64 MB
            max_fragment_size: 256 * 1024 * 1024,    // 256 MB
            retry_count: 3,
            timeout_ms: 30_000,
        }
    }
}

impl DistributedConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the local execution threshold in bytes.
    pub fn with_local_threshold_bytes(mut self, bytes: usize) -> Self {
        self.local_threshold_bytes = bytes;
        self
    }

    /// Set the maximum fragment size in bytes.
    pub fn with_max_fragment_size(mut self, bytes: usize) -> Self {
        self.max_fragment_size = bytes;
        self
    }

    /// Set the retry count for failed fragments.
    pub fn with_retry_count(mut self, count: usize) -> Self {
        self.retry_count = count;
        self
    }

    /// Set the fragment execution timeout in milliseconds.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Runtime statistics for distributed execution.
#[derive(Debug, Clone, Default)]
pub struct DistributedStats {
    /// Total queries executed.
    pub queries_executed: usize,
    /// Total fragments executed across all queries.
    pub fragments_executed: usize,
    /// Total fragments that failed.
    pub fragments_failed: usize,
    /// Total fragments retried.
    pub fragments_retried: usize,
    /// Total rows returned.
    pub rows_returned: usize,
    /// Total bytes transferred.
    pub bytes_transferred: usize,
}

// ---------------------------------------------------------------------------
// Result collector
// ---------------------------------------------------------------------------

/// Collects and merges [`RecordBatch`] results from multiple fragments.
pub struct ResultCollector {
    results: HashMap<usize, Vec<RecordBatch>>,
}

impl ResultCollector {
    /// Create a new empty result collector.
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }

    /// Record results for a fragment.
    pub fn add_result(&mut self, fragment_id: usize, batches: Vec<RecordBatch>) {
        self.results.insert(fragment_id, batches);
    }

    /// Check whether results have been collected for the given fragment.
    pub fn has_result(&self, fragment_id: usize) -> bool {
        self.results.contains_key(&fragment_id)
    }

    /// Return the number of fragments whose results have been collected.
    pub fn collected_count(&self) -> usize {
        self.results.len()
    }

    /// Merge all collected results into a single list of [`RecordBatch`]es.
    ///
    /// The batches are concatenated in fragment-id order. If all fragments share
    /// the same schema the output is additionally compacted into a single batch.
    pub fn merge_results(&self, fragment_order: &[usize]) -> Result<Vec<RecordBatch>> {
        let mut merged: Vec<RecordBatch> = Vec::new();

        for frag_id in fragment_order {
            if let Some(batches) = self.results.get(frag_id) {
                merged.extend(batches.iter().cloned());
            }
        }

        if merged.is_empty() {
            return Ok(merged);
        }

        // Attempt to compact all batches into one if they share a schema.
        let schema = merged[0].schema();
        let all_same_schema = merged.iter().all(|b| b.schema() == schema);

        if all_same_schema && merged.len() > 1 {
            let compacted = concat_batches(&schema, &merged)
                .map_err(|e| BlazeError::execution(format!("Result merge error: {}", e)))?;
            Ok(vec![compacted])
        } else {
            Ok(merged)
        }
    }
}

impl Default for ResultCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

/// Central coordinator for distributed query execution.
///
/// The coordinator is responsible for:
/// - Tracking the cluster topology (registered nodes)
/// - Splitting SQL queries into distributable fragments
/// - Scheduling fragments on available nodes
/// - Collecting and merging fragment results
/// - Retrying failed fragments on healthy nodes
pub struct DistributedCoordinator {
    nodes: RwLock<Vec<ClusterNode>>,
    config: DistributedConfig,
    stats: RwLock<DistributedStats>,
    /// Maps table names to the node IDs where their data resides.
    table_locations: RwLock<HashMap<String, Vec<String>>>,
}

impl DistributedCoordinator {
    /// Create a new coordinator with the given configuration.
    pub fn new(config: DistributedConfig) -> Self {
        Self {
            nodes: RwLock::new(Vec::new()),
            config,
            stats: RwLock::new(DistributedStats::default()),
            table_locations: RwLock::new(HashMap::new()),
        }
    }

    /// Create a coordinator with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(DistributedConfig::default())
    }

    // -- Node management ----------------------------------------------------

    /// Register a new node in the cluster.
    pub fn add_node(&self, node: ClusterNode) {
        self.nodes.write().push(node);
    }

    /// Remove a node from the cluster by its ID.
    pub fn remove_node(&self, node_id: &str) -> Option<ClusterNode> {
        let mut nodes = self.nodes.write();
        if let Some(pos) = nodes.iter().position(|n| n.id == node_id) {
            Some(nodes.remove(pos))
        } else {
            None
        }
    }

    /// Return a snapshot of all registered nodes.
    pub fn list_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().clone()
    }

    /// Return only nodes that are currently available for work.
    pub fn available_nodes(&self) -> Vec<ClusterNode> {
        self.nodes
            .read()
            .iter()
            .filter(|n| n.is_available())
            .cloned()
            .collect()
    }

    /// Mark a node as down.
    pub fn mark_node_down(&self, node_id: &str) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.iter_mut().find(|n| n.id == node_id) {
            node.status = NodeStatus::Down;
        }
    }

    /// Mark a node as draining.
    pub fn mark_node_draining(&self, node_id: &str) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.iter_mut().find(|n| n.id == node_id) {
            node.status = NodeStatus::Draining;
        }
    }

    /// Return the number of registered nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    // -- Table location management ------------------------------------------

    /// Register the location of a table (which nodes hold its data).
    pub fn register_table_location(&self, table: impl Into<String>, node_ids: Vec<String>) {
        self.table_locations.write().insert(table.into(), node_ids);
    }

    /// Look up which nodes hold data for a table.
    pub fn table_location(&self, table: &str) -> Option<Vec<String>> {
        self.table_locations.read().get(table).cloned()
    }

    // -- Plan splitting ------------------------------------------------------

    /// Split a SQL query into a distributed plan.
    ///
    /// Tables whose locations are registered will produce leaf fragments targeting
    /// the appropriate nodes. A root gather fragment collects all intermediate
    /// results on the coordinator.
    pub fn plan_query(&self, sql: &str) -> Result<DistributedPlan> {
        let available = self.available_nodes();
        if available.is_empty() {
            return Err(BlazeError::execution(
                "No available nodes in the cluster to execute the query",
            ));
        }

        let tables = extract_table_names(sql);
        let locations = self.table_locations.read();
        let retry_count = self.config.retry_count;

        let mut fragments: Vec<PlanFragment> = Vec::new();
        let mut leaf_ids: Vec<usize> = Vec::new();

        if tables.is_empty() {
            // No tables detected — run the whole query as a single fragment on
            // the first available node.
            let frag = PlanFragment::new(0, sql)
                .with_target_nodes(vec![available[0].id.clone()])
                .with_exchange(ExchangeStrategy::Gather)
                .with_retries(retry_count);
            fragments.push(frag);
            leaf_ids.push(0);
        } else {
            for (idx, table) in tables.iter().enumerate() {
                let target_nodes = locations
                    .get(table.as_str())
                    .cloned()
                    .unwrap_or_else(|| vec![available[idx % available.len()].id.clone()]);

                let fragment_sql = format!("SELECT * FROM {}", table);
                let exchange = if target_nodes.len() > 1 {
                    ExchangeStrategy::Shuffle { keys: vec![] }
                } else {
                    ExchangeStrategy::None
                };

                let frag = PlanFragment::new(idx, fragment_sql)
                    .with_target_nodes(target_nodes)
                    .with_exchange(exchange)
                    .with_retries(retry_count);
                leaf_ids.push(idx);
                fragments.push(frag);
            }
        }

        // Root gather fragment
        let root_id = fragments.len();
        let root_frag = PlanFragment::new(root_id, sql.to_string())
            .with_target_nodes(vec![available[0].id.clone()])
            .with_dependencies(leaf_ids)
            .with_exchange(ExchangeStrategy::Gather)
            .with_retries(retry_count);
        fragments.push(root_frag);

        Ok(DistributedPlan::new(fragments, root_id))
    }

    // -- Execution -----------------------------------------------------------

    /// Execute a distributed plan and return the merged results.
    ///
    /// Fragments are executed in dependency order. Currently execution is
    /// simulated locally — a real implementation would dispatch fragments to
    /// remote nodes via the Flight protocol.
    pub fn execute(&self, plan: DistributedPlan) -> Result<Vec<RecordBatch>> {
        let order = plan.execution_order()?;
        let mut collector = ResultCollector::new();
        let start = Instant::now();

        for &frag_id in &order {
            let frag = plan
                .fragments
                .iter()
                .find(|f| f.id == frag_id)
                .ok_or_else(|| {
                    BlazeError::internal(format!("Fragment {} not found in plan", frag_id))
                })?;

            let result = self.execute_fragment(frag);

            match result {
                Ok(batches) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    collector.add_result(frag_id, batches);
                    let mut stats = self.stats.write();
                    stats.fragments_executed += 1;
                    stats.rows_returned += rows;
                }
                Err(e) => {
                    let mut stats = self.stats.write();
                    stats.fragments_failed += 1;
                    return Err(e);
                }
            }
        }

        let mut stats = self.stats.write();
        stats.queries_executed += 1;
        let _elapsed = start.elapsed();

        collector.merge_results(&order)
    }

    /// Execute a single fragment.
    ///
    /// This method implements retry logic: if execution fails and the fragment
    /// has retries remaining, it will be re-attempted on another available node.
    fn execute_fragment(&self, fragment: &PlanFragment) -> Result<Vec<RecordBatch>> {
        let mut attempts = fragment.retries_remaining + 1;
        let mut last_error: Option<BlazeError> = None;

        while attempts > 0 {
            // Pick the first available target node, falling back to any
            // available node in the cluster.
            let target_id = fragment
                .target_nodes
                .iter()
                .find(|nid| {
                    self.nodes
                        .read()
                        .iter()
                        .any(|n| &n.id == *nid && n.is_available())
                })
                .cloned()
                .or_else(|| self.available_nodes().first().map(|n| n.id.clone()));

            match target_id {
                Some(_node_id) => {
                    // In a full implementation we would dispatch the fragment SQL
                    // to the remote node via Flight DoAction/DoGet.  For now we
                    // return an empty batch set to represent a successful remote
                    // execution.
                    return Ok(Vec::new());
                }
                None => {
                    last_error = Some(BlazeError::execution(
                        "No available nodes to execute fragment",
                    ));
                    {
                        self.stats.write().fragments_retried += 1;
                    }
                    attempts -= 1;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            BlazeError::execution("Fragment execution failed after all retries")
        }))
    }

    // -- Statistics ----------------------------------------------------------

    /// Return a snapshot of the current execution statistics.
    pub fn stats(&self) -> DistributedStats {
        self.stats.read().clone()
    }

    /// Reset all execution statistics.
    pub fn reset_stats(&self) {
        *self.stats.write() = DistributedStats::default();
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &DistributedConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Naive extraction of table names from a SQL string.
///
/// This is a best-effort heuristic that looks for `FROM <table>` and
/// `JOIN <table>` patterns.  A production implementation would use the
/// parsed AST instead.
fn extract_table_names(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let upper_tokens: Vec<&str> = upper.split_whitespace().collect();
    let mut tables = Vec::new();

    for (i, token) in upper_tokens.iter().enumerate() {
        if (*token == "FROM" || *token == "JOIN") && i + 1 < tokens.len() {
            let candidate = tokens[i + 1]
                .trim_end_matches(',')
                .trim_end_matches(';')
                .trim_end_matches(')')
                .to_string();
            if !candidate.is_empty()
                && candidate != "("
                && !candidate.starts_with('(')
                && !tables.contains(&candidate)
            {
                tables.push(candidate);
            }
        }
    }

    tables
}

// ---------------------------------------------------------------------------
// Scatter-Gather Execution Engine
// ---------------------------------------------------------------------------

/// Strategy for scattering data to worker nodes.
#[derive(Debug, Clone, PartialEq)]
pub enum ScatterStrategy {
    /// Send entire dataset to all nodes (broadcast).
    Broadcast,
    /// Hash-partition by a key column and send each partition to a different node.
    HashPartition {
        key_column: String,
        num_partitions: usize,
    },
    /// Round-robin distribution of batches to nodes.
    RoundRobin { num_partitions: usize },
    /// Range-partition by a key column.
    RangePartition {
        key_column: String,
        boundaries: Vec<i64>,
    },
}

/// A task assigned to a worker node in the scatter phase.
#[derive(Debug, Clone)]
pub struct ScatterTask {
    pub task_id: String,
    pub node_id: String,
    pub fragment_id: usize,
    pub partition_id: usize,
    pub status: TaskStatus,
    pub attempt: usize,
    pub max_retries: usize,
}

/// Status of a distributed task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

/// Result of a scatter-gather execution.
#[derive(Debug)]
pub struct ScatterGatherResult {
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub retried_tasks: usize,
    pub batches: Vec<RecordBatch>,
}

/// Scatter-gather executor that distributes work across nodes.
pub struct ScatterGatherExecutor {
    coordinator: Arc<DistributedCoordinator>,
    task_tracker: parking_lot::RwLock<HashMap<String, ScatterTask>>,
}

impl ScatterGatherExecutor {
    pub fn new(coordinator: Arc<DistributedCoordinator>) -> Self {
        Self {
            coordinator,
            task_tracker: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Scatter data across available nodes using the given strategy.
    pub fn scatter(
        &self,
        data: &[RecordBatch],
        strategy: &ScatterStrategy,
        fragment_id: usize,
    ) -> Result<Vec<ScatterTask>> {
        let nodes = self.coordinator.available_nodes();
        if nodes.is_empty() {
            return Err(BlazeError::execution("No available nodes for scatter"));
        }

        let num_partitions = match strategy {
            ScatterStrategy::Broadcast => nodes.len(),
            ScatterStrategy::HashPartition { num_partitions, .. } => *num_partitions,
            ScatterStrategy::RoundRobin { num_partitions } => *num_partitions,
            ScatterStrategy::RangePartition { boundaries, .. } => boundaries.len() + 1,
        };

        let mut tasks = Vec::new();
        let max_retries = self.coordinator.config().retry_count;

        for partition_id in 0..num_partitions {
            let node_idx = partition_id % nodes.len();
            let task = ScatterTask {
                task_id: format!("task-{}-{}", fragment_id, partition_id),
                node_id: nodes[node_idx].id.clone(),
                fragment_id,
                partition_id,
                status: TaskStatus::Pending,
                attempt: 0,
                max_retries,
            };
            self.task_tracker
                .write()
                .insert(task.task_id.clone(), task.clone());
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Gather results from completed tasks.
    pub fn gather(&self, tasks: &[ScatterTask]) -> ScatterGatherResult {
        let tracker = self.task_tracker.read();
        let mut completed = 0;
        let mut failed = 0;
        let mut retried = 0;

        for task in tasks {
            if let Some(tracked) = tracker.get(&task.task_id) {
                match tracked.status {
                    TaskStatus::Completed => completed += 1,
                    TaskStatus::Failed => failed += 1,
                    TaskStatus::Retrying => retried += 1,
                    _ => {}
                }
            }
        }

        ScatterGatherResult {
            total_tasks: tasks.len(),
            completed_tasks: completed,
            failed_tasks: failed,
            retried_tasks: retried,
            batches: Vec::new(),
        }
    }

    /// Mark a task as completed.
    pub fn complete_task(&self, task_id: &str) {
        if let Some(task) = self.task_tracker.write().get_mut(task_id) {
            task.status = TaskStatus::Completed;
        }
    }

    /// Mark a task as failed, potentially retrying.
    pub fn fail_task(&self, task_id: &str) -> TaskStatus {
        if let Some(task) = self.task_tracker.write().get_mut(task_id) {
            if task.attempt < task.max_retries {
                task.attempt += 1;
                task.status = TaskStatus::Retrying;
                TaskStatus::Retrying
            } else {
                task.status = TaskStatus::Failed;
                TaskStatus::Failed
            }
        } else {
            TaskStatus::Failed
        }
    }

    /// Cancel all tasks for a fragment.
    pub fn cancel_fragment(&self, fragment_id: usize) {
        let mut tracker = self.task_tracker.write();
        for task in tracker.values_mut() {
            if task.fragment_id == fragment_id {
                task.status = TaskStatus::Cancelled;
            }
        }
    }

    /// Get the status of a specific task.
    pub fn task_status(&self, task_id: &str) -> Option<TaskStatus> {
        self.task_tracker.read().get(task_id).map(|t| t.status)
    }

    /// Get all active (non-terminal) tasks.
    pub fn active_tasks(&self) -> Vec<ScatterTask> {
        self.task_tracker
            .read()
            .values()
            .filter(|t| {
                matches!(
                    t.status,
                    TaskStatus::Pending | TaskStatus::Running | TaskStatus::Retrying
                )
            })
            .cloned()
            .collect()
    }
}

/// Health checker for cluster nodes.
pub struct NodeHealthChecker {
    check_interval_ms: u64,
    failure_threshold: usize,
    failure_counts: parking_lot::RwLock<HashMap<String, usize>>,
}

impl NodeHealthChecker {
    pub fn new(check_interval_ms: u64, failure_threshold: usize) -> Self {
        Self {
            check_interval_ms,
            failure_threshold,
            failure_counts: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Record a successful health check.
    pub fn record_success(&self, node_id: &str) {
        self.failure_counts.write().remove(node_id);
    }

    /// Record a failed health check. Returns true if node should be marked down.
    pub fn record_failure(&self, node_id: &str) -> bool {
        let mut counts = self.failure_counts.write();
        let count = counts.entry(node_id.to_string()).or_insert(0);
        *count += 1;
        *count >= self.failure_threshold
    }

    /// Get the failure count for a node.
    pub fn failure_count(&self, node_id: &str) -> usize {
        self.failure_counts
            .read()
            .get(node_id)
            .copied()
            .unwrap_or(0)
    }

    /// Get the check interval in milliseconds.
    pub fn check_interval_ms(&self) -> u64 {
        self.check_interval_ms
    }
}

// ---------------------------------------------------------------------------
// Data Partitioning
// ---------------------------------------------------------------------------

/// Partitioning scheme for distributing data across nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionScheme {
    /// Hash a column and assign rows to partitions via modulo.
    Hash { column_index: usize },
    /// Range-based partitioning using sorted split points.
    Range {
        column_index: usize,
        split_points: Vec<i64>,
    },
    /// Distribute rows cyclically across partitions.
    RoundRobin,
    /// Clone the entire dataset to every partition.
    Broadcast,
}

/// Partitions `RecordBatch`es according to a [`PartitionScheme`].
pub struct DataPartitioner {
    scheme: PartitionScheme,
    num_partitions: usize,
}

impl DataPartitioner {
    pub fn new(scheme: PartitionScheme, num_partitions: usize) -> Self {
        Self {
            scheme,
            num_partitions,
        }
    }

    /// Split `batches` into `num_partitions` buckets.
    pub fn partition(&self, batches: &[RecordBatch]) -> Result<Vec<Vec<RecordBatch>>> {
        if self.num_partitions == 0 {
            return Err(BlazeError::execution("num_partitions must be > 0"));
        }
        match &self.scheme {
            PartitionScheme::Hash { column_index } => self.hash_partition(batches, *column_index),
            PartitionScheme::Range {
                column_index,
                split_points,
            } => self.range_partition(batches, *column_index, split_points),
            PartitionScheme::RoundRobin => self.round_robin_partition(batches),
            PartitionScheme::Broadcast => self.broadcast_partition(batches),
        }
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn scheme(&self) -> &PartitionScheme {
        &self.scheme
    }

    // -- private helpers -----------------------------------------------------

    fn hash_partition(
        &self,
        batches: &[RecordBatch],
        col_idx: usize,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        use arrow::array::Array;
        let mut buckets: Vec<Vec<RecordBatch>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();

        for batch in batches {
            if col_idx >= batch.num_columns() {
                return Err(BlazeError::execution(format!(
                    "Column index {} out of range ({})",
                    col_idx,
                    batch.num_columns()
                )));
            }
            let col = batch.column(col_idx);
            // Assign each row by hashing its debug representation.
            let mut row_indices: Vec<Vec<usize>> =
                (0..self.num_partitions).map(|_| Vec::new()).collect();
            for row in 0..batch.num_rows() {
                let hash = if col.is_null(row) {
                    0usize
                } else {
                    // Simple hash: use the formatted scalar as bytes.
                    let s = format!("{:?}", col.slice(row, 1));
                    s.bytes().fold(0usize, |acc, b| {
                        acc.wrapping_mul(31).wrapping_add(b as usize)
                    })
                };
                row_indices[hash % self.num_partitions].push(row);
            }
            for (part, indices) in row_indices.into_iter().enumerate() {
                if !indices.is_empty() {
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            let idx_arr = arrow::array::UInt32Array::from(
                                indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
                            );
                            arrow::compute::take(c.as_ref(), &idx_arr, None)
                                .map_err(|e| BlazeError::execution(format!("take error: {e}")))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let rb = RecordBatch::try_new(batch.schema(), cols)
                        .map_err(|e| BlazeError::execution(format!("batch error: {e}")))?;
                    buckets[part].push(rb);
                }
            }
        }
        Ok(buckets)
    }

    fn range_partition(
        &self,
        batches: &[RecordBatch],
        col_idx: usize,
        split_points: &[i64],
    ) -> Result<Vec<Vec<RecordBatch>>> {
        use arrow::array::{Array, AsArray};
        let mut buckets: Vec<Vec<RecordBatch>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();

        for batch in batches {
            if col_idx >= batch.num_columns() {
                return Err(BlazeError::execution("Column index out of range"));
            }
            let col = batch.column(col_idx);
            let int_col = col
                .as_primitive_opt::<arrow::datatypes::Int64Type>()
                .ok_or_else(|| BlazeError::execution("Range partitioning requires Int64 column"))?;
            let mut row_indices: Vec<Vec<usize>> =
                (0..self.num_partitions).map(|_| Vec::new()).collect();
            for row in 0..batch.num_rows() {
                let val = if int_col.is_null(row) {
                    i64::MIN
                } else {
                    int_col.value(row)
                };
                let mut part = split_points.len(); // last bucket by default
                for (i, &sp) in split_points.iter().enumerate() {
                    if val < sp {
                        part = i;
                        break;
                    }
                }
                let part = part.min(self.num_partitions - 1);
                row_indices[part].push(row);
            }
            for (part, indices) in row_indices.into_iter().enumerate() {
                if !indices.is_empty() {
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            let idx_arr = arrow::array::UInt32Array::from(
                                indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
                            );
                            arrow::compute::take(c.as_ref(), &idx_arr, None)
                                .map_err(|e| BlazeError::execution(format!("take error: {e}")))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let rb = RecordBatch::try_new(batch.schema(), cols)
                        .map_err(|e| BlazeError::execution(format!("batch error: {e}")))?;
                    buckets[part].push(rb);
                }
            }
        }
        Ok(buckets)
    }

    fn round_robin_partition(&self, batches: &[RecordBatch]) -> Result<Vec<Vec<RecordBatch>>> {
        let mut buckets: Vec<Vec<RecordBatch>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();
        let mut counter = 0usize;

        for batch in batches {
            let mut row_indices: Vec<Vec<usize>> =
                (0..self.num_partitions).map(|_| Vec::new()).collect();
            for row in 0..batch.num_rows() {
                row_indices[counter % self.num_partitions].push(row);
                counter += 1;
            }
            for (part, indices) in row_indices.into_iter().enumerate() {
                if !indices.is_empty() {
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            let idx_arr = arrow::array::UInt32Array::from(
                                indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
                            );
                            arrow::compute::take(c.as_ref(), &idx_arr, None)
                                .map_err(|e| BlazeError::execution(format!("take error: {e}")))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let rb = RecordBatch::try_new(batch.schema(), cols)
                        .map_err(|e| BlazeError::execution(format!("batch error: {e}")))?;
                    buckets[part].push(rb);
                }
            }
        }
        Ok(buckets)
    }

    fn broadcast_partition(&self, batches: &[RecordBatch]) -> Result<Vec<Vec<RecordBatch>>> {
        let mut buckets: Vec<Vec<RecordBatch>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();
        for bucket in &mut buckets {
            bucket.extend(batches.iter().cloned());
        }
        Ok(buckets)
    }
}

// ---------------------------------------------------------------------------
// Exchange Operators
// ---------------------------------------------------------------------------

/// Exchange operators that redistribute data between distributed execution stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeOperator {
    /// Repartition data according to a given scheme.
    Repartition {
        scheme: PartitionScheme,
        num_partitions: usize,
    },
    /// Broadcast data to a set of target nodes.
    BroadcastExchange { target_nodes: Vec<String> },
    /// Gather results from all nodes onto the coordinator.
    GatherExchange,
    /// Co-locate two inputs by join keys so matching rows land on the same node.
    ColocatedJoinExchange { left_key: String, right_key: String },
}

impl ExchangeOperator {
    /// Return a human-readable description of this exchange.
    pub fn describe(&self) -> String {
        match self {
            Self::Repartition {
                scheme,
                num_partitions,
            } => {
                format!("Repartition({:?}, {} partitions)", scheme, num_partitions)
            }
            Self::BroadcastExchange { target_nodes } => {
                format!("BroadcastExchange(targets: [{}])", target_nodes.join(", "))
            }
            Self::GatherExchange => "GatherExchange".to_string(),
            Self::ColocatedJoinExchange {
                left_key,
                right_key,
            } => {
                format!(
                    "ColocatedJoinExchange(left={}, right={})",
                    left_key, right_key
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Resource Governor
// ---------------------------------------------------------------------------

/// Statistics tracked by the [`ResourceGovernor`].
#[derive(Debug, Clone, Default)]
pub struct ResourceStats {
    pub queries_admitted: usize,
    pub queries_rejected: usize,
    pub queries_timed_out: usize,
}

/// A resource grant representing an allocated execution slot.
#[derive(Debug)]
pub struct ResourceGrant {
    pub grant_id: u64,
    pub memory_budget: usize,
    pub timeout_ms: u64,
    pub acquired_at: Instant,
}

/// Governs resource allocation for distributed query execution.
pub struct ResourceGovernor {
    max_concurrent_queries: usize,
    max_memory_per_query: usize,
    query_timeout_ms: u64,
    active: RwLock<HashMap<u64, ResourceGrant>>,
    next_id: RwLock<u64>,
    stats: RwLock<ResourceStats>,
}

impl ResourceGovernor {
    pub fn new(
        max_concurrent_queries: usize,
        max_memory_per_query: usize,
        query_timeout_ms: u64,
    ) -> Self {
        Self {
            max_concurrent_queries,
            max_memory_per_query,
            query_timeout_ms,
            active: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
            stats: RwLock::new(ResourceStats::default()),
        }
    }

    /// Try to acquire a resource slot. Returns an error if capacity is exceeded.
    pub fn try_acquire(&self) -> Result<ResourceGrant> {
        let active = self.active.read();
        if active.len() >= self.max_concurrent_queries {
            self.stats.write().queries_rejected += 1;
            return Err(BlazeError::execution(format!(
                "Resource limit reached: {} of {} slots in use",
                active.len(),
                self.max_concurrent_queries,
            )));
        }
        drop(active);

        let mut id_guard = self.next_id.write();
        let grant_id = *id_guard;
        *id_guard += 1;
        drop(id_guard);

        let grant = ResourceGrant {
            grant_id,
            memory_budget: self.max_memory_per_query,
            timeout_ms: self.query_timeout_ms,
            acquired_at: Instant::now(),
        };

        self.active.write().insert(
            grant_id,
            ResourceGrant {
                grant_id,
                memory_budget: self.max_memory_per_query,
                timeout_ms: self.query_timeout_ms,
                acquired_at: grant.acquired_at,
            },
        );
        self.stats.write().queries_admitted += 1;

        Ok(grant)
    }

    /// Release a previously acquired resource grant.
    pub fn release(&self, grant_id: u64) {
        self.active.write().remove(&grant_id);
    }

    /// Number of queries currently holding a resource slot.
    pub fn active_queries(&self) -> usize {
        self.active.read().len()
    }

    /// Record a timed-out query (and release its slot).
    pub fn record_timeout(&self, grant_id: u64) {
        self.active.write().remove(&grant_id);
        self.stats.write().queries_timed_out += 1;
    }

    /// Return a snapshot of resource statistics.
    pub fn stats(&self) -> ResourceStats {
        self.stats.read().clone()
    }

    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }

    pub fn max_memory_per_query(&self) -> usize {
        self.max_memory_per_query
    }

    pub fn query_timeout_ms(&self) -> u64 {
        self.query_timeout_ms
    }
}

// ---------------------------------------------------------------------------
// Result Streaming
// ---------------------------------------------------------------------------

/// Strategy used when merging results from multiple fragment executions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Simply append all batches in arrival order.
    Append,
    /// Sort the merged result by a given column.
    Sort { column: String, descending: bool },
    /// Aggregate the merged result using a given expression description.
    Aggregate { expression: String },
}

/// Collects streaming results from distributed fragment executions with
/// configurable merge strategies.
pub struct StreamingResultCollector {
    strategy: MergeStrategy,
    results: Vec<(usize, Vec<RecordBatch>)>,
    total_rows: usize,
    total_bytes: usize,
}

impl StreamingResultCollector {
    pub fn new(strategy: MergeStrategy) -> Self {
        Self {
            strategy,
            results: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
        }
    }

    /// Add results from a fragment execution.
    pub fn add_result(&mut self, fragment_id: usize, batches: Vec<RecordBatch>) {
        for b in &batches {
            self.total_rows += b.num_rows();
            self.total_bytes += b.get_array_memory_size();
        }
        self.results.push((fragment_id, batches));
    }

    /// Merge all collected results according to the configured strategy.
    pub fn merge_results(&self) -> Result<Vec<RecordBatch>> {
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for (_frag_id, batches) in &self.results {
            all_batches.extend(batches.iter().cloned());
        }

        if all_batches.is_empty() {
            return Ok(all_batches);
        }

        match &self.strategy {
            MergeStrategy::Append => Ok(all_batches),
            MergeStrategy::Sort { column, descending } => {
                // Find column index by name in the first batch schema.
                let schema = all_batches[0].schema();
                let col_idx = schema.index_of(column).map_err(|_| {
                    BlazeError::execution(format!("Sort column '{}' not found", column))
                })?;
                // Compact into a single batch then sort.
                let compacted = concat_batches(&schema, &all_batches)
                    .map_err(|e| BlazeError::execution(format!("concat error: {e}")))?;
                let sort_col = compacted.column(col_idx).clone();
                let options = arrow::compute::SortOptions {
                    descending: *descending,
                    nulls_first: false,
                };
                let indices = arrow::compute::sort_to_indices(&sort_col, Some(options), None)
                    .map_err(|e| BlazeError::execution(format!("sort error: {e}")))?;
                let sorted_cols = compacted
                    .columns()
                    .iter()
                    .map(|c| {
                        arrow::compute::take(c.as_ref(), &indices, None)
                            .map_err(|e| BlazeError::execution(format!("take error: {e}")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let sorted = RecordBatch::try_new(schema, sorted_cols)
                    .map_err(|e| BlazeError::execution(format!("batch error: {e}")))?;
                Ok(vec![sorted])
            }
            MergeStrategy::Aggregate { expression: _ } => {
                // Full aggregation is delegated to the executor; here we just
                // compact the batches so downstream can process them.
                let schema = all_batches[0].schema();
                let compacted = concat_batches(&schema, &all_batches)
                    .map_err(|e| BlazeError::execution(format!("concat error: {e}")))?;
                Ok(vec![compacted])
            }
        }
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn strategy(&self) -> &MergeStrategy {
        &self.strategy
    }

    pub fn fragment_count(&self) -> usize {
        self.results.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::Schema as ArrowSchema;

    use crate::flight::FlightEndpoint;

    /// Helper: create a coordinator with two active nodes.
    fn coordinator_with_nodes() -> DistributedCoordinator {
        let config = DistributedConfig::new().with_retry_count(2);
        let coord = DistributedCoordinator::new(config);
        coord.add_node(ClusterNode::new(
            "node-1",
            FlightEndpoint::new("10.0.0.1", 8815),
        ));
        coord.add_node(ClusterNode::new(
            "node-2",
            FlightEndpoint::new("10.0.0.2", 8815),
        ));
        coord
    }

    #[test]
    fn test_node_management() {
        let coord = coordinator_with_nodes();
        assert_eq!(coord.node_count(), 2);
        assert_eq!(coord.available_nodes().len(), 2);

        // Mark one node down.
        coord.mark_node_down("node-1");
        assert_eq!(coord.available_nodes().len(), 1);
        assert_eq!(coord.available_nodes()[0].id, "node-2");

        // Remove a node.
        let removed = coord.remove_node("node-2");
        assert!(removed.is_some());
        assert_eq!(coord.node_count(), 1);
    }

    #[test]
    fn test_plan_query_single_table() {
        let coord = coordinator_with_nodes();
        coord.register_table_location("orders", vec!["node-1".into()]);

        let plan = coord
            .plan_query("SELECT * FROM orders WHERE total > 100")
            .unwrap();

        // Should have a leaf fragment for "orders" plus a root gather fragment.
        assert_eq!(plan.fragment_count(), 2);
        assert_eq!(plan.fragments[0].target_nodes, vec!["node-1".to_string()]);
        assert_eq!(
            plan.fragments.last().unwrap().exchange_type,
            ExchangeStrategy::Gather
        );
    }

    #[test]
    fn test_plan_query_join() {
        let coord = coordinator_with_nodes();
        coord.register_table_location("orders", vec!["node-1".into()]);
        coord.register_table_location("customers", vec!["node-2".into()]);

        let plan = coord
            .plan_query("SELECT * FROM orders JOIN customers ON orders.cid = customers.id")
            .unwrap();

        // Two leaf fragments (orders, customers) + root gather.
        assert_eq!(plan.fragment_count(), 3);
        assert_eq!(plan.root_fragment_id, 2);

        let order = plan.execution_order().unwrap();
        // Root must come last.
        assert_eq!(*order.last().unwrap(), plan.root_fragment_id);
    }

    #[test]
    fn test_plan_serialization_roundtrip() {
        let coord = coordinator_with_nodes();
        coord.register_table_location("t1", vec!["node-1".into()]);

        let plan = coord.plan_query("SELECT * FROM t1").unwrap();

        let bytes = serialize_plan(&plan).unwrap();
        assert!(!bytes.is_empty());

        let restored = deserialize_plan(&bytes).unwrap();
        assert_eq!(restored.fragment_count(), plan.fragment_count());
        assert_eq!(restored.root_fragment_id, plan.root_fragment_id);
        assert_eq!(restored.fragments[0].sql, plan.fragments[0].sql);
    }

    #[test]
    fn test_result_collector_merge() {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "name",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));

        let batch_a = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["Alice", "Bob"]))],
        )
        .unwrap();

        let batch_b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["Charlie"]))],
        )
        .unwrap();

        let mut collector = ResultCollector::new();
        collector.add_result(0, vec![batch_a]);
        collector.add_result(1, vec![batch_b]);

        assert_eq!(collector.collected_count(), 2);
        assert!(collector.has_result(0));

        let merged = collector.merge_results(&[0, 1]).unwrap();
        // Both batches share the same schema, so they get compacted.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].num_rows(), 3);
    }

    #[test]
    fn test_execute_distributed_plan() {
        let coord = coordinator_with_nodes();
        coord.register_table_location("events", vec!["node-1".into()]);

        let plan = coord.plan_query("SELECT * FROM events").unwrap();
        let results = coord.execute(plan).unwrap();

        // Fragment execution is simulated — returns empty batches.
        assert!(results.is_empty() || results.iter().all(|b| b.num_rows() == 0));

        let stats = coord.stats();
        assert_eq!(stats.queries_executed, 1);
        assert!(stats.fragments_executed > 0);
    }

    #[test]
    fn test_config_builder() {
        let config = DistributedConfig::new()
            .with_local_threshold_bytes(1024)
            .with_max_fragment_size(2048)
            .with_retry_count(5)
            .with_timeout_ms(10_000);

        assert_eq!(config.local_threshold_bytes, 1024);
        assert_eq!(config.max_fragment_size, 2048);
        assert_eq!(config.retry_count, 5);
        assert_eq!(config.timeout_ms, 10_000);
    }

    #[test]
    fn test_no_available_nodes_error() {
        let coord = DistributedCoordinator::with_defaults();
        let result = coord.plan_query("SELECT 1");
        assert!(result.is_err());
    }

    #[test]
    fn test_execution_order_cycle_detection() {
        // Create a plan with a dependency cycle: 0 depends on 1, 1 depends on 0.
        let frag0 = PlanFragment::new(0, "SELECT 1").with_dependencies(vec![1]);
        let frag1 = PlanFragment::new(1, "SELECT 2").with_dependencies(vec![0]);
        let plan = DistributedPlan::new(vec![frag0, frag1], 1);

        let result = plan.execution_order();
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_table_names() {
        let tables =
            extract_table_names("SELECT * FROM orders JOIN customers ON orders.id = customers.oid");
        assert_eq!(tables, vec!["orders".to_string(), "customers".to_string()]);

        let tables = extract_table_names("SELECT 1 + 1");
        assert!(tables.is_empty());
    }

    #[test]
    fn test_node_draining() {
        let coord = coordinator_with_nodes();
        coord.mark_node_draining("node-1");

        let nodes = coord.list_nodes();
        let node1 = nodes.iter().find(|n| n.id == "node-1").unwrap();
        assert_eq!(node1.status, NodeStatus::Draining);
        assert!(!node1.is_available());
    }

    // --- Scatter-Gather and Fault Tolerance tests ---

    #[test]
    fn test_scatter_creates_tasks() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);

        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let strategy = ScatterStrategy::RoundRobin { num_partitions: 3 };
        let tasks = executor.scatter(&[batch], &strategy, 0).unwrap();
        assert_eq!(tasks.len(), 3);
        assert!(tasks.iter().all(|t| t.status == TaskStatus::Pending));
    }

    #[test]
    fn test_scatter_broadcast() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::Broadcast;
        let tasks = executor.scatter(&[], &strategy, 0).unwrap();
        assert_eq!(tasks.len(), 2); // 2 available nodes
    }

    #[test]
    fn test_scatter_no_nodes() {
        let coord = Arc::new(DistributedCoordinator::with_defaults());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 2 };
        let result = executor.scatter(&[], &strategy, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_completion() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 2 };
        let tasks = executor.scatter(&[], &strategy, 0).unwrap();

        executor.complete_task(&tasks[0].task_id);
        assert_eq!(
            executor.task_status(&tasks[0].task_id),
            Some(TaskStatus::Completed)
        );
        assert_eq!(
            executor.task_status(&tasks[1].task_id),
            Some(TaskStatus::Pending)
        );
    }

    #[test]
    fn test_task_failure_and_retry() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 1 };
        let tasks = executor.scatter(&[], &strategy, 0).unwrap();

        // First failure should retry
        let status = executor.fail_task(&tasks[0].task_id);
        assert_eq!(status, TaskStatus::Retrying);

        // Keep failing until max retries
        for _ in 0..10 {
            executor.fail_task(&tasks[0].task_id);
        }
        assert_eq!(
            executor.task_status(&tasks[0].task_id),
            Some(TaskStatus::Failed)
        );
    }

    #[test]
    fn test_cancel_fragment() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 3 };
        let tasks = executor.scatter(&[], &strategy, 5).unwrap();

        executor.cancel_fragment(5);
        for task in &tasks {
            assert_eq!(
                executor.task_status(&task.task_id),
                Some(TaskStatus::Cancelled)
            );
        }
    }

    #[test]
    fn test_gather_results() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 3 };
        let tasks = executor.scatter(&[], &strategy, 0).unwrap();

        executor.complete_task(&tasks[0].task_id);
        executor.complete_task(&tasks[1].task_id);
        executor.fail_task(&tasks[2].task_id);
        // Exhaust retries
        for _ in 0..10 {
            executor.fail_task(&tasks[2].task_id);
        }

        let result = executor.gather(&tasks);
        assert_eq!(result.total_tasks, 3);
        assert_eq!(result.completed_tasks, 2);
        assert_eq!(result.failed_tasks, 1);
    }

    #[test]
    fn test_active_tasks() {
        let coord = Arc::new(coordinator_with_nodes());
        let executor = ScatterGatherExecutor::new(coord);
        let strategy = ScatterStrategy::RoundRobin { num_partitions: 3 };
        let tasks = executor.scatter(&[], &strategy, 0).unwrap();

        assert_eq!(executor.active_tasks().len(), 3);
        executor.complete_task(&tasks[0].task_id);
        assert_eq!(executor.active_tasks().len(), 2);
    }

    #[test]
    fn test_health_checker() {
        let checker = NodeHealthChecker::new(5000, 3);

        // Successes reset counter
        checker.record_success("node-1");
        assert_eq!(checker.failure_count("node-1"), 0);

        // Failures accumulate
        assert!(!checker.record_failure("node-1"));
        assert!(!checker.record_failure("node-1"));
        assert_eq!(checker.failure_count("node-1"), 2);

        // Third failure crosses threshold
        assert!(checker.record_failure("node-1"));
        assert_eq!(checker.failure_count("node-1"), 3);

        // Success resets
        checker.record_success("node-1");
        assert_eq!(checker.failure_count("node-1"), 0);
    }

    #[test]
    fn test_health_checker_config() {
        let checker = NodeHealthChecker::new(3000, 5);
        assert_eq!(checker.check_interval_ms(), 3000);
    }

    // -----------------------------------------------------------------------
    // Data Partitioning tests
    // -----------------------------------------------------------------------

    fn make_int_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn test_hash_partition_distributes_rows() {
        let batch = make_int_batch(vec![1, 2, 3, 4, 5, 6]);
        let partitioner = DataPartitioner::new(PartitionScheme::Hash { column_index: 0 }, 3);
        let buckets = partitioner.partition(&[batch]).unwrap();

        assert_eq!(buckets.len(), 3);
        let total: usize = buckets
            .iter()
            .flat_map(|b| b.iter().map(|r| r.num_rows()))
            .sum();
        assert_eq!(total, 6);
    }

    #[test]
    fn test_round_robin_partition() {
        let batch = make_int_batch(vec![10, 20, 30, 40]);
        let partitioner = DataPartitioner::new(PartitionScheme::RoundRobin, 2);
        let buckets = partitioner.partition(&[batch]).unwrap();

        assert_eq!(buckets.len(), 2);
        let total: usize = buckets
            .iter()
            .flat_map(|b| b.iter().map(|r| r.num_rows()))
            .sum();
        assert_eq!(total, 4);
        // Each partition should have 2 rows (round-robin evenly).
        for bucket in &buckets {
            let rows: usize = bucket.iter().map(|r| r.num_rows()).sum();
            assert_eq!(rows, 2);
        }
    }

    #[test]
    fn test_broadcast_partition() {
        let batch = make_int_batch(vec![1, 2, 3]);
        let partitioner = DataPartitioner::new(PartitionScheme::Broadcast, 4);
        let buckets = partitioner.partition(&[batch]).unwrap();

        assert_eq!(buckets.len(), 4);
        for bucket in &buckets {
            let rows: usize = bucket.iter().map(|r| r.num_rows()).sum();
            assert_eq!(rows, 3);
        }
    }

    #[test]
    fn test_range_partition() {
        let batch = make_int_batch(vec![5, 15, 25, 35, 45]);
        let partitioner = DataPartitioner::new(
            PartitionScheme::Range {
                column_index: 0,
                split_points: vec![10, 30],
            },
            3,
        );
        let buckets = partitioner.partition(&[batch]).unwrap();

        assert_eq!(buckets.len(), 3);
        let total: usize = buckets
            .iter()
            .flat_map(|b| b.iter().map(|r| r.num_rows()))
            .sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_partition_zero_partitions_error() {
        let batch = make_int_batch(vec![1]);
        let partitioner = DataPartitioner::new(PartitionScheme::RoundRobin, 0);
        assert!(partitioner.partition(&[batch]).is_err());
    }

    // -----------------------------------------------------------------------
    // Exchange Operator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_exchange_operator_describe() {
        let rp = ExchangeOperator::Repartition {
            scheme: PartitionScheme::Hash { column_index: 0 },
            num_partitions: 4,
        };
        assert!(rp.describe().contains("Repartition"));
        assert!(rp.describe().contains("4 partitions"));

        let bc = ExchangeOperator::BroadcastExchange {
            target_nodes: vec!["n1".into(), "n2".into()],
        };
        assert!(bc.describe().contains("n1"));

        let ge = ExchangeOperator::GatherExchange;
        assert_eq!(ge.describe(), "GatherExchange");

        let cj = ExchangeOperator::ColocatedJoinExchange {
            left_key: "order_id".into(),
            right_key: "id".into(),
        };
        assert!(cj.describe().contains("order_id"));
        assert!(cj.describe().contains("id"));
    }

    // -----------------------------------------------------------------------
    // Resource Governor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_resource_governor_acquire_release() {
        let gov = ResourceGovernor::new(2, 1024 * 1024, 5000);
        assert_eq!(gov.active_queries(), 0);

        let g1 = gov.try_acquire().unwrap();
        assert_eq!(gov.active_queries(), 1);
        assert_eq!(g1.memory_budget, 1024 * 1024);

        let g2 = gov.try_acquire().unwrap();
        assert_eq!(gov.active_queries(), 2);

        // Third should fail — at capacity.
        assert!(gov.try_acquire().is_err());

        gov.release(g1.grant_id);
        assert_eq!(gov.active_queries(), 1);

        // Now there's room again.
        let _g3 = gov.try_acquire().unwrap();
        assert_eq!(gov.active_queries(), 2);
        gov.release(g2.grant_id);
    }

    #[test]
    fn test_resource_governor_stats() {
        let gov = ResourceGovernor::new(1, 512, 1000);

        let g = gov.try_acquire().unwrap();
        let _ = gov.try_acquire(); // rejected

        let stats = gov.stats();
        assert_eq!(stats.queries_admitted, 1);
        assert_eq!(stats.queries_rejected, 1);

        gov.record_timeout(g.grant_id);
        let stats = gov.stats();
        assert_eq!(stats.queries_timed_out, 1);
        assert_eq!(gov.active_queries(), 0);
    }

    #[test]
    fn test_resource_governor_config() {
        let gov = ResourceGovernor::new(10, 2048, 3000);
        assert_eq!(gov.max_concurrent_queries(), 10);
        assert_eq!(gov.max_memory_per_query(), 2048);
        assert_eq!(gov.query_timeout_ms(), 3000);
    }

    // -----------------------------------------------------------------------
    // Streaming Result Collector tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_streaming_collector_append() {
        let mut collector = StreamingResultCollector::new(MergeStrategy::Append);

        let b1 = make_int_batch(vec![1, 2]);
        let b2 = make_int_batch(vec![3, 4, 5]);

        collector.add_result(0, vec![b1]);
        collector.add_result(1, vec![b2]);

        assert_eq!(collector.total_rows(), 5);
        assert!(collector.total_bytes() > 0);
        assert_eq!(collector.fragment_count(), 2);

        let merged = collector.merge_results().unwrap();
        let total: usize = merged.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_streaming_collector_sort() {
        let mut collector = StreamingResultCollector::new(MergeStrategy::Sort {
            column: "id".to_string(),
            descending: true,
        });

        collector.add_result(0, vec![make_int_batch(vec![3, 1])]);
        collector.add_result(1, vec![make_int_batch(vec![4, 2])]);

        let merged = collector.merge_results().unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].num_rows(), 4);

        // Verify descending order.
        let col = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 4);
        assert_eq!(col.value(1), 3);
        assert_eq!(col.value(2), 2);
        assert_eq!(col.value(3), 1);
    }

    #[test]
    fn test_streaming_collector_aggregate_compacts() {
        let mut collector = StreamingResultCollector::new(MergeStrategy::Aggregate {
            expression: "SUM(id)".to_string(),
        });
        collector.add_result(0, vec![make_int_batch(vec![10, 20])]);
        collector.add_result(1, vec![make_int_batch(vec![30])]);

        let merged = collector.merge_results().unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].num_rows(), 3);
    }
}
