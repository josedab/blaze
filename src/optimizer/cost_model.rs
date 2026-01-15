//! Cost Model for Query Optimization
//!
//! Defines cost factors and estimation functions for different physical operators.

use crate::error::Result;
use crate::planner::LogicalPlan;

/// Default CPU cost factor (cost per tuple processed).
pub const DEFAULT_CPU_COST: f64 = 0.01;

/// Default I/O cost factor (cost per page read).
pub const DEFAULT_IO_COST: f64 = 1.0;

/// Default network cost factor (cost per tuple transferred).
pub const DEFAULT_NETWORK_COST: f64 = 2.0;

/// Default memory cost factor (cost per byte of memory used).
pub const DEFAULT_MEMORY_COST: f64 = 0.001;

/// Estimated cost of an execution plan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    /// CPU processing cost
    pub cpu: f64,
    /// I/O cost (disk reads)
    pub io: f64,
    /// Network cost (for distributed queries)
    pub network: f64,
    /// Memory cost
    pub memory: f64,
}

impl Cost {
    /// Create a new cost with all components.
    pub fn new(cpu: f64, io: f64, network: f64, memory: f64) -> Self {
        Self { cpu, io, network, memory }
    }

    /// Create a zero cost.
    pub fn zero() -> Self {
        Self { cpu: 0.0, io: 0.0, network: 0.0, memory: 0.0 }
    }

    /// Create a cost with only CPU component.
    pub fn cpu(cpu: f64) -> Self {
        Self { cpu, io: 0.0, network: 0.0, memory: 0.0 }
    }

    /// Create a cost with only I/O component.
    pub fn io(io: f64) -> Self {
        Self { cpu: 0.0, io, network: 0.0, memory: 0.0 }
    }

    /// Calculate total weighted cost.
    pub fn total(&self) -> f64 {
        self.cpu + self.io + self.network + self.memory
    }

    /// Add two costs.
    pub fn add(&self, other: &Cost) -> Cost {
        Cost {
            cpu: self.cpu + other.cpu,
            io: self.io + other.io,
            network: self.network + other.network,
            memory: self.memory + other.memory,
        }
    }

    /// Multiply cost by a factor.
    pub fn multiply(&self, factor: f64) -> Cost {
        Cost {
            cpu: self.cpu * factor,
            io: self.io * factor,
            network: self.network * factor,
            memory: self.memory * factor,
        }
    }
}

impl Default for Cost {
    fn default() -> Self {
        Self::zero()
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost::add(&self, &other)
    }
}

impl std::ops::AddAssign for Cost {
    fn add_assign(&mut self, other: Cost) {
        self.cpu += other.cpu;
        self.io += other.io;
        self.network += other.network;
        self.memory += other.memory;
    }
}

/// Cost model configuration.
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Cost per tuple processed
    cpu_cost_factor: f64,
    /// Cost per page read from disk
    io_cost_factor: f64,
    /// Cost per tuple transferred over network
    network_cost_factor: f64,
    /// Cost per byte of memory used
    memory_cost_factor: f64,
    /// Average page size in bytes
    page_size: usize,
    /// Average tuple width in bytes
    avg_tuple_width: usize,
}

impl CostModel {
    /// Create a new cost model with custom factors.
    pub fn new(
        cpu_cost_factor: f64,
        io_cost_factor: f64,
        network_cost_factor: f64,
        memory_cost_factor: f64,
    ) -> Self {
        Self {
            cpu_cost_factor,
            io_cost_factor,
            network_cost_factor,
            memory_cost_factor,
            page_size: 8192,
            avg_tuple_width: 100,
        }
    }

    /// Get CPU cost factor.
    pub fn cpu_cost_factor(&self) -> f64 {
        self.cpu_cost_factor
    }

    /// Get I/O cost factor.
    pub fn io_cost_factor(&self) -> f64 {
        self.io_cost_factor
    }

    /// Estimate cost of a table scan.
    pub fn scan_cost(&self, num_rows: usize, row_width: usize) -> Cost {
        let pages = (num_rows * row_width) / self.page_size + 1;
        Cost {
            cpu: num_rows as f64 * self.cpu_cost_factor,
            io: pages as f64 * self.io_cost_factor,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate cost of a filter operation.
    pub fn filter_cost(&self, input_rows: usize, _selectivity: f64) -> Cost {
        Cost {
            cpu: input_rows as f64 * self.cpu_cost_factor,
            io: 0.0,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate cost of a projection operation.
    pub fn projection_cost(&self, input_rows: usize, num_columns: usize) -> Cost {
        Cost {
            cpu: input_rows as f64 * num_columns as f64 * self.cpu_cost_factor * 0.1,
            io: 0.0,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate cost of a hash join.
    pub fn hash_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        output_rows: usize,
    ) -> Cost {
        // Build phase: hash the smaller input
        let (build_rows, probe_rows) = if left_rows < right_rows {
            (left_rows, right_rows)
        } else {
            (right_rows, left_rows)
        };

        let build_cost = build_rows as f64 * self.cpu_cost_factor * 2.0; // Hashing + insertion
        let probe_cost = probe_rows as f64 * self.cpu_cost_factor * 1.5; // Hash lookup
        let output_cost = output_rows as f64 * self.cpu_cost_factor;

        // Memory for hash table
        let hash_table_memory = build_rows * self.avg_tuple_width;

        Cost {
            cpu: build_cost + probe_cost + output_cost,
            io: 0.0,
            network: 0.0,
            memory: hash_table_memory as f64 * self.memory_cost_factor,
        }
    }

    /// Estimate cost of a nested loop join.
    pub fn nested_loop_join_cost(
        &self,
        outer_rows: usize,
        inner_rows: usize,
        output_rows: usize,
    ) -> Cost {
        Cost {
            cpu: (outer_rows * inner_rows) as f64 * self.cpu_cost_factor
                + output_rows as f64 * self.cpu_cost_factor,
            io: 0.0,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate cost of a merge join (assuming sorted inputs).
    pub fn merge_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        output_rows: usize,
    ) -> Cost {
        Cost {
            cpu: (left_rows + right_rows + output_rows) as f64 * self.cpu_cost_factor,
            io: 0.0,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate cost of sorting.
    pub fn sort_cost(&self, num_rows: usize, row_width: usize) -> Cost {
        if num_rows <= 1 {
            return Cost::zero();
        }

        let n_log_n = num_rows as f64 * (num_rows as f64).log2();

        // Check if we need external sort
        let data_size = num_rows * row_width;
        let memory_threshold = 100 * 1024 * 1024; // 100MB

        if data_size > memory_threshold {
            // External sort: multiple passes
            let num_runs = (data_size / memory_threshold) + 1;
            let pages = data_size / self.page_size + 1;

            Cost {
                cpu: n_log_n * self.cpu_cost_factor,
                io: (pages * 2 * num_runs) as f64 * self.io_cost_factor, // Read and write per run
                network: 0.0,
                memory: memory_threshold as f64 * self.memory_cost_factor,
            }
        } else {
            // In-memory sort
            Cost {
                cpu: n_log_n * self.cpu_cost_factor,
                io: 0.0,
                network: 0.0,
                memory: data_size as f64 * self.memory_cost_factor,
            }
        }
    }

    /// Estimate cost of hash aggregation.
    pub fn hash_aggregate_cost(
        &self,
        input_rows: usize,
        num_groups: usize,
        num_aggregates: usize,
    ) -> Cost {
        let hash_cost = input_rows as f64 * self.cpu_cost_factor * 1.5; // Hash + aggregate update
        let finalize_cost = num_groups as f64 * num_aggregates as f64 * self.cpu_cost_factor;

        Cost {
            cpu: hash_cost + finalize_cost,
            io: 0.0,
            network: 0.0,
            memory: (num_groups * self.avg_tuple_width) as f64 * self.memory_cost_factor,
        }
    }

    /// Estimate cost of a limit operation.
    pub fn limit_cost(&self, fetch: usize) -> Cost {
        Cost {
            cpu: fetch as f64 * self.cpu_cost_factor * 0.1,
            io: 0.0,
            network: 0.0,
            memory: 0.0,
        }
    }

    /// Estimate the total cost of a logical plan.
    pub fn estimate(&self, plan: &LogicalPlan) -> Result<Cost> {
        match plan {
            LogicalPlan::TableScan { projection, schema, .. } => {
                let num_cols = projection.as_ref().map(|p| p.len()).unwrap_or(schema.fields().len());
                let row_width = num_cols * 8; // Assume 8 bytes per column average
                // For now, estimate 1000 rows since we don't have actual stats
                Ok(self.scan_cost(1000, row_width))
            }
            LogicalPlan::Filter { input, .. } => {
                let input_cost = self.estimate(input)?;
                let filter_cost = self.filter_cost(1000, 0.5); // Default selectivity
                Ok(input_cost + filter_cost)
            }
            LogicalPlan::Projection { input, exprs, .. } => {
                let input_cost = self.estimate(input)?;
                let proj_cost = self.projection_cost(1000, exprs.len());
                Ok(input_cost + proj_cost)
            }
            LogicalPlan::Aggregate { input, group_by, aggr_exprs, .. } => {
                let input_cost = self.estimate(input)?;
                let num_groups = if group_by.is_empty() { 1 } else { 100 }; // Estimate
                let agg_cost = self.hash_aggregate_cost(1000, num_groups, aggr_exprs.len());
                Ok(input_cost + agg_cost)
            }
            LogicalPlan::Sort { input, .. } => {
                let input_cost = self.estimate(input)?;
                let sort_cost = self.sort_cost(1000, 100);
                Ok(input_cost + sort_cost)
            }
            LogicalPlan::Limit { input, fetch, .. } => {
                let input_cost = self.estimate(input)?;
                let limit_cost = self.limit_cost(fetch.unwrap_or(1000));
                Ok(input_cost + limit_cost)
            }
            LogicalPlan::Join { left, right, .. } => {
                let left_cost = self.estimate(left)?;
                let right_cost = self.estimate(right)?;
                let join_cost = self.hash_join_cost(1000, 1000, 1000);
                Ok(left_cost + right_cost + join_cost)
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                let left_cost = self.estimate(left)?;
                let right_cost = self.estimate(right)?;
                let join_cost = self.nested_loop_join_cost(1000, 1000, 1000000);
                Ok(left_cost + right_cost + join_cost)
            }
            LogicalPlan::Distinct { input, .. } => {
                let input_cost = self.estimate(input)?;
                let distinct_cost = self.hash_aggregate_cost(1000, 500, 0);
                Ok(input_cost + distinct_cost)
            }
            LogicalPlan::SetOperation { left, right, .. } => {
                let left_cost = self.estimate(left)?;
                let right_cost = self.estimate(right)?;
                Ok(left_cost + right_cost)
            }
            LogicalPlan::Values { .. } => {
                Ok(Cost::zero())
            }
            LogicalPlan::SubqueryAlias { input, .. } => {
                self.estimate(input)
            }
            LogicalPlan::Window { input, .. } => {
                let input_cost = self.estimate(input)?;
                let window_cost = Cost::cpu(1000.0 * self.cpu_cost_factor * 2.0);
                Ok(input_cost + window_cost)
            }
            LogicalPlan::EmptyRelation { .. } => Ok(Cost::zero()),
            LogicalPlan::Explain { .. } => Ok(Cost::zero()),
            LogicalPlan::ExplainAnalyze { plan, .. } => self.estimate(plan),
            LogicalPlan::CreateTable { .. } => Ok(Cost::zero()),
            LogicalPlan::DropTable { .. } => Ok(Cost::zero()),
            LogicalPlan::Insert { input, .. } => self.estimate(input),
            LogicalPlan::Delete { .. } => Ok(Cost::zero()),
            LogicalPlan::Update { .. } => Ok(Cost::zero()),
            LogicalPlan::Copy { input, .. } => {
                let input_cost = self.estimate(input)?;
                // Add IO cost for writing to file
                let io_cost = Cost::io(1000.0 * self.io_cost_factor);
                Ok(input_cost + io_cost)
            }
        }
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new(
            DEFAULT_CPU_COST,
            DEFAULT_IO_COST,
            DEFAULT_NETWORK_COST,
            DEFAULT_MEMORY_COST,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_addition() {
        let c1 = Cost::new(1.0, 2.0, 3.0, 4.0);
        let c2 = Cost::new(0.5, 1.5, 2.5, 3.5);
        let sum = c1 + c2;

        assert!((sum.cpu - 1.5).abs() < 0.001);
        assert!((sum.io - 3.5).abs() < 0.001);
        assert!((sum.network - 5.5).abs() < 0.001);
        assert!((sum.memory - 7.5).abs() < 0.001);
    }

    #[test]
    fn test_cost_total() {
        let cost = Cost::new(1.0, 2.0, 3.0, 4.0);
        assert!((cost.total() - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_scan_cost() {
        let model = CostModel::default();
        let cost = model.scan_cost(10000, 100);

        assert!(cost.cpu > 0.0);
        assert!(cost.io > 0.0);
    }

    #[test]
    fn test_hash_join_cost() {
        let model = CostModel::default();
        let cost = model.hash_join_cost(1000, 5000, 500);

        assert!(cost.cpu > 0.0);
        assert!(cost.memory > 0.0);
    }

    #[test]
    fn test_sort_cost_small() {
        let model = CostModel::default();
        let cost = model.sort_cost(1000, 100);

        assert!(cost.cpu > 0.0);
        assert!(cost.io == 0.0); // In-memory sort
    }

    #[test]
    fn test_nested_loop_more_expensive() {
        let model = CostModel::default();
        let hash_cost = model.hash_join_cost(10000, 10000, 1000);
        let nested_cost = model.nested_loop_join_cost(10000, 10000, 1000);

        // Nested loop should be more expensive for large tables
        assert!(nested_cost.total() > hash_cost.total());
    }
}
