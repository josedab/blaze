//! Cost-Based Query Optimizer
//!
//! This module provides a cost-based optimizer that uses statistics to choose
//! the best execution plan for a query.
//!
//! # Components
//!
//! - **Statistics**: Table and column statistics for cardinality estimation
//! - **Cost Model**: Cost functions for different physical operators
//! - **Join Ordering**: Dynamic programming-based join order optimization
//! - **Cardinality Estimation**: Row count estimation using histograms and selectivity
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::optimizer::{CostBasedOptimizer, Statistics};
//!
//! let optimizer = CostBasedOptimizer::new();
//! let optimized_plan = optimizer.optimize(logical_plan, &statistics)?;
//! ```

mod cardinality;
mod cost_model;
mod join_ordering;
mod statistics;

pub use cardinality::CardinalityEstimator;
pub use cost_model::{Cost, CostModel, DEFAULT_CPU_COST, DEFAULT_IO_COST};
pub use join_ordering::JoinOrderOptimizer;
pub use statistics::{ColumnStatistics, Histogram, StatisticsManager, TableStatistics};

use crate::error::Result;
use crate::planner::LogicalPlan;

/// Cost-based query optimizer.
pub struct CostBasedOptimizer {
    cost_model: CostModel,
    cardinality_estimator: CardinalityEstimator,
    join_optimizer: JoinOrderOptimizer,
}

impl CostBasedOptimizer {
    /// Create a new cost-based optimizer with default settings.
    pub fn new() -> Self {
        Self {
            cost_model: CostModel::default(),
            cardinality_estimator: CardinalityEstimator::new(),
            join_optimizer: JoinOrderOptimizer::new(),
        }
    }

    /// Create a new optimizer with a custom cost model.
    pub fn with_cost_model(cost_model: CostModel) -> Self {
        Self {
            cost_model,
            cardinality_estimator: CardinalityEstimator::new(),
            join_optimizer: JoinOrderOptimizer::new(),
        }
    }

    /// Optimize a logical plan using cost-based optimization.
    pub fn optimize(
        &self,
        plan: &LogicalPlan,
        stats_manager: &StatisticsManager,
    ) -> Result<LogicalPlan> {
        // First, estimate cardinalities for all nodes
        let plan_with_stats = self.cardinality_estimator.estimate(plan, stats_manager)?;

        // Optimize join ordering if there are multiple joins
        let plan_with_optimized_joins = self
            .join_optimizer
            .optimize(&plan_with_stats, &self.cost_model)?;

        Ok(plan_with_optimized_joins)
    }

    /// Estimate the cost of a logical plan.
    pub fn estimate_cost(
        &self,
        plan: &LogicalPlan,
        stats_manager: &StatisticsManager,
    ) -> Result<Cost> {
        let plan_with_stats = self.cardinality_estimator.estimate(plan, stats_manager)?;
        self.cost_model.estimate(&plan_with_stats)
    }

    /// Get the cost model.
    pub fn cost_model(&self) -> &CostModel {
        &self.cost_model
    }
}

impl Default for CostBasedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_creation() {
        let optimizer = CostBasedOptimizer::new();
        assert!(optimizer.cost_model.cpu_cost_factor() > 0.0);
    }
}
