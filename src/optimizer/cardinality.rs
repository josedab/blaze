//! Cardinality Estimation
//!
//! Estimates the number of rows produced by query operators using statistics.


use crate::error::Result;
use crate::planner::{BinaryOp, LogicalExpr, LogicalPlan, JoinType};

use super::statistics::StatisticsManager;

/// Default selectivity for equality predicates.
const DEFAULT_EQ_SELECTIVITY: f64 = 0.1;

/// Default selectivity for range predicates.
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;

/// Default selectivity for LIKE predicates.
const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;

/// Default selectivity for IS NULL predicates.
const DEFAULT_NULL_SELECTIVITY: f64 = 0.01;

/// Default number of rows when no statistics available.
const DEFAULT_ROW_COUNT: usize = 1000;

/// Cardinality estimator for query plans.
#[derive(Debug, Default)]
pub struct CardinalityEstimator {
    /// Use statistics when available
    use_statistics: bool,
}

impl CardinalityEstimator {
    /// Create a new cardinality estimator.
    pub fn new() -> Self {
        Self {
            use_statistics: true,
        }
    }

    /// Create an estimator that ignores statistics.
    pub fn without_statistics() -> Self {
        Self {
            use_statistics: false,
        }
    }

    /// Estimate cardinality for a logical plan and return the same plan.
    /// In a full implementation, this would annotate each node with its estimated cardinality.
    pub fn estimate(
        &self,
        plan: &LogicalPlan,
        stats_manager: &StatisticsManager,
    ) -> Result<LogicalPlan> {
        // For now, we just return a clone of the plan
        // In a full implementation, we would annotate each node with its estimated cardinality
        let _ = self.estimate_plan(plan, stats_manager)?;
        Ok(plan.clone())
    }

    /// Estimate cardinality for a logical plan node.
    pub fn estimate_plan(
        &self,
        plan: &LogicalPlan,
        stats_manager: &StatisticsManager,
    ) -> Result<usize> {
        match plan {
            LogicalPlan::TableScan { table_ref, .. } => {
                self.estimate_scan(&table_ref.to_string(), stats_manager)
            }
            LogicalPlan::Filter { input, predicate, .. } => {
                let input_cardinality = self.estimate_plan(input, stats_manager)?;
                let selectivity = self.estimate_selectivity(predicate, stats_manager);
                Ok((input_cardinality as f64 * selectivity).ceil() as usize)
            }
            LogicalPlan::Projection { input, .. } => {
                self.estimate_plan(input, stats_manager)
            }
            LogicalPlan::Aggregate { input, group_by, .. } => {
                let input_cardinality = self.estimate_plan(input, stats_manager)?;
                self.estimate_aggregate(input_cardinality, group_by.len())
            }
            LogicalPlan::Sort { input, .. } => {
                self.estimate_plan(input, stats_manager)
            }
            LogicalPlan::Limit { input, fetch, .. } => {
                let input_cardinality = self.estimate_plan(input, stats_manager)?;
                Ok(fetch.unwrap_or(input_cardinality).min(input_cardinality))
            }
            LogicalPlan::Join { left, right, join_type, .. } => {
                let left_cardinality = self.estimate_plan(left, stats_manager)?;
                let right_cardinality = self.estimate_plan(right, stats_manager)?;
                self.estimate_join(left_cardinality, right_cardinality, join_type)
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                let left_cardinality = self.estimate_plan(left, stats_manager)?;
                let right_cardinality = self.estimate_plan(right, stats_manager)?;
                Ok(left_cardinality * right_cardinality)
            }
            LogicalPlan::Distinct { input, .. } => {
                let input_cardinality = self.estimate_plan(input, stats_manager)?;
                // Assume distinct removes about half the rows
                Ok((input_cardinality as f64 * 0.5).ceil() as usize)
            }
            LogicalPlan::SetOperation { left, right, .. } => {
                let left_cardinality = self.estimate_plan(left, stats_manager)?;
                let right_cardinality = self.estimate_plan(right, stats_manager)?;
                Ok(left_cardinality + right_cardinality)
            }
            LogicalPlan::Values { values, .. } => {
                Ok(values.len())
            }
            LogicalPlan::SubqueryAlias { input, .. } => {
                self.estimate_plan(input, stats_manager)
            }
            LogicalPlan::Window { input, .. } => {
                self.estimate_plan(input, stats_manager)
            }
            LogicalPlan::EmptyRelation { produce_one_row, .. } => {
                Ok(if *produce_one_row { 1 } else { 0 })
            }
            LogicalPlan::Explain { .. } => {
                Ok(1) // Explain returns a single row
            }
            LogicalPlan::ExplainAnalyze { plan, .. } => {
                self.estimate_plan(plan, stats_manager)
            }
            LogicalPlan::CreateTable { .. } => Ok(0),
            LogicalPlan::DropTable { .. } => Ok(0),
            LogicalPlan::Insert { input, .. } => {
                self.estimate_plan(input, stats_manager)
            }
            LogicalPlan::Delete { .. } => Ok(0),
            LogicalPlan::Update { .. } => Ok(0),
        }
    }

    /// Estimate cardinality for a table scan.
    fn estimate_scan(&self, table_name: &str, stats_manager: &StatisticsManager) -> Result<usize> {
        if self.use_statistics {
            if let Some(stats) = stats_manager.get(table_name) {
                return Ok(stats.row_count);
            }
        }
        Ok(DEFAULT_ROW_COUNT)
    }

    /// Estimate selectivity of a predicate.
    pub fn estimate_selectivity(
        &self,
        expr: &LogicalExpr,
        stats_manager: &StatisticsManager,
    ) -> f64 {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                self.estimate_binary_selectivity(left, op, right, stats_manager)
            }
            LogicalExpr::Not(inner) => {
                1.0 - self.estimate_selectivity(inner, stats_manager)
            }
            LogicalExpr::IsNull(_) => DEFAULT_NULL_SELECTIVITY,
            LogicalExpr::IsNotNull(_) => 1.0 - DEFAULT_NULL_SELECTIVITY,
            LogicalExpr::Between { .. } => {
                // BETWEEN is equivalent to two range predicates
                DEFAULT_RANGE_SELECTIVITY * DEFAULT_RANGE_SELECTIVITY
            }
            LogicalExpr::Like { .. } => DEFAULT_LIKE_SELECTIVITY,
            LogicalExpr::InList { list, negated, .. } => {
                let base_selectivity = list.len() as f64 * DEFAULT_EQ_SELECTIVITY;
                if *negated {
                    1.0 - base_selectivity.min(1.0)
                } else {
                    base_selectivity.min(1.0)
                }
            }
            LogicalExpr::Exists { .. } => 0.5, // Assume 50% selectivity for EXISTS
            LogicalExpr::Literal(value) => {
                // Boolean literals
                if let crate::types::ScalarValue::Boolean(Some(b)) = value {
                    if *b { 1.0 } else { 0.0 }
                } else {
                    1.0
                }
            }
            _ => 1.0, // No filtering for other expressions
        }
    }

    /// Estimate selectivity for binary expressions.
    fn estimate_binary_selectivity(
        &self,
        left: &LogicalExpr,
        op: &BinaryOp,
        right: &LogicalExpr,
        stats_manager: &StatisticsManager,
    ) -> f64 {
        match op {
            BinaryOp::And => {
                let left_sel = self.estimate_selectivity(left, stats_manager);
                let right_sel = self.estimate_selectivity(right, stats_manager);
                left_sel * right_sel
            }
            BinaryOp::Or => {
                let left_sel = self.estimate_selectivity(left, stats_manager);
                let right_sel = self.estimate_selectivity(right, stats_manager);
                // P(A OR B) = P(A) + P(B) - P(A AND B)
                left_sel + right_sel - (left_sel * right_sel)
            }
            BinaryOp::Eq => {
                // Check if comparing column to literal
                if self.is_column_to_literal(left, right) {
                    if let Some(col_name) = self.get_column_name(left).or_else(|| self.get_column_name(right)) {
                        // Try to use statistics
                        for table_name in stats_manager.list_tables() {
                            if let Some(table_stats) = stats_manager.get(&table_name) {
                                if let Some(col_stats) = table_stats.column(&col_name) {
                                    return col_stats.selectivity_eq("");
                                }
                            }
                        }
                    }
                }
                DEFAULT_EQ_SELECTIVITY
            }
            BinaryOp::NotEq => 1.0 - DEFAULT_EQ_SELECTIVITY,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => DEFAULT_RANGE_SELECTIVITY,
            _ => 1.0,
        }
    }

    /// Check if expression is column compared to literal.
    fn is_column_to_literal(&self, left: &LogicalExpr, right: &LogicalExpr) -> bool {
        matches!(
            (left, right),
            (LogicalExpr::Column(_), LogicalExpr::Literal(_))
                | (LogicalExpr::Literal(_), LogicalExpr::Column(_))
        )
    }

    /// Get column name from expression.
    fn get_column_name(&self, expr: &LogicalExpr) -> Option<String> {
        match expr {
            LogicalExpr::Column(col) => Some(col.name.clone()),
            _ => None,
        }
    }

    /// Estimate cardinality for aggregate operations.
    fn estimate_aggregate(&self, input_cardinality: usize, num_group_keys: usize) -> Result<usize> {
        if num_group_keys == 0 {
            // Scalar aggregate - returns one row
            return Ok(1);
        }

        // Estimate number of groups
        // Use heuristic: assume each additional group key reduces cardinality
        let reduction_factor = match num_group_keys {
            1 => 0.1,  // Single key: 10% of input
            2 => 0.05, // Two keys: 5% of input
            _ => 0.01, // Many keys: 1% of input
        };

        let estimated = (input_cardinality as f64 * reduction_factor).ceil() as usize;
        Ok(estimated.max(1))
    }

    /// Estimate cardinality for join operations.
    fn estimate_join(
        &self,
        left_cardinality: usize,
        right_cardinality: usize,
        join_type: &JoinType,
    ) -> Result<usize> {
        match join_type {
            JoinType::Inner => {
                // Assume 10% selectivity for inner joins
                let estimated = (left_cardinality * right_cardinality) as f64 * 0.1;
                Ok(estimated.ceil() as usize)
            }
            JoinType::Left => {
                // Left join preserves all left rows, plus matched right rows
                let matched = (right_cardinality as f64 * 0.1).ceil() as usize;
                Ok(left_cardinality + matched)
            }
            JoinType::Right => {
                // Right join preserves all right rows, plus matched left rows
                let matched = (left_cardinality as f64 * 0.1).ceil() as usize;
                Ok(right_cardinality + matched)
            }
            JoinType::Full => {
                // Full join: union of left and right plus matches
                Ok(left_cardinality + right_cardinality)
            }
            JoinType::Cross => {
                // Cartesian product
                Ok(left_cardinality * right_cardinality)
            }
            JoinType::LeftSemi | JoinType::RightSemi => {
                // At most one side's cardinality
                let estimated = (left_cardinality as f64 * 0.5).ceil() as usize;
                Ok(estimated)
            }
            JoinType::LeftAnti | JoinType::RightAnti => {
                // Rows from one side that don't match
                let estimated = (left_cardinality as f64 * 0.5).ceil() as usize;
                Ok(estimated)
            }
        }
    }
}

/// Statistics about estimated cardinality for a plan node.
#[derive(Debug, Clone)]
pub struct CardinalityInfo {
    /// Estimated number of rows
    pub row_count: usize,
    /// Estimated row width in bytes
    pub row_width: usize,
    /// Estimated distinct values per column
    pub distinct_counts: Vec<usize>,
}

impl CardinalityInfo {
    /// Create new cardinality info.
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            row_width: 100, // Default estimate
            distinct_counts: Vec::new(),
        }
    }

    /// Set row width.
    pub fn with_row_width(mut self, width: usize) -> Self {
        self.row_width = width;
        self
    }

    /// Estimated total size in bytes.
    pub fn estimated_size(&self) -> usize {
        self.row_count * self.row_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::catalog::ResolvedTableRef;
    use crate::types::{DataType, Field, Schema};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ])
    }

    fn create_test_table_ref() -> ResolvedTableRef {
        ResolvedTableRef::new("default", "main", "test_table")
    }

    #[test]
    fn test_estimate_scan_without_stats() {
        let estimator = CardinalityEstimator::new();
        let stats_manager = StatisticsManager::new();

        let cardinality = estimator.estimate_scan("test_table", &stats_manager).unwrap();
        assert_eq!(cardinality, DEFAULT_ROW_COUNT);
    }

    #[test]
    fn test_estimate_scan_with_stats() {
        use super::super::statistics::TableStatistics;

        let estimator = CardinalityEstimator::new();
        let stats_manager = StatisticsManager::new();

        let table_stats = TableStatistics::new("test_table", 50000);
        stats_manager.register(table_stats).unwrap();

        let cardinality = estimator.estimate_scan("test_table", &stats_manager).unwrap();
        assert_eq!(cardinality, 50000);
    }

    #[test]
    fn test_estimate_filter() {
        let estimator = CardinalityEstimator::new();
        let stats_manager = StatisticsManager::new();

        let schema = create_test_schema();
        let scan = LogicalPlan::TableScan {
            table_ref: create_test_table_ref(),
            projection: None,
            schema,
            filters: vec![],
            time_travel: None,
        };

        let filter = LogicalPlan::Filter {
            input: Arc::new(scan),
            predicate: LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(crate::planner::Column::unqualified("id"))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(crate::types::ScalarValue::Int32(Some(1)))),
            },
        };

        let cardinality = estimator.estimate_plan(&filter, &stats_manager).unwrap();
        // Should apply DEFAULT_EQ_SELECTIVITY (0.1) to DEFAULT_ROW_COUNT (1000)
        assert_eq!(cardinality, 100);
    }

    #[test]
    fn test_estimate_aggregate() {
        let estimator = CardinalityEstimator::new();

        // Scalar aggregate
        let cardinality = estimator.estimate_aggregate(1000, 0).unwrap();
        assert_eq!(cardinality, 1);

        // Single group key
        let cardinality = estimator.estimate_aggregate(1000, 1).unwrap();
        assert_eq!(cardinality, 100); // 10% of 1000

        // Multiple group keys
        let cardinality = estimator.estimate_aggregate(1000, 3).unwrap();
        assert_eq!(cardinality, 10); // 1% of 1000
    }

    #[test]
    fn test_estimate_join() {
        let estimator = CardinalityEstimator::new();

        // Inner join
        let cardinality = estimator.estimate_join(1000, 500, &JoinType::Inner).unwrap();
        assert_eq!(cardinality, 50000); // 1000 * 500 * 0.1

        // Cross join
        let cardinality = estimator.estimate_join(100, 100, &JoinType::Cross).unwrap();
        assert_eq!(cardinality, 10000); // 100 * 100
    }

    #[test]
    fn test_selectivity_and() {
        let estimator = CardinalityEstimator::new();
        let stats_manager = StatisticsManager::new();

        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(crate::planner::Column::unqualified("a"))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(crate::types::ScalarValue::Int32(Some(1)))),
            }),
            op: BinaryOp::And,
            right: Box::new(LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(crate::planner::Column::unqualified("b"))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(crate::types::ScalarValue::Int32(Some(2)))),
            }),
        };

        let selectivity = estimator.estimate_selectivity(&expr, &stats_manager);
        // 0.1 * 0.1 = 0.01
        assert!((selectivity - 0.01).abs() < 0.001);
    }

    #[test]
    fn test_selectivity_or() {
        let estimator = CardinalityEstimator::new();
        let stats_manager = StatisticsManager::new();

        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(crate::planner::Column::unqualified("a"))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(crate::types::ScalarValue::Int32(Some(1)))),
            }),
            op: BinaryOp::Or,
            right: Box::new(LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(crate::planner::Column::unqualified("b"))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(crate::types::ScalarValue::Int32(Some(2)))),
            }),
        };

        let selectivity = estimator.estimate_selectivity(&expr, &stats_manager);
        // 0.1 + 0.1 - 0.1*0.1 = 0.19
        assert!((selectivity - 0.19).abs() < 0.001);
    }

    #[test]
    fn test_cardinality_info() {
        let info = CardinalityInfo::new(10000)
            .with_row_width(50);

        assert_eq!(info.row_count, 10000);
        assert_eq!(info.row_width, 50);
        assert_eq!(info.estimated_size(), 500000);
    }
}
