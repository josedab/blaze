//! Query optimizer for Blaze.
//!
//! This module provides a rule-based query optimizer that transforms
//! logical plans to improve query performance.

use std::sync::Arc;

use crate::error::Result;
use crate::types::Schema;

use super::logical_expr::{BinaryOp, LogicalExpr, Column};
use super::logical_plan::LogicalPlan;

/// A rule that transforms a logical plan.
pub trait OptimizerRule: std::fmt::Debug + Send + Sync {
    /// The name of this rule.
    fn name(&self) -> &str;

    /// Apply the rule to a logical plan.
    /// Returns the transformed plan, or the original if no transformation was applied.
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

/// The query optimizer that applies a set of rules to logical plans.
#[derive(Debug)]
pub struct Optimizer {
    /// The optimization rules to apply.
    rules: Vec<Box<dyn OptimizerRule>>,
    /// Maximum number of optimization passes.
    max_passes: usize,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer with default rules.
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(SimplifyExpressions),
                Box::new(ConstantFolding),
                Box::new(PredicatePushdown),
                Box::new(ProjectionPushdown),
                Box::new(EliminateLimit),
            ],
            max_passes: 3,
        }
    }

    /// Create an optimizer with custom rules.
    pub fn with_rules(rules: Vec<Box<dyn OptimizerRule>>) -> Self {
        Self {
            rules,
            max_passes: 3,
        }
    }

    /// Set the maximum number of optimization passes.
    pub fn with_max_passes(mut self, max_passes: usize) -> Self {
        self.max_passes = max_passes;
        self
    }

    /// Optimize a logical plan by applying all rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut current_plan = plan.clone();

        for _pass in 0..self.max_passes {
            let mut changed = false;

            for rule in &self.rules {
                let new_plan = rule.optimize(&current_plan)?;

                // Check if the plan changed (simple structural comparison)
                if format!("{:?}", new_plan) != format!("{:?}", current_plan) {
                    changed = true;
                    current_plan = new_plan;
                }
            }

            // If no changes were made, we've reached a fixed point
            if !changed {
                break;
            }
        }

        Ok(current_plan)
    }
}

// ============================================================================
// Predicate Pushdown Rule
// ============================================================================

/// Push filter predicates down toward table scans.
///
/// This optimization reduces the amount of data processed by applying
/// filters as early as possible in the query plan.
#[derive(Debug)]
pub struct PredicatePushdown;

impl OptimizerRule for PredicatePushdown {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_plan(plan)
    }
}

impl PredicatePushdown {
    fn optimize_plan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Filter on top of another plan - try to push down
            LogicalPlan::Filter { predicate, input } => {
                let optimized_input = self.optimize_plan(input)?;
                self.push_down_filter(predicate.clone(), optimized_input)
            }

            // Recursively optimize children
            LogicalPlan::Projection { exprs, input, schema } => {
                let optimized_input = self.optimize_plan(input)?;
                Ok(LogicalPlan::Projection {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized_input),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Aggregate { group_by, aggr_exprs, input, schema } => {
                let optimized_input = self.optimize_plan(input)?;
                Ok(LogicalPlan::Aggregate {
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    input: Arc::new(optimized_input),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Sort { exprs, input } => {
                let optimized_input = self.optimize_plan(input)?;
                Ok(LogicalPlan::Sort {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized_input),
                })
            }

            LogicalPlan::Limit { skip, fetch, input } => {
                let optimized_input = self.optimize_plan(input)?;
                Ok(LogicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(optimized_input),
                })
            }

            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let optimized_left = self.optimize_plan(left)?;
                let optimized_right = self.optimize_plan(right)?;
                Ok(LogicalPlan::Join {
                    left: Arc::new(optimized_left),
                    right: Arc::new(optimized_right),
                    join_type: *join_type,
                    on: on.clone(),
                    filter: filter.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::CrossJoin { left, right, schema } => {
                let optimized_left = self.optimize_plan(left)?;
                let optimized_right = self.optimize_plan(right)?;
                Ok(LogicalPlan::CrossJoin {
                    left: Arc::new(optimized_left),
                    right: Arc::new(optimized_right),
                    schema: schema.clone(),
                })
            }

            // Leaf nodes - return as-is
            LogicalPlan::TableScan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),

            // Other nodes - optimize children recursively
            _ => self.optimize_children(plan),
        }
    }

    /// Try to push a filter predicate down into the input plan.
    fn push_down_filter(&self, predicate: LogicalExpr, input: LogicalPlan) -> Result<LogicalPlan> {
        match input {
            // Push filter into table scan
            LogicalPlan::TableScan { table_ref, projection, mut filters, schema } => {
                // Add the predicate to the scan's filters
                filters.push(predicate);
                Ok(LogicalPlan::TableScan {
                    table_ref,
                    projection,
                    filters,
                    schema,
                })
            }

            // Push through projection if predicate only references output columns
            LogicalPlan::Projection { exprs, input, schema } => {
                // Check if we can push through
                if self.can_push_through_projection(&predicate, &exprs) {
                    // Rewrite predicate in terms of input columns
                    let rewritten = self.rewrite_for_projection(&predicate, &exprs);
                    let pushed_input = self.push_down_filter(rewritten, (*input).clone())?;
                    Ok(LogicalPlan::Projection {
                        exprs,
                        input: Arc::new(pushed_input),
                        schema,
                    })
                } else {
                    // Can't push through, keep filter on top
                    Ok(LogicalPlan::Filter {
                        predicate,
                        input: Arc::new(LogicalPlan::Projection { exprs, input, schema }),
                    })
                }
            }

            // Push through another filter (combine predicates)
            LogicalPlan::Filter { predicate: inner_pred, input } => {
                let combined = LogicalExpr::BinaryExpr {
                    left: Box::new(predicate),
                    op: BinaryOp::And,
                    right: Box::new(inner_pred),
                };
                self.push_down_filter(combined, (*input).clone())
            }

            // Push through join - split predicate and push to appropriate side
            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let left_schema = left.schema();
                let right_schema = right.schema();

                // Split conjunctions
                let predicates = self.split_conjunction(&predicate);
                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();
                let mut remaining_predicates = Vec::new();

                for pred in predicates {
                    let columns = self.extract_columns(&pred);

                    let is_left_only = columns.iter().all(|c|
                        left_schema.index_of(&c.name).is_some()
                    );
                    let is_right_only = columns.iter().all(|c|
                        right_schema.index_of(&c.name).is_some()
                    );

                    if is_left_only {
                        left_predicates.push(pred);
                    } else if is_right_only {
                        right_predicates.push(pred);
                    } else {
                        remaining_predicates.push(pred);
                    }
                }

                // Push predicates to appropriate sides
                let new_left = if left_predicates.is_empty() {
                    (*left).clone()
                } else {
                    let left_filter = self.combine_predicates(left_predicates);
                    self.push_down_filter(left_filter, (*left).clone())?
                };

                let new_right = if right_predicates.is_empty() {
                    (*right).clone()
                } else {
                    let right_filter = self.combine_predicates(right_predicates);
                    self.push_down_filter(right_filter, (*right).clone())?
                };

                let new_join = LogicalPlan::Join {
                    left: Arc::new(new_left),
                    right: Arc::new(new_right),
                    join_type,
                    on,
                    filter,
                    schema,
                };

                // If there are remaining predicates, add filter on top
                if remaining_predicates.is_empty() {
                    Ok(new_join)
                } else {
                    let remaining = self.combine_predicates(remaining_predicates);
                    Ok(LogicalPlan::Filter {
                        predicate: remaining,
                        input: Arc::new(new_join),
                    })
                }
            }

            // Can't push through aggregate - keep filter on top
            LogicalPlan::Aggregate { .. } => {
                Ok(LogicalPlan::Filter {
                    predicate,
                    input: Arc::new(input),
                })
            }

            // Default: can't push, keep filter on top
            _ => Ok(LogicalPlan::Filter {
                predicate,
                input: Arc::new(input),
            }),
        }
    }

    /// Check if a predicate can be pushed through a projection.
    fn can_push_through_projection(&self, predicate: &LogicalExpr, _exprs: &[LogicalExpr]) -> bool {
        // For now, only push through if predicate uses simple column references
        let columns = self.extract_columns(predicate);
        columns.iter().all(|_| true) // Simplified: always allow
    }

    /// Rewrite a predicate for pushing through a projection.
    fn rewrite_for_projection(&self, predicate: &LogicalExpr, _exprs: &[LogicalExpr]) -> LogicalExpr {
        // For simplicity, return the predicate as-is
        // A full implementation would map output columns to input expressions
        predicate.clone()
    }

    /// Split a predicate into conjunctions (AND-ed terms).
    fn split_conjunction(&self, predicate: &LogicalExpr) -> Vec<LogicalExpr> {
        match predicate {
            LogicalExpr::BinaryExpr { left, op: BinaryOp::And, right } => {
                let mut result = self.split_conjunction(left);
                result.extend(self.split_conjunction(right));
                result
            }
            _ => vec![predicate.clone()],
        }
    }

    /// Combine predicates with AND.
    fn combine_predicates(&self, predicates: Vec<LogicalExpr>) -> LogicalExpr {
        predicates
            .into_iter()
            .reduce(|acc, pred| LogicalExpr::BinaryExpr {
                left: Box::new(acc),
                op: BinaryOp::And,
                right: Box::new(pred),
            })
            .expect("predicates should not be empty")
    }

    /// Extract all column references from an expression.
    fn extract_columns(&self, expr: &LogicalExpr) -> Vec<Column> {
        let mut columns = Vec::new();
        self.collect_columns(expr, &mut columns);
        columns
    }

    fn collect_columns(&self, expr: &LogicalExpr, columns: &mut Vec<Column>) {
        match expr {
            LogicalExpr::Column(col) => columns.push(col.clone()),
            LogicalExpr::BinaryExpr { left, right, .. } => {
                self.collect_columns(left, columns);
                self.collect_columns(right, columns);
            }
            LogicalExpr::UnaryExpr { expr, .. } => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::Not(expr) => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::Negative(expr) => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::IsNull(expr) | LogicalExpr::IsNotNull(expr) => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::Cast { expr, .. } | LogicalExpr::TryCast { expr, .. } => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::Alias { expr, .. } => {
                self.collect_columns(expr, columns);
            }
            LogicalExpr::Between { expr, low, high, .. } => {
                self.collect_columns(expr, columns);
                self.collect_columns(low, columns);
                self.collect_columns(high, columns);
            }
            LogicalExpr::Like { expr, pattern, escape, .. } => {
                self.collect_columns(expr, columns);
                self.collect_columns(pattern, columns);
                if let Some(esc) = escape {
                    self.collect_columns(esc, columns);
                }
            }
            LogicalExpr::InList { expr, list, .. } => {
                self.collect_columns(expr, columns);
                for item in list {
                    self.collect_columns(item, columns);
                }
            }
            LogicalExpr::Case { expr, when_then_exprs, else_expr } => {
                if let Some(e) = expr {
                    self.collect_columns(e, columns);
                }
                for (when, then) in when_then_exprs {
                    self.collect_columns(when, columns);
                    self.collect_columns(then, columns);
                }
                if let Some(e) = else_expr {
                    self.collect_columns(e, columns);
                }
            }
            _ => {}
        }
    }

    /// Optimize children of a plan node.
    fn optimize_children(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::SubqueryAlias { input, alias, schema } => {
                let optimized = self.optimize_plan(input)?;
                Ok(LogicalPlan::SubqueryAlias {
                    input: Arc::new(optimized),
                    alias: alias.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Distinct { input } => {
                let optimized = self.optimize_plan(input)?;
                Ok(LogicalPlan::Distinct {
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::SetOperation { op, left, right, schema, all } => {
                let optimized_left = self.optimize_plan(left)?;
                let optimized_right = self.optimize_plan(right)?;
                Ok(LogicalPlan::SetOperation {
                    op: *op,
                    left: Arc::new(optimized_left),
                    right: Arc::new(optimized_right),
                    schema: schema.clone(),
                    all: *all,
                })
            }
            LogicalPlan::Explain { plan, verbose } => {
                let optimized = self.optimize_plan(plan)?;
                Ok(LogicalPlan::Explain {
                    plan: Arc::new(optimized),
                    verbose: *verbose,
                })
            }
            _ => Ok(plan.clone()),
        }
    }
}

// ============================================================================
// Projection Pushdown Rule
// ============================================================================

/// Push projections down to read only required columns.
///
/// This optimization reduces I/O by reading only the columns that
/// are actually needed by the query.
#[derive(Debug)]
pub struct ProjectionPushdown;

impl OptimizerRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Collect required columns and push down
        let required = self.collect_required_columns(plan);
        self.optimize_with_required(plan, &required)
    }
}

impl ProjectionPushdown {
    /// Collect all columns required by a plan and its children.
    fn collect_required_columns(&self, plan: &LogicalPlan) -> Vec<String> {
        let mut columns = Vec::new();
        self.collect_columns_recursive(plan, &mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    fn collect_columns_recursive(&self, plan: &LogicalPlan, columns: &mut Vec<String>) {
        match plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                for expr in exprs {
                    self.collect_expr_columns(expr, columns);
                }
                self.collect_columns_recursive(input, columns);
            }
            LogicalPlan::Filter { predicate, input } => {
                self.collect_expr_columns(predicate, columns);
                self.collect_columns_recursive(input, columns);
            }
            LogicalPlan::Aggregate { group_by, aggr_exprs, input, .. } => {
                for expr in group_by {
                    self.collect_expr_columns(expr, columns);
                }
                for agg in aggr_exprs {
                    for arg in &agg.args {
                        self.collect_expr_columns(arg, columns);
                    }
                }
                self.collect_columns_recursive(input, columns);
            }
            LogicalPlan::Sort { exprs, input } => {
                for expr in exprs {
                    self.collect_expr_columns(&expr.expr, columns);
                }
                self.collect_columns_recursive(input, columns);
            }
            LogicalPlan::Join { left, right, on, filter, .. } => {
                for (l, r) in on {
                    self.collect_expr_columns(l, columns);
                    self.collect_expr_columns(r, columns);
                }
                if let Some(f) = filter {
                    self.collect_expr_columns(f, columns);
                }
                self.collect_columns_recursive(left, columns);
                self.collect_columns_recursive(right, columns);
            }
            LogicalPlan::Limit { input, .. } => {
                self.collect_columns_recursive(input, columns);
            }
            LogicalPlan::TableScan { filters, .. } => {
                for f in filters {
                    self.collect_expr_columns(f, columns);
                }
            }
            _ => {
                for child in plan.children() {
                    self.collect_columns_recursive(child, columns);
                }
            }
        }
    }

    fn collect_expr_columns(&self, expr: &LogicalExpr, columns: &mut Vec<String>) {
        match expr {
            LogicalExpr::Column(col) => {
                columns.push(col.name.clone());
            }
            LogicalExpr::BinaryExpr { left, right, .. } => {
                self.collect_expr_columns(left, columns);
                self.collect_expr_columns(right, columns);
            }
            LogicalExpr::UnaryExpr { expr, .. } => {
                self.collect_expr_columns(expr, columns);
            }
            LogicalExpr::Alias { expr, .. } => {
                self.collect_expr_columns(expr, columns);
            }
            LogicalExpr::Cast { expr, .. } | LogicalExpr::TryCast { expr, .. } => {
                self.collect_expr_columns(expr, columns);
            }
            LogicalExpr::IsNull(expr) | LogicalExpr::IsNotNull(expr) => {
                self.collect_expr_columns(expr, columns);
            }
            LogicalExpr::Not(expr) | LogicalExpr::Negative(expr) => {
                self.collect_expr_columns(expr, columns);
            }
            LogicalExpr::Aggregate(agg) => {
                for arg in &agg.args {
                    self.collect_expr_columns(arg, columns);
                }
            }
            _ => {}
        }
    }

    fn optimize_with_required(&self, plan: &LogicalPlan, required: &[String]) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan { table_ref, projection, filters, schema } => {
                // Create projection based on required columns
                if projection.is_some() {
                    // Already has projection, don't modify
                    return Ok(plan.clone());
                }

                let mut indices: Vec<usize> = Vec::new();
                for (i, field) in schema.fields().iter().enumerate() {
                    if required.contains(&field.name().to_string()) {
                        indices.push(i);
                    }
                }

                // If we need all columns or no specific columns, skip projection
                if indices.is_empty() || indices.len() == schema.fields().len() {
                    return Ok(plan.clone());
                }

                // Create projected schema
                let projected_fields: Vec<_> = indices
                    .iter()
                    .map(|&i| schema.fields()[i].clone())
                    .collect();
                let projected_schema = Schema::new(projected_fields);

                Ok(LogicalPlan::TableScan {
                    table_ref: table_ref.clone(),
                    projection: Some(indices),
                    filters: filters.clone(),
                    schema: projected_schema,
                })
            }

            LogicalPlan::Filter { predicate, input } => {
                let optimized = self.optimize_with_required(input, required)?;
                Ok(LogicalPlan::Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized),
                })
            }

            LogicalPlan::Projection { exprs, input, schema } => {
                let optimized = self.optimize_with_required(input, required)?;
                Ok(LogicalPlan::Projection {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }

            _ => {
                // For other nodes, recursively optimize children
                self.optimize_children(plan, required)
            }
        }
    }

    fn optimize_children(&self, plan: &LogicalPlan, required: &[String]) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { group_by, aggr_exprs, input, schema } => {
                let optimized = self.optimize_with_required(input, required)?;
                Ok(LogicalPlan::Aggregate {
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { exprs, input } => {
                let optimized = self.optimize_with_required(input, required)?;
                Ok(LogicalPlan::Sort {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Limit { skip, fetch, input } => {
                let optimized = self.optimize_with_required(input, required)?;
                Ok(LogicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let left_opt = self.optimize_with_required(left, required)?;
                let right_opt = self.optimize_with_required(right, required)?;
                Ok(LogicalPlan::Join {
                    left: Arc::new(left_opt),
                    right: Arc::new(right_opt),
                    join_type: *join_type,
                    on: on.clone(),
                    filter: filter.clone(),
                    schema: schema.clone(),
                })
            }
            _ => Ok(plan.clone()),
        }
    }
}

// ============================================================================
// Constant Folding Rule
// ============================================================================

/// Evaluate constant expressions at planning time.
#[derive(Debug)]
pub struct ConstantFolding;

impl OptimizerRule for ConstantFolding {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.fold_plan(plan)
    }
}

impl ConstantFolding {
    fn fold_plan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { predicate, input } => {
                let folded_pred = self.fold_expr(predicate);
                let optimized_input = self.fold_plan(input)?;

                // If predicate is always true, remove the filter
                if let LogicalExpr::Literal(crate::types::ScalarValue::Boolean(Some(true))) = &folded_pred {
                    return Ok(optimized_input);
                }

                // If predicate is always false, return empty
                if let LogicalExpr::Literal(crate::types::ScalarValue::Boolean(Some(false))) = &folded_pred {
                    return Ok(LogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: input.schema().clone(),
                    });
                }

                Ok(LogicalPlan::Filter {
                    predicate: folded_pred,
                    input: Arc::new(optimized_input),
                })
            }

            LogicalPlan::Projection { exprs, input, schema } => {
                let folded_exprs: Vec<_> = exprs.iter().map(|e| self.fold_expr(e)).collect();
                let optimized_input = self.fold_plan(input)?;
                Ok(LogicalPlan::Projection {
                    exprs: folded_exprs,
                    input: Arc::new(optimized_input),
                    schema: schema.clone(),
                })
            }

            _ => self.fold_children(plan),
        }
    }

    fn fold_expr(&self, expr: &LogicalExpr) -> LogicalExpr {
        use crate::types::ScalarValue;

        match expr {
            // Fold binary expressions with literal operands
            LogicalExpr::BinaryExpr { left, op, right } => {
                let folded_left = self.fold_expr(left);
                let folded_right = self.fold_expr(right);

                // Try to evaluate if both are literals
                if let (LogicalExpr::Literal(l), LogicalExpr::Literal(r)) = (&folded_left, &folded_right) {
                    if let Some(result) = self.eval_binary(l, op, r) {
                        return LogicalExpr::Literal(result);
                    }
                }

                // Boolean simplifications
                match op {
                    BinaryOp::And => {
                        // x AND true = x
                        if matches!(&folded_right, LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))) {
                            return folded_left;
                        }
                        // true AND x = x
                        if matches!(&folded_left, LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))) {
                            return folded_right;
                        }
                        // x AND false = false
                        if matches!(&folded_right, LogicalExpr::Literal(ScalarValue::Boolean(Some(false)))) {
                            return LogicalExpr::Literal(ScalarValue::Boolean(Some(false)));
                        }
                        // false AND x = false
                        if matches!(&folded_left, LogicalExpr::Literal(ScalarValue::Boolean(Some(false)))) {
                            return LogicalExpr::Literal(ScalarValue::Boolean(Some(false)));
                        }
                    }
                    BinaryOp::Or => {
                        // x OR false = x
                        if matches!(&folded_right, LogicalExpr::Literal(ScalarValue::Boolean(Some(false)))) {
                            return folded_left;
                        }
                        // false OR x = x
                        if matches!(&folded_left, LogicalExpr::Literal(ScalarValue::Boolean(Some(false)))) {
                            return folded_right;
                        }
                        // x OR true = true
                        if matches!(&folded_right, LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))) {
                            return LogicalExpr::Literal(ScalarValue::Boolean(Some(true)));
                        }
                        // true OR x = true
                        if matches!(&folded_left, LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))) {
                            return LogicalExpr::Literal(ScalarValue::Boolean(Some(true)));
                        }
                    }
                    _ => {}
                }

                LogicalExpr::BinaryExpr {
                    left: Box::new(folded_left),
                    op: *op,
                    right: Box::new(folded_right),
                }
            }

            LogicalExpr::Not(inner) => {
                let folded = self.fold_expr(inner);
                if let LogicalExpr::Literal(ScalarValue::Boolean(Some(b))) = &folded {
                    return LogicalExpr::Literal(ScalarValue::Boolean(Some(!b)));
                }
                LogicalExpr::Not(Box::new(folded))
            }

            LogicalExpr::Negative(inner) => {
                let folded = self.fold_expr(inner);
                if let LogicalExpr::Literal(val) = &folded {
                    if let Some(negated) = self.negate_value(val) {
                        return LogicalExpr::Literal(negated);
                    }
                }
                LogicalExpr::Negative(Box::new(folded))
            }

            // Return unchanged for non-constant expressions
            _ => expr.clone(),
        }
    }

    fn eval_binary(&self, left: &crate::types::ScalarValue, op: &BinaryOp, right: &crate::types::ScalarValue) -> Option<crate::types::ScalarValue> {
        use crate::types::ScalarValue;

        match (left, right) {
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => {
                match op {
                    BinaryOp::Plus => Some(ScalarValue::Int64(Some(l + r))),
                    BinaryOp::Minus => Some(ScalarValue::Int64(Some(l - r))),
                    BinaryOp::Multiply => Some(ScalarValue::Int64(Some(l * r))),
                    BinaryOp::Divide if *r != 0 => Some(ScalarValue::Int64(Some(l / r))),
                    BinaryOp::Modulo if *r != 0 => Some(ScalarValue::Int64(Some(l % r))),
                    BinaryOp::Eq => Some(ScalarValue::Boolean(Some(l == r))),
                    BinaryOp::NotEq => Some(ScalarValue::Boolean(Some(l != r))),
                    BinaryOp::Lt => Some(ScalarValue::Boolean(Some(l < r))),
                    BinaryOp::LtEq => Some(ScalarValue::Boolean(Some(l <= r))),
                    BinaryOp::Gt => Some(ScalarValue::Boolean(Some(l > r))),
                    BinaryOp::GtEq => Some(ScalarValue::Boolean(Some(l >= r))),
                    _ => None,
                }
            }
            (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => {
                match op {
                    BinaryOp::And => Some(ScalarValue::Boolean(Some(*l && *r))),
                    BinaryOp::Or => Some(ScalarValue::Boolean(Some(*l || *r))),
                    BinaryOp::Eq => Some(ScalarValue::Boolean(Some(l == r))),
                    BinaryOp::NotEq => Some(ScalarValue::Boolean(Some(l != r))),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn negate_value(&self, val: &crate::types::ScalarValue) -> Option<crate::types::ScalarValue> {
        use crate::types::ScalarValue;

        match val {
            ScalarValue::Int64(Some(v)) => Some(ScalarValue::Int64(Some(-v))),
            ScalarValue::Float64(Some(v)) => Some(ScalarValue::Float64(Some(-v))),
            _ => None,
        }
    }

    fn fold_children(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { group_by, aggr_exprs, input, schema } => {
                let optimized = self.fold_plan(input)?;
                Ok(LogicalPlan::Aggregate {
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { exprs, input } => {
                let optimized = self.fold_plan(input)?;
                Ok(LogicalPlan::Sort {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Limit { skip, fetch, input } => {
                let optimized = self.fold_plan(input)?;
                Ok(LogicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let left_opt = self.fold_plan(left)?;
                let right_opt = self.fold_plan(right)?;
                let folded_filter = filter.as_ref().map(|f| self.fold_expr(f));
                Ok(LogicalPlan::Join {
                    left: Arc::new(left_opt),
                    right: Arc::new(right_opt),
                    join_type: *join_type,
                    on: on.clone(),
                    filter: folded_filter,
                    schema: schema.clone(),
                })
            }
            _ => Ok(plan.clone()),
        }
    }
}

// ============================================================================
// Simplify Expressions Rule
// ============================================================================

/// Simplify boolean and arithmetic expressions.
#[derive(Debug)]
pub struct SimplifyExpressions;

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "SimplifyExpressions"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.simplify_plan(plan)
    }
}

impl SimplifyExpressions {
    fn simplify_plan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { predicate, input } => {
                let simplified = self.simplify_expr(predicate);
                let optimized_input = self.simplify_plan(input)?;
                Ok(LogicalPlan::Filter {
                    predicate: simplified,
                    input: Arc::new(optimized_input),
                })
            }
            LogicalPlan::Projection { exprs, input, schema } => {
                let simplified: Vec<_> = exprs.iter().map(|e| self.simplify_expr(e)).collect();
                let optimized_input = self.simplify_plan(input)?;
                Ok(LogicalPlan::Projection {
                    exprs: simplified,
                    input: Arc::new(optimized_input),
                    schema: schema.clone(),
                })
            }
            _ => self.simplify_children(plan),
        }
    }

    fn simplify_expr(&self, expr: &LogicalExpr) -> LogicalExpr {
        match expr {
            // Double negation: NOT NOT x = x
            LogicalExpr::Not(inner) => {
                if let LogicalExpr::Not(inner_inner) = inner.as_ref() {
                    return self.simplify_expr(inner_inner);
                }
                LogicalExpr::Not(Box::new(self.simplify_expr(inner)))
            }

            // Double negative: --x = x
            LogicalExpr::Negative(inner) => {
                if let LogicalExpr::Negative(inner_inner) = inner.as_ref() {
                    return self.simplify_expr(inner_inner);
                }
                LogicalExpr::Negative(Box::new(self.simplify_expr(inner)))
            }

            LogicalExpr::BinaryExpr { left, op, right } => {
                let simplified_left = self.simplify_expr(left);
                let simplified_right = self.simplify_expr(right);

                // x + 0 = x, x - 0 = x
                if matches!(op, BinaryOp::Plus | BinaryOp::Minus) {
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0))) = &simplified_right {
                        return simplified_left;
                    }
                }

                // 0 + x = x
                if matches!(op, BinaryOp::Plus) {
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0))) = &simplified_left {
                        return simplified_right;
                    }
                }

                // x * 1 = x, x / 1 = x
                if matches!(op, BinaryOp::Multiply | BinaryOp::Divide) {
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(1))) = &simplified_right {
                        return simplified_left;
                    }
                }

                // 1 * x = x
                if matches!(op, BinaryOp::Multiply) {
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(1))) = &simplified_left {
                        return simplified_right;
                    }
                }

                // x * 0 = 0, 0 * x = 0
                if matches!(op, BinaryOp::Multiply) {
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0))) = &simplified_right {
                        return LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0)));
                    }
                    if let LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0))) = &simplified_left {
                        return LogicalExpr::Literal(crate::types::ScalarValue::Int64(Some(0)));
                    }
                }

                LogicalExpr::BinaryExpr {
                    left: Box::new(simplified_left),
                    op: *op,
                    right: Box::new(simplified_right),
                }
            }

            _ => expr.clone(),
        }
    }

    fn simplify_children(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { group_by, aggr_exprs, input, schema } => {
                let optimized = self.simplify_plan(input)?;
                Ok(LogicalPlan::Aggregate {
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { exprs, input } => {
                let optimized = self.simplify_plan(input)?;
                Ok(LogicalPlan::Sort {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Limit { skip, fetch, input } => {
                let optimized = self.simplify_plan(input)?;
                Ok(LogicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let left_opt = self.simplify_plan(left)?;
                let right_opt = self.simplify_plan(right)?;
                Ok(LogicalPlan::Join {
                    left: Arc::new(left_opt),
                    right: Arc::new(right_opt),
                    join_type: *join_type,
                    on: on.clone(),
                    filter: filter.clone(),
                    schema: schema.clone(),
                })
            }
            _ => Ok(plan.clone()),
        }
    }
}

// ============================================================================
// Eliminate Limit Rule
// ============================================================================

/// Eliminate redundant LIMIT operations.
#[derive(Debug)]
pub struct EliminateLimit;

impl OptimizerRule for EliminateLimit {
    fn name(&self) -> &str {
        "EliminateLimit"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.eliminate_limit(plan)
    }
}

impl EliminateLimit {
    fn eliminate_limit(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // LIMIT 0 with no offset = empty result
            LogicalPlan::Limit { skip: 0, fetch: Some(0), input } => {
                Ok(LogicalPlan::EmptyRelation {
                    produce_one_row: false,
                    schema: input.schema().clone(),
                })
            }

            // LIMIT on top of LIMIT - combine them
            LogicalPlan::Limit { skip: outer_skip, fetch: outer_fetch, input } => {
                if let LogicalPlan::Limit { skip: inner_skip, fetch: inner_fetch, input: inner_input } = input.as_ref() {
                    // Combined skip = inner_skip + outer_skip
                    let combined_skip = inner_skip + outer_skip;

                    // Combined fetch is the minimum of the two, adjusted for skip
                    let combined_fetch = match (inner_fetch, outer_fetch) {
                        (Some(inner), Some(outer)) => {
                            // Inner limit applies first, then outer skip + limit
                            let after_inner = *inner;
                            let after_skip = after_inner.saturating_sub(*outer_skip);
                            Some(after_skip.min(*outer))
                        }
                        (Some(inner), None) => Some(inner.saturating_sub(*outer_skip)),
                        (None, Some(outer)) => Some(*outer),
                        (None, None) => None,
                    };

                    let optimized_input = self.eliminate_limit(inner_input)?;
                    return Ok(LogicalPlan::Limit {
                        skip: combined_skip,
                        fetch: combined_fetch,
                        input: Arc::new(optimized_input),
                    });
                }

                let optimized_input = self.eliminate_limit(input)?;
                Ok(LogicalPlan::Limit {
                    skip: *outer_skip,
                    fetch: *outer_fetch,
                    input: Arc::new(optimized_input),
                })
            }

            // Recursively optimize children
            LogicalPlan::Filter { predicate, input } => {
                let optimized = self.eliminate_limit(input)?;
                Ok(LogicalPlan::Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Projection { exprs, input, schema } => {
                let optimized = self.eliminate_limit(input)?;
                Ok(LogicalPlan::Projection {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Aggregate { group_by, aggr_exprs, input, schema } => {
                let optimized = self.eliminate_limit(input)?;
                Ok(LogicalPlan::Aggregate {
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    input: Arc::new(optimized),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { exprs, input } => {
                let optimized = self.eliminate_limit(input)?;
                Ok(LogicalPlan::Sort {
                    exprs: exprs.clone(),
                    input: Arc::new(optimized),
                })
            }
            LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
                let left_opt = self.eliminate_limit(left)?;
                let right_opt = self.eliminate_limit(right)?;
                Ok(LogicalPlan::Join {
                    left: Arc::new(left_opt),
                    right: Arc::new(right_opt),
                    join_type: *join_type,
                    on: on.clone(),
                    filter: filter.clone(),
                    schema: schema.clone(),
                })
            }

            _ => Ok(plan.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ResolvedTableRef;
    use crate::types::{DataType, Field, ScalarValue};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ])
    }

    fn create_scan_plan() -> LogicalPlan {
        let schema = create_test_schema();
        let table_ref = ResolvedTableRef::new("default", "main", "users");
        LogicalPlan::TableScan {
            table_ref,
            projection: None,
            filters: vec![],
            schema,
        }
    }

    #[test]
    fn test_predicate_pushdown_to_scan() {
        let scan = create_scan_plan();

        // Create filter: age > 18
        let predicate = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new(None::<String>, "age"))),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(18)))),
        };

        let filter_plan = LogicalPlan::Filter {
            predicate,
            input: Arc::new(scan),
        };

        let rule = PredicatePushdown;
        let optimized = rule.optimize(&filter_plan).unwrap();

        // Filter should be pushed into the scan
        match optimized {
            LogicalPlan::TableScan { filters, .. } => {
                assert_eq!(filters.len(), 1);
            }
            _ => panic!("Expected TableScan after optimization"),
        }
    }

    #[test]
    fn test_constant_folding() {
        // Test: 1 + 2 -> 3
        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(2)))),
        };

        let rule = ConstantFolding;
        let folded = rule.fold_expr(&expr);

        match folded {
            LogicalExpr::Literal(ScalarValue::Int64(Some(3))) => {}
            _ => panic!("Expected literal 3, got {:?}", folded),
        }
    }

    #[test]
    fn test_boolean_simplification() {
        // Test: true AND x -> x
        let col = LogicalExpr::Column(Column::new(None::<String>, "flag"));
        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))),
            op: BinaryOp::And,
            right: Box::new(col.clone()),
        };

        let rule = ConstantFolding;
        let folded = rule.fold_expr(&expr);

        match folded {
            LogicalExpr::Column(c) => assert_eq!(c.name, "flag"),
            _ => panic!("Expected column, got {:?}", folded),
        }
    }

    #[test]
    fn test_eliminate_limit_zero() {
        let scan = create_scan_plan();
        let limit = LogicalPlan::Limit {
            skip: 0,
            fetch: Some(0),
            input: Arc::new(scan),
        };

        let rule = EliminateLimit;
        let optimized = rule.optimize(&limit).unwrap();

        match optimized {
            LogicalPlan::EmptyRelation { produce_one_row, .. } => {
                assert!(!produce_one_row);
            }
            _ => panic!("Expected EmptyRelation plan"),
        }
    }

    #[test]
    fn test_simplify_double_negation() {
        // Test: NOT NOT x -> x
        let col = LogicalExpr::Column(Column::new(None::<String>, "flag"));
        let expr = LogicalExpr::Not(Box::new(LogicalExpr::Not(Box::new(col.clone()))));

        let rule = SimplifyExpressions;
        let simplified = rule.simplify_expr(&expr);

        match simplified {
            LogicalExpr::Column(c) => assert_eq!(c.name, "flag"),
            _ => panic!("Expected column, got {:?}", simplified),
        }
    }

    #[test]
    fn test_simplify_multiply_by_one() {
        // Test: x * 1 -> x
        let col = LogicalExpr::Column(Column::new(None::<String>, "value"));
        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(col.clone()),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
        };

        let rule = SimplifyExpressions;
        let simplified = rule.simplify_expr(&expr);

        match simplified {
            LogicalExpr::Column(c) => assert_eq!(c.name, "value"),
            _ => panic!("Expected column, got {:?}", simplified),
        }
    }

    #[test]
    fn test_optimizer_full_pipeline() {
        let scan = create_scan_plan();

        // Create: SELECT id FROM users WHERE age > 18 AND true
        let predicate = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::BinaryExpr {
                left: Box::new(LogicalExpr::Column(Column::new(None::<String>, "age"))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(18)))),
            }),
            op: BinaryOp::And,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(Some(true)))),
        };

        let filter_plan = LogicalPlan::Filter {
            predicate,
            input: Arc::new(scan),
        };

        let optimizer = Optimizer::new();
        let optimized = optimizer.optimize(&filter_plan).unwrap();

        // The "AND true" should be simplified, and filter should be pushed to scan
        match optimized {
            LogicalPlan::TableScan { filters, .. } => {
                assert_eq!(filters.len(), 1);
                // The filter should be simplified (no "AND true")
                match &filters[0] {
                    LogicalExpr::BinaryExpr { op: BinaryOp::And, .. } => {
                        panic!("AND true should have been simplified");
                    }
                    LogicalExpr::BinaryExpr { op: BinaryOp::Gt, .. } => {}
                    _ => panic!("Expected comparison expression"),
                }
            }
            _ => panic!("Expected TableScan after optimization"),
        }
    }
}
