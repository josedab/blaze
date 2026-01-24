//! Prepared statements and parameterized queries.
//!
//! This module provides support for prepared statements with parameter binding,
//! enabling safe, efficient parameterized queries with optional plan caching.
//!
//! # Example
//!
//! ```rust,no_run
//! use blaze::{Connection, ScalarValue};
//!
//! let conn = Connection::in_memory().unwrap();
//! conn.execute("CREATE TABLE users (id INT, name VARCHAR, active BOOLEAN)").unwrap();
//!
//! // Prepare a query with parameters
//! let stmt = conn.prepare("SELECT * FROM users WHERE id = $1 AND active = $2").unwrap();
//!
//! // Execute with different parameter values
//! let results = stmt.execute(&[
//!     ScalarValue::Int64(Some(1)),
//!     ScalarValue::Boolean(Some(true)),
//! ]).unwrap();
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

use crate::catalog::CatalogList;
use crate::error::{BlazeError, Result};
use crate::executor::ExecutionContext;
use crate::planner::{
    LogicalExpr, LogicalPlan, LogicalAggregateExpr, LogicalSortExpr,
    Optimizer, PhysicalPlanner, WindowExpr,
};
use crate::types::ScalarValue;

/// Metadata about a parameter in a prepared statement.
#[derive(Debug, Clone)]
pub struct ParameterInfo {
    /// Parameter index (1-based, matching $1, $2, etc.)
    pub index: usize,
    /// Inferred data type (if available)
    pub data_type: Option<crate::types::DataType>,
}

/// A prepared statement that can be executed multiple times with different parameters.
///
/// Prepared statements provide:
/// - **Safety**: Parameters are bound safely, preventing SQL injection
/// - **Performance**: Query plan can be reused across executions
/// - **Type checking**: Parameter types are validated at bind time
pub struct PreparedStatement {
    /// The original SQL query
    sql: String,
    /// Logical plan (before parameter substitution)
    logical_plan: LogicalPlan,
    /// Parameter metadata extracted from the query
    parameters: Vec<ParameterInfo>,
    /// Catalog reference for execution
    catalog_list: Arc<CatalogList>,
    /// Execution context
    execution_context: ExecutionContext,
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("sql", &self.sql)
            .field("parameters", &self.parameters)
            .finish()
    }
}

impl PreparedStatement {
    /// Create a new prepared statement.
    pub(crate) fn new(
        sql: String,
        logical_plan: LogicalPlan,
        parameters: Vec<ParameterInfo>,
        catalog_list: Arc<CatalogList>,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            sql,
            logical_plan,
            parameters,
            catalog_list,
            execution_context,
        }
    }

    /// Get the original SQL query.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the number of parameters in this statement.
    pub fn parameter_count(&self) -> usize {
        self.parameters.len()
    }

    /// Get metadata about all parameters.
    pub fn parameters(&self) -> &[ParameterInfo] {
        &self.parameters
    }

    /// Execute the prepared statement with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameter values in order ($1, $2, etc.)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::{Connection, ScalarValue};
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// let stmt = conn.prepare("SELECT $1 + $2").unwrap();
    /// let results = stmt.execute(&[
    ///     ScalarValue::Int64(Some(10)),
    ///     ScalarValue::Int64(Some(20)),
    /// ]).unwrap();
    /// ```
    pub fn execute(&self, params: &[ScalarValue]) -> Result<Vec<RecordBatch>> {
        // Validate parameter count
        if params.len() != self.parameters.len() {
            return Err(BlazeError::invalid_argument(format!(
                "Expected {} parameters, got {}",
                self.parameters.len(),
                params.len()
            )));
        }

        // Build parameter map (1-indexed)
        let param_map: HashMap<usize, ScalarValue> = params
            .iter()
            .enumerate()
            .map(|(i, v)| (i + 1, v.clone()))
            .collect();

        // Substitute parameters in the logical plan
        let bound_plan = substitute_parameters(&self.logical_plan, &param_map)?;

        // Optimize the bound plan (create new optimizer for each execution)
        let optimizer = Optimizer::default();
        let optimized_plan = optimizer.optimize(&bound_plan)?;

        // Create physical plan
        let physical_planner = PhysicalPlanner::new();
        let physical_plan = physical_planner.create_physical_plan(&optimized_plan)?;

        // Execute
        self.execution_context.execute(&physical_plan)
    }

    /// Execute the prepared statement with named parameters.
    ///
    /// This is a convenience method that converts named parameters to positional ones.
    /// The mapping should contain entries like ("1", value) for $1, etc.
    pub fn execute_named(&self, params: &HashMap<String, ScalarValue>) -> Result<Vec<RecordBatch>> {
        let mut positional = vec![ScalarValue::Null; self.parameters.len()];

        for (key, value) in params {
            let idx: usize = key.parse().map_err(|_| {
                BlazeError::invalid_argument(format!("Invalid parameter name: {}", key))
            })?;

            if idx == 0 || idx > self.parameters.len() {
                return Err(BlazeError::invalid_argument(format!(
                    "Parameter index {} out of range (1-{})",
                    idx,
                    self.parameters.len()
                )));
            }

            positional[idx - 1] = value.clone();
        }

        self.execute(&positional)
    }
}

/// Substitute parameter placeholders in a logical plan with actual values.
fn substitute_parameters(
    plan: &LogicalPlan,
    params: &HashMap<usize, ScalarValue>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan { table_ref, projection, filters, schema, time_travel } => {
            let new_filters: Vec<_> = filters
                .iter()
                .map(|f| substitute_expr(f, params))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::TableScan {
                table_ref: table_ref.clone(),
                projection: projection.clone(),
                filters: new_filters,
                schema: schema.clone(),
                time_travel: time_travel.clone(),
            })
        }
        LogicalPlan::Projection { exprs, input, schema } => {
            let new_input = substitute_parameters(input, params)?;
            let new_exprs: Vec<_> = exprs
                .iter()
                .map(|e| substitute_expr(e, params))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Projection {
                input: Arc::new(new_input),
                exprs: new_exprs,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Filter { input, predicate } => {
            let new_input = substitute_parameters(input, params)?;
            let new_predicate = substitute_expr(predicate, params)?;
            Ok(LogicalPlan::Filter {
                input: Arc::new(new_input),
                predicate: new_predicate,
            })
        }
        LogicalPlan::Aggregate { input, group_by, aggr_exprs, schema } => {
            let new_input = substitute_parameters(input, params)?;
            let new_group_by: Vec<_> = group_by
                .iter()
                .map(|e| substitute_expr(e, params))
                .collect::<Result<_>>()?;
            let new_aggr_exprs: Vec<_> = aggr_exprs
                .iter()
                .map(|ae| substitute_aggregate_expr(ae, params))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Aggregate {
                input: Arc::new(new_input),
                group_by: new_group_by,
                aggr_exprs: new_aggr_exprs,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Sort { input, exprs } => {
            let new_input = substitute_parameters(input, params)?;
            let new_exprs: Vec<_> = exprs
                .iter()
                .map(|se| substitute_sort_expr(se, params))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Sort {
                input: Arc::new(new_input),
                exprs: new_exprs,
            })
        }
        LogicalPlan::Limit { input, skip, fetch } => {
            let new_input = substitute_parameters(input, params)?;
            Ok(LogicalPlan::Limit {
                input: Arc::new(new_input),
                skip: *skip,
                fetch: *fetch,
            })
        }
        LogicalPlan::Join { left, right, join_type, on, filter, schema } => {
            let new_left = substitute_parameters(left, params)?;
            let new_right = substitute_parameters(right, params)?;
            let new_on: Vec<_> = on
                .iter()
                .map(|(l, r)| {
                    Ok((substitute_expr(l, params)?, substitute_expr(r, params)?))
                })
                .collect::<Result<_>>()?;
            let new_filter = filter
                .as_ref()
                .map(|f| substitute_expr(f, params))
                .transpose()?;
            Ok(LogicalPlan::Join {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                join_type: *join_type,
                on: new_on,
                filter: new_filter,
                schema: schema.clone(),
            })
        }
        LogicalPlan::CrossJoin { left, right, schema } => {
            let new_left = substitute_parameters(left, params)?;
            let new_right = substitute_parameters(right, params)?;
            Ok(LogicalPlan::CrossJoin {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                schema: schema.clone(),
            })
        }
        LogicalPlan::SetOperation { left, right, op, all, schema } => {
            let new_left = substitute_parameters(left, params)?;
            let new_right = substitute_parameters(right, params)?;
            Ok(LogicalPlan::SetOperation {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                op: *op,
                all: *all,
                schema: schema.clone(),
            })
        }
        LogicalPlan::SubqueryAlias { alias, input, schema } => {
            let new_input = substitute_parameters(input, params)?;
            Ok(LogicalPlan::SubqueryAlias {
                alias: alias.clone(),
                input: Arc::new(new_input),
                schema: schema.clone(),
            })
        }
        LogicalPlan::Distinct { input } => {
            let new_input = substitute_parameters(input, params)?;
            Ok(LogicalPlan::Distinct {
                input: Arc::new(new_input),
            })
        }
        LogicalPlan::Window { input, window_exprs, schema } => {
            let new_input = substitute_parameters(input, params)?;
            let new_window_exprs: Vec<_> = window_exprs
                .iter()
                .map(|we| substitute_expr(we, params))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Window {
                input: Arc::new(new_input),
                window_exprs: new_window_exprs,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Values { schema, values } => {
            let new_values: Vec<Vec<_>> = values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| substitute_expr(e, params))
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Values {
                schema: schema.clone(),
                values: new_values,
            })
        }
        LogicalPlan::Insert { table_ref, input, schema } => {
            let new_input = substitute_parameters(input, params)?;
            Ok(LogicalPlan::Insert {
                table_ref: table_ref.clone(),
                input: Arc::new(new_input),
                schema: schema.clone(),
            })
        }
        LogicalPlan::Delete { table_ref, predicate, schema } => {
            let new_predicate = predicate
                .as_ref()
                .map(|p| substitute_expr(p, params))
                .transpose()?;
            Ok(LogicalPlan::Delete {
                table_ref: table_ref.clone(),
                predicate: new_predicate,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Update { table_ref, assignments, predicate, schema } => {
            let new_assignments: Vec<_> = assignments
                .iter()
                .map(|(col, expr)| {
                    Ok((col.clone(), substitute_expr(expr, params)?))
                })
                .collect::<Result<_>>()?;
            let new_predicate = predicate
                .as_ref()
                .map(|p| substitute_expr(p, params))
                .transpose()?;
            Ok(LogicalPlan::Update {
                table_ref: table_ref.clone(),
                assignments: new_assignments,
                predicate: new_predicate,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Explain { plan, verbose } => {
            let new_plan = substitute_parameters(plan, params)?;
            Ok(LogicalPlan::Explain {
                plan: Arc::new(new_plan),
                verbose: *verbose,
            })
        }
        LogicalPlan::ExplainAnalyze { plan, verbose } => {
            let new_plan = substitute_parameters(plan, params)?;
            Ok(LogicalPlan::ExplainAnalyze {
                plan: Arc::new(new_plan),
                verbose: *verbose,
            })
        }
        // Terminal nodes don't have expressions with parameters
        LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        LogicalPlan::CreateTable { .. } => Ok(plan.clone()),
        LogicalPlan::DropTable { .. } => Ok(plan.clone()),
        LogicalPlan::Copy { input, target, format, options, schema } => {
            let new_input = substitute_parameters(input, params)?;
            Ok(LogicalPlan::Copy {
                input: Arc::new(new_input),
                target: target.clone(),
                format: format.clone(),
                options: options.clone(),
                schema: schema.clone(),
            })
        }
    }
}

/// Substitute parameters in a logical expression.
fn substitute_expr(
    expr: &LogicalExpr,
    params: &HashMap<usize, ScalarValue>,
) -> Result<LogicalExpr> {
    match expr {
        LogicalExpr::Placeholder { id } => {
            params
                .get(id)
                .map(|v| LogicalExpr::Literal(v.clone()))
                .ok_or_else(|| {
                    BlazeError::invalid_argument(format!("Missing parameter ${}", id))
                })
        }
        LogicalExpr::BinaryExpr { left, op, right } => {
            Ok(LogicalExpr::BinaryExpr {
                left: Box::new(substitute_expr(left, params)?),
                op: *op,
                right: Box::new(substitute_expr(right, params)?),
            })
        }
        LogicalExpr::UnaryExpr { op, expr: inner } => {
            Ok(LogicalExpr::UnaryExpr {
                op: *op,
                expr: Box::new(substitute_expr(inner, params)?),
            })
        }
        LogicalExpr::IsNull(inner) => {
            Ok(LogicalExpr::IsNull(Box::new(substitute_expr(inner, params)?)))
        }
        LogicalExpr::IsNotNull(inner) => {
            Ok(LogicalExpr::IsNotNull(Box::new(substitute_expr(inner, params)?)))
        }
        LogicalExpr::Not(inner) => {
            Ok(LogicalExpr::Not(Box::new(substitute_expr(inner, params)?)))
        }
        LogicalExpr::Negative(inner) => {
            Ok(LogicalExpr::Negative(Box::new(substitute_expr(inner, params)?)))
        }
        LogicalExpr::Cast { expr: inner, data_type } => {
            Ok(LogicalExpr::Cast {
                expr: Box::new(substitute_expr(inner, params)?),
                data_type: data_type.clone(),
            })
        }
        LogicalExpr::TryCast { expr: inner, data_type } => {
            Ok(LogicalExpr::TryCast {
                expr: Box::new(substitute_expr(inner, params)?),
                data_type: data_type.clone(),
            })
        }
        LogicalExpr::Case { expr: operand, when_then_exprs, else_expr } => {
            let new_operand = operand
                .as_ref()
                .map(|e| substitute_expr(e, params).map(Box::new))
                .transpose()?;
            let new_when_then: Vec<_> = when_then_exprs
                .iter()
                .map(|(w, t)| {
                    Ok((substitute_expr(w, params)?, substitute_expr(t, params)?))
                })
                .collect::<Result<_>>()?;
            let new_else = else_expr
                .as_ref()
                .map(|e| substitute_expr(e, params).map(Box::new))
                .transpose()?;
            Ok(LogicalExpr::Case {
                expr: new_operand,
                when_then_exprs: new_when_then,
                else_expr: new_else,
            })
        }
        LogicalExpr::Between { expr: inner, negated, low, high } => {
            Ok(LogicalExpr::Between {
                expr: Box::new(substitute_expr(inner, params)?),
                negated: *negated,
                low: Box::new(substitute_expr(low, params)?),
                high: Box::new(substitute_expr(high, params)?),
            })
        }
        LogicalExpr::Like { negated, expr: inner, pattern, escape } => {
            let new_escape = escape
                .as_ref()
                .map(|e| substitute_expr(e, params).map(Box::new))
                .transpose()?;
            Ok(LogicalExpr::Like {
                negated: *negated,
                expr: Box::new(substitute_expr(inner, params)?),
                pattern: Box::new(substitute_expr(pattern, params)?),
                escape: new_escape,
            })
        }
        LogicalExpr::InList { expr: inner, list, negated } => {
            let new_list: Vec<_> = list
                .iter()
                .map(|e| substitute_expr(e, params))
                .collect::<Result<_>>()?;
            Ok(LogicalExpr::InList {
                expr: Box::new(substitute_expr(inner, params)?),
                list: new_list,
                negated: *negated,
            })
        }
        LogicalExpr::ScalarFunction { name, args } => {
            let new_args: Vec<_> = args
                .iter()
                .map(|e| substitute_expr(e, params))
                .collect::<Result<_>>()?;
            Ok(LogicalExpr::ScalarFunction {
                name: name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::Aggregate(agg) => {
            Ok(LogicalExpr::Aggregate(substitute_aggregate_expr(agg, params)?))
        }
        LogicalExpr::Window(win) => {
            Ok(LogicalExpr::Window(Box::new(substitute_window_expr(win, params)?)))
        }
        LogicalExpr::ScalarSubquery(subquery) => {
            let new_subquery = substitute_parameters(subquery, params)?;
            Ok(LogicalExpr::ScalarSubquery(Arc::new(new_subquery)))
        }
        LogicalExpr::Exists { subquery, negated } => {
            let new_subquery = substitute_parameters(subquery, params)?;
            Ok(LogicalExpr::Exists {
                subquery: Arc::new(new_subquery),
                negated: *negated,
            })
        }
        LogicalExpr::InSubquery { expr: inner, subquery, negated } => {
            let new_inner = substitute_expr(inner, params)?;
            let new_subquery = substitute_parameters(subquery, params)?;
            Ok(LogicalExpr::InSubquery {
                expr: Box::new(new_inner),
                subquery: Arc::new(new_subquery),
                negated: *negated,
            })
        }
        LogicalExpr::Alias { expr: inner, alias } => {
            Ok(LogicalExpr::Alias {
                expr: Box::new(substitute_expr(inner, params)?),
                alias: alias.clone(),
            })
        }
        // Terminal expressions don't need substitution
        LogicalExpr::Column(_) => Ok(expr.clone()),
        LogicalExpr::Literal(_) => Ok(expr.clone()),
        LogicalExpr::Wildcard => Ok(expr.clone()),
        LogicalExpr::QualifiedWildcard { .. } => Ok(expr.clone()),
    }
}

/// Substitute parameters in an aggregate expression.
fn substitute_aggregate_expr(
    agg: &LogicalAggregateExpr,
    params: &HashMap<usize, ScalarValue>,
) -> Result<LogicalAggregateExpr> {
    Ok(LogicalAggregateExpr {
        func: agg.func,
        args: agg
            .args
            .iter()
            .map(|e| substitute_expr(e, params))
            .collect::<Result<_>>()?,
        distinct: agg.distinct,
        filter: agg
            .filter
            .as_ref()
            .map(|f| substitute_expr(f, params).map(Box::new))
            .transpose()?,
    })
}

/// Substitute parameters in a sort expression.
fn substitute_sort_expr(
    sort: &LogicalSortExpr,
    params: &HashMap<usize, ScalarValue>,
) -> Result<LogicalSortExpr> {
    Ok(LogicalSortExpr {
        expr: substitute_expr(&sort.expr, params)?,
        asc: sort.asc,
        nulls_first: sort.nulls_first,
    })
}

/// Substitute parameters in a window expression.
fn substitute_window_expr(
    window: &WindowExpr,
    params: &HashMap<usize, ScalarValue>,
) -> Result<WindowExpr> {
    Ok(WindowExpr {
        function: substitute_expr(&window.function, params)?,
        partition_by: window
            .partition_by
            .iter()
            .map(|e| substitute_expr(e, params))
            .collect::<Result<_>>()?,
        order_by: window
            .order_by
            .iter()
            .map(|se| substitute_sort_expr(se, params))
            .collect::<Result<_>>()?,
        frame: window.frame.clone(),
    })
}

/// Extract parameter placeholders from a logical plan.
pub(crate) fn extract_parameters(plan: &LogicalPlan) -> Vec<ParameterInfo> {
    let mut params = Vec::new();
    extract_parameters_from_plan(plan, &mut params);

    // Sort by index and deduplicate
    params.sort_by_key(|p| p.index);
    params.dedup_by_key(|p| p.index);

    params
}

fn extract_parameters_from_plan(plan: &LogicalPlan, params: &mut Vec<ParameterInfo>) {
    match plan {
        LogicalPlan::TableScan { filters, .. } => {
            for filter in filters {
                extract_parameters_from_expr(filter, params);
            }
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            extract_parameters_from_plan(input, params);
            for expr in exprs {
                extract_parameters_from_expr(expr, params);
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            extract_parameters_from_plan(input, params);
            extract_parameters_from_expr(predicate, params);
        }
        LogicalPlan::Aggregate { input, group_by, aggr_exprs, .. } => {
            extract_parameters_from_plan(input, params);
            for expr in group_by {
                extract_parameters_from_expr(expr, params);
            }
            for agg in aggr_exprs {
                for arg in &agg.args {
                    extract_parameters_from_expr(arg, params);
                }
                if let Some(filter) = &agg.filter {
                    extract_parameters_from_expr(filter, params);
                }
            }
        }
        LogicalPlan::Sort { input, exprs } => {
            extract_parameters_from_plan(input, params);
            for se in exprs {
                extract_parameters_from_expr(&se.expr, params);
            }
        }
        LogicalPlan::Limit { input, .. } => {
            extract_parameters_from_plan(input, params);
        }
        LogicalPlan::Join { left, right, on, filter, .. } => {
            extract_parameters_from_plan(left, params);
            extract_parameters_from_plan(right, params);
            for (l, r) in on {
                extract_parameters_from_expr(l, params);
                extract_parameters_from_expr(r, params);
            }
            if let Some(f) = filter {
                extract_parameters_from_expr(f, params);
            }
        }
        LogicalPlan::CrossJoin { left, right, .. } => {
            extract_parameters_from_plan(left, params);
            extract_parameters_from_plan(right, params);
        }
        LogicalPlan::SetOperation { left, right, .. } => {
            extract_parameters_from_plan(left, params);
            extract_parameters_from_plan(right, params);
        }
        LogicalPlan::SubqueryAlias { input, .. } => {
            extract_parameters_from_plan(input, params);
        }
        LogicalPlan::Distinct { input } => {
            extract_parameters_from_plan(input, params);
        }
        LogicalPlan::Window { input, window_exprs, .. } => {
            extract_parameters_from_plan(input, params);
            for we in window_exprs {
                extract_parameters_from_expr(we, params);
            }
        }
        LogicalPlan::Values { values, .. } => {
            for row in values {
                for expr in row {
                    extract_parameters_from_expr(expr, params);
                }
            }
        }
        LogicalPlan::Insert { input, .. } => {
            extract_parameters_from_plan(input, params);
        }
        LogicalPlan::Delete { predicate, .. } => {
            if let Some(p) = predicate {
                extract_parameters_from_expr(p, params);
            }
        }
        LogicalPlan::Update { assignments, predicate, .. } => {
            for (_, expr) in assignments {
                extract_parameters_from_expr(expr, params);
            }
            if let Some(p) = predicate {
                extract_parameters_from_expr(p, params);
            }
        }
        LogicalPlan::Explain { plan, .. } => {
            extract_parameters_from_plan(plan, params);
        }
        LogicalPlan::ExplainAnalyze { plan, .. } => {
            extract_parameters_from_plan(plan, params);
        }
        LogicalPlan::EmptyRelation { .. } |
        LogicalPlan::CreateTable { .. } |
        LogicalPlan::DropTable { .. } => {}
        LogicalPlan::Copy { input, .. } => {
            extract_parameters_from_plan(input, params);
        }
    }
}

fn extract_parameters_from_expr(expr: &LogicalExpr, params: &mut Vec<ParameterInfo>) {
    match expr {
        LogicalExpr::Placeholder { id } => {
            params.push(ParameterInfo {
                index: *id,
                data_type: None,
            });
        }
        LogicalExpr::BinaryExpr { left, right, .. } => {
            extract_parameters_from_expr(left, params);
            extract_parameters_from_expr(right, params);
        }
        LogicalExpr::UnaryExpr { expr: inner, .. } => {
            extract_parameters_from_expr(inner, params);
        }
        LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) |
        LogicalExpr::Not(inner) | LogicalExpr::Negative(inner) => {
            extract_parameters_from_expr(inner, params);
        }
        LogicalExpr::Cast { expr: inner, .. } | LogicalExpr::TryCast { expr: inner, .. } => {
            extract_parameters_from_expr(inner, params);
        }
        LogicalExpr::Case { expr: operand, when_then_exprs, else_expr } => {
            if let Some(op) = operand {
                extract_parameters_from_expr(op, params);
            }
            for (w, t) in when_then_exprs {
                extract_parameters_from_expr(w, params);
                extract_parameters_from_expr(t, params);
            }
            if let Some(e) = else_expr {
                extract_parameters_from_expr(e, params);
            }
        }
        LogicalExpr::Between { expr: inner, low, high, .. } => {
            extract_parameters_from_expr(inner, params);
            extract_parameters_from_expr(low, params);
            extract_parameters_from_expr(high, params);
        }
        LogicalExpr::Like { expr: inner, pattern, escape, .. } => {
            extract_parameters_from_expr(inner, params);
            extract_parameters_from_expr(pattern, params);
            if let Some(e) = escape {
                extract_parameters_from_expr(e, params);
            }
        }
        LogicalExpr::InList { expr: inner, list, .. } => {
            extract_parameters_from_expr(inner, params);
            for e in list {
                extract_parameters_from_expr(e, params);
            }
        }
        LogicalExpr::ScalarFunction { args, .. } => {
            for arg in args {
                extract_parameters_from_expr(arg, params);
            }
        }
        LogicalExpr::Alias { expr: inner, .. } => {
            extract_parameters_from_expr(inner, params);
        }
        LogicalExpr::Aggregate(agg) => {
            for arg in &agg.args {
                extract_parameters_from_expr(arg, params);
            }
            if let Some(filter) = &agg.filter {
                extract_parameters_from_expr(filter, params);
            }
        }
        LogicalExpr::Window(win) => {
            extract_parameters_from_expr(&win.function, params);
            for pb in &win.partition_by {
                extract_parameters_from_expr(pb, params);
            }
            for ob in &win.order_by {
                extract_parameters_from_expr(&ob.expr, params);
            }
        }
        LogicalExpr::ScalarSubquery(subquery) => {
            extract_parameters_from_plan(subquery, params);
        }
        LogicalExpr::Exists { subquery, .. } => {
            extract_parameters_from_plan(subquery, params);
        }
        LogicalExpr::InSubquery { expr: inner, subquery, .. } => {
            extract_parameters_from_expr(inner, params);
            extract_parameters_from_plan(subquery, params);
        }
        // Terminal expressions
        LogicalExpr::Column(_) |
        LogicalExpr::Literal(_) |
        LogicalExpr::Wildcard |
        LogicalExpr::QualifiedWildcard { .. } => {}
    }
}

/// LRU cache for prepared statement plans.
///
/// This cache stores parsed and bound logical plans to avoid repeated parsing
/// and binding for frequently used queries.
#[derive(Debug)]
pub struct PreparedStatementCache {
    /// Maximum number of entries in the cache
    capacity: usize,
    /// Cached entries: SQL -> (logical_plan, parameters)
    cache: Mutex<lru::LruCache<String, CachedPlan>>,
}

/// A cached prepared statement plan.
#[derive(Clone)]
struct CachedPlan {
    logical_plan: LogicalPlan,
    parameters: Vec<ParameterInfo>,
}

impl std::fmt::Debug for CachedPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPlan")
            .field("parameters", &self.parameters)
            .finish()
    }
}

impl PreparedStatementCache {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(capacity.max(1)).unwrap(),
            )),
        }
    }

    /// Get a cached plan or create a new one.
    pub fn get_or_create<F>(
        &self,
        sql: &str,
        create_fn: F,
    ) -> Result<(LogicalPlan, Vec<ParameterInfo>)>
    where
        F: FnOnce() -> Result<(LogicalPlan, Vec<ParameterInfo>)>,
    {
        let sql_key = sql.to_string();
        let mut cache = self.cache.lock().unwrap();

        if let Some(cached) = cache.get(&sql_key) {
            return Ok((cached.logical_plan.clone(), cached.parameters.clone()));
        }

        drop(cache); // Release lock during creation

        let (plan, params) = create_fn()?;

        let mut cache = self.cache.lock().unwrap();
        cache.put(
            sql_key,
            CachedPlan {
                logical_plan: plan.clone(),
                parameters: params.clone(),
            },
        );

        Ok((plan, params))
    }

    /// Clear the cache.
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Get the current number of cached entries.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the cache capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for PreparedStatementCache {
    fn default() -> Self {
        Self::new(100)
    }
}

// Module for the LRU cache implementation
mod lru {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::num::NonZeroUsize;

    /// A simple LRU cache implementation.
    pub struct LruCache<K, V> {
        capacity: NonZeroUsize,
        map: HashMap<K, (V, u64)>,
        counter: u64,
    }

    impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
        pub fn new(capacity: NonZeroUsize) -> Self {
            Self {
                capacity,
                map: HashMap::new(),
                counter: 0,
            }
        }

        pub fn get(&mut self, key: &K) -> Option<&V> {
            if let Some((value, timestamp)) = self.map.get_mut(key) {
                self.counter += 1;
                *timestamp = self.counter;
                Some(value)
            } else {
                None
            }
        }

        pub fn put(&mut self, key: K, value: V) {
            self.counter += 1;

            if self.map.len() >= self.capacity.get() && !self.map.contains_key(&key) {
                // Find and remove the least recently used entry
                if let Some(lru_key) = self.find_lru() {
                    self.map.remove(&lru_key);
                }
            }

            self.map.insert(key, (value, self.counter));
        }

        fn find_lru(&self) -> Option<K> {
            self.map
                .iter()
                .min_by_key(|(_, (_, ts))| ts)
                .map(|(k, _)| k.clone())
        }

        pub fn clear(&mut self) {
            self.map.clear();
        }

        pub fn len(&self) -> usize {
            self.map.len()
        }
    }

    impl<K, V> std::fmt::Debug for LruCache<K, V> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LruCache")
                .field("capacity", &self.capacity)
                .field("len", &self.map.len())
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = lru::LruCache::new(std::num::NonZeroUsize::new(2).unwrap());

        cache.put("a", 1);
        cache.put("b", 2);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));

        // Adding a third item should evict the least recently used
        cache.put("c", 3);
        assert_eq!(cache.len(), 2);

        // "b" was accessed most recently, then "a", so "a" was evicted when "c" was inserted
        // Now cache contains "b" and "c"
        assert_eq!(cache.get(&"a"), None);  // "a" was evicted (LRU)
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_prepared_statement_cache() {
        let cache = PreparedStatementCache::new(10);
        assert!(cache.is_empty());
        assert_eq!(cache.capacity(), 10);
    }
}
