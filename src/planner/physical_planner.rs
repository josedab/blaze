//! Physical query planner.
//!
//! This module converts logical plans into physical plans that can be executed.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;

use crate::error::{BlazeError, Result};
use crate::planner::logical_expr::{BinaryOp, LogicalExpr, UnaryOp};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_expr::{
    BetweenExpr, BinaryExpr, BitwiseNotExpr, CaseExpr, CastExpr, ColumnExpr, ExistsExpr,
    InListExpr, InSubqueryExpr, IsNotNullExpr, IsNullExpr, LikeExpr, LiteralExpr, NotExpr,
    PhysicalExpr, ScalarFunctionExpr, ScalarSubqueryExpr,
};
use crate::planner::physical_plan::{
    AggregateExpr, CopyFormat as PhysicalCopyFormat, PhysicalPlan, SortExpr,
    WindowExpr as PhysicalWindowExpr, WindowFunction,
};
use crate::types::ScalarValue;

use arrow::record_batch::RecordBatch;

/// Type alias for a function that executes a physical plan and returns results.
pub type SubqueryExecutor = Box<dyn Fn(&PhysicalPlan) -> Result<Vec<RecordBatch>> + Send + Sync>;

/// Physical query planner.
///
/// Converts logical plans into physical plans that can be executed by the
/// query execution engine.
pub struct PhysicalPlanner {
    /// Optional executor for evaluating subqueries during planning.
    subquery_executor: Option<SubqueryExecutor>,
}

impl PhysicalPlanner {
    /// Create a new physical planner.
    pub fn new() -> Self {
        Self {
            subquery_executor: None,
        }
    }

    /// Create a new physical planner with a subquery executor.
    /// The executor is used to evaluate uncorrelated subqueries during planning.
    pub fn with_subquery_executor(executor: SubqueryExecutor) -> Self {
        Self {
            subquery_executor: Some(executor),
        }
    }

    /// Create a physical plan from a logical plan.
    pub fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<PhysicalPlan> {
        match logical_plan {
            LogicalPlan::TableScan {
                table_ref,
                schema,
                projection,
                filters,
                time_travel,
            } => {
                // The schema in LogicalPlan::TableScan is already the output schema
                // (possibly projected by the optimizer). The projection field contains
                // indices into the ORIGINAL table schema for the storage layer to use.
                let output_schema = Arc::new(schema.to_arrow());

                // Convert filter expressions
                // IMPORTANT: Filters are evaluated against the ORIGINAL batch (before projection),
                // so we need to map column names to their original indices when there's a projection.
                let physical_filters: Vec<Arc<dyn PhysicalExpr>> = filters
                    .iter()
                    .map(|f| {
                        if let Some(proj) = projection {
                            // Create a mapping from column name to original index
                            self.create_physical_expr_with_projection_mapping(
                                f,
                                &output_schema,
                                proj,
                            )
                        } else {
                            self.create_physical_expr(f, &output_schema)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PhysicalPlan::Scan {
                    table_name: table_ref.table.clone(),
                    projection: projection.clone(),
                    schema: output_schema,
                    filters: physical_filters,
                    time_travel: time_travel.clone(),
                })
            }

            LogicalPlan::Projection {
                exprs,
                schema,
                input,
            } => {
                let input_plan = self.create_physical_plan(input)?;
                let input_schema = self.get_plan_schema(&input_plan);

                let physical_exprs: Vec<Arc<dyn PhysicalExpr>> = exprs
                    .iter()
                    .map(|e| self.create_physical_expr(e, &input_schema))
                    .collect::<Result<Vec<_>>>()?;

                let output_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::Projection {
                    exprs: physical_exprs,
                    schema: output_schema,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Filter { predicate, input } => {
                let input_plan = self.create_physical_plan(input)?;
                let input_schema = self.get_plan_schema(&input_plan);

                let physical_predicate = self.create_physical_expr(predicate, &input_schema)?;

                Ok(PhysicalPlan::Filter {
                    predicate: physical_predicate,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                schema,
                input,
            } => {
                let input_plan = self.create_physical_plan(input)?;
                let input_schema = self.get_plan_schema(&input_plan);

                let physical_group_by: Vec<Arc<dyn PhysicalExpr>> = group_by
                    .iter()
                    .map(|e| self.create_physical_expr(e, &input_schema))
                    .collect::<Result<Vec<_>>>()?;

                let physical_aggrs: Vec<AggregateExpr> = aggr_exprs
                    .iter()
                    .map(|a| {
                        let args: Vec<Arc<dyn PhysicalExpr>> = a
                            .args
                            .iter()
                            .map(|arg| self.create_physical_expr(arg, &input_schema))
                            .collect::<Result<Vec<_>>>()?;

                        Ok(AggregateExpr {
                            func: a.func,
                            args,
                            distinct: a.distinct,
                            alias: None,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let output_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::HashAggregate {
                    group_by: physical_group_by,
                    aggr_exprs: physical_aggrs,
                    schema: output_schema,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Sort { exprs, input } => {
                let input_plan = self.create_physical_plan(input)?;
                let input_schema = self.get_plan_schema(&input_plan);

                let physical_sort_exprs: Vec<SortExpr> = exprs
                    .iter()
                    .map(|s| {
                        Ok(SortExpr {
                            expr: self.create_physical_expr(&s.expr, &input_schema)?,
                            ascending: s.asc,
                            nulls_first: s.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PhysicalPlan::Sort {
                    exprs: physical_sort_exprs,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Limit { skip, fetch, input } => {
                let input_plan = self.create_physical_plan(input)?;

                Ok(PhysicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
                schema,
                ..
            } => {
                let left_plan = self.create_physical_plan(left)?;
                let right_plan = self.create_physical_plan(right)?;
                let left_schema = self.get_plan_schema(&left_plan);
                let right_schema = self.get_plan_schema(&right_plan);

                let left_keys: Vec<Arc<dyn PhysicalExpr>> = on
                    .iter()
                    .map(|(l, _)| self.create_physical_expr(l, &left_schema))
                    .collect::<Result<Vec<_>>>()?;

                let right_keys: Vec<Arc<dyn PhysicalExpr>> = on
                    .iter()
                    .map(|(_, r)| self.create_physical_expr(r, &right_schema))
                    .collect::<Result<Vec<_>>>()?;

                let output_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::HashJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type: *join_type,
                    left_keys,
                    right_keys,
                    schema: output_schema,
                })
            }

            LogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let left_plan = self.create_physical_plan(left)?;
                let right_plan = self.create_physical_plan(right)?;
                let output_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::CrossJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    schema: output_schema,
                })
            }

            LogicalPlan::SetOperation {
                op: _,
                left,
                right,
                schema,
                ..
            } => {
                let left_plan = self.create_physical_plan(left)?;
                let right_plan = self.create_physical_plan(right)?;
                let output_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::Union {
                    inputs: vec![left_plan, right_plan],
                    schema: output_schema,
                })
            }

            LogicalPlan::SubqueryAlias { input, .. } => {
                // Just pass through to the inner plan
                self.create_physical_plan(input)
            }

            LogicalPlan::Distinct { input } => {
                // Distinct is implemented as a group by all columns
                let input_plan = self.create_physical_plan(input)?;
                let schema = self.get_plan_schema(&input_plan);

                let group_by: Vec<Arc<dyn PhysicalExpr>> = (0..schema.fields().len())
                    .map(|i| {
                        Arc::new(ColumnExpr::new(schema.field(i).name().clone(), i))
                            as Arc<dyn PhysicalExpr>
                    })
                    .collect();

                Ok(PhysicalPlan::HashAggregate {
                    group_by,
                    aggr_exprs: vec![],
                    schema,
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Window {
                window_exprs,
                input,
                schema,
            } => {
                let input_plan = self.create_physical_plan(input)?;
                let input_schema = input_plan.schema();

                // Convert logical window expressions to physical
                let physical_window_exprs: Vec<PhysicalWindowExpr> = window_exprs
                    .iter()
                    .map(|expr| self.create_window_expr(expr, &input_schema))
                    .collect::<Result<Vec<_>>>()?;

                Ok(PhysicalPlan::Window {
                    window_exprs: physical_window_exprs,
                    schema: Arc::new(schema.to_arrow()),
                    input: Box::new(input_plan),
                })
            }

            LogicalPlan::Values { values, schema } => {
                // Convert values to record batches
                let arrow_schema = Arc::new(schema.to_arrow());

                if values.is_empty() {
                    return Ok(PhysicalPlan::Empty {
                        produce_one_row: false,
                        schema: arrow_schema,
                    });
                }

                // For now, create empty batches
                // Full implementation would materialize values
                Ok(PhysicalPlan::Values {
                    data: vec![],
                    schema: arrow_schema,
                })
            }

            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => {
                let arrow_schema = Arc::new(schema.to_arrow());

                Ok(PhysicalPlan::Empty {
                    produce_one_row: *produce_one_row,
                    schema: arrow_schema,
                })
            }

            LogicalPlan::Explain { plan, verbose } => {
                let input_plan = self.create_physical_plan(plan)?;
                // Explain output is a single column with the plan description
                let explain_schema = ArrowSchema::new(vec![arrow::datatypes::Field::new(
                    "plan",
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]);
                Ok(PhysicalPlan::Explain {
                    input: Box::new(input_plan),
                    verbose: *verbose,
                    schema: Arc::new(explain_schema),
                })
            }

            LogicalPlan::ExplainAnalyze { plan, verbose } => {
                let input_plan = self.create_physical_plan(plan)?;
                // ExplainAnalyze output is a single column with the plan and statistics
                let explain_schema = ArrowSchema::new(vec![arrow::datatypes::Field::new(
                    "plan",
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]);
                Ok(PhysicalPlan::ExplainAnalyze {
                    input: Box::new(input_plan),
                    verbose: *verbose,
                    schema: Arc::new(explain_schema),
                })
            }

            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Update { .. } => {
                // DDL/DML operations are handled at a higher level
                Err(BlazeError::not_implemented(
                    "DDL/DML operations should be handled by Connection",
                ))
            }

            LogicalPlan::Copy {
                input,
                target,
                format,
                options,
                ..
            } => {
                let physical_input = self.create_physical_plan(input)?;
                let physical_format = match format {
                    crate::planner::logical_plan::CopyFormat::Parquet => {
                        PhysicalCopyFormat::Parquet
                    }
                    crate::planner::logical_plan::CopyFormat::Csv => PhysicalCopyFormat::Csv,
                    crate::planner::logical_plan::CopyFormat::Json => PhysicalCopyFormat::Json,
                };

                Ok(PhysicalPlan::Copy {
                    input: Box::new(physical_input),
                    target: target.clone(),
                    format: physical_format,
                    options: options.clone(),
                    schema: Arc::new(ArrowSchema::empty()),
                })
            }
        }
    }

    /// Create a physical expression from a logical expression.
    /// Public API for creating physical expressions outside of plan context.
    pub fn create_physical_expr_public(
        &self,
        expr: &LogicalExpr,
        schema: &Arc<ArrowSchema>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.create_physical_expr(expr, schema)
    }

    /// Create a physical expression from a logical expression.
    fn create_physical_expr(
        &self,
        expr: &LogicalExpr,
        schema: &Arc<ArrowSchema>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(col) => {
                // Find column index in schema
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == &col.name)
                    .ok_or_else(|| {
                        let available: Vec<String> =
                            schema.fields().iter().map(|f| f.name().clone()).collect();
                        BlazeError::schema_with_suggestions(&col.name, &available, "Column")
                    })?;

                Ok(Arc::new(ColumnExpr::new(col.name.clone(), idx)))
            }

            LogicalExpr::Literal(value) => Ok(Arc::new(LiteralExpr::new(value.clone()))),

            LogicalExpr::BinaryExpr { left, op, right } => {
                let left_expr = self.create_physical_expr(left, schema)?;
                let right_expr = self.create_physical_expr(right, schema)?;

                let op_str = match op {
                    BinaryOp::Eq => "eq",
                    BinaryOp::NotEq => "neq",
                    BinaryOp::Lt => "lt",
                    BinaryOp::LtEq => "lte",
                    BinaryOp::Gt => "gt",
                    BinaryOp::GtEq => "gte",
                    BinaryOp::Plus => "plus",
                    BinaryOp::Minus => "minus",
                    BinaryOp::Multiply => "multiply",
                    BinaryOp::Divide => "divide",
                    BinaryOp::Modulo => "modulo",
                    BinaryOp::And => "and",
                    BinaryOp::Or => "or",
                    BinaryOp::BitwiseAnd => "bitand",
                    BinaryOp::BitwiseOr => "bitor",
                    BinaryOp::BitwiseXor => "bitxor",
                    BinaryOp::Concat => "concat",
                };

                Ok(Arc::new(BinaryExpr::new(left_expr, op_str, right_expr)))
            }

            LogicalExpr::UnaryExpr { op, expr } => {
                let inner = self.create_physical_expr(expr, schema)?;

                match op {
                    UnaryOp::Not => Ok(Arc::new(NotExpr::new(inner))),
                    UnaryOp::Negative => {
                        // Implement negation as 0 - expr
                        let zero = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(0))));
                        Ok(Arc::new(BinaryExpr::new(zero, "minus", inner)))
                    }
                    UnaryOp::BitwiseNot => {
                        Ok(Arc::new(BitwiseNotExpr::new(inner)))
                    }
                }
            }

            LogicalExpr::IsNull(expr) => {
                let inner = self.create_physical_expr(expr, schema)?;
                Ok(Arc::new(IsNullExpr::new(inner)))
            }

            LogicalExpr::IsNotNull(expr) => {
                let inner = self.create_physical_expr(expr, schema)?;
                Ok(Arc::new(IsNotNullExpr::new(inner)))
            }

            LogicalExpr::Cast { expr, data_type } => {
                let inner = self.create_physical_expr(expr, schema)?;
                let arrow_type = data_type.to_arrow();
                Ok(Arc::new(CastExpr::new(inner, arrow_type)))
            }

            LogicalExpr::TryCast { expr, data_type } => {
                // TryCast is like Cast but returns NULL on failure
                // For now, implement the same as Cast
                let inner = self.create_physical_expr(expr, schema)?;
                let arrow_type = data_type.to_arrow();
                Ok(Arc::new(CastExpr::new(inner, arrow_type)))
            }

            LogicalExpr::Not(expr) => {
                let inner = self.create_physical_expr(expr, schema)?;
                Ok(Arc::new(NotExpr::new(inner)))
            }

            LogicalExpr::Negative(expr) => {
                // Implement negation as 0 - expr
                let inner = self.create_physical_expr(expr, schema)?;
                let zero = Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(0))));
                Ok(Arc::new(BinaryExpr::new(zero, "minus", inner)))
            }

            LogicalExpr::Alias { expr, .. } => {
                // Aliases don't affect evaluation, just pass through
                self.create_physical_expr(expr, schema)
            }

            LogicalExpr::Case { expr, when_then_exprs, else_expr } => {
                let operand = if let Some(e) = expr {
                    Some(self.create_physical_expr(e, schema)?)
                } else {
                    None
                };

                let when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = when_then_exprs
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        Ok((
                            self.create_physical_expr(when_expr, schema)?,
                            self.create_physical_expr(then_expr, schema)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_result = if let Some(e) = else_expr {
                    Some(self.create_physical_expr(e, schema)?)
                } else {
                    None
                };

                Ok(Arc::new(CaseExpr::new(operand, when_then, else_result)))
            }

            LogicalExpr::Between { expr, negated, low, high } => {
                let expr_phys = self.create_physical_expr(expr, schema)?;
                let low_phys = self.create_physical_expr(low, schema)?;
                let high_phys = self.create_physical_expr(high, schema)?;
                Ok(Arc::new(BetweenExpr::new(expr_phys, low_phys, high_phys, *negated)))
            }

            LogicalExpr::Like { negated, expr, pattern, escape: _ } => {
                let expr_phys = self.create_physical_expr(expr, schema)?;
                let pattern_phys = self.create_physical_expr(pattern, schema)?;
                // TODO: Handle escape character and case sensitivity
                Ok(Arc::new(LikeExpr::new(expr_phys, pattern_phys, *negated, false)))
            }

            LogicalExpr::InList { expr, list, negated } => {
                let expr_phys = self.create_physical_expr(expr, schema)?;
                let list_phys: Vec<Arc<dyn PhysicalExpr>> = list
                    .iter()
                    .map(|e| self.create_physical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(InListExpr::new(expr_phys, list_phys, *negated)))
            }

            LogicalExpr::Aggregate(_) => {
                Err(BlazeError::analysis(
                    "Aggregate expressions should be handled at the plan level",
                ))
            }

            LogicalExpr::Window(_) => {
                Err(BlazeError::not_implemented("Window expression in physical plan"))
            }

            LogicalExpr::ScalarSubquery(subquery_plan) => {
                // Create physical plan for the subquery
                let physical_subquery = self.create_physical_plan(subquery_plan)?;

                // Execute the subquery if we have an executor
                if let Some(ref executor) = self.subquery_executor {
                    let batches = executor(&physical_subquery)?;

                    // Extract the scalar value from the result
                    let value = if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                        ScalarValue::Null
                    } else {
                        // Get the first value from the first column of the first non-empty batch
                        let batch = batches.iter().find(|b| b.num_rows() > 0)
                            .ok_or_else(|| BlazeError::execution("Scalar subquery returned no rows"))?;

                        if batch.num_columns() == 0 {
                            return Err(BlazeError::execution("Scalar subquery returned no columns"));
                        }

                        ScalarValue::try_from_array(batch.column(0), 0)?
                    };

                    Ok(Arc::new(ScalarSubqueryExpr::new(value)))
                } else {
                    Err(BlazeError::not_implemented(
                        "Scalar subquery requires a subquery executor. Use PhysicalPlanner::with_subquery_executor()"
                    ))
                }
            }

            LogicalExpr::Exists { subquery, negated } => {
                // Create physical plan for the subquery
                let physical_subquery = self.create_physical_plan(subquery)?;

                // Execute the subquery if we have an executor
                if let Some(ref executor) = self.subquery_executor {
                    let batches = executor(&physical_subquery)?;

                    // Check if any rows were returned
                    let exists = batches.iter().any(|b| b.num_rows() > 0);

                    Ok(Arc::new(ExistsExpr::new(exists, *negated)))
                } else {
                    Err(BlazeError::not_implemented(
                        "EXISTS subquery requires a subquery executor. Use PhysicalPlanner::with_subquery_executor()"
                    ))
                }
            }

            LogicalExpr::InSubquery { expr, subquery, negated } => {
                // Create physical plan for the subquery
                let physical_subquery = self.create_physical_plan(subquery)?;

                // Create physical expression for the left-hand side
                let physical_expr = self.create_physical_expr(expr, schema)?;

                // Execute the subquery if we have an executor
                if let Some(ref executor) = self.subquery_executor {
                    let batches = executor(&physical_subquery)?;

                    // Collect all values from the subquery result
                    let mut values = Vec::new();
                    for batch in &batches {
                        if batch.num_columns() > 0 {
                            let col = batch.column(0);
                            for i in 0..batch.num_rows() {
                                values.push(ScalarValue::try_from_array(col, i)?);
                            }
                        }
                    }

                    Ok(Arc::new(InSubqueryExpr::new(physical_expr, values, *negated)))
                } else {
                    Err(BlazeError::not_implemented(
                        "IN subquery requires a subquery executor. Use PhysicalPlanner::with_subquery_executor()"
                    ))
                }
            }

            LogicalExpr::ScalarFunction { name, args } => {
                let args_phys: Vec<Arc<dyn PhysicalExpr>> = args
                    .iter()
                    .map(|e| self.create_physical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(ScalarFunctionExpr::new(name.clone(), args_phys)))
            }

            LogicalExpr::Wildcard => {
                Err(BlazeError::analysis(
                    "Wildcard should be expanded before physical planning",
                ))
            }

            LogicalExpr::QualifiedWildcard { .. } => {
                Err(BlazeError::analysis(
                    "Qualified wildcard should be expanded before physical planning",
                ))
            }

            LogicalExpr::Placeholder { id } => {
                Err(BlazeError::not_implemented(format!(
                    "Query placeholders (${}). Use string formatting to substitute parameter values before execution.",
                    id
                )))
            }
        }
    }

    /// Create a physical expression with projection mapping for filter pushdown.
    /// When filters are pushed to TableScan and there's a projection, the filter
    /// expressions need to use ORIGINAL column indices (before projection), because
    /// filters are evaluated against the original batch in storage.
    fn create_physical_expr_with_projection_mapping(
        &self,
        expr: &LogicalExpr,
        projected_schema: &Arc<ArrowSchema>,
        projection: &[usize],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(col) => {
                // Find column position in the projected schema
                let projected_idx = projected_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == &col.name)
                    .ok_or_else(|| {
                        let available: Vec<String> = projected_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect();
                        BlazeError::schema_with_suggestions(&col.name, &available, "Column")
                    })?;

                // Map to original index using projection array
                let original_idx = projection[projected_idx];

                Ok(Arc::new(ColumnExpr::new(col.name.clone(), original_idx)))
            }

            LogicalExpr::Literal(value) => Ok(Arc::new(LiteralExpr::new(value.clone()))),

            LogicalExpr::BinaryExpr { left, op, right } => {
                let left_expr = self.create_physical_expr_with_projection_mapping(
                    left,
                    projected_schema,
                    projection,
                )?;
                let right_expr = self.create_physical_expr_with_projection_mapping(
                    right,
                    projected_schema,
                    projection,
                )?;

                let op_str = match op {
                    BinaryOp::Eq => "eq",
                    BinaryOp::NotEq => "neq",
                    BinaryOp::Lt => "lt",
                    BinaryOp::LtEq => "lte",
                    BinaryOp::Gt => "gt",
                    BinaryOp::GtEq => "gte",
                    BinaryOp::Plus => "plus",
                    BinaryOp::Minus => "minus",
                    BinaryOp::Multiply => "multiply",
                    BinaryOp::Divide => "divide",
                    BinaryOp::Modulo => "modulo",
                    BinaryOp::And => "and",
                    BinaryOp::Or => "or",
                    BinaryOp::BitwiseAnd => "bitand",
                    BinaryOp::BitwiseOr => "bitor",
                    BinaryOp::BitwiseXor => "bitxor",
                    BinaryOp::Concat => "concat",
                };

                Ok(Arc::new(BinaryExpr::new(left_expr, op_str, right_expr)))
            }

            LogicalExpr::Not(inner) => {
                let inner_expr = self.create_physical_expr_with_projection_mapping(
                    inner,
                    projected_schema,
                    projection,
                )?;
                Ok(Arc::new(NotExpr::new(inner_expr)))
            }

            LogicalExpr::IsNull(inner) => {
                let inner_expr = self.create_physical_expr_with_projection_mapping(
                    inner,
                    projected_schema,
                    projection,
                )?;
                Ok(Arc::new(IsNullExpr::new(inner_expr)))
            }

            LogicalExpr::IsNotNull(inner) => {
                let inner_expr = self.create_physical_expr_with_projection_mapping(
                    inner,
                    projected_schema,
                    projection,
                )?;
                Ok(Arc::new(IsNotNullExpr::new(inner_expr)))
            }

            // For expressions that don't contain column references, delegate to regular method
            _ => self.create_physical_expr(expr, projected_schema),
        }
    }

    /// Get the output schema of a physical plan.
    fn get_plan_schema(&self, plan: &PhysicalPlan) -> Arc<ArrowSchema> {
        match plan {
            PhysicalPlan::Scan { schema, .. } => schema.clone(),
            PhysicalPlan::Filter { input, .. } => self.get_plan_schema(input),
            PhysicalPlan::Projection { schema, .. } => schema.clone(),
            PhysicalPlan::HashAggregate { schema, .. } => schema.clone(),
            PhysicalPlan::Sort { input, .. } => self.get_plan_schema(input),
            PhysicalPlan::Limit { input, .. } => self.get_plan_schema(input),
            PhysicalPlan::HashJoin { schema, .. } => schema.clone(),
            PhysicalPlan::CrossJoin { schema, .. } => schema.clone(),
            PhysicalPlan::SortMergeJoin { schema, .. } => schema.clone(),
            PhysicalPlan::Union { schema, .. } => schema.clone(),
            PhysicalPlan::Values { schema, .. } => schema.clone(),
            PhysicalPlan::Empty { schema, .. } => schema.clone(),
            PhysicalPlan::Explain { schema, .. } => schema.clone(),
            PhysicalPlan::Window { schema, .. } => schema.clone(),
            PhysicalPlan::ExplainAnalyze { schema, .. } => schema.clone(),
            PhysicalPlan::Copy { schema, .. } => schema.clone(),
        }
    }

    /// Create a physical window expression from a logical window expression.
    fn create_window_expr(
        &self,
        expr: &LogicalExpr,
        schema: &Arc<ArrowSchema>,
    ) -> Result<PhysicalWindowExpr> {
        match expr {
            LogicalExpr::Window(window_expr) => {
                // Convert the window function
                let (func, args) = self.parse_window_function(&window_expr.function, schema)?;

                // Convert partition by expressions
                let partition_by: Vec<Arc<dyn PhysicalExpr>> = window_expr
                    .partition_by
                    .iter()
                    .map(|e| self.create_physical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;

                // Convert order by expressions
                let order_by: Vec<SortExpr> = window_expr
                    .order_by
                    .iter()
                    .map(|sort_expr| {
                        let expr = self.create_physical_expr(&sort_expr.expr, schema)?;
                        Ok(SortExpr::new(expr, sort_expr.asc, sort_expr.nulls_first))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PhysicalWindowExpr::new(
                    func,
                    args,
                    partition_by,
                    order_by,
                    None, // alias will be handled at projection level
                ))
            }
            _ => Err(BlazeError::analysis("Expected window expression")),
        }
    }

    /// Parse a window function from a logical expression.
    fn parse_window_function(
        &self,
        func_expr: &LogicalExpr,
        schema: &Arc<ArrowSchema>,
    ) -> Result<(WindowFunction, Vec<Arc<dyn PhysicalExpr>>)> {
        match func_expr {
            // Handle aggregate functions used as window functions
            LogicalExpr::Aggregate(agg) => {
                let args: Vec<Arc<dyn PhysicalExpr>> = agg
                    .args
                    .iter()
                    .map(|e| self.create_physical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((WindowFunction::Aggregate(agg.func), args))
            }
            // Handle scalar functions that are actually window functions
            LogicalExpr::ScalarFunction { name, args } => {
                let func = match name.to_uppercase().as_str() {
                    "ROW_NUMBER" => WindowFunction::RowNumber,
                    "RANK" => WindowFunction::Rank,
                    "DENSE_RANK" => WindowFunction::DenseRank,
                    "NTILE" => WindowFunction::Ntile,
                    "PERCENT_RANK" => WindowFunction::PercentRank,
                    "CUME_DIST" => WindowFunction::CumeDist,
                    "LAG" => WindowFunction::Lag,
                    "LEAD" => WindowFunction::Lead,
                    "FIRST_VALUE" => WindowFunction::FirstValue,
                    "LAST_VALUE" => WindowFunction::LastValue,
                    "NTH_VALUE" => WindowFunction::NthValue,
                    _ => {
                        return Err(BlazeError::not_implemented(format!(
                            "Window function: {}",
                            name
                        )))
                    }
                };
                let physical_args: Vec<Arc<dyn PhysicalExpr>> = args
                    .iter()
                    .map(|e| self.create_physical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((func, physical_args))
            }
            _ => Err(BlazeError::analysis(
                "Expected aggregate or scalar function in window expression",
            )),
        }
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ResolvedTableRef;
    use crate::planner::logical_expr::{AggregateExpr, Column as LogicalColumn};
    use crate::planner::logical_plan::LogicalPlanBuilder;
    use crate::planner::logical_plan::SetOperation;
    use crate::planner::JoinType;
    use crate::types::{DataType, Field, Schema};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ])
    }

    fn create_test_table_ref() -> ResolvedTableRef {
        ResolvedTableRef::new("default", "main", "users")
    }

    #[test]
    fn test_physical_planner_scan() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let table_ref = ResolvedTableRef::new("default", "main", "users");
        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema.clone()).build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Scan plan"),
        }
    }

    #[test]
    fn test_physical_planner_scan_with_projection() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        // Create projection expressions for id and age columns
        let projection_exprs = vec![
            LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "id".to_string(),
            }),
            LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "age".to_string(),
            }),
        ];

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .project(projection_exprs)
            .unwrap()
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match &physical_plan {
            PhysicalPlan::Projection { schema, .. } => {
                assert_eq!(schema.fields().len(), 2);
            }
            _ => panic!("Expected Projection plan"),
        }
    }

    #[test]
    fn test_physical_planner_filter() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        // Build: SELECT * FROM users WHERE age > 30
        let filter_expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "age".to_string(),
            })),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(30)))),
        };

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .filter(filter_expr)
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Filter { .. } => {}
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_physical_planner_limit() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .limit(0, Some(10))
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Limit { skip, fetch, .. } => {
                assert_eq!(skip, 0);
                assert_eq!(fetch, Some(10));
            }
            _ => panic!("Expected Limit plan"),
        }
    }

    #[test]
    fn test_physical_planner_limit_with_offset() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .limit(5, Some(10))
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Limit { skip, fetch, .. } => {
                assert_eq!(skip, 5);
                assert_eq!(fetch, Some(10));
            }
            _ => panic!("Expected Limit plan"),
        }
    }

    #[test]
    fn test_physical_planner_sort() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        let sort_exprs = vec![crate::planner::logical_expr::SortExpr {
            expr: LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "age".to_string(),
            }),
            asc: true,
            nulls_first: false,
        }];

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .sort(sort_exprs)
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Sort { exprs, .. } => {
                assert_eq!(exprs.len(), 1);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn test_physical_planner_aggregate() {
        let schema = Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        let table_ref = create_test_table_ref();

        // Build: SELECT category, SUM(amount) FROM users GROUP BY category
        let group_exprs = vec![LogicalExpr::Column(LogicalColumn {
            relation: None,
            name: "category".to_string(),
        })];

        let agg_exprs = vec![AggregateExpr::sum(LogicalExpr::Column(LogicalColumn {
            relation: None,
            name: "amount".to_string(),
        }))];

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .aggregate(group_exprs, agg_exprs)
            .unwrap()
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::HashAggregate {
                group_by,
                aggr_exprs,
                ..
            } => {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggr_exprs.len(), 1);
            }
            _ => panic!("Expected HashAggregate plan"),
        }
    }

    #[test]
    fn test_physical_planner_distinct() {
        let schema = create_test_schema();
        let table_ref = create_test_table_ref();

        let logical_plan = LogicalPlanBuilder::scan(table_ref, schema)
            .distinct()
            .build();

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        // DISTINCT is implemented as a HashAggregate with all columns as group keys
        match physical_plan {
            PhysicalPlan::HashAggregate { aggr_exprs, .. } => {
                assert!(aggr_exprs.is_empty()); // No aggregate functions, just grouping
            }
            _ => panic!("Expected HashAggregate plan for DISTINCT"),
        }
    }

    #[test]
    fn test_physical_planner_new() {
        let planner = PhysicalPlanner::new();
        // Just verify construction works
        let _ = planner;
    }

    #[test]
    fn test_create_physical_expr_literal_int() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::empty());

        let expr = LogicalExpr::Literal(ScalarValue::Int64(Some(42)));
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        // LiteralExpr::name() returns "literal"
        assert_eq!(physical.name(), "literal");
    }

    #[test]
    fn test_create_physical_expr_literal_string() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::empty());

        let expr = LogicalExpr::Literal(ScalarValue::Utf8(Some("hello".to_string())));
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        // LiteralExpr::name() returns "literal"
        assert_eq!(physical.name(), "literal");
    }

    #[test]
    fn test_create_physical_expr_literal_bool() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::empty());

        let expr = LogicalExpr::Literal(ScalarValue::Boolean(Some(true)));
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        // LiteralExpr::name() returns "literal"
        assert_eq!(physical.name(), "literal");
    }

    #[test]
    fn test_create_physical_expr_column() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let expr = LogicalExpr::Column(LogicalColumn {
            relation: None,
            name: "name".to_string(),
        });
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert_eq!(physical.name(), "name");
    }

    #[test]
    fn test_create_physical_expr_binary_plus() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::empty());

        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(2)))),
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().contains("+") || physical.name().contains("plus"));
    }

    #[test]
    fn test_create_physical_expr_binary_eq() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));

        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "id".to_string(),
            })),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().contains("=") || physical.name().contains("eq"));
    }

    #[test]
    fn test_create_physical_expr_is_null() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            ArrowDataType::Int64,
            true,
        )]));

        let expr = LogicalExpr::IsNull(Box::new(LogicalExpr::Column(LogicalColumn {
            relation: None,
            name: "value".to_string(),
        })));
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("null"));
    }

    #[test]
    fn test_create_physical_expr_is_not_null() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            ArrowDataType::Int64,
            true,
        )]));

        let expr = LogicalExpr::IsNotNull(Box::new(LogicalExpr::Column(LogicalColumn {
            relation: None,
            name: "value".to_string(),
        })));
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(
            physical.name().to_lowercase().contains("null")
                || physical.name().to_lowercase().contains("not")
        );
    }

    #[test]
    fn test_create_physical_expr_not() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "active",
            ArrowDataType::Boolean,
            false,
        )]));

        let expr = LogicalExpr::UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "active".to_string(),
            })),
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("not") || physical.name().contains("!"));
    }

    #[test]
    fn test_create_physical_expr_between() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "age",
            ArrowDataType::Int64,
            false,
        )]));

        let expr = LogicalExpr::Between {
            expr: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "age".to_string(),
            })),
            negated: false,
            low: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(18)))),
            high: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(65)))),
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("between"));
    }

    #[test]
    fn test_create_physical_expr_like() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            false,
        )]));

        let expr = LogicalExpr::Like {
            negated: false,
            expr: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "name".to_string(),
            })),
            pattern: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some(
                "A%".to_string(),
            )))),
            escape: None,
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("like"));
    }

    #[test]
    fn test_create_physical_expr_in_list() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "status",
            ArrowDataType::Utf8,
            false,
        )]));

        let expr = LogicalExpr::InList {
            expr: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "status".to_string(),
            })),
            list: vec![
                LogicalExpr::Literal(ScalarValue::Utf8(Some("active".to_string()))),
                LogicalExpr::Literal(ScalarValue::Utf8(Some("pending".to_string()))),
            ],
            negated: false,
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("in"));
    }

    #[test]
    fn test_create_physical_expr_case() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "age",
            ArrowDataType::Int64,
            false,
        )]));

        let expr = LogicalExpr::Case {
            expr: None,
            when_then_exprs: vec![(
                LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn {
                        relation: None,
                        name: "age".to_string(),
                    })),
                    op: BinaryOp::Gt,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(18)))),
                },
                LogicalExpr::Literal(ScalarValue::Utf8(Some("adult".to_string()))),
            )],
            else_expr: Some(Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some(
                "minor".to_string(),
            ))))),
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(physical.name().to_lowercase().contains("case"));
    }

    #[test]
    fn test_create_physical_expr_cast() {
        let planner = PhysicalPlanner::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            ArrowDataType::Int64,
            false,
        )]));

        let expr = LogicalExpr::Cast {
            expr: Box::new(LogicalExpr::Column(LogicalColumn {
                relation: None,
                name: "value".to_string(),
            })),
            data_type: DataType::Float64,
        };
        let physical = planner.create_physical_expr(&expr, &schema).unwrap();

        assert!(
            physical.name().to_lowercase().contains("cast")
                || physical.name().to_lowercase().contains("float")
        );
    }

    #[test]
    fn test_physical_planner_inner_join() {
        let left_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let right_schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
        ]);

        let left_table_ref = ResolvedTableRef::new("default", "main", "users");
        let right_table_ref = ResolvedTableRef::new("default", "main", "orders");

        let left_plan = LogicalPlanBuilder::scan(left_table_ref, left_schema.clone()).build();
        let right_plan = LogicalPlanBuilder::scan(right_table_ref, right_schema.clone()).build();

        let join_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("user_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
        ]);

        let logical_plan = LogicalPlan::Join {
            left: Arc::new(left_plan),
            right: Arc::new(right_plan),
            join_type: JoinType::Inner,
            on: vec![(
                LogicalExpr::Column(LogicalColumn {
                    relation: None,
                    name: "id".to_string(),
                }),
                LogicalExpr::Column(LogicalColumn {
                    relation: None,
                    name: "user_id".to_string(),
                }),
            )],
            filter: None,
            schema: join_schema,
        };

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::HashJoin { join_type, .. } => {
                assert_eq!(join_type, JoinType::Inner);
            }
            _ => panic!("Expected HashJoin plan"),
        }
    }

    #[test]
    fn test_physical_planner_cross_join() {
        let left_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let right_schema = Schema::new(vec![Field::new("b", DataType::Int64, false)]);

        let left_table_ref = ResolvedTableRef::new("default", "main", "t1");
        let right_table_ref = ResolvedTableRef::new("default", "main", "t2");

        let left_plan = LogicalPlanBuilder::scan(left_table_ref, left_schema.clone()).build();
        let right_plan = LogicalPlanBuilder::scan(right_table_ref, right_schema.clone()).build();

        let cross_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let logical_plan = LogicalPlan::CrossJoin {
            left: Arc::new(left_plan),
            right: Arc::new(right_plan),
            schema: cross_schema,
        };

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::CrossJoin { .. } => {}
            _ => panic!("Expected CrossJoin plan"),
        }
    }

    #[test]
    fn test_physical_planner_union() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let table_ref1 = ResolvedTableRef::new("default", "main", "t1");
        let table_ref2 = ResolvedTableRef::new("default", "main", "t2");

        let left_plan = LogicalPlanBuilder::scan(table_ref1, schema.clone()).build();
        let right_plan = LogicalPlanBuilder::scan(table_ref2, schema.clone()).build();

        let logical_plan = LogicalPlan::SetOperation {
            op: SetOperation::Union,
            left: Arc::new(left_plan),
            right: Arc::new(right_plan),
            all: true,
            schema: schema.clone(),
        };

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Union { inputs, .. } => {
                assert_eq!(inputs.len(), 2);
            }
            _ => panic!("Expected Union plan"),
        }
    }

    #[test]
    fn test_physical_planner_values() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let logical_plan = LogicalPlan::Values {
            values: vec![vec![
                LogicalExpr::Literal(ScalarValue::Int64(Some(1))),
                LogicalExpr::Literal(ScalarValue::Utf8(Some("hello".to_string()))),
            ]],
            schema: schema.clone(),
        };

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        // Note: Current implementation returns empty data placeholder
        match physical_plan {
            PhysicalPlan::Values { .. } => {}
            _ => panic!("Expected Values plan"),
        }
    }

    #[test]
    fn test_physical_planner_empty_relation() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let logical_plan = LogicalPlan::EmptyRelation {
            produce_one_row: false,
            schema: schema.clone(),
        };

        let planner = PhysicalPlanner::new();
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();

        match physical_plan {
            PhysicalPlan::Empty {
                produce_one_row, ..
            } => {
                assert!(!produce_one_row);
            }
            _ => panic!("Expected Empty plan"),
        }
    }
}
