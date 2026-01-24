//! SQL binder that converts parsed SQL to logical plans.

use std::sync::Arc;

use crate::catalog::{CatalogList, ResolvedTableRef, TableRef};
use crate::error::{BlazeError, Result};
use crate::sql::{self, Statement};
use crate::sql::parser::Expr;
use crate::types::{DataType, Field, ScalarValue, Schema};

use super::logical_expr::{
    AggregateExpr, AggregateFunc, BinaryOp, Column, LogicalExpr, SortExpr, UnaryOp,
    WindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use super::logical_plan::{JoinType, LogicalPlan, LogicalPlanBuilder, SetOperation, TimeTravelSpec};

/// Maximum allowed depth for nested queries/expressions to prevent stack overflow.
const MAX_PLAN_DEPTH: usize = 128;

/// Context for binding SQL to logical plans.
pub struct BindContext {
    /// Alias to schema mapping for subqueries
    pub aliases: Vec<(String, Schema)>,
    /// CTEs stored as (name, plan) for use when referenced
    pub ctes: Vec<(String, LogicalPlan)>,
    /// Current nesting depth for depth limiting
    depth: usize,
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            aliases: vec![],
            ctes: vec![],
            depth: 0,
        }
    }

    pub fn with_alias(&mut self, alias: String, schema: Schema) {
        self.aliases.push((alias, schema));
    }

    pub fn with_cte(&mut self, name: String, plan: LogicalPlan) {
        self.ctes.push((name, plan));
    }

    pub fn get_cte(&self, name: &str) -> Option<&LogicalPlan> {
        self.ctes.iter().rev().find(|(n, _)| n == name).map(|(_, p)| p)
    }

    /// Increment depth and return error if exceeded.
    pub fn enter_scope(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_PLAN_DEPTH {
            return Err(BlazeError::analysis(format!(
                "Query plan depth limit exceeded (max {}). Simplify your query or reduce nesting.",
                MAX_PLAN_DEPTH
            )));
        }
        Ok(())
    }

    /// Decrement depth when leaving a scope.
    pub fn leave_scope(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    /// Get current depth.
    pub fn depth(&self) -> usize {
        self.depth
    }
}

impl Default for BindContext {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL binder that converts parsed SQL statements to logical plans.
pub struct Binder {
    /// Catalog list for resolving table names
    catalog_list: Arc<CatalogList>,
    /// Default catalog name
    default_catalog: String,
    /// Default schema name
    default_schema: String,
}

impl Binder {
    /// Create a new binder.
    pub fn new(catalog_list: Arc<CatalogList>) -> Self {
        Self {
            catalog_list,
            default_catalog: "default".to_string(),
            default_schema: "main".to_string(),
        }
    }

    /// Set the default catalog.
    pub fn with_default_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.default_catalog = catalog.into();
        self
    }

    /// Set the default schema.
    pub fn with_default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = schema.into();
        self
    }

    /// Bind a SQL statement to a logical plan.
    pub fn bind(&self, stmt: Statement) -> Result<LogicalPlan> {
        let mut ctx = BindContext::new();
        self.bind_statement(stmt, &mut ctx)
    }

    /// Bind a SQL expression to a logical expression.
    /// This is a public API for binding expressions outside of a query context.
    pub fn bind_expr_public(&self, expr: Expr, ctx: &mut BindContext) -> Result<LogicalExpr> {
        self.bind_expr(expr, ctx)
    }

    fn bind_statement(&self, stmt: Statement, ctx: &mut BindContext) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.bind_query(*query, ctx),
            Statement::CreateTable(create) => self.bind_create_table(create),
            Statement::DropTable(drop) => self.bind_drop_table(drop),
            Statement::Insert(insert) => self.bind_insert(insert, ctx),
            Statement::Update(update) => self.bind_update(update, ctx),
            Statement::Delete(delete) => self.bind_delete(delete, ctx),
            Statement::Explain(inner) => {
                let plan = self.bind_statement(*inner, ctx)?;
                Ok(LogicalPlan::Explain {
                    plan: Arc::new(plan),
                    verbose: false,
                })
            }
            Statement::ExplainAnalyze { statement, verbose } => {
                let plan = self.bind_statement(*statement, ctx)?;
                Ok(LogicalPlan::ExplainAnalyze {
                    plan: Arc::new(plan),
                    verbose,
                })
            }
            Statement::ShowTables => self.bind_show_tables(),
            Statement::Copy(copy) => self.bind_copy(copy, ctx),
            _ => Err(BlazeError::not_implemented(format!(
                "Statement type: {:?}",
                stmt
            ))),
        }
    }

    fn bind_copy(&self, copy: sql::parser::Copy, ctx: &mut BindContext) -> Result<LogicalPlan> {
        use super::logical_plan::{CopyFormat, CopyOptions};

        // Only COPY TO is supported for now
        if copy.direction != sql::parser::CopyDirection::To {
            return Err(BlazeError::not_implemented("Only COPY TO is supported"));
        }

        // Determine the target path
        let target = match copy.target {
            sql::parser::CopyTarget::File(filename) => filename,
            sql::parser::CopyTarget::Stdout => return Err(BlazeError::not_implemented("COPY TO STDOUT not supported")),
            sql::parser::CopyTarget::Stdin => return Err(BlazeError::analysis("COPY TO STDIN is invalid")),
        };

        // Build a scan from the table
        let table_ref = TableRef::from_parts(&copy.table_name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);
        let schema = self.get_table_schema(&resolved)?;

        let input = if copy.columns.is_empty() {
            LogicalPlan::TableScan {
                table_ref: resolved,
                projection: None,
                filters: vec![],
                schema,
                time_travel: None,
            }
        } else {
            // Project specific columns
            let projection: Vec<usize> = copy.columns
                .iter()
                .filter_map(|c| schema.index_of(c.as_str()))
                .collect();
            let proj_schema = schema.project(&projection)?;
            LogicalPlan::TableScan {
                table_ref: resolved,
                projection: Some(projection),
                filters: vec![],
                schema: proj_schema,
                time_travel: None,
            }
        };

        // Determine format from file extension or explicit format option
        let format = if let Some(ref fmt) = copy.format {
            CopyFormat::from_str(fmt).unwrap_or(CopyFormat::Parquet)
        } else if let Some(ext) = std::path::Path::new(&target).extension() {
            match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                "parquet" | "pq" => CopyFormat::Parquet,
                "csv" => CopyFormat::Csv,
                "json" | "jsonl" | "ndjson" => CopyFormat::Json,
                _ => CopyFormat::Parquet, // Default
            }
        } else {
            CopyFormat::Parquet
        };

        // Parse options
        let mut options = CopyOptions::default();
        options.header = true; // Default to include header for CSV

        for (key, value) in &copy.options {
            match key.to_lowercase().as_str() {
                "header" => options.header = value.to_lowercase() == "true",
                "delimiter" => options.delimiter = value.chars().next(),
                _ => {} // Ignore unknown options
            }
        }

        Ok(LogicalPlan::Copy {
            input: Arc::new(input),
            target,
            format,
            options,
            schema: Schema::empty(),
        })
    }

    fn bind_query(&self, query: sql::parser::Query, ctx: &mut BindContext) -> Result<LogicalPlan> {
        // Track query nesting depth to prevent stack overflow
        ctx.enter_scope()?;
        let result = self.bind_query_inner(query, ctx);
        ctx.leave_scope();
        result
    }

    fn bind_query_inner(&self, query: sql::parser::Query, ctx: &mut BindContext) -> Result<LogicalPlan> {
        // Handle CTEs
        if let Some(with) = query.with {
            for cte in with.ctes {
                let cte_plan = self.bind_query(*cte.query, ctx)?;
                ctx.with_alias(cte.alias.clone(), cte_plan.schema().clone());
                ctx.with_cte(cte.alias, cte_plan);
            }
        }

        // Bind the main query body
        let mut plan = self.bind_set_expr(query.body, ctx)?;

        // Apply ORDER BY
        if !query.order_by.is_empty() {
            let sort_exprs: Vec<SortExpr> = query
                .order_by
                .into_iter()
                .map(|o| self.bind_order_by(o, ctx))
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlanBuilder::from(plan).sort(sort_exprs).build();
        }

        // Apply LIMIT/OFFSET
        let skip = query
            .offset
            .map(|e| self.eval_const_expr(&e))
            .transpose()?
            .unwrap_or(0) as usize;

        let fetch = query
            .limit
            .map(|e| self.eval_const_expr(&e))
            .transpose()?
            .map(|v| v as usize);

        if skip > 0 || fetch.is_some() {
            plan = LogicalPlanBuilder::from(plan).limit(skip, fetch).build();
        }

        Ok(plan)
    }

    fn bind_set_expr(&self, set_expr: sql::parser::SetExpr, ctx: &mut BindContext) -> Result<LogicalPlan> {
        match set_expr {
            sql::parser::SetExpr::Select(select) => self.bind_select(*select, ctx),
            sql::parser::SetExpr::Union { left, right, all } => {
                let left_plan = self.bind_set_expr(*left, ctx)?;
                let right_plan = self.bind_set_expr(*right, ctx)?;
                Ok(LogicalPlan::SetOperation {
                    schema: left_plan.schema().clone(),
                    left: Arc::new(left_plan),
                    right: Arc::new(right_plan),
                    op: SetOperation::Union,
                    all,
                })
            }
            sql::parser::SetExpr::Intersect { left, right, all } => {
                let left_plan = self.bind_set_expr(*left, ctx)?;
                let right_plan = self.bind_set_expr(*right, ctx)?;
                Ok(LogicalPlan::SetOperation {
                    schema: left_plan.schema().clone(),
                    left: Arc::new(left_plan),
                    right: Arc::new(right_plan),
                    op: SetOperation::Intersect,
                    all,
                })
            }
            sql::parser::SetExpr::Except { left, right, all } => {
                let left_plan = self.bind_set_expr(*left, ctx)?;
                let right_plan = self.bind_set_expr(*right, ctx)?;
                Ok(LogicalPlan::SetOperation {
                    schema: left_plan.schema().clone(),
                    left: Arc::new(left_plan),
                    right: Arc::new(right_plan),
                    op: SetOperation::Except,
                    all,
                })
            }
            sql::parser::SetExpr::Values(rows) => {
                let values: Vec<Vec<LogicalExpr>> = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| self.bind_expr(e, ctx))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Infer schema from first row
                let schema = if let Some(first_row) = values.first() {
                    let fields: Vec<Field> = first_row
                        .iter()
                        .enumerate()
                        .map(|(i, _)| Field::new(format!("column{}", i), DataType::Utf8, true))
                        .collect();
                    Schema::new(fields)
                } else {
                    Schema::empty()
                };

                Ok(LogicalPlan::Values { schema, values })
            }
        }
    }

    fn bind_select(&self, select: sql::parser::Select, ctx: &mut BindContext) -> Result<LogicalPlan> {
        // 1. Bind FROM clause
        let mut plan = self.bind_from(&select.from, ctx)?;

        // 2. Bind WHERE clause
        if let Some(selection) = select.selection {
            let predicate = self.bind_expr(selection, ctx)?;
            plan = LogicalPlanBuilder::from(plan).filter(predicate).build();
        }

        // 3. Check for aggregation
        let has_aggregate = select.projection.iter().any(|item| {
            if let sql::parser::SelectItem::Expr { expr, .. } = item {
                self.has_aggregate(expr)
            } else {
                false
            }
        });

        // Check for window functions
        let has_window = select.projection.iter().any(|item| {
            if let sql::parser::SelectItem::Expr { expr, .. } = item {
                self.has_window(expr)
            } else {
                false
            }
        });

        let has_group_by = !select.group_by.is_empty();

        if has_aggregate || has_group_by {
            // Bind GROUP BY and aggregates
            let group_by: Vec<LogicalExpr> = select
                .group_by
                .into_iter()
                .map(|e| self.bind_expr(e, ctx))
                .collect::<Result<Vec<_>>>()?;

            let (projection, aggr_exprs) = self.extract_aggregates(&select.projection, ctx)?;

            // Clone for later use
            let group_by_clone = group_by.clone();
            let aggr_exprs_clone = aggr_exprs.clone();

            plan = LogicalPlanBuilder::from(plan)
                .aggregate(group_by, aggr_exprs)?
                .build();

            // Apply HAVING
            if let Some(having) = select.having {
                let predicate = self.bind_expr(having, ctx)?;
                plan = LogicalPlanBuilder::from(plan).filter(predicate).build();
            }

            // Apply projection only if we have expressions that aren't direct aggregate
            // references or group by columns. For simple "SELECT COUNT(*) FROM t" queries,
            // the Aggregate node already produces the correct output schema.
            let needs_projection = projection.iter().any(|e| {
                !self.is_simple_aggregate_ref(e) && !self.is_group_by_column(e, &group_by_clone)
            });

            if needs_projection && !projection.is_empty() {
                // For complex expressions involving aggregates, we need to replace
                // aggregate expressions with column references to the aggregate output
                let new_projection = self.rewrite_aggregate_refs(&projection, &aggr_exprs_clone);
                if !new_projection.is_empty() {
                    plan = LogicalPlanBuilder::from(plan).project(new_projection)?.build();
                }
            }
        } else if has_window {
            // Handle window functions
            let (projection, window_exprs) = self.extract_windows(&select.projection, ctx)?;

            if !window_exprs.is_empty() {
                // Create the output schema by adding window columns to input schema
                // Clone the schema so we can still use it after moving plan
                let input_schema = plan.schema().clone();
                let mut output_fields = input_schema.fields().to_vec();

                // Add a field for each window function
                for (i, _window_expr) in window_exprs.iter().enumerate() {
                    let field_name = format!("window_{}", i);
                    // Window functions typically return Int64 (for ROW_NUMBER, RANK, etc.)
                    output_fields.push(Field::new(&field_name, DataType::Int64, true));
                }

                let output_schema = Schema::new(output_fields);

                plan = LogicalPlan::Window {
                    window_exprs: window_exprs.clone(),
                    input: Arc::new(plan),
                    schema: output_schema.clone(),
                };

                // Apply final projection to select only the requested columns
                // Replace window expressions with column references
                let final_projection = self.rewrite_window_refs(&projection, &window_exprs, &input_schema);
                if !final_projection.is_empty() {
                    plan = LogicalPlanBuilder::from(plan).project(final_projection)?.build();
                }
            }
        } else {
            // Simple projection
            let projection: Vec<LogicalExpr> = select
                .projection
                .into_iter()
                .map(|item| self.bind_select_item(item, ctx))
                .collect::<Result<Vec<_>>>()?;

            if !projection.is_empty() && !matches!(projection[0], LogicalExpr::Wildcard) {
                plan = LogicalPlanBuilder::from(plan).project(projection)?.build();
            }
        }

        // 4. Apply DISTINCT
        if select.distinct {
            plan = LogicalPlanBuilder::from(plan).distinct().build();
        }

        Ok(plan)
    }

    fn rewrite_window_refs(
        &self,
        projection: &[LogicalExpr],
        window_exprs: &[LogicalExpr],
        input_schema: &Schema,
    ) -> Vec<LogicalExpr> {
        projection
            .iter()
            .map(|expr| self.replace_window_with_column(expr, window_exprs, input_schema))
            .collect()
    }

    fn replace_window_with_column(
        &self,
        expr: &LogicalExpr,
        window_exprs: &[LogicalExpr],
        input_schema: &Schema,
    ) -> LogicalExpr {
        // Check if this expression matches any window expression
        for (i, window_expr) in window_exprs.iter().enumerate() {
            if format!("{:?}", expr) == format!("{:?}", window_expr) {
                let col_name = format!("window_{}", i);
                return LogicalExpr::Column(Column::new(None::<String>, col_name));
            }
        }

        // Recursively replace in sub-expressions
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => LogicalExpr::BinaryExpr {
                left: Box::new(self.replace_window_with_column(left, window_exprs, input_schema)),
                op: *op,
                right: Box::new(self.replace_window_with_column(right, window_exprs, input_schema)),
            },
            LogicalExpr::Alias { expr, alias } => LogicalExpr::Alias {
                expr: Box::new(self.replace_window_with_column(expr, window_exprs, input_schema)),
                alias: alias.clone(),
            },
            LogicalExpr::Case { expr, when_then_exprs, else_expr } => LogicalExpr::Case {
                expr: expr.as_ref().map(|e| Box::new(self.replace_window_with_column(e, window_exprs, input_schema))),
                when_then_exprs: when_then_exprs
                    .iter()
                    .map(|(when, then)| {
                        (
                            self.replace_window_with_column(when, window_exprs, input_schema),
                            self.replace_window_with_column(then, window_exprs, input_schema),
                        )
                    })
                    .collect(),
                else_expr: else_expr.as_ref().map(|e| Box::new(self.replace_window_with_column(e, window_exprs, input_schema))),
            },
            _ => expr.clone(),
        }
    }

    fn bind_from(
        &self,
        from: &[sql::parser::TableWithJoins],
        ctx: &mut BindContext,
    ) -> Result<LogicalPlan> {
        if from.is_empty() {
            // No FROM clause - return empty relation that produces one row
            return Ok(LogicalPlan::EmptyRelation {
                produce_one_row: true,
                schema: Schema::empty(),
            });
        }

        let mut plan: Option<LogicalPlan> = None;

        for table_with_joins in from {
            let mut current = self.bind_table_factor(&table_with_joins.relation, ctx)?;

            // Apply joins
            for join in &table_with_joins.joins {
                let right = self.bind_table_factor(&join.relation, ctx)?;
                let join_type = self.convert_join_type(join.join_type);

                let on = match &join.constraint {
                    sql::parser::JoinConstraint::On(expr) => {
                        // Parse the ON condition into join keys
                        let condition = self.bind_expr(expr.clone(), ctx)?;
                        self.extract_join_keys(&condition)?
                    }
                    sql::parser::JoinConstraint::Using(cols) => cols
                        .iter()
                        .map(|c| {
                            (
                                LogicalExpr::column(c.clone()),
                                LogicalExpr::column(c.clone()),
                            )
                        })
                        .collect(),
                    sql::parser::JoinConstraint::Natural => {
                        // Natural join - find common columns
                        vec![]
                    }
                    sql::parser::JoinConstraint::None => vec![],
                };

                current = LogicalPlanBuilder::from(current)
                    .join(right, join_type, on)?
                    .build();
            }

            // Cross join with previous tables
            plan = Some(match plan {
                Some(p) => LogicalPlanBuilder::from(p).cross_join(current)?.build(),
                None => current,
            });
        }

        plan.ok_or_else(|| BlazeError::internal("Empty FROM clause"))
    }

    fn bind_table_factor(
        &self,
        factor: &sql::parser::TableFactor,
        ctx: &mut BindContext,
    ) -> Result<LogicalPlan> {
        match factor {
            sql::parser::TableFactor::Table { name, alias, time_travel } => {
                // First check if this is a CTE reference
                let table_name = name.last().map(|s| s.as_str()).unwrap_or("");
                if let Some(cte_plan) = ctx.get_cte(table_name) {
                    // Use the CTE plan, wrapped in a SubqueryAlias
                    let mut plan = LogicalPlanBuilder::from(cte_plan.clone())
                        .alias(table_name)
                        .build();

                    // Apply additional alias if specified
                    if let Some(a) = alias {
                        plan = LogicalPlanBuilder::from(plan).alias(&a.name).build();
                        ctx.with_alias(a.name.clone(), plan.schema().clone());
                    }

                    return Ok(plan);
                }

                let table_ref = TableRef::from_parts(name)?;
                let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

                // Try to get schema from catalog
                let schema = self.get_table_schema(&resolved)?;

                // Convert time travel specification from parser to logical plan format
                let logical_time_travel = self.convert_time_travel(time_travel.as_ref(), ctx)?;

                let mut plan = LogicalPlanBuilder::scan_with_time_travel(
                    resolved,
                    schema,
                    logical_time_travel,
                )
                .build();

                // Apply alias
                if let Some(a) = alias {
                    plan = LogicalPlanBuilder::from(plan).alias(&a.name).build();
                    ctx.with_alias(a.name.clone(), plan.schema().clone());
                }

                Ok(plan)
            }
            sql::parser::TableFactor::Derived { subquery, alias } => {
                let plan = self.bind_query(*subquery.clone(), ctx)?;
                if let Some(a) = alias {
                    let aliased = LogicalPlanBuilder::from(plan).alias(&a.name).build();
                    ctx.with_alias(a.name.clone(), aliased.schema().clone());
                    Ok(aliased)
                } else {
                    Ok(plan)
                }
            }
            sql::parser::TableFactor::NestedJoin(twj) => self.bind_from(&[*twj.clone()], ctx),
            sql::parser::TableFactor::TableFunction { name, args, alias } => {
                // Handle table functions like read_parquet, read_csv, etc.
                let func_name = name.last().map(|s| s.as_str()).unwrap_or("");
                self.bind_table_function(func_name, args, alias.as_ref(), ctx)
            }
        }
    }

    fn bind_table_function(
        &self,
        name: &str,
        _args: &[sql::parser::Expr],
        alias: Option<&sql::parser::TableAlias>,
        ctx: &mut BindContext,
    ) -> Result<LogicalPlan> {
        match name.to_lowercase().as_str() {
            "read_parquet" | "read_csv" | "read_json" => {
                // For now, create a placeholder table scan
                let table_ref = ResolvedTableRef::new("default", "main", name);
                let schema = Schema::empty();
                let mut plan = LogicalPlanBuilder::scan(table_ref, schema).build();

                if let Some(a) = alias {
                    plan = LogicalPlanBuilder::from(plan).alias(&a.name).build();
                    ctx.with_alias(a.name.clone(), plan.schema().clone());
                }

                Ok(plan)
            }
            _ => Err(BlazeError::not_implemented(format!(
                "Table function: {}",
                name
            ))),
        }
    }

    /// Convert parser time travel specification to logical plan time travel spec.
    fn convert_time_travel(
        &self,
        time_travel: Option<&sql::parser::TimeTravelSpec>,
        ctx: &mut BindContext,
    ) -> Result<Option<TimeTravelSpec>> {
        match time_travel {
            None => Ok(None),
            Some(sql::parser::TimeTravelSpec::Version(version)) => {
                Ok(Some(TimeTravelSpec::Version(*version)))
            }
            Some(sql::parser::TimeTravelSpec::Timestamp(expr)) => {
                // Evaluate the timestamp expression
                let bound_expr = self.bind_expr(expr.clone(), ctx)?;
                let ts = self.eval_timestamp_expr(&bound_expr)?;
                Ok(Some(TimeTravelSpec::Timestamp(ts)))
            }
            Some(sql::parser::TimeTravelSpec::SystemTimeAsOf(expr)) => {
                // FOR SYSTEM_TIME AS OF is the SQL standard syntax
                let bound_expr = self.bind_expr(expr.clone(), ctx)?;
                let ts = self.eval_timestamp_expr(&bound_expr)?;
                Ok(Some(TimeTravelSpec::Timestamp(ts)))
            }
        }
    }

    /// Evaluate a timestamp expression to get a concrete DateTime value.
    fn eval_timestamp_expr(&self, expr: &LogicalExpr) -> Result<chrono::DateTime<chrono::Utc>> {
        use chrono::{DateTime, NaiveDateTime, Utc};
        use crate::types::TimeUnit;

        match expr {
            LogicalExpr::Literal(ScalarValue::Utf8(Some(s))) => {
                // Try to parse as timestamp string
                // Support ISO 8601 format: "2024-01-15T10:30:00Z" or "2024-01-15 10:30:00"
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Ok(dt.with_timezone(&Utc));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Ok(dt.and_utc());
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(dt.and_utc());
                }
                Err(BlazeError::analysis(format!(
                    "Cannot parse timestamp from string: {}",
                    s
                )))
            }
            LogicalExpr::Literal(ScalarValue::Timestamp { value: Some(ts), unit, .. }) => {
                // Convert timestamp value based on unit
                let micros = match unit {
                    TimeUnit::Second => ts * 1_000_000,
                    TimeUnit::Millisecond => ts * 1_000,
                    TimeUnit::Microsecond => *ts,
                    TimeUnit::Nanosecond => ts / 1_000,
                };
                let secs = micros / 1_000_000;
                let nsecs = ((micros % 1_000_000) * 1000) as u32;
                Ok(DateTime::from_timestamp(secs, nsecs).unwrap_or_else(Utc::now))
            }
            LogicalExpr::Literal(ScalarValue::Int64(Some(ts))) => {
                // Assume Unix timestamp in seconds
                Ok(DateTime::from_timestamp(*ts, 0).unwrap_or_else(Utc::now))
            }
            _ => Err(BlazeError::analysis(
                "Time travel requires a constant timestamp expression",
            )),
        }
    }

    fn bind_select_item(
        &self,
        item: sql::parser::SelectItem,
        ctx: &mut BindContext,
    ) -> Result<LogicalExpr> {
        match item {
            sql::parser::SelectItem::Expr { expr, alias } => {
                let bound = self.bind_expr(expr, ctx)?;
                if let Some(a) = alias {
                    Ok(bound.alias(a))
                } else {
                    Ok(bound)
                }
            }
            sql::parser::SelectItem::Wildcard => Ok(LogicalExpr::Wildcard),
            sql::parser::SelectItem::QualifiedWildcard(qualifier) => Ok(LogicalExpr::QualifiedWildcard {
                qualifier: qualifier.join("."),
            }),
        }
    }

    fn bind_expr(&self, expr: sql::parser::Expr, ctx: &mut BindContext) -> Result<LogicalExpr> {
        match expr {
            sql::parser::Expr::Column(col_ref) => Ok(LogicalExpr::Column(Column::new(
                col_ref.relation,
                col_ref.name,
            ))),
            sql::parser::Expr::Literal(lit) => Ok(LogicalExpr::Literal(self.bind_literal(lit)?)),
            sql::parser::Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr {
                left: Box::new(self.bind_expr(*left, ctx)?),
                op: self.convert_binary_op(op)?,
                right: Box::new(self.bind_expr(*right, ctx)?),
            }),
            sql::parser::Expr::UnaryOp { op, expr } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                match op {
                    sql::parser::UnaryOperator::Not => Ok(LogicalExpr::Not(Box::new(bound_expr))),
                    sql::parser::UnaryOperator::Minus => Ok(LogicalExpr::Negative(Box::new(bound_expr))),
                    sql::parser::UnaryOperator::Plus => Ok(bound_expr),
                    sql::parser::UnaryOperator::BitwiseNot => Ok(LogicalExpr::UnaryExpr {
                        op: UnaryOp::BitwiseNot,
                        expr: Box::new(bound_expr),
                    }),
                }
            }
            sql::parser::Expr::Function(func) => self.bind_function(func, ctx),
            sql::parser::Expr::Aggregate {
                function,
                args,
                distinct,
                filter,
            } => {
                let agg_func = self.convert_aggregate_func(&function)?;

                // Handle COUNT(*) specially - replace wildcard with a literal
                // COUNT(*) counts all rows, so we use a constant 1 as the argument
                let bound_args: Vec<LogicalExpr> = args
                    .into_iter()
                    .map(|a| {
                        if matches!(a, sql::parser::Expr::Wildcard) {
                            // For COUNT(*), use a literal 1 instead of wildcard
                            // This counts all rows including NULLs
                            if matches!(function, sql::parser::AggregateFunction::Count) {
                                Ok(LogicalExpr::Literal(ScalarValue::Int64(Some(1))))
                            } else {
                                self.bind_expr(a, ctx)
                            }
                        } else {
                            self.bind_expr(a, ctx)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let mut agg = AggregateExpr::new(agg_func, bound_args);
                if distinct {
                    agg = agg.distinct();
                }
                if let Some(f) = filter {
                    agg = agg.filter(self.bind_expr(*f, ctx)?);
                }

                Ok(LogicalExpr::Aggregate(agg))
            }
            sql::parser::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let expr = operand
                    .map(|e| self.bind_expr(*e, ctx))
                    .transpose()?
                    .map(Box::new);

                let when_then_exprs: Vec<(LogicalExpr, LogicalExpr)> = conditions
                    .into_iter()
                    .zip(results.into_iter())
                    .map(|(c, r)| Ok((self.bind_expr(c, ctx)?, self.bind_expr(r, ctx)?)))
                    .collect::<Result<Vec<_>>>()?;

                let else_expr = else_result
                    .map(|e| self.bind_expr(*e, ctx))
                    .transpose()?
                    .map(Box::new);

                Ok(LogicalExpr::Case {
                    expr,
                    when_then_exprs,
                    else_expr,
                })
            }
            sql::parser::Expr::Cast { expr, data_type } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                let dt = self.parse_data_type(&data_type)?;
                Ok(LogicalExpr::Cast {
                    expr: Box::new(bound_expr),
                    data_type: dt,
                })
            }
            sql::parser::Expr::IsNull { expr, negated } => {
                let bound = self.bind_expr(*expr, ctx)?;
                if negated {
                    Ok(LogicalExpr::IsNotNull(Box::new(bound)))
                } else {
                    Ok(LogicalExpr::IsNull(Box::new(bound)))
                }
            }
            sql::parser::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                let bound_list: Vec<LogicalExpr> = list
                    .into_iter()
                    .map(|e| self.bind_expr(e, ctx))
                    .collect::<Result<Vec<_>>>()?;

                Ok(LogicalExpr::InList {
                    expr: Box::new(bound_expr),
                    list: bound_list,
                    negated,
                })
            }
            sql::parser::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                let bound_low = self.bind_expr(*low, ctx)?;
                let bound_high = self.bind_expr(*high, ctx)?;

                Ok(LogicalExpr::Between {
                    expr: Box::new(bound_expr),
                    negated,
                    low: Box::new(bound_low),
                    high: Box::new(bound_high),
                })
            }
            sql::parser::Expr::Like {
                expr,
                pattern,
                escape,
                negated,
            } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                let bound_pattern = self.bind_expr(*pattern, ctx)?;
                let bound_escape = escape.map(|e| self.bind_expr(*e, ctx)).transpose()?;

                Ok(LogicalExpr::Like {
                    negated,
                    expr: Box::new(bound_expr),
                    pattern: Box::new(bound_pattern),
                    escape: bound_escape.map(Box::new),
                })
            }
            sql::parser::Expr::Subquery(query) => {
                let plan = self.bind_query(*query, ctx)?;
                Ok(LogicalExpr::ScalarSubquery(Arc::new(plan)))
            }
            sql::parser::Expr::Exists { subquery, negated } => {
                let plan = self.bind_query(*subquery, ctx)?;
                Ok(LogicalExpr::Exists {
                    subquery: Arc::new(plan),
                    negated,
                })
            }
            sql::parser::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let bound_expr = self.bind_expr(*expr, ctx)?;
                let plan = self.bind_query(*subquery, ctx)?;
                Ok(LogicalExpr::InSubquery {
                    expr: Box::new(bound_expr),
                    subquery: Arc::new(plan),
                    negated,
                })
            }
            sql::parser::Expr::Nested(inner) => self.bind_expr(*inner, ctx),
            sql::parser::Expr::Wildcard => Ok(LogicalExpr::Wildcard),
            sql::parser::Expr::Parameter(id) => Ok(LogicalExpr::Placeholder { id }),
            sql::parser::Expr::Window {
                function,
                partition_by,
                order_by,
                frame,
            } => {
                let bound_func = self.bind_expr(*function, ctx)?;
                let bound_partition_by: Vec<LogicalExpr> = partition_by
                    .into_iter()
                    .map(|e| self.bind_expr(e, ctx))
                    .collect::<Result<Vec<_>>>()?;
                let bound_order_by: Vec<SortExpr> = order_by
                    .into_iter()
                    .map(|ob| {
                        Ok(SortExpr {
                            expr: self.bind_expr(ob.expr, ctx)?,
                            asc: ob.asc,
                            nulls_first: ob.nulls_first.unwrap_or(false),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let bound_frame = frame.map(|f| self.bind_window_frame(f)).transpose()?;

                Ok(LogicalExpr::Window(Box::new(WindowExpr {
                    function: bound_func,
                    partition_by: bound_partition_by,
                    order_by: bound_order_by,
                    frame: bound_frame,
                })))
            }
            sql::parser::Expr::Array(_) | sql::parser::Expr::Tuple(_) => {
                Err(BlazeError::not_implemented("Array and Tuple expressions"))
            }
        }
    }

    fn bind_window_frame(&self, frame: sql::parser::WindowFrame) -> Result<WindowFrame> {
        let units = match frame.units {
            sql::parser::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
            sql::parser::WindowFrameUnits::Range => WindowFrameUnits::Range,
            sql::parser::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
        };
        let start = self.bind_window_frame_bound(frame.start)?;
        let end = frame
            .end
            .map(|b| self.bind_window_frame_bound(b))
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);

        Ok(WindowFrame { units, start, end })
    }

    fn bind_window_frame_bound(
        &self,
        bound: sql::parser::WindowFrameBound,
    ) -> Result<WindowFrameBound> {
        match bound {
            sql::parser::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            sql::parser::WindowFrameBound::Preceding(expr) => {
                let value = expr.map(|e| self.extract_literal_u64(*e)).transpose()?;
                Ok(WindowFrameBound::Preceding(value))
            }
            sql::parser::WindowFrameBound::Following(expr) => {
                let value = expr.map(|e| self.extract_literal_u64(*e)).transpose()?;
                Ok(WindowFrameBound::Following(value))
            }
        }
    }

    fn extract_literal_u64(&self, expr: sql::parser::Expr) -> Result<u64> {
        match expr {
            sql::parser::Expr::Literal(sql::parser::Literal::Integer(i)) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(BlazeError::analysis("Window frame bound must be non-negative"))
                }
            }
            _ => Err(BlazeError::analysis(
                "Window frame bound must be a literal integer",
            )),
        }
    }

    fn bind_literal(&self, lit: sql::parser::Literal) -> Result<ScalarValue> {
        match lit {
            sql::parser::Literal::Null => Ok(ScalarValue::Null),
            sql::parser::Literal::Boolean(b) => Ok(ScalarValue::Boolean(Some(b))),
            sql::parser::Literal::Integer(i) => Ok(ScalarValue::Int64(Some(i))),
            sql::parser::Literal::Float(f) => Ok(ScalarValue::Float64(Some(f))),
            sql::parser::Literal::String(s) => Ok(ScalarValue::Utf8(Some(s))),
            sql::parser::Literal::Interval { .. } => Err(BlazeError::not_implemented("Interval literals")),
        }
    }

    fn bind_function(&self, func: sql::parser::FunctionCall, ctx: &mut BindContext) -> Result<LogicalExpr> {
        let name = func.name.last().cloned().unwrap_or_default();
        let args: Vec<LogicalExpr> = func
            .args
            .into_iter()
            .map(|a| self.bind_expr(a, ctx))
            .collect::<Result<Vec<_>>>()?;

        Ok(LogicalExpr::ScalarFunction { name, args })
    }

    fn bind_order_by(&self, order_by: sql::parser::OrderByExpr, ctx: &mut BindContext) -> Result<SortExpr> {
        let expr = self.bind_expr(order_by.expr, ctx)?;
        Ok(SortExpr::new(
            expr,
            order_by.asc,
            order_by.nulls_first.unwrap_or(!order_by.asc),
        ))
    }

    fn bind_create_table(&self, create: sql::parser::CreateTable) -> Result<LogicalPlan> {
        let table_ref = TableRef::from_parts(&create.name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

        let fields: Vec<Field> = create
            .columns
            .into_iter()
            .map(|col| {
                let dt = self.parse_data_type(&col.data_type)?;
                Ok(Field::new(&col.name, dt, col.nullable))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(LogicalPlan::CreateTable {
            name: resolved,
            schema: Schema::new(fields),
            if_not_exists: create.if_not_exists,
        })
    }

    fn bind_drop_table(&self, drop: sql::parser::DropTable) -> Result<LogicalPlan> {
        let table_ref = TableRef::from_parts(&drop.name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

        Ok(LogicalPlan::DropTable {
            name: resolved,
            if_exists: drop.if_exists,
        })
    }

    fn bind_insert(&self, insert: sql::parser::Insert, ctx: &mut BindContext) -> Result<LogicalPlan> {
        let table_ref = TableRef::from_parts(&insert.table_name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

        let input = match insert.source {
            sql::parser::InsertSource::Values(rows) => {
                let values: Vec<Vec<LogicalExpr>> = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| self.bind_expr(e, ctx))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;

                let schema = if let Some(first) = values.first() {
                    let fields: Vec<Field> = first
                        .iter()
                        .enumerate()
                        .map(|(i, _)| Field::new(format!("col{}", i), DataType::Utf8, true))
                        .collect();
                    Schema::new(fields)
                } else {
                    Schema::empty()
                };

                LogicalPlan::Values { schema, values }
            }
            sql::parser::InsertSource::Query(query) => self.bind_query(*query, ctx)?,
        };

        Ok(LogicalPlan::Insert {
            table_ref: resolved,
            schema: Schema::new(vec![Field::new("count", DataType::Int64, false)]),
            input: Arc::new(input),
        })
    }

    fn bind_update(&self, update: sql::parser::Update, ctx: &mut BindContext) -> Result<LogicalPlan> {
        let table_ref = TableRef::from_parts(&update.table_name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

        let assignments: Vec<(String, LogicalExpr)> = update
            .assignments
            .into_iter()
            .map(|(col, expr)| Ok((col, self.bind_expr(expr, ctx)?)))
            .collect::<Result<Vec<_>>>()?;

        let predicate = update.selection.map(|e| self.bind_expr(e, ctx)).transpose()?;

        Ok(LogicalPlan::Update {
            table_ref: resolved,
            assignments,
            predicate,
            schema: Schema::new(vec![Field::new("count", DataType::Int64, false)]),
        })
    }

    fn bind_delete(&self, delete: sql::parser::Delete, ctx: &mut BindContext) -> Result<LogicalPlan> {
        let table_ref = TableRef::from_parts(&delete.table_name)?;
        let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

        let predicate = delete.selection.map(|e| self.bind_expr(e, ctx)).transpose()?;

        Ok(LogicalPlan::Delete {
            table_ref: resolved,
            predicate,
            schema: Schema::new(vec![Field::new("count", DataType::Int64, false)]),
        })
    }

    fn bind_show_tables(&self) -> Result<LogicalPlan> {
        // Return information about tables
        let schema = Schema::new(vec![
            Field::new("catalog", DataType::Utf8, false),
            Field::new("schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);

        Ok(LogicalPlan::Values {
            schema,
            values: vec![],
        })
    }

    // Helper methods

    fn get_table_schema(&self, table_ref: &ResolvedTableRef) -> Result<Schema> {
        if let Some(catalog) = self.catalog_list.catalog(&table_ref.catalog) {
            if let Some(schema) = catalog.schema(&table_ref.schema) {
                if let Some(table) = schema.table(&table_ref.table) {
                    return Ok(table.schema().clone());
                }
            }
        }
        // Return empty schema for now if table not found
        Ok(Schema::empty())
    }

    fn convert_join_type(&self, join_type: sql::parser::JoinType) -> JoinType {
        match join_type {
            sql::parser::JoinType::Inner => JoinType::Inner,
            sql::parser::JoinType::Left => JoinType::Left,
            sql::parser::JoinType::Right => JoinType::Right,
            sql::parser::JoinType::Full => JoinType::Full,
            sql::parser::JoinType::Cross => JoinType::Cross,
            sql::parser::JoinType::LeftSemi => JoinType::LeftSemi,
            sql::parser::JoinType::RightSemi => JoinType::RightSemi,
            sql::parser::JoinType::LeftAnti => JoinType::LeftAnti,
            sql::parser::JoinType::RightAnti => JoinType::RightAnti,
        }
    }

    fn convert_binary_op(&self, op: sql::parser::BinaryOperator) -> Result<BinaryOp> {
        match op {
            sql::parser::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            sql::parser::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            sql::parser::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            sql::parser::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            sql::parser::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            sql::parser::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            sql::parser::BinaryOperator::And => Ok(BinaryOp::And),
            sql::parser::BinaryOperator::Or => Ok(BinaryOp::Or),
            sql::parser::BinaryOperator::Plus => Ok(BinaryOp::Plus),
            sql::parser::BinaryOperator::Minus => Ok(BinaryOp::Minus),
            sql::parser::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            sql::parser::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            sql::parser::BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
            sql::parser::BinaryOperator::Concat => Ok(BinaryOp::Concat),
            sql::parser::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
            sql::parser::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
            sql::parser::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
        }
    }

    fn convert_aggregate_func(&self, func: &sql::parser::AggregateFunction) -> Result<AggregateFunc> {
        match func {
            sql::parser::AggregateFunction::Count => Ok(AggregateFunc::Count),
            sql::parser::AggregateFunction::Sum => Ok(AggregateFunc::Sum),
            sql::parser::AggregateFunction::Avg => Ok(AggregateFunc::Avg),
            sql::parser::AggregateFunction::Min => Ok(AggregateFunc::Min),
            sql::parser::AggregateFunction::Max => Ok(AggregateFunc::Max),
            sql::parser::AggregateFunction::First => Ok(AggregateFunc::First),
            sql::parser::AggregateFunction::Last => Ok(AggregateFunc::Last),
            sql::parser::AggregateFunction::ArrayAgg => Ok(AggregateFunc::ArrayAgg),
            sql::parser::AggregateFunction::StringAgg => Ok(AggregateFunc::StringAgg),
            sql::parser::AggregateFunction::ApproxCountDistinct => Ok(AggregateFunc::ApproxCountDistinct),
            sql::parser::AggregateFunction::ApproxPercentile => Ok(AggregateFunc::ApproxPercentile),
            sql::parser::AggregateFunction::ApproxMedian => Ok(AggregateFunc::ApproxMedian),
            _ => Err(BlazeError::not_implemented(format!(
                "Aggregate function: {:?}",
                func
            ))),
        }
    }

    fn parse_data_type(&self, type_str: &str) -> Result<DataType> {
        let upper = type_str.to_uppercase();
        match upper.as_str() {
            "INT" | "INTEGER" | "INT4" => Ok(DataType::Int32),
            "BIGINT" | "INT8" => Ok(DataType::Int64),
            "SMALLINT" | "INT2" => Ok(DataType::Int16),
            "TINYINT" | "INT1" => Ok(DataType::Int8),
            "FLOAT" | "REAL" | "FLOAT4" => Ok(DataType::Float32),
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Float64),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "VARCHAR" | "TEXT" | "STRING" | "CHAR" => Ok(DataType::Utf8),
            "DATE" => Ok(DataType::Date32),
            "TIMESTAMP" => Ok(DataType::Timestamp {
                unit: crate::types::TimeUnit::Microsecond,
                timezone: None,
            }),
            "BLOB" | "BYTEA" | "BINARY" => Ok(DataType::Binary),
            _ => {
                // Try to parse DECIMAL(p, s)
                if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
                    Ok(DataType::Decimal128 {
                        precision: 38,
                        scale: 9,
                    })
                } else {
                    Ok(DataType::Utf8) // Default to string
                }
            }
        }
    }

    fn has_aggregate(&self, expr: &sql::parser::Expr) -> bool {
        match expr {
            sql::parser::Expr::Aggregate { .. } => true,
            sql::parser::Expr::BinaryOp { left, right, .. } => {
                self.has_aggregate(left) || self.has_aggregate(right)
            }
            sql::parser::Expr::Function(func) => {
                let name = func.name.last().map(|s| s.to_uppercase()).unwrap_or_default();
                matches!(
                    name.as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "FIRST" | "LAST"
                )
            }
            _ => false,
        }
    }

    fn has_window(&self, expr: &sql::parser::Expr) -> bool {
        match expr {
            sql::parser::Expr::Window { .. } => true,
            sql::parser::Expr::BinaryOp { left, right, .. } => {
                self.has_window(left) || self.has_window(right)
            }
            sql::parser::Expr::UnaryOp { expr, .. } => self.has_window(expr),
            sql::parser::Expr::Nested(inner) => self.has_window(inner),
            sql::parser::Expr::Case { operand, conditions, results, else_result } => {
                operand.as_ref().map(|e| self.has_window(e)).unwrap_or(false)
                    || conditions.iter().any(|e| self.has_window(e))
                    || results.iter().any(|e| self.has_window(e))
                    || else_result.as_ref().map(|e| self.has_window(e)).unwrap_or(false)
            }
            _ => false,
        }
    }

    fn extract_windows(
        &self,
        projection: &[sql::parser::SelectItem],
        ctx: &mut BindContext,
    ) -> Result<(Vec<LogicalExpr>, Vec<LogicalExpr>)> {
        let mut exprs = Vec::new();
        let mut window_exprs = Vec::new();

        for item in projection {
            if let sql::parser::SelectItem::Expr { expr, alias } = item {
                let bound = self.bind_expr(expr.clone(), ctx)?;

                // Extract window expressions
                self.collect_windows(&bound, &mut window_exprs);

                let final_expr = if let Some(a) = alias {
                    bound.alias(a.clone())
                } else {
                    bound
                };
                exprs.push(final_expr);
            } else if let sql::parser::SelectItem::Wildcard = item {
                exprs.push(LogicalExpr::Wildcard);
            } else if let sql::parser::SelectItem::QualifiedWildcard(qualifier) = item {
                exprs.push(LogicalExpr::QualifiedWildcard {
                    qualifier: qualifier.join("."),
                });
            }
        }

        Ok((exprs, window_exprs))
    }

    fn collect_windows(&self, expr: &LogicalExpr, windows: &mut Vec<LogicalExpr>) {
        match expr {
            LogicalExpr::Window(_) => {
                if !windows.iter().any(|w| format!("{:?}", w) == format!("{:?}", expr)) {
                    windows.push(expr.clone());
                }
            }
            LogicalExpr::BinaryExpr { left, right, .. } => {
                self.collect_windows(left, windows);
                self.collect_windows(right, windows);
            }
            LogicalExpr::UnaryExpr { expr, .. } => {
                self.collect_windows(expr, windows);
            }
            LogicalExpr::Alias { expr, .. } => {
                self.collect_windows(expr, windows);
            }
            LogicalExpr::Case { expr, when_then_exprs, else_expr } => {
                if let Some(e) = expr {
                    self.collect_windows(e, windows);
                }
                for (when, then) in when_then_exprs {
                    self.collect_windows(when, windows);
                    self.collect_windows(then, windows);
                }
                if let Some(e) = else_expr {
                    self.collect_windows(e, windows);
                }
            }
            _ => {}
        }
    }

    /// Check if an expression is a direct aggregate reference (possibly with alias).
    fn is_simple_aggregate_ref(&self, expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::Aggregate(_) => true,
            LogicalExpr::Alias { expr, .. } => self.is_simple_aggregate_ref(expr),
            _ => false,
        }
    }

    /// Check if an expression is a group by column reference.
    fn is_group_by_column(&self, expr: &LogicalExpr, group_by: &[LogicalExpr]) -> bool {
        match expr {
            LogicalExpr::Column(col) => {
                group_by.iter().any(|gb| {
                    if let LogicalExpr::Column(gb_col) = gb {
                        gb_col.name == col.name
                    } else {
                        false
                    }
                })
            }
            LogicalExpr::Alias { expr, .. } => self.is_group_by_column(expr, group_by),
            _ => false,
        }
    }

    /// Rewrite aggregate expressions in a projection to column references.
    /// This is used when the projection contains complex expressions that involve
    /// aggregates (e.g., COUNT(*) + 1).
    fn rewrite_aggregate_refs(
        &self,
        projection: &[LogicalExpr],
        _aggr_exprs: &[AggregateExpr],
    ) -> Vec<LogicalExpr> {
        projection
            .iter()
            .map(|expr| self.rewrite_expr_aggregate_refs(expr))
            .collect()
    }

    fn rewrite_expr_aggregate_refs(&self, expr: &LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::Aggregate(agg) => {
                // Replace with column reference to aggregate output
                LogicalExpr::column(agg.name())
            }
            LogicalExpr::Alias { expr, alias } => LogicalExpr::Alias {
                expr: Box::new(self.rewrite_expr_aggregate_refs(expr)),
                alias: alias.clone(),
            },
            LogicalExpr::BinaryExpr { left, op, right } => LogicalExpr::BinaryExpr {
                left: Box::new(self.rewrite_expr_aggregate_refs(left)),
                op: op.clone(),
                right: Box::new(self.rewrite_expr_aggregate_refs(right)),
            },
            LogicalExpr::Cast { expr, data_type } => LogicalExpr::Cast {
                expr: Box::new(self.rewrite_expr_aggregate_refs(expr)),
                data_type: data_type.clone(),
            },
            _ => expr.clone(),
        }
    }

    fn extract_aggregates(
        &self,
        projection: &[sql::parser::SelectItem],
        ctx: &mut BindContext,
    ) -> Result<(Vec<LogicalExpr>, Vec<AggregateExpr>)> {
        let mut exprs = Vec::new();
        let mut aggregates = Vec::new();

        for item in projection {
            if let sql::parser::SelectItem::Expr { expr, alias } = item {
                let bound = self.bind_expr(expr.clone(), ctx)?;
                if let LogicalExpr::Aggregate(agg) = bound.clone() {
                    aggregates.push(agg);
                }
                let final_expr = if let Some(a) = alias {
                    bound.alias(a.clone())
                } else {
                    bound
                };
                exprs.push(final_expr);
            }
        }

        Ok((exprs, aggregates))
    }

    fn extract_join_keys(
        &self,
        condition: &LogicalExpr,
    ) -> Result<Vec<(LogicalExpr, LogicalExpr)>> {
        match condition {
            LogicalExpr::BinaryExpr {
                left,
                op: BinaryOp::Eq,
                right,
            } => Ok(vec![((**left).clone(), (**right).clone())]),
            LogicalExpr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut keys = self.extract_join_keys(left)?;
                keys.extend(self.extract_join_keys(right)?);
                Ok(keys)
            }
            _ => Ok(vec![]),
        }
    }

    fn eval_const_expr(&self, expr: &sql::parser::Expr) -> Result<i64> {
        match expr {
            sql::parser::Expr::Literal(sql::parser::Literal::Integer(i)) => Ok(*i),
            _ => Err(BlazeError::plan("Expected constant integer expression")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_binder() -> Binder {
        let catalog_list = Arc::new(CatalogList::default());
        Binder::new(catalog_list)
    }

    #[test]
    fn test_bind_simple_select() {
        let binder = create_binder();
        let stmt = sql::parser::Parser::parse_one("SELECT 1").unwrap();
        let plan = binder.bind(stmt).unwrap();

        assert!(matches!(plan, LogicalPlan::Projection { .. }));
    }

    #[test]
    fn test_bind_select_from_table() {
        let binder = create_binder();
        let stmt = sql::parser::Parser::parse_one("SELECT * FROM users").unwrap();
        let plan = binder.bind(stmt).unwrap();

        // Should be TableScan since wildcard doesn't require projection
        assert!(matches!(plan, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn test_bind_select_with_filter() {
        let binder = create_binder();
        let stmt = sql::parser::Parser::parse_one("SELECT * FROM users WHERE id > 10").unwrap();
        let plan = binder.bind(stmt).unwrap();

        assert!(matches!(plan, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_bind_aggregate() {
        let binder = create_binder();
        let stmt = sql::parser::Parser::parse_one("SELECT COUNT(*) FROM orders").unwrap();
        let plan = binder.bind(stmt).unwrap();

        let display = format!("{}", plan);
        assert!(display.contains("Aggregate") || display.contains("Projection"));
    }
}
