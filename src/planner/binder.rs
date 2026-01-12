//! SQL binder that converts parsed SQL to logical plans.

use std::sync::Arc;

use crate::catalog::{CatalogList, ResolvedTableRef, TableRef};
use crate::error::{BlazeError, Result};
use crate::sql::{self, Statement};
use crate::types::{DataType, Field, ScalarValue, Schema};

use super::logical_expr::{
    AggregateExpr, AggregateFunc, BinaryOp, Column, LogicalExpr, SortExpr, UnaryOp,
};
use super::logical_plan::{JoinType, LogicalPlan, LogicalPlanBuilder, SetOperation};

/// Context for binding SQL to logical plans.
pub struct BindContext {
    /// Alias to schema mapping for subqueries
    pub aliases: Vec<(String, Schema)>,
}

impl BindContext {
    pub fn new() -> Self {
        Self { aliases: vec![] }
    }

    pub fn with_alias(&mut self, alias: String, schema: Schema) {
        self.aliases.push((alias, schema));
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
            _ => Err(BlazeError::not_implemented(format!(
                "Statement type: {:?}",
                stmt
            ))),
        }
    }

    fn bind_query(&self, query: sql::parser::Query, ctx: &mut BindContext) -> Result<LogicalPlan> {
        // Handle CTEs
        if let Some(with) = query.with {
            for cte in with.ctes {
                let cte_plan = self.bind_query(*cte.query, ctx)?;
                ctx.with_alias(cte.alias, cte_plan.schema().clone());
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

        let has_group_by = !select.group_by.is_empty();

        if has_aggregate || has_group_by {
            // Bind GROUP BY and aggregates
            let group_by: Vec<LogicalExpr> = select
                .group_by
                .into_iter()
                .map(|e| self.bind_expr(e, ctx))
                .collect::<Result<Vec<_>>>()?;

            let (projection, aggr_exprs) = self.extract_aggregates(&select.projection, ctx)?;

            plan = LogicalPlanBuilder::from(plan)
                .aggregate(group_by, aggr_exprs)?
                .build();

            // Apply HAVING
            if let Some(having) = select.having {
                let predicate = self.bind_expr(having, ctx)?;
                plan = LogicalPlanBuilder::from(plan).filter(predicate).build();
            }

            // Apply projection
            if !projection.is_empty() {
                plan = LogicalPlanBuilder::from(plan).project(projection)?.build();
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
            sql::parser::TableFactor::Table { name, alias } => {
                let table_ref = TableRef::from_parts(name)?;
                let resolved = table_ref.resolve(&self.default_catalog, &self.default_schema);

                // Try to get schema from catalog
                let schema = self.get_table_schema(&resolved)?;

                let mut plan = LogicalPlanBuilder::scan(resolved, schema).build();

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
        args: &[sql::parser::Expr],
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
                let agg_func = self.convert_aggregate_func(function)?;
                let bound_args: Vec<LogicalExpr> = args
                    .into_iter()
                    .map(|a| self.bind_expr(a, ctx))
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
            _ => Err(BlazeError::not_implemented(format!(
                "Expression: {:?}",
                expr
            ))),
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

    fn convert_aggregate_func(&self, func: sql::parser::AggregateFunction) -> Result<AggregateFunc> {
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
