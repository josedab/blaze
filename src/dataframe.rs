//! Native DataFrame API for Blaze.
//!
//! Provides a fluent, lazy API for building and executing analytical queries
//! programmatically without writing SQL strings.
//!
//! # Example
//!
//! ```rust,no_run
//! use blaze::Connection;
//! use blaze::dataframe::DataFrame;
//!
//! let conn = Connection::in_memory().unwrap();
//! // ... register tables ...
//! let results = DataFrame::table(&conn, "users")
//!     .select(&["name", "age"])
//!     .filter("age > 21")
//!     .order_by("name", true)
//!     .limit(10)
//!     .collect()
//!     .unwrap();
//! ```

use std::fmt;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::catalog::CatalogList;
use crate::error::Result;
use crate::executor::ExecutionContext;
use crate::planner::{Binder, Optimizer, PhysicalPlanner};
use crate::sql::parser::Parser;
use crate::types::Schema;
use crate::Connection;

/// Join type for DataFrame operations.
#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL OUTER"),
            JoinType::Cross => write!(f, "CROSS"),
        }
    }
}

/// An ORDER BY expression with direction.
#[derive(Debug, Clone)]
struct OrderByExpr {
    expr: String,
    ascending: bool,
}

/// A JOIN clause in the query.
#[derive(Debug, Clone)]
struct JoinClause {
    table: String,
    join_type: JoinType,
    on: String,
}

/// Builds SQL strings from DataFrame operations.
#[derive(Debug, Clone)]
struct SqlBuilder {
    source: String,
    projections: Vec<String>,
    filters: Vec<String>,
    group_by: Vec<String>,
    aggregates: Vec<String>,
    order_by: Vec<OrderByExpr>,
    limit: Option<usize>,
    offset: Option<usize>,
    joins: Vec<JoinClause>,
    having: Vec<String>,
    distinct: bool,
}

impl SqlBuilder {
    fn new(source: String) -> Self {
        Self {
            source,
            projections: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            joins: Vec::new(),
            having: Vec::new(),
            distinct: false,
        }
    }

    /// Build from a raw SQL string (used for union/pivot/unpivot).
    fn from_raw(sql: String) -> Self {
        let mut builder = Self::new(format!("({}) AS _raw", sql));
        builder.source = format!("({}) AS _raw", sql);
        builder
    }

    /// Build a SQL query from a raw SQL source (subquery wrapping).
    fn from_sql(sql: String) -> Self {
        Self::new(format!("({}) AS _subq", sql))
    }

    fn build(&self) -> String {
        let mut sql = String::from("SELECT ");

        if self.distinct {
            sql.push_str("DISTINCT ");
        }

        // SELECT clause
        let select_items = self.select_items();
        if select_items.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&select_items.join(", "));
        }

        // FROM clause
        sql.push_str(" FROM ");
        sql.push_str(&self.source);

        // JOIN clauses
        for join in &self.joins {
            sql.push(' ');
            sql.push_str(&format!(
                "{} JOIN {} ON {}",
                join.join_type, join.table, join.on
            ));
        }

        // WHERE clause
        if !self.filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.filters.join(" AND "));
        }

        // GROUP BY clause
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // HAVING clause
        if !self.having.is_empty() {
            sql.push_str(" HAVING ");
            sql.push_str(&self.having.join(" AND "));
        }

        // ORDER BY clause
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let exprs: Vec<String> = self
                .order_by
                .iter()
                .map(|o| {
                    if o.ascending {
                        format!("{} ASC", o.expr)
                    } else {
                        format!("{} DESC", o.expr)
                    }
                })
                .collect();
            sql.push_str(&exprs.join(", "));
        }

        // LIMIT clause
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // OFFSET clause
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }

    /// Combine projections and aggregates into the SELECT items list.
    fn select_items(&self) -> Vec<String> {
        let mut items = self.projections.clone();
        items.extend(self.aggregates.clone());
        items
    }
}

/// A lazy DataFrame that builds a query plan.
///
/// Operations on a DataFrame are lazy — they build up an internal SQL
/// representation. The query is only executed when a terminal operation
/// like [`collect`](DataFrame::collect) or [`count`](DataFrame::count) is called.
pub struct DataFrame {
    catalog_list: Arc<CatalogList>,
    execution_context: ExecutionContext,
    sql_builder: SqlBuilder,
}

impl DataFrame {
    /// Create a DataFrame from an existing table registered in the connection.
    pub fn table(conn: &Connection, table_name: &str) -> Self {
        let catalog_list = conn.catalog_list();
        let execution_context = ExecutionContext::new().with_catalog_list(catalog_list.clone());
        Self {
            catalog_list,
            execution_context,
            sql_builder: SqlBuilder::new(table_name.to_string()),
        }
    }

    /// Create a DataFrame from a raw SQL query.
    ///
    /// The SQL is wrapped as a subquery so further DataFrame operations
    /// can be chained on top of it.
    pub fn sql(conn: &Connection, sql: &str) -> Self {
        let catalog_list = conn.catalog_list();
        let execution_context = ExecutionContext::new().with_catalog_list(catalog_list.clone());
        Self {
            catalog_list,
            execution_context,
            sql_builder: SqlBuilder::from_sql(sql.to_string()),
        }
    }

    /// Project specific columns.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.sql_builder.projections = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Project with arbitrary SQL expressions (e.g. `"price * qty AS total"`).
    pub fn select_expr(mut self, exprs: &[&str]) -> Self {
        self.sql_builder.projections = exprs.iter().map(|e| e.to_string()).collect();
        self
    }

    /// Add a WHERE filter condition.
    ///
    /// Multiple calls are combined with AND.
    pub fn filter(mut self, predicate: &str) -> Self {
        self.sql_builder.filters.push(predicate.to_string());
        self
    }

    /// Join with another table.
    pub fn join(mut self, other_table: &str, join_type: JoinType, on_condition: &str) -> Self {
        self.sql_builder.joins.push(JoinClause {
            table: other_table.to_string(),
            join_type,
            on: on_condition.to_string(),
        });
        self
    }

    /// Set the GROUP BY columns.
    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.sql_builder.group_by = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Add aggregate expressions (e.g. `"SUM(amount) AS total"`).
    pub fn agg(mut self, aggregates: &[&str]) -> Self {
        self.sql_builder.aggregates = aggregates.iter().map(|a| a.to_string()).collect();
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, column: &str, ascending: bool) -> Self {
        self.sql_builder.order_by.push(OrderByExpr {
            expr: column.to_string(),
            ascending,
        });
        self
    }

    /// Set the LIMIT.
    pub fn limit(mut self, n: usize) -> Self {
        self.sql_builder.limit = Some(n);
        self
    }

    /// Set the OFFSET.
    pub fn offset(mut self, n: usize) -> Self {
        self.sql_builder.offset = Some(n);
        self
    }

    /// Add a HAVING condition.
    ///
    /// Multiple calls are combined with AND.
    pub fn having(mut self, condition: &str) -> Self {
        self.sql_builder.having.push(condition.to_string());
        self
    }

    /// Generate the SQL string for this DataFrame.
    pub fn to_sql(&self) -> String {
        self.sql_builder.build()
    }

    /// Execute the query and collect all result batches.
    pub fn collect(&self) -> Result<Vec<RecordBatch>> {
        let sql = self.to_sql();
        let statements = Parser::parse(&sql)?;
        let Some(statement) = statements.into_iter().next() else {
            return Ok(vec![]);
        };
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;
        let optimized = Optimizer::default().optimize(&logical_plan)?;
        let physical_plan = PhysicalPlanner::new().create_physical_plan(&optimized)?;
        self.execution_context.execute(&physical_plan)
    }

    /// Get the output schema by planning (but not executing) the query.
    pub fn schema(&self) -> Result<Schema> {
        let sql = self.to_sql();
        let statements = Parser::parse(&sql)?;
        let Some(statement) = statements.into_iter().next() else {
            return Ok(Schema::empty());
        };
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;
        Ok(logical_plan.schema().clone())
    }

    /// Execute the query and return the total row count.
    pub fn count(&self) -> Result<usize> {
        let batches = self.collect()?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    /// Execute the query and return a pretty-printed string of the results.
    pub fn show(&self, max_rows: usize) -> Result<String> {
        let df = Self {
            catalog_list: self.catalog_list.clone(),
            execution_context: self.execution_context.clone(),
            sql_builder: {
                let mut b = self.sql_builder.clone();
                if b.limit.is_none() || b.limit.unwrap() > max_rows {
                    b.limit = Some(max_rows);
                }
                b
            },
        };
        let batches = df.collect()?;
        Ok(arrow::util::pretty::pretty_format_batches(&batches)
            .map_err(|e| crate::error::BlazeError::execution(e.to_string()))?
            .to_string())
    }
}

/// Column reference helper.
pub struct Col(pub String);

/// Create a column reference.
pub fn col(name: &str) -> Col {
    Col(name.to_string())
}

/// Literal value for expressions.
pub enum Lit {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

/// Create an integer literal.
pub fn lit_int(v: i64) -> Lit {
    Lit::Int(v)
}

/// Create a string literal.
pub fn lit_str(v: &str) -> Lit {
    Lit::Str(v.to_string())
}

/// Create a float literal.
pub fn lit_float(v: f64) -> Lit {
    Lit::Float(v)
}

impl fmt::Display for Col {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Lit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lit::Int(v) => write!(f, "{}", v),
            Lit::Float(v) => write!(f, "{}", v),
            Lit::Str(v) => write!(f, "'{}'", v),
            Lit::Bool(v) => write!(f, "{}", v),
            Lit::Null => write!(f, "NULL"),
        }
    }
}

// ---------------------------------------------------------------------------
// Expression DSL — type-safe expression builder for DataFrame operations
// ---------------------------------------------------------------------------

/// A composable expression that generates SQL fragments.
#[derive(Debug, Clone)]
pub struct Expr {
    sql: String,
}

impl Expr {
    /// Create an expression from raw SQL.
    pub fn raw(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    /// Alias this expression.
    pub fn alias(self, name: &str) -> Self {
        Self {
            sql: format!("{} AS {}", self.sql, name),
        }
    }

    // --- Comparison operators ---

    pub fn eq(self, other: Expr) -> Self {
        Self {
            sql: format!("{} = {}", self.sql, other.sql),
        }
    }

    pub fn ne(self, other: Expr) -> Self {
        Self {
            sql: format!("{} <> {}", self.sql, other.sql),
        }
    }

    pub fn gt(self, other: Expr) -> Self {
        Self {
            sql: format!("{} > {}", self.sql, other.sql),
        }
    }

    pub fn ge(self, other: Expr) -> Self {
        Self {
            sql: format!("{} >= {}", self.sql, other.sql),
        }
    }

    pub fn lt(self, other: Expr) -> Self {
        Self {
            sql: format!("{} < {}", self.sql, other.sql),
        }
    }

    pub fn le(self, other: Expr) -> Self {
        Self {
            sql: format!("{} <= {}", self.sql, other.sql),
        }
    }

    // --- Arithmetic ---

    pub fn add(self, other: Expr) -> Self {
        Self {
            sql: format!("({} + {})", self.sql, other.sql),
        }
    }

    pub fn sub(self, other: Expr) -> Self {
        Self {
            sql: format!("({} - {})", self.sql, other.sql),
        }
    }

    pub fn mul(self, other: Expr) -> Self {
        Self {
            sql: format!("({} * {})", self.sql, other.sql),
        }
    }

    pub fn div(self, other: Expr) -> Self {
        Self {
            sql: format!("({} / {})", self.sql, other.sql),
        }
    }

    // --- Logical ---

    pub fn and(self, other: Expr) -> Self {
        Self {
            sql: format!("({} AND {})", self.sql, other.sql),
        }
    }

    pub fn or(self, other: Expr) -> Self {
        Self {
            sql: format!("({} OR {})", self.sql, other.sql),
        }
    }

    pub fn not(self) -> Self {
        Self {
            sql: format!("NOT ({})", self.sql),
        }
    }

    // --- Predicates ---

    pub fn is_null(self) -> Self {
        Self {
            sql: format!("{} IS NULL", self.sql),
        }
    }

    pub fn is_not_null(self) -> Self {
        Self {
            sql: format!("{} IS NOT NULL", self.sql),
        }
    }

    pub fn between(self, low: Expr, high: Expr) -> Self {
        Self {
            sql: format!("{} BETWEEN {} AND {}", self.sql, low.sql, high.sql),
        }
    }

    pub fn like(self, pattern: &str) -> Self {
        Self {
            sql: format!("{} LIKE '{}'", self.sql, pattern),
        }
    }

    pub fn in_list(self, values: &[Expr]) -> Self {
        let vals: Vec<&str> = values.iter().map(|e| e.sql.as_str()).collect();
        Self {
            sql: format!("{} IN ({})", self.sql, vals.join(", ")),
        }
    }

    // --- Aggregate wrappers ---

    pub fn sum(self) -> Self {
        Self {
            sql: format!("SUM({})", self.sql),
        }
    }

    pub fn avg(self) -> Self {
        Self {
            sql: format!("AVG({})", self.sql),
        }
    }

    pub fn min(self) -> Self {
        Self {
            sql: format!("MIN({})", self.sql),
        }
    }

    pub fn max(self) -> Self {
        Self {
            sql: format!("MAX({})", self.sql),
        }
    }

    pub fn count(self) -> Self {
        Self {
            sql: format!("COUNT({})", self.sql),
        }
    }

    pub fn count_distinct(self) -> Self {
        Self {
            sql: format!("COUNT(DISTINCT {})", self.sql),
        }
    }

    // --- String functions ---

    pub fn upper(self) -> Self {
        Self {
            sql: format!("UPPER({})", self.sql),
        }
    }

    pub fn lower(self) -> Self {
        Self {
            sql: format!("LOWER({})", self.sql),
        }
    }

    pub fn trim(self) -> Self {
        Self {
            sql: format!("TRIM({})", self.sql),
        }
    }

    pub fn length(self) -> Self {
        Self {
            sql: format!("LENGTH({})", self.sql),
        }
    }

    // --- Cast ---

    pub fn cast(self, type_name: &str) -> Self {
        Self {
            sql: format!("CAST({} AS {})", self.sql, type_name),
        }
    }

    /// Get the SQL fragment for this expression.
    pub fn to_sql(&self) -> &str {
        &self.sql
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sql)
    }
}

impl From<Col> for Expr {
    fn from(c: Col) -> Self {
        Expr { sql: c.0 }
    }
}

impl From<Lit> for Expr {
    fn from(l: Lit) -> Self {
        Expr { sql: l.to_string() }
    }
}

/// Create a column expression.
pub fn expr_col(name: &str) -> Expr {
    Expr {
        sql: name.to_string(),
    }
}

/// Create an integer literal expression.
pub fn expr_lit(v: i64) -> Expr {
    Expr { sql: v.to_string() }
}

/// Create a float literal expression.
pub fn expr_lit_f64(v: f64) -> Expr {
    Expr { sql: v.to_string() }
}

/// Create a string literal expression.
pub fn expr_lit_str(v: &str) -> Expr {
    Expr {
        sql: format!("'{}'", v),
    }
}

// ---------------------------------------------------------------------------
// Extended DataFrame operations
// ---------------------------------------------------------------------------

impl DataFrame {
    /// Filter using an Expr DSL expression.
    pub fn filter_expr(self, expr: Expr) -> Self {
        self.filter(&expr.sql)
    }

    /// Select using Expr DSL expressions.
    pub fn select_exprs(mut self, exprs: &[Expr]) -> Self {
        let strs: Vec<String> = exprs.iter().map(|e| e.sql.clone()).collect();
        self.sql_builder.projections = strs;
        self
    }

    /// Add a computed column (equivalent to Polars `with_column`).
    pub fn with_column(mut self, expr: &str, alias: &str) -> Self {
        self.sql_builder
            .projections
            .push(format!("{} AS {}", expr, alias));
        self
    }

    /// Rename a column by selecting it with an alias.
    pub fn rename(mut self, old_name: &str, new_name: &str) -> Self {
        for proj in &mut self.sql_builder.projections {
            if proj == old_name {
                *proj = format!("{} AS {}", old_name, new_name);
            }
        }
        if self.sql_builder.projections.is_empty() {
            self.sql_builder
                .projections
                .push(format!("{} AS {}", old_name, new_name));
        }
        self
    }

    /// Select distinct rows.
    pub fn distinct(mut self) -> Self {
        self.sql_builder.distinct = true;
        self
    }

    /// Drop specific columns (selects all except named columns).
    pub fn drop_columns(self, columns: &[&str]) -> Self {
        let schema = self.schema();
        if let Ok(schema) = schema {
            let remaining: Vec<&str> = schema
                .fields()
                .iter()
                .map(|f| f.name())
                .filter(|name| !columns.contains(name))
                .collect();
            self.select(&remaining)
        } else {
            self
        }
    }

    /// Perform a UNION ALL with another query.
    pub fn union_all(self, other: &DataFrame) -> Self {
        let sql = format!("{} UNION ALL {}", self.to_sql(), other.to_sql());
        Self {
            catalog_list: self.catalog_list,
            execution_context: self.execution_context,
            sql_builder: SqlBuilder::from_raw(sql),
        }
    }

    /// Perform a UNION (deduplicated) with another query.
    pub fn union(self, other: &DataFrame) -> Self {
        let sql = format!("{} UNION {}", self.to_sql(), other.to_sql());
        Self {
            catalog_list: self.catalog_list,
            execution_context: self.execution_context,
            sql_builder: SqlBuilder::from_raw(sql),
        }
    }

    /// Pivot: rotate rows into columns using aggregation.
    pub fn pivot(
        self,
        group_col: &str,
        pivot_col: &str,
        value_col: &str,
        pivot_values: &[&str],
        agg_fn: &str,
    ) -> Self {
        let mut projections = vec![group_col.to_string()];
        for pv in pivot_values {
            projections.push(format!(
                "{}(CASE WHEN {} = '{}' THEN {} END) AS {}",
                agg_fn, pivot_col, pv, value_col, pv
            ));
        }
        let sql = format!(
            "SELECT {} FROM ({}) AS _pivot GROUP BY {}",
            projections.join(", "),
            self.to_sql(),
            group_col
        );
        Self {
            catalog_list: self.catalog_list,
            execution_context: self.execution_context,
            sql_builder: SqlBuilder::from_raw(sql),
        }
    }

    /// Unpivot/Melt: rotate columns into rows.
    pub fn unpivot(
        self,
        id_cols: &[&str],
        value_cols: &[&str],
        var_name: &str,
        value_name: &str,
    ) -> Self {
        let id_select = id_cols.join(", ");
        let source_sql = self.to_sql();

        let unions: Vec<String> = value_cols
            .iter()
            .map(|vc| {
                format!(
                    "SELECT {}, '{}' AS {}, {} AS {} FROM ({}) AS _unpivot",
                    id_select, vc, var_name, vc, value_name, source_sql
                )
            })
            .collect();

        let sql = unions.join(" UNION ALL ");
        Self {
            catalog_list: self.catalog_list,
            execution_context: self.execution_context,
            sql_builder: SqlBuilder::from_raw(sql),
        }
    }

    /// Apply a window function over the DataFrame.
    pub fn with_row_number(mut self, name: &str, order_by: &str) -> Self {
        self.sql_builder.projections.push(format!(
            "ROW_NUMBER() OVER (ORDER BY {}) AS {}",
            order_by, name
        ));
        self
    }

    /// Describe: compute summary statistics for numeric columns.
    pub fn describe(&self) -> Result<Vec<RecordBatch>> {
        let schema = self.schema()?;
        let numeric_cols: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|f| {
                matches!(
                    f.data_type(),
                    crate::types::DataType::Int8
                        | crate::types::DataType::Int16
                        | crate::types::DataType::Int32
                        | crate::types::DataType::Int64
                        | crate::types::DataType::Float32
                        | crate::types::DataType::Float64
                )
            })
            .map(|f| f.name())
            .collect();

        if numeric_cols.is_empty() {
            return Ok(vec![]);
        }

        let mut agg_parts = Vec::new();
        for col_name in &numeric_cols {
            agg_parts.push(format!("COUNT({c}) AS {c}_count", c = col_name));
        }
        // Separate query for min/max to handle type differences
        let count_sql = format!("SELECT {} FROM ({})", agg_parts.join(", "), self.to_sql());

        let statements = Parser::parse(&count_sql)?;
        let Some(statement) = statements.into_iter().next() else {
            return Ok(vec![]);
        };
        let binder = Binder::new(self.catalog_list.clone());
        let plan = binder.bind(statement)?;
        let optimized = Optimizer::default().optimize(&plan)?;
        let physical = PhysicalPlanner::new().create_physical_plan(&optimized)?;
        self.execution_context.execute(&physical)
    }

    /// Alias the source table.
    pub fn table_alias(mut self, name: &str) -> Self {
        self.sql_builder.source = format!("({}) AS {}", self.sql_builder.source, name);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;

    /// Helper: create a connection with a `users` table and an `orders` table.
    fn setup_test_tables() -> Connection {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE users (id BIGINT, name VARCHAR, age BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();

        conn.execute("CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DOUBLE)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1, 100.0)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 1, 200.0)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 2, 150.0)")
            .unwrap();
        conn
    }

    #[test]
    fn test_dataframe_select() {
        let conn = setup_test_tables();
        let results = DataFrame::table(&conn, "users")
            .select(&["name", "age"])
            .collect()
            .unwrap();
        assert_eq!(results.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(results[0].num_columns(), 2);
    }

    #[test]
    fn test_dataframe_filter() {
        let conn = setup_test_tables();
        let count = DataFrame::table(&conn, "users")
            .filter("age > 28")
            .count()
            .unwrap();
        assert_eq!(count, 2); // Alice(30) and Charlie(35)
    }

    #[test]
    fn test_dataframe_join() {
        let conn = setup_test_tables();
        let count = DataFrame::table(&conn, "users")
            .join("orders", JoinType::Inner, "users.id = orders.user_id")
            .count()
            .unwrap();
        assert_eq!(count, 3); // Alice has 2 orders, Bob has 1
    }

    #[test]
    fn test_dataframe_group_by_agg() {
        let conn = setup_test_tables();
        let results = DataFrame::table(&conn, "orders")
            .group_by(&["user_id"])
            .agg(&["SUM(amount) AS total"])
            .collect()
            .unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // Two users with orders
    }

    #[test]
    fn test_dataframe_order_by_limit() {
        let conn = setup_test_tables();
        let results = DataFrame::table(&conn, "users")
            .order_by("age", false)
            .limit(2)
            .collect()
            .unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_dataframe_to_sql() {
        let sql = DataFrame::table(&Connection::in_memory().unwrap(), "users")
            .select(&["name", "age"])
            .filter("age > 21")
            .order_by("name", true)
            .limit(10)
            .to_sql();

        assert!(sql.contains("SELECT name, age"));
        assert!(sql.contains("FROM users"));
        assert!(sql.contains("WHERE age > 21"));
        assert!(sql.contains("ORDER BY name ASC"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_dataframe_chaining() {
        let conn = setup_test_tables();
        let count = DataFrame::table(&conn, "users")
            .select(&["name", "age"])
            .filter("age > 20")
            .order_by("age", true)
            .limit(10)
            .offset(0)
            .count()
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_dataframe_count() {
        let conn = setup_test_tables();
        let count = DataFrame::table(&conn, "users").count().unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_dataframe_from_sql() {
        let conn = setup_test_tables();
        let count = DataFrame::sql(&conn, "SELECT * FROM users WHERE age > 28")
            .count()
            .unwrap();
        assert_eq!(count, 2);
    }

    // --- Extended DataFrame API tests ---

    #[test]
    fn test_expr_dsl_comparison() {
        let expr = expr_col("age").gt(expr_lit(21));
        assert_eq!(expr.to_sql(), "age > 21");

        let expr = expr_col("name").eq(expr_lit_str("Alice"));
        assert_eq!(expr.to_sql(), "name = 'Alice'");
    }

    #[test]
    fn test_expr_dsl_arithmetic() {
        let expr = expr_col("price").mul(expr_col("qty")).alias("total");
        assert_eq!(expr.to_sql(), "(price * qty) AS total");
    }

    #[test]
    fn test_expr_dsl_logical() {
        let expr = expr_col("age")
            .gt(expr_lit(18))
            .and(expr_col("active").eq(Expr::raw("true")));
        assert_eq!(expr.to_sql(), "(age > 18 AND active = true)");
    }

    #[test]
    fn test_expr_dsl_aggregate() {
        let expr = expr_col("amount").sum().alias("total");
        assert_eq!(expr.to_sql(), "SUM(amount) AS total");
    }

    #[test]
    fn test_dataframe_filter_expr() {
        let conn = setup_test_tables();
        let count = DataFrame::table(&conn, "users")
            .filter_expr(expr_col("age").gt(expr_lit(28)))
            .count()
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_dataframe_with_column() {
        let conn = setup_test_tables();
        let sql = DataFrame::table(&conn, "users")
            .select(&["name", "age"])
            .with_column("age * 2", "double_age")
            .to_sql();
        assert!(sql.contains("age * 2 AS double_age"));
    }

    #[test]
    fn test_dataframe_distinct() {
        let conn = setup_test_tables();
        let sql = DataFrame::table(&conn, "users")
            .select(&["age"])
            .distinct()
            .to_sql();
        assert!(sql.contains("SELECT DISTINCT"));
    }

    #[test]
    fn test_dataframe_describe() {
        let conn = setup_test_tables();
        let result = DataFrame::table(&conn, "users").describe().unwrap();
        assert!(!result.is_empty());
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1); // One summary row
    }

    #[test]
    fn test_dataframe_union_all() {
        let conn = setup_test_tables();
        let df1 = DataFrame::table(&conn, "users").filter("age > 30");
        let df2 = DataFrame::table(&conn, "users").filter("age < 28");
        let count = df1.union_all(&df2).count().unwrap();
        assert_eq!(count, 2); // Charlie(35) + Bob(25)
    }

    #[test]
    fn test_dataframe_pivot_sql() {
        let conn = setup_test_tables();
        let sql = DataFrame::table(&conn, "orders")
            .pivot("user_id", "id", "amount", &["1", "2", "3"], "SUM")
            .to_sql();
        assert!(sql.contains("CASE WHEN"));
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn test_dataframe_unpivot_sql() {
        let conn = setup_test_tables();
        let sql = DataFrame::table(&conn, "users")
            .select(&["id", "name", "age"])
            .unpivot(&["id"], &["name", "age"], "variable", "value")
            .to_sql();
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("'name' AS variable"));
        assert!(sql.contains("'age' AS variable"));
    }

    #[test]
    fn test_expr_is_null() {
        let expr = expr_col("name").is_null();
        assert_eq!(expr.to_sql(), "name IS NULL");
    }

    #[test]
    fn test_expr_between() {
        let expr = expr_col("age").between(expr_lit(18), expr_lit(65));
        assert_eq!(expr.to_sql(), "age BETWEEN 18 AND 65");
    }

    #[test]
    fn test_expr_like() {
        let expr = expr_col("name").like("A%");
        assert_eq!(expr.to_sql(), "name LIKE 'A%'");
    }

    #[test]
    fn test_expr_cast() {
        let expr = expr_col("age").cast("VARCHAR");
        assert_eq!(expr.to_sql(), "CAST(age AS VARCHAR)");
    }
}
