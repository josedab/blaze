//! SQL parser implementation using sqlparser-rs.

use sqlparser::ast::{self as sql_ast, ObjectName};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::error::{BlazeError, Result};

/// Blaze's SQL statement representation.
#[derive(Debug, Clone)]
pub enum Statement {
    /// SELECT query
    Query(Box<Query>),
    /// CREATE TABLE statement
    CreateTable(CreateTable),
    /// DROP TABLE statement
    DropTable(DropTable),
    /// INSERT statement
    Insert(Insert),
    /// UPDATE statement
    Update(Update),
    /// DELETE statement
    Delete(Delete),
    /// EXPLAIN statement
    Explain(Box<Statement>),
    /// EXPLAIN ANALYZE statement - executes and shows runtime stats
    ExplainAnalyze {
        statement: Box<Statement>,
        verbose: bool,
    },
    /// DESCRIBE/SHOW statement
    Describe(String),
    /// SET variable statement
    SetVariable { name: String, value: String },
    /// SHOW TABLES
    ShowTables,
    /// COPY statement for data import/export
    Copy(Copy),
    /// BEGIN TRANSACTION
    BeginTransaction,
    /// COMMIT
    Commit,
    /// ROLLBACK
    Rollback,
    /// SAVEPOINT name
    Savepoint(String),
    /// RELEASE SAVEPOINT name
    ReleaseSavepoint(String),
    /// ROLLBACK TO SAVEPOINT name
    RollbackToSavepoint(String),
}

/// A SELECT query.
#[derive(Debug, Clone)]
pub struct Query {
    /// WITH clause (CTEs)
    pub with: Option<With>,
    /// The body of the query
    pub body: SetExpr,
    /// ORDER BY clause
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT clause
    pub limit: Option<Expr>,
    /// OFFSET clause
    pub offset: Option<Expr>,
}

/// WITH clause for Common Table Expressions.
#[derive(Debug, Clone)]
pub struct With {
    /// Whether this is a recursive CTE
    pub recursive: bool,
    /// The CTE definitions
    pub ctes: Vec<Cte>,
}

/// A single CTE definition.
#[derive(Debug, Clone)]
pub struct Cte {
    /// CTE name
    pub alias: String,
    /// CTE query
    pub query: Box<Query>,
    /// Optional column aliases
    pub columns: Vec<String>,
}

/// Set expression (SELECT, UNION, INTERSECT, EXCEPT).
#[derive(Debug, Clone)]
pub enum SetExpr {
    /// A SELECT statement
    Select(Box<Select>),
    /// UNION of two queries
    Union {
        left: Box<SetExpr>,
        right: Box<SetExpr>,
        all: bool,
    },
    /// INTERSECT of two queries
    Intersect {
        left: Box<SetExpr>,
        right: Box<SetExpr>,
        all: bool,
    },
    /// EXCEPT of two queries
    Except {
        left: Box<SetExpr>,
        right: Box<SetExpr>,
        all: bool,
    },
    /// VALUES clause
    Values(Vec<Vec<Expr>>),
}

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct Select {
    /// Whether DISTINCT is specified
    pub distinct: bool,
    /// The projection (SELECT list)
    pub projection: Vec<SelectItem>,
    /// FROM clause
    pub from: Vec<TableWithJoins>,
    /// WHERE clause
    pub selection: Option<Expr>,
    /// GROUP BY clause
    pub group_by: Vec<Expr>,
    /// HAVING clause
    pub having: Option<Expr>,
    /// Window definitions
    pub window_defs: Vec<WindowDef>,
}

/// A SELECT list item.
#[derive(Debug, Clone)]
pub enum SelectItem {
    /// An expression with optional alias
    Expr { expr: Expr, alias: Option<String> },
    /// Wildcard (*)
    Wildcard,
    /// Qualified wildcard (table.*)
    QualifiedWildcard(Vec<String>),
}

/// A table reference with joins.
#[derive(Debug, Clone)]
pub struct TableWithJoins {
    /// The base table
    pub relation: TableFactor,
    /// Joins on the table
    pub joins: Vec<Join>,
}

/// Time travel specification for Delta Lake tables.
#[derive(Debug, Clone)]
pub enum TimeTravelSpec {
    /// Travel to a specific version number
    Version(i64),
    /// Travel to a specific timestamp
    Timestamp(Expr),
    /// FOR SYSTEM_TIME AS OF expression (SQL standard)
    SystemTimeAsOf(Expr),
}

/// A table factor (table, subquery, or table function).
#[derive(Debug, Clone)]
pub enum TableFactor {
    /// A table reference
    Table {
        name: Vec<String>,
        alias: Option<TableAlias>,
        /// Optional time travel specification for Delta Lake tables
        time_travel: Option<TimeTravelSpec>,
    },
    /// A subquery
    Derived {
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// A table function
    TableFunction {
        name: Vec<String>,
        args: Vec<Expr>,
        alias: Option<TableAlias>,
    },
    /// Parenthesized table factor
    NestedJoin(Box<TableWithJoins>),
}

/// Table alias with optional column aliases.
#[derive(Debug, Clone)]
pub struct TableAlias {
    /// The alias name
    pub name: String,
    /// Optional column aliases
    pub columns: Vec<String>,
}

/// A join.
#[derive(Debug, Clone)]
pub struct Join {
    /// The table being joined
    pub relation: TableFactor,
    /// The join type
    pub join_type: JoinType,
    /// The join constraint
    pub constraint: JoinConstraint,
}

/// Join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

/// Join constraints.
#[derive(Debug, Clone)]
pub enum JoinConstraint {
    /// ON clause
    On(Expr),
    /// USING clause
    Using(Vec<String>),
    /// Natural join (no explicit constraint)
    Natural,
    /// No constraint (cross join)
    None,
}

/// An SQL expression.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference
    Column(ColumnRef),
    /// Literal value
    Literal(Literal),
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// Function call
    Function(FunctionCall),
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// CAST expression
    Cast { expr: Box<Expr>, data_type: String },
    /// IS NULL expression
    IsNull { expr: Box<Expr>, negated: bool },
    /// IN expression
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// IN subquery
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// EXISTS subquery
    Exists { subquery: Box<Query>, negated: bool },
    /// Scalar subquery
    Subquery(Box<Query>),
    /// BETWEEN expression
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// LIKE expression
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    /// Aggregate function
    Aggregate {
        function: AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
    },
    /// Window function
    Window {
        function: Box<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
    },
    /// Array expression
    Array(Vec<Expr>),
    /// Tuple/Row expression
    Tuple(Vec<Expr>),
    /// Parameter placeholder ($1, ?)
    Parameter(usize),
    /// Nested expression (parenthesized)
    Nested(Box<Expr>),
    /// Wildcard (*)
    Wildcard,
}

/// Column reference.
#[derive(Debug, Clone)]
pub struct ColumnRef {
    /// Optional table/alias qualifier
    pub relation: Option<String>,
    /// Column name
    pub name: String,
}

/// Literal value.
#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Interval { value: String, unit: Option<String> },
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // String
    Concat,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    BitwiseNot,
}

/// Function call.
#[derive(Debug, Clone)]
pub struct FunctionCall {
    /// Function name
    pub name: Vec<String>,
    /// Arguments
    pub args: Vec<Expr>,
    /// Whether DISTINCT is specified
    pub distinct: bool,
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    ArrayAgg,
    StringAgg,
    GroupConcat,
    /// Approximate count distinct using HyperLogLog
    ApproxCountDistinct,
    /// Approximate percentile using T-Digest
    ApproxPercentile,
    /// Approximate median (50th percentile)
    ApproxMedian,
}

/// ORDER BY expression.
#[derive(Debug, Clone)]
pub struct OrderByExpr {
    /// The expression to order by
    pub expr: Expr,
    /// Ascending or descending
    pub asc: bool,
    /// NULLS FIRST or NULLS LAST
    pub nulls_first: Option<bool>,
}

/// Window definition.
#[derive(Debug, Clone)]
pub struct WindowDef {
    /// Window name
    pub name: String,
    /// PARTITION BY clause
    pub partition_by: Vec<Expr>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByExpr>,
    /// Window frame specification
    pub frame: Option<WindowFrame>,
}

/// Window frame specification.
#[derive(Debug, Clone)]
pub struct WindowFrame {
    /// Frame units (ROWS, RANGE, GROUPS)
    pub units: WindowFrameUnits,
    /// Frame start bound
    pub start: WindowFrameBound,
    /// Frame end bound (if specified)
    pub end: Option<WindowFrameBound>,
}

/// Window frame units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

/// Window frame bound.
#[derive(Debug, Clone)]
pub enum WindowFrameBound {
    CurrentRow,
    Preceding(Option<Box<Expr>>),
    Following(Option<Box<Expr>>),
}

/// CREATE TABLE statement.
#[derive(Debug, Clone)]
pub struct CreateTable {
    /// Table name
    pub name: Vec<String>,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Whether IF NOT EXISTS is specified
    pub if_not_exists: bool,
    /// AS SELECT query
    pub query: Option<Box<Query>>,
}

/// Column definition.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Whether nullable
    pub nullable: bool,
    /// Default value
    pub default: Option<Expr>,
}

/// DROP TABLE statement.
#[derive(Debug, Clone)]
pub struct DropTable {
    /// Table name
    pub name: Vec<String>,
    /// Whether IF EXISTS is specified
    pub if_exists: bool,
}

/// INSERT statement.
#[derive(Debug, Clone)]
pub struct Insert {
    /// Target table
    pub table_name: Vec<String>,
    /// Target columns (empty = all)
    pub columns: Vec<String>,
    /// Source data
    pub source: InsertSource,
}

/// INSERT data source.
#[derive(Debug, Clone)]
pub enum InsertSource {
    /// VALUES clause
    Values(Vec<Vec<Expr>>),
    /// SELECT query
    Query(Box<Query>),
}

/// UPDATE statement.
#[derive(Debug, Clone)]
pub struct Update {
    /// Target table
    pub table_name: Vec<String>,
    /// SET assignments
    pub assignments: Vec<(String, Expr)>,
    /// FROM clause
    pub from: Option<Vec<TableWithJoins>>,
    /// WHERE clause
    pub selection: Option<Expr>,
}

/// DELETE statement.
#[derive(Debug, Clone)]
pub struct Delete {
    /// Target table
    pub table_name: Vec<String>,
    /// WHERE clause
    pub selection: Option<Expr>,
}

/// COPY statement for data import/export.
#[derive(Debug, Clone)]
pub struct Copy {
    /// Table name
    pub table_name: Vec<String>,
    /// Columns (empty = all)
    pub columns: Vec<String>,
    /// Source/destination path or STDIN/STDOUT
    pub target: CopyTarget,
    /// Copy direction
    pub direction: CopyDirection,
    /// File format
    pub format: Option<String>,
    /// Additional options
    pub options: Vec<(String, String)>,
}

/// COPY target.
#[derive(Debug, Clone)]
pub enum CopyTarget {
    File(String),
    Stdin,
    Stdout,
}

/// COPY direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyDirection {
    From,
    To,
}

/// SQL Parser for Blaze.
pub struct Parser;

impl Parser {
    /// Parse a SQL string into statements.
    pub fn parse(sql: &str) -> Result<Vec<Statement>> {
        let dialect = GenericDialect {};
        let ast = SqlParser::parse_sql(&dialect, sql)?;

        ast.into_iter().map(Self::convert_statement).collect()
    }

    /// Parse a single SQL statement.
    pub fn parse_one(sql: &str) -> Result<Statement> {
        let mut statements = Self::parse(sql)?;
        if statements.len() != 1 {
            return Err(BlazeError::parse(format!(
                "Expected 1 statement, got {}",
                statements.len()
            )));
        }
        Ok(statements.remove(0))
    }

    fn convert_statement(stmt: sql_ast::Statement) -> Result<Statement> {
        match stmt {
            sql_ast::Statement::Query(query) => {
                Ok(Statement::Query(Box::new(Self::convert_query(*query)?)))
            }
            sql_ast::Statement::CreateTable(create) => {
                Ok(Statement::CreateTable(Self::convert_create_table(create)?))
            }
            sql_ast::Statement::Drop {
                object_type: sql_ast::ObjectType::Table,
                names,
                if_exists,
                ..
            } => Ok(Statement::DropTable(DropTable {
                name: Self::convert_object_name(&names[0]),
                if_exists,
            })),
            sql_ast::Statement::Insert(insert) => {
                Ok(Statement::Insert(Self::convert_insert(insert)?))
            }
            sql_ast::Statement::Update {
                table,
                assignments,
                from,
                selection,
                ..
            } => Ok(Statement::Update(Update {
                table_name: Self::convert_table_factor_name(&table.relation)?,
                assignments: assignments
                    .into_iter()
                    .map(|a| {
                        // In newer sqlparser, target is an enum AssignmentTarget
                        let target_name = match &a.target {
                            sql_ast::AssignmentTarget::ColumnName(names) => {
                                names.0.first().map(|i| i.to_string()).unwrap_or_default()
                            }
                            sql_ast::AssignmentTarget::Tuple(_) => {
                                return Err(BlazeError::not_implemented(
                                    "Tuple assignment targets",
                                ));
                            }
                        };
                        Ok((target_name, Self::convert_expr(a.value)?))
                    })
                    .collect::<Result<Vec<_>>>()?,
                from: if let Some(f) = from {
                    // from is a TableWithJoins in newer sqlparser
                    Some(vec![Self::convert_table_with_joins(f)?])
                } else {
                    None
                },
                selection: selection.map(Self::convert_expr).transpose()?,
            })),
            sql_ast::Statement::Delete(delete) => {
                // In newer sqlparser, from is a FromTable enum
                let table_name = match &delete.from {
                    sql_ast::FromTable::WithFromKeyword(tables) => tables
                        .first()
                        .map(|f| match &f.relation {
                            sql_ast::TableFactor::Table { name, .. } => {
                                Self::convert_object_name(name)
                            }
                            _ => vec![],
                        })
                        .unwrap_or_default(),
                    sql_ast::FromTable::WithoutKeyword(tables) => tables
                        .first()
                        .map(|f| match &f.relation {
                            sql_ast::TableFactor::Table { name, .. } => {
                                Self::convert_object_name(name)
                            }
                            _ => vec![],
                        })
                        .unwrap_or_default(),
                };
                Ok(Statement::Delete(Delete {
                    table_name,
                    selection: delete.selection.map(Self::convert_expr).transpose()?,
                }))
            }
            sql_ast::Statement::Explain {
                statement,
                analyze,
                verbose,
                ..
            } => {
                let inner_stmt = Self::convert_statement(*statement)?;
                if analyze {
                    Ok(Statement::ExplainAnalyze {
                        statement: Box::new(inner_stmt),
                        verbose,
                    })
                } else {
                    Ok(Statement::Explain(Box::new(inner_stmt)))
                }
            }
            sql_ast::Statement::ShowTables { .. } => Ok(Statement::ShowTables),
            sql_ast::Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => {
                let (table_name, columns) = match source {
                    sql_ast::CopySource::Table {
                        table_name,
                        columns,
                    } => {
                        let name = table_name.0.iter().map(|i| i.to_string()).collect();
                        let cols = columns.iter().map(|i| i.to_string()).collect();
                        (name, cols)
                    }
                    _ => return Err(BlazeError::not_implemented("COPY from query")),
                };
                let direction = if to {
                    CopyDirection::To
                } else {
                    CopyDirection::From
                };
                let copy_target = match target {
                    sql_ast::CopyTarget::File { filename } => CopyTarget::File(filename),
                    sql_ast::CopyTarget::Stdin => CopyTarget::Stdin,
                    sql_ast::CopyTarget::Stdout => CopyTarget::Stdout,
                    _ => return Err(BlazeError::not_implemented("COPY target type")),
                };
                let opts: Vec<(String, String)> = options
                    .iter()
                    .filter_map(|opt| match opt {
                        sql_ast::CopyOption::Format(f) => {
                            Some(("format".to_string(), f.to_string()))
                        }
                        sql_ast::CopyOption::Delimiter(d) => {
                            Some(("delimiter".to_string(), d.to_string()))
                        }
                        sql_ast::CopyOption::Header(h) => {
                            Some(("header".to_string(), h.to_string()))
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Statement::Copy(Copy {
                    table_name,
                    columns,
                    target: copy_target,
                    direction,
                    format: None,
                    options: opts,
                }))
            }
            sql_ast::Statement::SetVariable {
                variables, value, ..
            } => {
                let name = variables
                    .first()
                    .map(|v| match v {
                        sql_ast::ObjectName(parts) => parts
                            .iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join("."),
                    })
                    .unwrap_or_default();
                let val = value.first().map(|e| format!("{}", e)).unwrap_or_default();
                Ok(Statement::SetVariable { name, value: val })
            }
            sql_ast::Statement::StartTransaction { .. } => Ok(Statement::BeginTransaction),
            sql_ast::Statement::Commit { .. } => Ok(Statement::Commit),
            sql_ast::Statement::Rollback { savepoint, .. } => {
                if let Some(sp) = savepoint {
                    Ok(Statement::RollbackToSavepoint(sp.to_string()))
                } else {
                    Ok(Statement::Rollback)
                }
            }
            sql_ast::Statement::Savepoint { name } => Ok(Statement::Savepoint(name.to_string())),
            sql_ast::Statement::ReleaseSavepoint { name } => {
                Ok(Statement::ReleaseSavepoint(name.to_string()))
            }
            _ => Err(BlazeError::not_implemented(format!(
                "Statement type: {:?}",
                stmt
            ))),
        }
    }

    fn convert_query(query: sql_ast::Query) -> Result<Query> {
        Ok(Query {
            with: query.with.map(Self::convert_with).transpose()?,
            body: Self::convert_set_expr(*query.body)?,
            order_by: query
                .order_by
                .map(|ob| {
                    ob.exprs
                        .into_iter()
                        .map(Self::convert_order_by)
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default(),
            limit: query.limit.map(Self::convert_expr).transpose()?,
            offset: query
                .offset
                .map(|o| Self::convert_expr(o.value))
                .transpose()?,
        })
    }

    fn convert_with(with: sql_ast::With) -> Result<With> {
        Ok(With {
            recursive: with.recursive,
            ctes: with
                .cte_tables
                .into_iter()
                .map(Self::convert_cte)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn convert_cte(cte: sql_ast::Cte) -> Result<Cte> {
        Ok(Cte {
            alias: cte.alias.name.to_string(),
            query: Box::new(Self::convert_query(*cte.query)?),
            columns: cte
                .alias
                .columns
                .into_iter()
                .map(|c| c.to_string())
                .collect(),
        })
    }

    fn convert_set_expr(set_expr: sql_ast::SetExpr) -> Result<SetExpr> {
        match set_expr {
            sql_ast::SetExpr::Select(select) => {
                Ok(SetExpr::Select(Box::new(Self::convert_select(*select)?)))
            }
            sql_ast::SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
                ..
            } => {
                let left = Box::new(Self::convert_set_expr(*left)?);
                let right = Box::new(Self::convert_set_expr(*right)?);
                let all = matches!(set_quantifier, sql_ast::SetQuantifier::All);
                match op {
                    sql_ast::SetOperator::Union => Ok(SetExpr::Union { left, right, all }),
                    sql_ast::SetOperator::Intersect => Ok(SetExpr::Intersect { left, right, all }),
                    sql_ast::SetOperator::Except => Ok(SetExpr::Except { left, right, all }),
                }
            }
            sql_ast::SetExpr::Values(values) => Ok(SetExpr::Values(
                values
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(Self::convert_expr).collect())
                    .collect::<Result<Vec<Vec<Expr>>>>()?,
            )),
            sql_ast::SetExpr::Query(q) => Self::convert_set_expr(*q.body),
            _ => Err(BlazeError::not_implemented(format!(
                "Set expression: {:?}",
                set_expr
            ))),
        }
    }

    fn convert_select(select: sql_ast::Select) -> Result<Select> {
        let distinct = match &select.distinct {
            Some(sql_ast::Distinct::Distinct) => true,
            Some(sql_ast::Distinct::On(_)) => true,
            None => false,
        };

        Ok(Select {
            distinct,
            projection: select
                .projection
                .into_iter()
                .map(Self::convert_select_item)
                .collect::<Result<Vec<_>>>()?,
            from: select
                .from
                .into_iter()
                .map(Self::convert_table_with_joins)
                .collect::<Result<Vec<_>>>()?,
            selection: select.selection.map(Self::convert_expr).transpose()?,
            group_by: match select.group_by {
                sql_ast::GroupByExpr::Expressions(exprs, _) => exprs
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
                sql_ast::GroupByExpr::All(_) => vec![],
            },
            having: select.having.map(Self::convert_expr).transpose()?,
            window_defs: select
                .named_window
                .into_iter()
                .map(Self::convert_named_window)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn convert_select_item(item: sql_ast::SelectItem) -> Result<SelectItem> {
        match item {
            sql_ast::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
                expr: Self::convert_expr(expr)?,
                alias: None,
            }),
            sql_ast::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
                expr: Self::convert_expr(expr)?,
                alias: Some(alias.to_string()),
            }),
            sql_ast::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
            sql_ast::SelectItem::QualifiedWildcard(name, _) => Ok(SelectItem::QualifiedWildcard(
                Self::convert_object_name(&name),
            )),
        }
    }

    fn convert_table_with_joins(twj: sql_ast::TableWithJoins) -> Result<TableWithJoins> {
        Ok(TableWithJoins {
            relation: Self::convert_table_factor(twj.relation)?,
            joins: twj
                .joins
                .into_iter()
                .map(Self::convert_join)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn convert_table_factor(factor: sql_ast::TableFactor) -> Result<TableFactor> {
        match factor {
            sql_ast::TableFactor::Table {
                name,
                alias,
                version,
                ..
            } => {
                // Convert time travel specification
                let time_travel = version.map(Self::convert_table_version).transpose()?;
                Ok(TableFactor::Table {
                    name: Self::convert_object_name(&name),
                    alias: alias.map(Self::convert_table_alias),
                    time_travel,
                })
            }
            sql_ast::TableFactor::Derived {
                subquery, alias, ..
            } => Ok(TableFactor::Derived {
                subquery: Box::new(Self::convert_query(*subquery)?),
                alias: alias.map(Self::convert_table_alias),
            }),
            sql_ast::TableFactor::TableFunction { expr: _, alias: _ } => {
                // TableFunction in newer sqlparser has an expr field, not name/args
                // For now, we'll extract what we can from the expression
                Err(BlazeError::not_implemented(
                    "Table functions are not yet supported",
                ))
            }
            sql_ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => Ok(TableFactor::NestedJoin(Box::new(
                Self::convert_table_with_joins(*table_with_joins)?,
            ))),
            _ => Err(BlazeError::not_implemented(format!(
                "Table factor: {:?}",
                factor
            ))),
        }
    }

    fn convert_table_factor_name(factor: &sql_ast::TableFactor) -> Result<Vec<String>> {
        match factor {
            sql_ast::TableFactor::Table { name, .. } => Ok(Self::convert_object_name(name)),
            _ => Err(BlazeError::parse("Expected table name")),
        }
    }

    fn convert_table_version(version: sql_ast::TableVersion) -> Result<TimeTravelSpec> {
        match version {
            sql_ast::TableVersion::ForSystemTimeAsOf(expr) => {
                let converted_expr = Self::convert_expr(expr)?;
                Ok(TimeTravelSpec::SystemTimeAsOf(converted_expr))
            }
        }
    }

    fn convert_table_alias(alias: sql_ast::TableAlias) -> TableAlias {
        TableAlias {
            name: alias.name.to_string(),
            columns: alias.columns.into_iter().map(|c| c.to_string()).collect(),
        }
    }

    fn convert_join(join: sql_ast::Join) -> Result<Join> {
        Ok(Join {
            relation: Self::convert_table_factor(join.relation)?,
            join_type: Self::convert_join_type(&join.join_operator),
            constraint: Self::convert_join_constraint(&join.join_operator)?,
        })
    }

    fn convert_join_type(op: &sql_ast::JoinOperator) -> JoinType {
        match op {
            sql_ast::JoinOperator::Inner(_) => JoinType::Inner,
            sql_ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            sql_ast::JoinOperator::RightOuter(_) => JoinType::Right,
            sql_ast::JoinOperator::FullOuter(_) => JoinType::Full,
            sql_ast::JoinOperator::CrossJoin => JoinType::Cross,
            sql_ast::JoinOperator::LeftSemi(_) => JoinType::LeftSemi,
            sql_ast::JoinOperator::RightSemi(_) => JoinType::RightSemi,
            sql_ast::JoinOperator::LeftAnti(_) => JoinType::LeftAnti,
            sql_ast::JoinOperator::RightAnti(_) => JoinType::RightAnti,
            _ => JoinType::Inner,
        }
    }

    fn convert_join_constraint(op: &sql_ast::JoinOperator) -> Result<JoinConstraint> {
        let constraint = match op {
            sql_ast::JoinOperator::Inner(c)
            | sql_ast::JoinOperator::LeftOuter(c)
            | sql_ast::JoinOperator::RightOuter(c)
            | sql_ast::JoinOperator::FullOuter(c)
            | sql_ast::JoinOperator::LeftSemi(c)
            | sql_ast::JoinOperator::RightSemi(c)
            | sql_ast::JoinOperator::LeftAnti(c)
            | sql_ast::JoinOperator::RightAnti(c) => c,
            sql_ast::JoinOperator::CrossJoin => return Ok(JoinConstraint::None),
            _ => return Ok(JoinConstraint::None),
        };

        match constraint {
            sql_ast::JoinConstraint::On(expr) => {
                Ok(JoinConstraint::On(Self::convert_expr(expr.clone())?))
            }
            sql_ast::JoinConstraint::Using(cols) => Ok(JoinConstraint::Using(
                cols.iter().map(|c| c.to_string()).collect(),
            )),
            sql_ast::JoinConstraint::Natural => Ok(JoinConstraint::Natural),
            sql_ast::JoinConstraint::None => Ok(JoinConstraint::None),
        }
    }

    fn convert_expr(expr: sql_ast::Expr) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Identifier(ident) => Ok(Expr::Column(ColumnRef {
                relation: None,
                name: ident.to_string(),
            })),
            sql_ast::Expr::CompoundIdentifier(idents) => {
                let parts: Vec<String> = idents.iter().map(|i| i.to_string()).collect();
                if parts.len() == 2 {
                    Ok(Expr::Column(ColumnRef {
                        relation: Some(parts[0].clone()),
                        name: parts[1].clone(),
                    }))
                } else {
                    Ok(Expr::Column(ColumnRef {
                        relation: Some(parts[..parts.len() - 1].join(".")),
                        name: parts.last().unwrap().clone(),
                    }))
                }
            }
            sql_ast::Expr::Value(value) => Ok(Expr::Literal(Self::convert_value(value)?)),
            sql_ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(Self::convert_expr(*left)?),
                op: Self::convert_binary_op(op)?,
                right: Box::new(Self::convert_expr(*right)?),
            }),
            sql_ast::Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: Self::convert_unary_op(op)?,
                expr: Box::new(Self::convert_expr(*expr)?),
            }),
            sql_ast::Expr::Function(func) => Self::convert_function(func),
            sql_ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Ok(Expr::Case {
                operand: operand
                    .map(|e| Self::convert_expr(*e))
                    .transpose()?
                    .map(Box::new),
                conditions: conditions
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
                results: results
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
                else_result: else_result
                    .map(|e| Self::convert_expr(*e))
                    .transpose()?
                    .map(Box::new),
            }),
            sql_ast::Expr::Cast {
                expr, data_type, ..
            } => Ok(Expr::Cast {
                expr: Box::new(Self::convert_expr(*expr)?),
                data_type: format!("{}", data_type),
            }),
            sql_ast::Expr::IsNull(expr) => Ok(Expr::IsNull {
                expr: Box::new(Self::convert_expr(*expr)?),
                negated: false,
            }),
            sql_ast::Expr::IsNotNull(expr) => Ok(Expr::IsNull {
                expr: Box::new(Self::convert_expr(*expr)?),
                negated: true,
            }),
            sql_ast::Expr::InList {
                expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Box::new(Self::convert_expr(*expr)?),
                list: list
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
                negated,
            }),
            sql_ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Ok(Expr::InSubquery {
                expr: Box::new(Self::convert_expr(*expr)?),
                subquery: Box::new(Self::convert_query(*subquery)?),
                negated,
            }),
            sql_ast::Expr::Exists { subquery, negated } => Ok(Expr::Exists {
                subquery: Box::new(Self::convert_query(*subquery)?),
                negated,
            }),
            sql_ast::Expr::Subquery(subquery) => {
                Ok(Expr::Subquery(Box::new(Self::convert_query(*subquery)?)))
            }
            sql_ast::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(Expr::Between {
                expr: Box::new(Self::convert_expr(*expr)?),
                low: Box::new(Self::convert_expr(*low)?),
                high: Box::new(Self::convert_expr(*high)?),
                negated,
            }),
            sql_ast::Expr::Like {
                expr,
                pattern,
                escape_char,
                negated,
                ..
            } => Ok(Expr::Like {
                expr: Box::new(Self::convert_expr(*expr)?),
                pattern: Box::new(Self::convert_expr(*pattern)?),
                escape: escape_char
                    .map(|c| Box::new(Expr::Literal(Literal::String(c.to_string())))),
                negated,
            }),
            sql_ast::Expr::Nested(expr) => Ok(Expr::Nested(Box::new(Self::convert_expr(*expr)?))),
            sql_ast::Expr::Tuple(exprs) => Ok(Expr::Tuple(
                exprs
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
            )),
            sql_ast::Expr::Array(arr) => Ok(Expr::Array(
                arr.elem
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
            )),
            sql_ast::Expr::Wildcard(_) => Ok(Expr::Wildcard),
            _ => Err(BlazeError::not_implemented(format!(
                "Expression: {:?}",
                expr
            ))),
        }
    }

    fn convert_value(value: sql_ast::Value) -> Result<Literal> {
        match value {
            sql_ast::Value::Null => Ok(Literal::Null),
            sql_ast::Value::Boolean(b) => Ok(Literal::Boolean(b)),
            sql_ast::Value::Number(n, _) => {
                if n.contains('.') || n.contains('e') || n.contains('E') {
                    n.parse::<f64>()
                        .map(Literal::Float)
                        .map_err(|_| BlazeError::parse(format!("Invalid float: {}", n)))
                } else {
                    n.parse::<i64>()
                        .map(Literal::Integer)
                        .map_err(|_| BlazeError::parse(format!("Invalid integer: {}", n)))
                }
            }
            sql_ast::Value::SingleQuotedString(s)
            | sql_ast::Value::DoubleQuotedString(s)
            | sql_ast::Value::DollarQuotedString(sql_ast::DollarQuotedString {
                value: s, ..
            }) => Ok(Literal::String(s)),
            _ => Err(BlazeError::not_implemented(format!("Value: {:?}", value))),
        }
    }

    fn convert_binary_op(op: sql_ast::BinaryOperator) -> Result<BinaryOperator> {
        match op {
            sql_ast::BinaryOperator::Plus => Ok(BinaryOperator::Plus),
            sql_ast::BinaryOperator::Minus => Ok(BinaryOperator::Minus),
            sql_ast::BinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            sql_ast::BinaryOperator::Divide => Ok(BinaryOperator::Divide),
            sql_ast::BinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
            sql_ast::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
            sql_ast::BinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
            sql_ast::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
            sql_ast::BinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
            sql_ast::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
            sql_ast::BinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
            sql_ast::BinaryOperator::And => Ok(BinaryOperator::And),
            sql_ast::BinaryOperator::Or => Ok(BinaryOperator::Or),
            sql_ast::BinaryOperator::StringConcat => Ok(BinaryOperator::Concat),
            sql_ast::BinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
            sql_ast::BinaryOperator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
            sql_ast::BinaryOperator::BitwiseXor => Ok(BinaryOperator::BitwiseXor),
            _ => Err(BlazeError::not_implemented(format!(
                "Binary operator: {:?}",
                op
            ))),
        }
    }

    fn convert_unary_op(op: sql_ast::UnaryOperator) -> Result<UnaryOperator> {
        match op {
            sql_ast::UnaryOperator::Plus => Ok(UnaryOperator::Plus),
            sql_ast::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
            sql_ast::UnaryOperator::Not => Ok(UnaryOperator::Not),
            sql_ast::UnaryOperator::PGBitwiseNot => Ok(UnaryOperator::BitwiseNot),
            _ => Err(BlazeError::not_implemented(format!(
                "Unary operator: {:?}",
                op
            ))),
        }
    }

    fn convert_function(func: sql_ast::Function) -> Result<Expr> {
        let name = Self::convert_object_name(&func.name);
        let func_name = name.last().map(|s| s.to_uppercase()).unwrap_or_default();

        // Check if it's an aggregate function
        let aggregate = match func_name.as_str() {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "FIRST" => Some(AggregateFunction::First),
            "LAST" => Some(AggregateFunction::Last),
            "ARRAY_AGG" => Some(AggregateFunction::ArrayAgg),
            "STRING_AGG" => Some(AggregateFunction::StringAgg),
            "GROUP_CONCAT" => Some(AggregateFunction::GroupConcat),
            "APPROX_COUNT_DISTINCT" => Some(AggregateFunction::ApproxCountDistinct),
            "APPROX_PERCENTILE" => Some(AggregateFunction::ApproxPercentile),
            "APPROX_MEDIAN" => Some(AggregateFunction::ApproxMedian),
            _ => None,
        };

        let args: Vec<Expr> = match &func.args {
            sql_ast::FunctionArguments::List(arg_list) => arg_list
                .args
                .iter()
                .filter_map(|arg| match arg {
                    sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(e)) => {
                        Some(Self::convert_expr(e.clone()))
                    }
                    sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Wildcard) => {
                        Some(Ok(Expr::Wildcard))
                    }
                    _ => None,
                })
                .collect::<Result<Vec<_>>>()?,
            _ => vec![],
        };

        let distinct = match &func.args {
            sql_ast::FunctionArguments::List(arg_list) => {
                matches!(
                    arg_list.duplicate_treatment,
                    Some(sql_ast::DuplicateTreatment::Distinct)
                )
            }
            _ => false,
        };

        if let Some(agg_func) = aggregate {
            return Ok(Expr::Aggregate {
                function: agg_func,
                args,
                distinct,
                filter: func
                    .filter
                    .map(|e| Self::convert_expr(*e))
                    .transpose()?
                    .map(Box::new),
            });
        }

        // Check for window function
        if let Some(over) = func.over {
            let base_expr = Expr::Function(FunctionCall {
                name,
                args,
                distinct,
            });

            return match over {
                sql_ast::WindowType::WindowSpec(spec) => Ok(Expr::Window {
                    function: Box::new(base_expr),
                    partition_by: spec
                        .partition_by
                        .into_iter()
                        .map(Self::convert_expr)
                        .collect::<Result<Vec<_>>>()?,
                    order_by: spec
                        .order_by
                        .into_iter()
                        .map(Self::convert_order_by)
                        .collect::<Result<Vec<_>>>()?,
                    frame: spec
                        .window_frame
                        .map(Self::convert_window_frame)
                        .transpose()?,
                }),
                sql_ast::WindowType::NamedWindow(_) => Ok(base_expr), // Named window references are resolved later
            };
        }

        Ok(Expr::Function(FunctionCall {
            name,
            args,
            distinct,
        }))
    }

    fn convert_order_by(order_by: sql_ast::OrderByExpr) -> Result<OrderByExpr> {
        Ok(OrderByExpr {
            expr: Self::convert_expr(order_by.expr)?,
            asc: order_by.asc.unwrap_or(true),
            nulls_first: order_by.nulls_first,
        })
    }

    fn convert_named_window(nw: sql_ast::NamedWindowDefinition) -> Result<WindowDef> {
        // NamedWindowDefinition is a tuple struct (Ident, NamedWindowExpr)
        let sql_ast::NamedWindowDefinition(name, spec) = nw;
        match spec {
            sql_ast::NamedWindowExpr::WindowSpec(ws) => Ok(WindowDef {
                name: name.to_string(),
                partition_by: ws
                    .partition_by
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<Vec<_>>>()?,
                order_by: ws
                    .order_by
                    .into_iter()
                    .map(Self::convert_order_by)
                    .collect::<Result<Vec<_>>>()?,
                frame: ws
                    .window_frame
                    .map(Self::convert_window_frame)
                    .transpose()?,
            }),
            sql_ast::NamedWindowExpr::NamedWindow(_) => Err(BlazeError::not_implemented(
                "Named window references in window definitions",
            )),
        }
    }

    fn convert_window_frame(frame: sql_ast::WindowFrame) -> Result<WindowFrame> {
        Ok(WindowFrame {
            units: match frame.units {
                sql_ast::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
                sql_ast::WindowFrameUnits::Range => WindowFrameUnits::Range,
                sql_ast::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
            },
            start: Self::convert_window_frame_bound(frame.start_bound)?,
            end: frame
                .end_bound
                .map(Self::convert_window_frame_bound)
                .transpose()?,
        })
    }

    fn convert_window_frame_bound(bound: sql_ast::WindowFrameBound) -> Result<WindowFrameBound> {
        match bound {
            sql_ast::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            sql_ast::WindowFrameBound::Preceding(e) => Ok(WindowFrameBound::Preceding(
                e.map(|e| Self::convert_expr(*e)).transpose()?.map(Box::new),
            )),
            sql_ast::WindowFrameBound::Following(e) => Ok(WindowFrameBound::Following(
                e.map(|e| Self::convert_expr(*e)).transpose()?.map(Box::new),
            )),
        }
    }

    fn convert_create_table(create: sql_ast::CreateTable) -> Result<CreateTable> {
        Ok(CreateTable {
            name: Self::convert_object_name(&create.name),
            columns: create
                .columns
                .into_iter()
                .map(Self::convert_column_def)
                .collect::<Result<Vec<_>>>()?,
            if_not_exists: create.if_not_exists,
            query: create
                .query
                .map(|q| Self::convert_query(*q))
                .transpose()?
                .map(Box::new),
        })
    }

    fn convert_column_def(col: sql_ast::ColumnDef) -> Result<ColumnDef> {
        let nullable = !col
            .options
            .iter()
            .any(|o| matches!(o.option, sql_ast::ColumnOption::NotNull));

        let default = col.options.iter().find_map(|o| {
            if let sql_ast::ColumnOption::Default(expr) = &o.option {
                Some(Self::convert_expr(expr.clone()))
            } else {
                None
            }
        });

        Ok(ColumnDef {
            name: col.name.to_string(),
            data_type: format!("{}", col.data_type),
            nullable,
            default: default.transpose()?,
        })
    }

    fn convert_insert(insert: sql_ast::Insert) -> Result<Insert> {
        let source = match insert.source {
            Some(source) => match *source.body {
                sql_ast::SetExpr::Values(values) => InsertSource::Values(
                    values
                        .rows
                        .into_iter()
                        .map(|row| row.into_iter().map(Self::convert_expr).collect())
                        .collect::<Result<Vec<Vec<Expr>>>>()?,
                ),
                _ => InsertSource::Query(Box::new(Self::convert_query(*source)?)),
            },
            None => InsertSource::Values(vec![]),
        };

        Ok(Insert {
            table_name: Self::convert_object_name(&insert.table_name),
            columns: insert.columns.into_iter().map(|c| c.to_string()).collect(),
            source,
        })
    }

    fn convert_object_name(name: &ObjectName) -> Vec<String> {
        name.0.iter().map(|i| i.to_string()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT a, b FROM t WHERE c > 10";
        let stmt = Parser::parse_one(sql).unwrap();

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body {
                assert_eq!(select.projection.len(), 2);
                assert_eq!(select.from.len(), 1);
                assert!(select.selection.is_some());
            } else {
                panic!("Expected Select");
            }
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_join() {
        let sql = "SELECT * FROM a INNER JOIN b ON a.id = b.id";
        let stmt = Parser::parse_one(sql).unwrap();

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body {
                assert_eq!(select.from.len(), 1);
                assert_eq!(select.from[0].joins.len(), 1);
                assert_eq!(select.from[0].joins[0].join_type, JoinType::Inner);
            } else {
                panic!("Expected Select");
            }
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_aggregate() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders GROUP BY customer_id";
        let stmt = Parser::parse_one(sql).unwrap();

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body {
                assert_eq!(select.projection.len(), 2);
                assert!(!select.group_by.is_empty());
            } else {
                panic!("Expected Select");
            }
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_cte() {
        let sql = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        let stmt = Parser::parse_one(sql).unwrap();

        if let Statement::Query(query) = stmt {
            assert!(query.with.is_some());
            let with = query.with.unwrap();
            assert_eq!(with.ctes.len(), 1);
            assert_eq!(with.ctes[0].alias, "cte");
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR)";
        let stmt = Parser::parse_one(sql).unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.name, vec!["users"]);
            assert_eq!(create.columns.len(), 2);
            assert!(!create.columns[0].nullable);
            assert!(create.columns[1].nullable);
        } else {
            panic!("Expected CreateTable");
        }
    }

    // Note: FOR SYSTEM_TIME AS OF syntax requires BigQuery or MsSql dialect.
    // The GenericDialect doesn't support it. Time travel can be used via API
    // with DeltaTable::open_at_version() or DeltaTable::open_at_timestamp().
}
