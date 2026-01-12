//! Logical expressions for Blaze query plans.

use std::fmt;
use std::sync::Arc;

use crate::types::{DataType, ScalarValue, Schema};

/// A column reference in a logical plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    /// Optional relation (table) name
    pub relation: Option<String>,
    /// Column name
    pub name: String,
}

impl Column {
    /// Create a new column reference.
    pub fn new(relation: Option<impl Into<String>>, name: impl Into<String>) -> Self {
        Self {
            relation: relation.map(|r| r.into()),
            name: name.into(),
        }
    }

    /// Create an unqualified column reference.
    pub fn unqualified(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    /// Create a qualified column reference.
    pub fn qualified(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            relation: Some(relation.into()),
            name: name.into(),
        }
    }

    /// Get the fully qualified name.
    pub fn qualified_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
        }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.qualified_name())
    }
}

impl From<&str> for Column {
    fn from(name: &str) -> Self {
        if let Some((relation, col)) = name.split_once('.') {
            Self::qualified(relation, col)
        } else {
            Self::unqualified(name)
        }
    }
}

/// A logical expression in a query plan.
#[derive(Debug, Clone)]
pub enum LogicalExpr {
    /// Column reference
    Column(Column),

    /// Literal value
    Literal(ScalarValue),

    /// Binary operation
    BinaryExpr {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },

    /// Unary operation
    UnaryExpr {
        op: UnaryOp,
        expr: Box<LogicalExpr>,
    },

    /// IS NULL check
    IsNull(Box<LogicalExpr>),

    /// IS NOT NULL check
    IsNotNull(Box<LogicalExpr>),

    /// CASE expression
    Case {
        expr: Option<Box<LogicalExpr>>,
        when_then_exprs: Vec<(LogicalExpr, LogicalExpr)>,
        else_expr: Option<Box<LogicalExpr>>,
    },

    /// CAST expression
    Cast {
        expr: Box<LogicalExpr>,
        data_type: DataType,
    },

    /// TRY_CAST expression (returns NULL on failure)
    TryCast {
        expr: Box<LogicalExpr>,
        data_type: DataType,
    },

    /// NOT expression
    Not(Box<LogicalExpr>),

    /// Negation
    Negative(Box<LogicalExpr>),

    /// BETWEEN expression
    Between {
        expr: Box<LogicalExpr>,
        negated: bool,
        low: Box<LogicalExpr>,
        high: Box<LogicalExpr>,
    },

    /// LIKE expression
    Like {
        negated: bool,
        expr: Box<LogicalExpr>,
        pattern: Box<LogicalExpr>,
        escape: Option<Box<LogicalExpr>>,
    },

    /// IN list expression
    InList {
        expr: Box<LogicalExpr>,
        list: Vec<LogicalExpr>,
        negated: bool,
    },

    /// Scalar function call
    ScalarFunction {
        name: String,
        args: Vec<LogicalExpr>,
    },

    /// Aggregate function
    Aggregate(AggregateExpr),

    /// Window function
    Window(Box<WindowExpr>),

    /// Scalar subquery
    ScalarSubquery(Arc<super::LogicalPlan>),

    /// EXISTS subquery
    Exists {
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// IN subquery
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// Alias (AS)
    Alias {
        expr: Box<LogicalExpr>,
        alias: String,
    },

    /// Wildcard (*)
    Wildcard,

    /// Qualified wildcard (table.*)
    QualifiedWildcard { qualifier: String },

    /// Placeholder for parameter ($1, $2, etc.)
    Placeholder { id: usize },
}

impl LogicalExpr {
    /// Create a column expression.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(Column::unqualified(name))
    }

    /// Create a qualified column expression.
    pub fn qualified_column(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Column(Column::qualified(relation, name))
    }

    /// Create a literal expression.
    pub fn literal(value: impl Into<ScalarValue>) -> Self {
        Self::Literal(value.into())
    }

    /// Create an alias.
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Self::Alias {
            expr: Box::new(self),
            alias: alias.into(),
        }
    }

    /// Create an AND expression.
    pub fn and(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    /// Create an OR expression.
    pub fn or(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }

    /// Create an equality expression.
    pub fn eq(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    /// Create a not-equal expression.
    pub fn not_eq(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::NotEq,
            right: Box::new(other),
        }
    }

    /// Create a less-than expression.
    pub fn lt(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other),
        }
    }

    /// Create a less-than-or-equal expression.
    pub fn lt_eq(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::LtEq,
            right: Box::new(other),
        }
    }

    /// Create a greater-than expression.
    pub fn gt(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other),
        }
    }

    /// Create a greater-than-or-equal expression.
    pub fn gt_eq(self, other: LogicalExpr) -> Self {
        Self::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::GtEq,
            right: Box::new(other),
        }
    }

    /// Check if this expression is a column reference.
    pub fn is_column(&self) -> bool {
        matches!(self, Self::Column(_))
    }

    /// Check if this expression is a literal.
    pub fn is_literal(&self) -> bool {
        matches!(self, Self::Literal(_))
    }

    /// Get the name of this expression (for output columns).
    pub fn name(&self) -> String {
        match self {
            Self::Column(col) => col.name.clone(),
            Self::Alias { alias, .. } => alias.clone(),
            Self::Literal(val) => val.to_string(),
            Self::BinaryExpr { left, op, right } => {
                format!("{} {} {}", left.name(), op, right.name())
            }
            Self::Aggregate(agg) => agg.name(),
            Self::ScalarFunction { name, .. } => name.clone(),
            Self::Wildcard => "*".to_string(),
            Self::QualifiedWildcard { qualifier } => format!("{}.*", qualifier),
            _ => "expr".to_string(),
        }
    }

    /// Get all column references in this expression.
    pub fn columns(&self) -> Vec<&Column> {
        let mut cols = Vec::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns<'a>(&'a self, cols: &mut Vec<&'a Column>) {
        match self {
            Self::Column(col) => cols.push(col),
            Self::BinaryExpr { left, right, .. } => {
                left.collect_columns(cols);
                right.collect_columns(cols);
            }
            Self::UnaryExpr { expr, .. } => expr.collect_columns(cols),
            Self::IsNull(expr) | Self::IsNotNull(expr) => expr.collect_columns(cols),
            Self::Not(expr) | Self::Negative(expr) => expr.collect_columns(cols),
            Self::Cast { expr, .. } | Self::TryCast { expr, .. } => expr.collect_columns(cols),
            Self::Alias { expr, .. } => expr.collect_columns(cols),
            Self::Case {
                expr,
                when_then_exprs,
                else_expr,
            } => {
                if let Some(e) = expr {
                    e.collect_columns(cols);
                }
                for (when, then) in when_then_exprs {
                    when.collect_columns(cols);
                    then.collect_columns(cols);
                }
                if let Some(e) = else_expr {
                    e.collect_columns(cols);
                }
            }
            Self::Between { expr, low, high, .. } => {
                expr.collect_columns(cols);
                low.collect_columns(cols);
                high.collect_columns(cols);
            }
            Self::Like { expr, pattern, escape, .. } => {
                expr.collect_columns(cols);
                pattern.collect_columns(cols);
                if let Some(e) = escape {
                    e.collect_columns(cols);
                }
            }
            Self::InList { expr, list, .. } => {
                expr.collect_columns(cols);
                for e in list {
                    e.collect_columns(cols);
                }
            }
            Self::ScalarFunction { args, .. } => {
                for arg in args {
                    arg.collect_columns(cols);
                }
            }
            Self::Aggregate(agg) => {
                for arg in &agg.args {
                    arg.collect_columns(cols);
                }
            }
            Self::Window(w) => {
                w.function.collect_columns(cols);
                for e in &w.partition_by {
                    e.collect_columns(cols);
                }
                for s in &w.order_by {
                    s.expr.collect_columns(cols);
                }
            }
            Self::InSubquery { expr, .. } => expr.collect_columns(cols),
            _ => {}
        }
    }
}

impl fmt::Display for LogicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(col) => write!(f, "{}", col),
            Self::Literal(val) => write!(f, "{}", val),
            Self::BinaryExpr { left, op, right } => {
                write!(f, "({} {} {})", left, op, right)
            }
            Self::UnaryExpr { op, expr } => write!(f, "{}{}", op, expr),
            Self::IsNull(expr) => write!(f, "{} IS NULL", expr),
            Self::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            Self::Not(expr) => write!(f, "NOT {}", expr),
            Self::Negative(expr) => write!(f, "-{}", expr),
            Self::Alias { expr, alias } => write!(f, "{} AS {}", expr, alias),
            Self::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Self::Aggregate(agg) => write!(f, "{}", agg),
            Self::ScalarFunction { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Self::Wildcard => write!(f, "*"),
            Self::QualifiedWildcard { qualifier } => write!(f, "{}.*", qualifier),
            _ => write!(f, "expr"),
        }
    }
}

/// Binary operators for logical expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
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
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    // String
    Concat,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::Modulo => write!(f, "%"),
            Self::Concat => write!(f, "||"),
            Self::BitwiseAnd => write!(f, "&"),
            Self::BitwiseOr => write!(f, "|"),
            Self::BitwiseXor => write!(f, "^"),
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Not,
    Negative,
    BitwiseNot,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Not => write!(f, "NOT "),
            Self::Negative => write!(f, "-"),
            Self::BitwiseNot => write!(f, "~"),
        }
    }
}

/// Aggregate function types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    ArrayAgg,
    StringAgg,
    CountDistinct,
    /// Approximate count distinct using HyperLogLog
    ApproxCountDistinct,
    /// Approximate percentile using T-Digest
    ApproxPercentile,
    /// Approximate median using T-Digest
    ApproxMedian,
}

impl fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => write!(f, "COUNT"),
            Self::Sum => write!(f, "SUM"),
            Self::Avg => write!(f, "AVG"),
            Self::Min => write!(f, "MIN"),
            Self::Max => write!(f, "MAX"),
            Self::First => write!(f, "FIRST"),
            Self::Last => write!(f, "LAST"),
            Self::ArrayAgg => write!(f, "ARRAY_AGG"),
            Self::StringAgg => write!(f, "STRING_AGG"),
            Self::CountDistinct => write!(f, "COUNT_DISTINCT"),
            Self::ApproxCountDistinct => write!(f, "APPROX_COUNT_DISTINCT"),
            Self::ApproxPercentile => write!(f, "APPROX_PERCENTILE"),
            Self::ApproxMedian => write!(f, "APPROX_MEDIAN"),
        }
    }
}

/// An aggregate expression.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    /// The aggregate function
    pub func: AggregateFunc,
    /// Arguments to the function
    pub args: Vec<LogicalExpr>,
    /// Whether DISTINCT is specified
    pub distinct: bool,
    /// Optional filter clause
    pub filter: Option<Box<LogicalExpr>>,
}

impl AggregateExpr {
    /// Create a new aggregate expression.
    pub fn new(func: AggregateFunc, args: Vec<LogicalExpr>) -> Self {
        Self {
            func,
            args,
            distinct: false,
            filter: None,
        }
    }

    /// Create a COUNT(*) expression.
    pub fn count_star() -> Self {
        Self::new(AggregateFunc::Count, vec![LogicalExpr::Wildcard])
    }

    /// Create a COUNT(column) expression.
    pub fn count(expr: LogicalExpr) -> Self {
        Self::new(AggregateFunc::Count, vec![expr])
    }

    /// Create a SUM expression.
    pub fn sum(expr: LogicalExpr) -> Self {
        Self::new(AggregateFunc::Sum, vec![expr])
    }

    /// Create an AVG expression.
    pub fn avg(expr: LogicalExpr) -> Self {
        Self::new(AggregateFunc::Avg, vec![expr])
    }

    /// Create a MIN expression.
    pub fn min(expr: LogicalExpr) -> Self {
        Self::new(AggregateFunc::Min, vec![expr])
    }

    /// Create a MAX expression.
    pub fn max(expr: LogicalExpr) -> Self {
        Self::new(AggregateFunc::Max, vec![expr])
    }

    /// Add DISTINCT.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a filter clause.
    pub fn filter(mut self, filter: LogicalExpr) -> Self {
        self.filter = Some(Box::new(filter));
        self
    }

    /// Get the name of this aggregate expression.
    pub fn name(&self) -> String {
        if self.args.is_empty() || matches!(self.args[0], LogicalExpr::Wildcard) {
            format!("{}(*)", self.func)
        } else {
            format!("{}({})", self.func, self.args[0].name())
        }
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.func)?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ")")?;
        if let Some(filter) = &self.filter {
            write!(f, " FILTER (WHERE {})", filter)?;
        }
        Ok(())
    }
}

/// A sort expression.
#[derive(Debug, Clone)]
pub struct SortExpr {
    /// The expression to sort by
    pub expr: LogicalExpr,
    /// Ascending order
    pub asc: bool,
    /// NULLS FIRST
    pub nulls_first: bool,
}

impl SortExpr {
    /// Create a new sort expression.
    pub fn new(expr: LogicalExpr, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }

    /// Create an ascending sort.
    pub fn asc(expr: LogicalExpr) -> Self {
        Self::new(expr, true, false)
    }

    /// Create a descending sort.
    pub fn desc(expr: LogicalExpr) -> Self {
        Self::new(expr, false, true)
    }
}

impl fmt::Display for SortExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " ASC")?;
        } else {
            write!(f, " DESC")?;
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST")?;
        } else {
            write!(f, " NULLS LAST")?;
        }
        Ok(())
    }
}

/// Window frame units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

/// Window frame bound.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    Preceding(Option<u64>),
    Following(Option<u64>),
}

/// Window frame.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::Preceding(None),
            end: WindowFrameBound::CurrentRow,
        }
    }
}

/// A window function expression.
#[derive(Debug, Clone)]
pub struct WindowExpr {
    /// The function (can be aggregate or window-specific)
    pub function: LogicalExpr,
    /// PARTITION BY expressions
    pub partition_by: Vec<LogicalExpr>,
    /// ORDER BY expressions
    pub order_by: Vec<SortExpr>,
    /// Window frame
    pub frame: Option<WindowFrame>,
}

impl WindowExpr {
    /// Create a new window expression.
    pub fn new(
        function: LogicalExpr,
        partition_by: Vec<LogicalExpr>,
        order_by: Vec<SortExpr>,
    ) -> Self {
        Self {
            function,
            partition_by,
            order_by,
            frame: None,
        }
    }
}

impl fmt::Display for WindowExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} OVER (", self.function)?;
        if !self.partition_by.is_empty() {
            write!(f, "PARTITION BY ")?;
            for (i, expr) in self.partition_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "ORDER BY ")?;
            for (i, sort) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", sort)?;
            }
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_expr() {
        let col = LogicalExpr::column("id");
        assert!(col.is_column());
        assert_eq!(col.name(), "id");

        let qual_col = LogicalExpr::qualified_column("users", "id");
        assert_eq!(qual_col.name(), "id");
    }

    #[test]
    fn test_binary_expr() {
        let expr = LogicalExpr::column("a")
            .gt(LogicalExpr::literal(10i32))
            .and(LogicalExpr::column("b").lt(LogicalExpr::literal(20i32)));

        let cols = expr.columns();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_aggregate_expr() {
        let agg = AggregateExpr::count_star();
        assert_eq!(format!("{}", agg), "COUNT(*)");

        let sum = AggregateExpr::sum(LogicalExpr::column("amount")).distinct();
        assert_eq!(format!("{}", sum), "SUM(DISTINCT amount)");
    }
}
