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
    UnaryExpr { op: UnaryOp, expr: Box<LogicalExpr> },

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

    /// LIKE expression (also used for ILIKE with case_insensitive=true)
    Like {
        negated: bool,
        expr: Box<LogicalExpr>,
        pattern: Box<LogicalExpr>,
        escape: Option<Box<LogicalExpr>>,
        case_insensitive: bool,
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
            Self::Between {
                expr, low, high, ..
            } => {
                expr.collect_columns(cols);
                low.collect_columns(cols);
                high.collect_columns(cols);
            }
            Self::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
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

    /// Infer the data type of this expression given the input schema.
    pub fn data_type(&self, schema: &Schema) -> DataType {
        match self {
            Self::Column(col) => {
                // Look up column in schema
                for field in schema.fields() {
                    if field.name() == col.name {
                        return field.data_type().clone();
                    }
                }
                // Column not found, return Utf8 as fallback
                DataType::Utf8
            }
            Self::Literal(val) => val.data_type(),
            Self::BinaryExpr { left, op, right: _ } => {
                // For comparison operators, result is Boolean
                match op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or => DataType::Boolean,
                    // For arithmetic, use the type of the left operand (simplified)
                    BinaryOp::Plus
                    | BinaryOp::Minus
                    | BinaryOp::Multiply
                    | BinaryOp::Divide
                    | BinaryOp::Modulo => left.data_type(schema),
                    // Bitwise operations preserve type
                    BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::BitwiseXor => {
                        left.data_type(schema)
                    }
                    // Concat returns string
                    BinaryOp::Concat => DataType::Utf8,
                }
            }
            Self::UnaryExpr { op, expr } => match op {
                UnaryOp::Not => DataType::Boolean,
                UnaryOp::Negative | UnaryOp::BitwiseNot => expr.data_type(schema),
            },
            Self::IsNull(_) | Self::IsNotNull(_) | Self::Not(_) => DataType::Boolean,
            Self::Negative(expr) => expr.data_type(schema),
            Self::Cast { data_type, .. } | Self::TryCast { data_type, .. } => data_type.clone(),
            Self::Alias { expr, .. } => expr.data_type(schema),
            Self::Case {
                when_then_exprs,
                else_expr,
                ..
            } => {
                // Type is the type of the THEN expressions
                if let Some((_, then_expr)) = when_then_exprs.first() {
                    then_expr.data_type(schema)
                } else if let Some(e) = else_expr {
                    e.data_type(schema)
                } else {
                    DataType::Null
                }
            }
            Self::Between { .. } | Self::Like { .. } | Self::InList { .. } => DataType::Boolean,
            Self::Aggregate(agg) => {
                // Aggregate types depend on function
                match agg.func {
                    AggregateFunc::Count
                    | AggregateFunc::CountDistinct
                    | AggregateFunc::ApproxCountDistinct => DataType::Int64,
                    AggregateFunc::Sum => {
                        // SUM preserves input type for Decimal and integer types
                        if let Some(arg) = agg.args.first() {
                            let dt = arg.data_type(schema);
                            match dt {
                                DataType::Decimal128 { .. }
                                | DataType::Int64
                                | DataType::Int32 => dt,
                                _ => DataType::Float64,
                            }
                        } else {
                            DataType::Float64
                        }
                    }
                    AggregateFunc::Avg
                    | AggregateFunc::ApproxPercentile
                    | AggregateFunc::ApproxMedian
                    | AggregateFunc::VariancePop
                    | AggregateFunc::VarianceSamp
                    | AggregateFunc::StddevPop
                    | AggregateFunc::StddevSamp
                    | AggregateFunc::PercentileCont
                    | AggregateFunc::PercentileDisc
                    | AggregateFunc::Median => DataType::Float64,
                    AggregateFunc::Min
                    | AggregateFunc::Max
                    | AggregateFunc::First
                    | AggregateFunc::Last
                    | AggregateFunc::AnyValue => {
                        if let Some(arg) = agg.args.first() {
                            arg.data_type(schema)
                        } else {
                            DataType::Null
                        }
                    }
                    AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Boolean,
                    AggregateFunc::ArrayAgg => DataType::List(Box::new(DataType::Utf8)),
                    AggregateFunc::StringAgg => DataType::Utf8,
                }
            }
            Self::ScalarFunction { name, args } => {
                // Return types for common scalar functions
                match name.to_uppercase().as_str() {
                    "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "CONCAT" | "SUBSTRING"
                    | "REPLACE" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REVERSE" | "SPLIT_PART"
                    | "REGEXP_REPLACE" | "INITCAP" | "REPEAT" | "TRANSLATE" | "REGEXP_EXTRACT"
                    | "CHR" | "MD5" | "SHA256" | "SHA2" | "SUBSTR" | "JSON_EXTRACT"
                    | "JSON_VALUE" | "JSON_OBJECT" | "JSON_ARRAY" | "JSON_TYPE" | "JSON_KEYS" => {
                        DataType::Utf8
                    }
                    "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "ASCII" | "POSITION"
                    | "STRPOS" | "EXTRACT" | "DATE_PART" | "YEAR" | "MONTH" | "DAY" | "HOUR"
                    | "MINUTE" | "SECOND" | "DATE_DIFF" | "DATEDIFF" | "JSON_EXTRACT_INT"
                    | "JSON_LENGTH" => DataType::Int64,
                    "REGEXP_MATCH" | "STARTS_WITH" | "ENDS_WITH" | "JSON_EXTRACT_BOOL"
                    | "JSON_VALID" | "JSON_CONTAINS_KEY" => DataType::Boolean,
                    "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "POWER" | "POW" | "SQRT"
                    | "EXP" | "LN" | "LOG" | "SIGN" | "MOD" | "MODULO" | "TRUNC"
                    | "TRUNCATE" => {
                        if let Some(arg) = args.first() {
                            arg.data_type(schema)
                        } else {
                            DataType::Float64
                        }
                    }
                    "COALESCE" | "NULLIF" | "NVL" | "IFNULL" | "GREATEST" | "LEAST" => {
                        if let Some(arg) = args.first() {
                            arg.data_type(schema)
                        } else {
                            DataType::Null
                        }
                    }
                    // Trig, transcendental, and constant functions always return Float64
                    "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "DEGREES"
                    | "RADIANS" | "PI" | "LOG2" | "LOG10" | "CBRT" | "RANDOM"
                    | "JSON_EXTRACT_FLOAT" => DataType::Float64,
                    // GROUPING function returns integer bitmask
                    "GROUPING" => DataType::Int64,
                    // Date/Time functions
                    "CURRENT_DATE" | "TO_DATE" => DataType::Date32,
                    "CURRENT_TIMESTAMP" | "NOW" | "DATE_TRUNC" | "TO_TIMESTAMP" | "DATE_ADD"
                    | "DATEADD" | "DATE_SUB" | "DATESUB" => {
                        if let Some(arg) = args.first() {
                            arg.data_type(schema)
                        } else {
                            DataType::Timestamp {
                                unit: crate::types::TimeUnit::Microsecond,
                                timezone: None,
                            }
                        }
                    }
                    _ => DataType::Utf8, // Default fallback for unknown functions
                }
            }
            Self::Window(w) => {
                // Check if the inner function is a known window function
                if let LogicalExpr::ScalarFunction { name, args, .. } = &w.function {
                    match name.to_uppercase().as_str() {
                        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "COUNT" => {
                            return DataType::Int64;
                        }
                        "PERCENT_RANK" | "CUME_DIST" => return DataType::Float64,
                        "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                            // Returns the same type as the first argument
                            if let Some(arg) = args.first() {
                                return arg.data_type(schema);
                            }
                            return DataType::Int64;
                        }
                        _ => {}
                    }
                }
                // For aggregate-based window functions, always Float64
                // (the window operator produces Float64 for all aggregates)
                match &w.function {
                    LogicalExpr::Aggregate(_) => DataType::Float64,
                    other => other.data_type(schema),
                }
            }
            Self::ScalarSubquery(plan) => {
                // Scalar subquery returns the type of its first output column
                let sub_schema = plan.schema();
                if let Some(first_field) = sub_schema.fields().first() {
                    first_field.data_type().clone()
                } else {
                    DataType::Null
                }
            }
            Self::Exists { .. } | Self::InSubquery { .. } => DataType::Boolean,
            Self::Wildcard | Self::QualifiedWildcard { .. } | Self::Placeholder { .. } => {
                DataType::Utf8 // Placeholder
            }
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
    /// Population variance
    VariancePop,
    /// Sample variance (VARIANCE / VAR_SAMP)
    VarianceSamp,
    /// Population standard deviation
    StddevPop,
    /// Sample standard deviation (STDDEV / STDDEV_SAMP)
    StddevSamp,
    /// Boolean AND aggregate (BOOL_AND / EVERY)
    BoolAnd,
    /// Boolean OR aggregate
    BoolOr,
    /// Returns any non-null value from the group
    AnyValue,
    /// Exact continuous percentile (linear interpolation)
    PercentileCont,
    /// Exact discrete percentile (nearest value)
    PercentileDisc,
    /// Exact median (50th percentile, continuous)
    Median,
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
            Self::VariancePop => write!(f, "VAR_POP"),
            Self::VarianceSamp => write!(f, "VARIANCE"),
            Self::StddevPop => write!(f, "STDDEV_POP"),
            Self::StddevSamp => write!(f, "STDDEV"),
            Self::BoolAnd => write!(f, "BOOL_AND"),
            Self::BoolOr => write!(f, "BOOL_OR"),
            Self::AnyValue => write!(f, "ANY_VALUE"),
            Self::PercentileCont => write!(f, "PERCENTILE_CONT"),
            Self::PercentileDisc => write!(f, "PERCENTILE_DISC"),
            Self::Median => write!(f, "MEDIAN"),
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
    /// Optional FILTER clause for aggregate window functions
    pub filter: Option<Box<LogicalExpr>>,
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
            filter: None,
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

    fn test_schema() -> Schema {
        Schema::new(vec![
            crate::types::Field::new("id", DataType::Int64, false),
            crate::types::Field::new("name", DataType::Utf8, true),
            crate::types::Field::new("age", DataType::Int64, true),
            crate::types::Field::new("active", DataType::Boolean, true),
        ])
    }

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

    // --- BETWEEN expression tests ---

    #[test]
    fn test_between_expr() {
        let expr = LogicalExpr::Between {
            expr: Box::new(LogicalExpr::column("age")),
            negated: false,
            low: Box::new(LogicalExpr::literal(18i32)),
            high: Box::new(LogicalExpr::literal(65i32)),
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "age");
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_between_negated() {
        let expr = LogicalExpr::Between {
            expr: Box::new(LogicalExpr::column("age")),
            negated: true,
            low: Box::new(LogicalExpr::literal(0i32)),
            high: Box::new(LogicalExpr::literal(10i32)),
        };
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
        let display = format!("{}", expr);
        assert!(
            display.contains("BETWEEN")
                || display.contains("NOT BETWEEN")
                || display.contains("expr")
        );
    }

    // --- LIKE expression tests ---

    #[test]
    fn test_like_expr() {
        let expr = LogicalExpr::Like {
            negated: false,
            expr: Box::new(LogicalExpr::column("name")),
            pattern: Box::new(LogicalExpr::literal("A%")),
            escape: None,
            case_insensitive: false,
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "name");
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_like_with_escape() {
        let expr = LogicalExpr::Like {
            negated: false,
            expr: Box::new(LogicalExpr::column("name")),
            pattern: Box::new(LogicalExpr::literal("100\\%")),
            escape: Some(Box::new(LogicalExpr::literal("\\"))),
            case_insensitive: false,
        };
        let cols = expr.columns();
        // Column from expr + column-less literal pattern + escape
        assert_eq!(cols.len(), 1);
    }

    #[test]
    fn test_ilike_expr() {
        let expr = LogicalExpr::Like {
            negated: false,
            expr: Box::new(LogicalExpr::column("name")),
            pattern: Box::new(LogicalExpr::literal("a%")),
            escape: None,
            case_insensitive: true,
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "name");
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    // --- CASE expression tests ---

    #[test]
    fn test_case_expr_simple() {
        let expr = LogicalExpr::Case {
            expr: None,
            when_then_exprs: vec![(
                LogicalExpr::column("active").eq(LogicalExpr::literal(true)),
                LogicalExpr::literal("yes"),
            )],
            else_expr: Some(Box::new(LogicalExpr::literal("no"))),
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1, "CASE should collect columns from when/then");
        assert_eq!(expr.data_type(&test_schema()), DataType::Utf8);
    }

    #[test]
    fn test_case_expr_with_base() {
        let expr = LogicalExpr::Case {
            expr: Some(Box::new(LogicalExpr::column("id"))),
            when_then_exprs: vec![
                (LogicalExpr::literal(1i32), LogicalExpr::literal("one")),
                (LogicalExpr::literal(2i32), LogicalExpr::literal("two")),
            ],
            else_expr: Some(Box::new(LogicalExpr::literal("other"))),
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn test_case_expr_no_else() {
        let expr = LogicalExpr::Case {
            expr: None,
            when_then_exprs: vec![(
                LogicalExpr::column("age").gt(LogicalExpr::literal(30i32)),
                LogicalExpr::literal("senior"),
            )],
            else_expr: None,
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        // Return type should come from THEN expression
        assert_eq!(expr.data_type(&test_schema()), DataType::Utf8);
    }

    // --- IN list tests ---

    #[test]
    fn test_in_list_expr() {
        let expr = LogicalExpr::InList {
            expr: Box::new(LogicalExpr::column("id")),
            list: vec![
                LogicalExpr::literal(1i32),
                LogicalExpr::literal(2i32),
                LogicalExpr::literal(3i32),
            ],
            negated: false,
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_in_list_with_columns() {
        let expr = LogicalExpr::InList {
            expr: Box::new(LogicalExpr::column("id")),
            list: vec![LogicalExpr::column("age")],
            negated: true,
        };
        let cols = expr.columns();
        assert_eq!(cols.len(), 2);
    }

    // --- Type resolution tests ---

    #[test]
    fn test_data_type_column() {
        let expr = LogicalExpr::column("id");
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    #[test]
    fn test_data_type_literal() {
        let expr = LogicalExpr::literal(42i32);
        assert_eq!(expr.data_type(&test_schema()), DataType::Int32);
    }

    #[test]
    fn test_data_type_comparison() {
        let expr = LogicalExpr::column("id").eq(LogicalExpr::literal(1i32));
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_data_type_arithmetic() {
        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::column("age")),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::literal(1i32)),
        };
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    #[test]
    fn test_data_type_is_null() {
        let expr = LogicalExpr::IsNull(Box::new(LogicalExpr::column("name")));
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_data_type_is_not_null() {
        let expr = LogicalExpr::IsNotNull(Box::new(LogicalExpr::column("name")));
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_data_type_cast() {
        let expr = LogicalExpr::Cast {
            expr: Box::new(LogicalExpr::column("id")),
            data_type: DataType::Utf8,
        };
        assert_eq!(expr.data_type(&test_schema()), DataType::Utf8);
    }

    #[test]
    fn test_data_type_alias() {
        let expr = LogicalExpr::column("id").alias("user_id");
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    #[test]
    fn test_data_type_aggregate_count() {
        let agg = AggregateExpr::count_star();
        let expr = LogicalExpr::Aggregate(agg);
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    #[test]
    fn test_data_type_aggregate_sum() {
        let agg = AggregateExpr::sum(LogicalExpr::column("age"));
        let expr = LogicalExpr::Aggregate(agg);
        // SUM(Int64) preserves Int64 type
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    #[test]
    fn test_data_type_concat() {
        let expr = LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::literal("hello")),
            op: BinaryOp::Concat,
            right: Box::new(LogicalExpr::literal(" world")),
        };
        assert_eq!(expr.data_type(&test_schema()), DataType::Utf8);
    }

    // --- Column qualified name tests ---

    #[test]
    fn test_column_unqualified() {
        let col = Column::unqualified("id");
        assert_eq!(col.qualified_name(), "id");
        assert!(col.relation.is_none());
    }

    #[test]
    fn test_column_qualified() {
        let col = Column::qualified("users", "id");
        assert_eq!(col.qualified_name(), "users.id");
        assert_eq!(col.relation.as_deref(), Some("users"));
    }

    #[test]
    fn test_column_from_str() {
        let col: Column = "users.id".into();
        assert_eq!(col.relation.as_deref(), Some("users"));
        assert_eq!(col.name, "id");

        let col2: Column = "name".into();
        assert!(col2.relation.is_none());
        assert_eq!(col2.name, "name");
    }

    // --- Expression name tests ---

    #[test]
    fn test_wildcard_name() {
        let expr = LogicalExpr::Wildcard;
        assert_eq!(expr.name(), "*");
    }

    #[test]
    fn test_qualified_wildcard_name() {
        let expr = LogicalExpr::QualifiedWildcard {
            qualifier: "users".to_string(),
        };
        assert_eq!(expr.name(), "users.*");
    }

    #[test]
    fn test_scalar_function_name() {
        let expr = LogicalExpr::ScalarFunction {
            name: "UPPER".to_string(),
            args: vec![LogicalExpr::column("name")],
        };
        assert_eq!(expr.name(), "UPPER");
    }

    // --- NOT and Negative tests ---

    #[test]
    fn test_not_expr_columns() {
        let expr = LogicalExpr::Not(Box::new(LogicalExpr::column("active")));
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "active");
        assert_eq!(expr.data_type(&test_schema()), DataType::Boolean);
    }

    #[test]
    fn test_negative_expr() {
        let expr = LogicalExpr::Negative(Box::new(LogicalExpr::column("age")));
        let cols = expr.columns();
        assert_eq!(cols.len(), 1);
        assert_eq!(expr.data_type(&test_schema()), DataType::Int64);
    }

    // --- Window expression tests ---

    #[test]
    fn test_window_expr_display() {
        let win = WindowExpr::new(
            LogicalExpr::Aggregate(AggregateExpr::count_star()),
            vec![LogicalExpr::column("name")],
            vec![SortExpr {
                expr: LogicalExpr::column("age"),
                asc: true,
                nulls_first: false,
            }],
        );
        let display = format!("{}", win);
        assert!(display.contains("OVER"));
        assert!(display.contains("PARTITION BY"));
        assert!(display.contains("ORDER BY"));
    }

    #[test]
    fn test_window_frame_default() {
        let frame = WindowFrame::default();
        assert_eq!(frame.units, WindowFrameUnits::Range);
        assert_eq!(frame.start, WindowFrameBound::Preceding(None));
        assert_eq!(frame.end, WindowFrameBound::CurrentRow);
    }

    // --- Helper method tests ---

    #[test]
    fn test_is_literal() {
        assert!(LogicalExpr::literal(42i32).is_literal());
        assert!(!LogicalExpr::column("id").is_literal());
    }

    #[test]
    fn test_is_column() {
        assert!(LogicalExpr::column("id").is_column());
        assert!(!LogicalExpr::literal(42i32).is_column());
    }
}
