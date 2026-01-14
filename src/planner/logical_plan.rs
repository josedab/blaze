//! Logical plan representation for Blaze.

use std::fmt;
use std::sync::{Arc, LazyLock};

use crate::catalog::ResolvedTableRef;
use crate::error::Result;
use crate::types::Schema;

use super::logical_expr::{AggregateExpr, AggregateFunc, LogicalExpr, SortExpr};

/// Time travel specification for Delta Lake tables.
#[derive(Debug, Clone)]
pub enum TimeTravelSpec {
    /// Travel to a specific version number
    Version(i64),
    /// Travel to a specific timestamp
    Timestamp(chrono::DateTime<chrono::Utc>),
}

/// Static empty schema for plans that don't produce rows.
static EMPTY_SCHEMA: LazyLock<Schema> = LazyLock::new(Schema::empty);

/// Join type for logical plans.
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

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "Inner"),
            JoinType::Left => write!(f, "Left"),
            JoinType::Right => write!(f, "Right"),
            JoinType::Full => write!(f, "Full"),
            JoinType::Cross => write!(f, "Cross"),
            JoinType::LeftSemi => write!(f, "LeftSemi"),
            JoinType::RightSemi => write!(f, "RightSemi"),
            JoinType::LeftAnti => write!(f, "LeftAnti"),
            JoinType::RightAnti => write!(f, "RightAnti"),
        }
    }
}

/// Set operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperation {
    Union,
    Intersect,
    Except,
}

/// A logical query plan.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan
    TableScan {
        /// Table reference
        table_ref: ResolvedTableRef,
        /// Projected columns (None = all)
        projection: Option<Vec<usize>>,
        /// Filters to push down
        filters: Vec<LogicalExpr>,
        /// Output schema
        schema: Schema,
        /// Time travel specification (for Delta Lake)
        time_travel: Option<TimeTravelSpec>,
    },

    /// Projection (SELECT expressions)
    Projection {
        /// The expressions to project
        exprs: Vec<LogicalExpr>,
        /// Input plan
        input: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// Filter (WHERE clause)
    Filter {
        /// Filter predicate
        predicate: LogicalExpr,
        /// Input plan
        input: Arc<LogicalPlan>,
    },

    /// Aggregate (GROUP BY)
    Aggregate {
        /// Grouping expressions
        group_by: Vec<LogicalExpr>,
        /// Aggregate expressions
        aggr_exprs: Vec<AggregateExpr>,
        /// Input plan
        input: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// Sort (ORDER BY)
    Sort {
        /// Sort expressions
        exprs: Vec<SortExpr>,
        /// Input plan
        input: Arc<LogicalPlan>,
    },

    /// Limit
    Limit {
        /// Number of rows to skip
        skip: usize,
        /// Number of rows to return
        fetch: Option<usize>,
        /// Input plan
        input: Arc<LogicalPlan>,
    },

    /// Join
    Join {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// Join type
        join_type: JoinType,
        /// Join condition
        on: Vec<(LogicalExpr, LogicalExpr)>,
        /// Additional filter
        filter: Option<LogicalExpr>,
        /// Output schema
        schema: Schema,
    },

    /// Cross join
    CrossJoin {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// Set operations (UNION, INTERSECT, EXCEPT)
    SetOperation {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// Set operation type
        op: SetOperation,
        /// ALL modifier
        all: bool,
        /// Output schema
        schema: Schema,
    },

    /// Subquery alias
    SubqueryAlias {
        /// Alias name
        alias: String,
        /// Input plan
        input: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// Distinct
    Distinct {
        /// Input plan
        input: Arc<LogicalPlan>,
    },

    /// Window function
    Window {
        /// Window expressions
        window_exprs: Vec<LogicalExpr>,
        /// Input plan
        input: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// Values (inline data)
    Values {
        /// Schema
        schema: Schema,
        /// Values
        values: Vec<Vec<LogicalExpr>>,
    },

    /// Empty relation (no rows)
    EmptyRelation {
        /// Whether to produce a single row with NULLs
        produce_one_row: bool,
        /// Schema
        schema: Schema,
    },

    /// Explain
    Explain {
        /// Plan to explain
        plan: Arc<LogicalPlan>,
        /// Verbose mode
        verbose: bool,
    },

    /// Explain Analyze - executes and shows runtime statistics
    ExplainAnalyze {
        /// Plan to explain and execute
        plan: Arc<LogicalPlan>,
        /// Verbose mode
        verbose: bool,
    },

    /// DDL: Create Table
    CreateTable {
        /// Table name
        name: ResolvedTableRef,
        /// Schema
        schema: Schema,
        /// If not exists
        if_not_exists: bool,
    },

    /// DDL: Drop Table
    DropTable {
        /// Table name
        name: ResolvedTableRef,
        /// If exists
        if_exists: bool,
    },

    /// DML: Insert
    Insert {
        /// Target table
        table_ref: ResolvedTableRef,
        /// Source plan
        input: Arc<LogicalPlan>,
        /// Output schema
        schema: Schema,
    },

    /// DML: Delete
    Delete {
        /// Target table
        table_ref: ResolvedTableRef,
        /// Filter predicate
        predicate: Option<LogicalExpr>,
        /// Schema
        schema: Schema,
    },

    /// DML: Update
    Update {
        /// Target table
        table_ref: ResolvedTableRef,
        /// Assignments
        assignments: Vec<(String, LogicalExpr)>,
        /// Filter predicate
        predicate: Option<LogicalExpr>,
        /// Schema
        schema: Schema,
    },
}

impl LogicalPlan {
    /// Get the output schema of this plan.
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::TableScan { schema, .. } => schema,
            LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::CrossJoin { schema, .. } => schema,
            LogicalPlan::SetOperation { schema, .. } => schema,
            LogicalPlan::SubqueryAlias { schema, .. } => schema,
            LogicalPlan::Distinct { input } => input.schema(),
            LogicalPlan::Window { schema, .. } => schema,
            LogicalPlan::Values { schema, .. } => schema,
            LogicalPlan::EmptyRelation { schema, .. } => schema,
            LogicalPlan::Explain { .. } => &EMPTY_SCHEMA,
            LogicalPlan::ExplainAnalyze { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Insert { schema, .. } => schema,
            LogicalPlan::Delete { schema, .. } => schema,
            LogicalPlan::Update { schema, .. } => schema,
        }
    }

    /// Get the child plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan { .. } => vec![],
            LogicalPlan::Projection { input, .. } => vec![input.as_ref()],
            LogicalPlan::Filter { input, .. } => vec![input.as_ref()],
            LogicalPlan::Aggregate { input, .. } => vec![input.as_ref()],
            LogicalPlan::Sort { input, .. } => vec![input.as_ref()],
            LogicalPlan::Limit { input, .. } => vec![input.as_ref()],
            LogicalPlan::Join { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            LogicalPlan::CrossJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            LogicalPlan::SetOperation { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            LogicalPlan::SubqueryAlias { input, .. } => vec![input.as_ref()],
            LogicalPlan::Distinct { input } => vec![input.as_ref()],
            LogicalPlan::Window { input, .. } => vec![input.as_ref()],
            LogicalPlan::Values { .. } => vec![],
            LogicalPlan::EmptyRelation { .. } => vec![],
            LogicalPlan::Explain { plan, .. } => vec![plan.as_ref()],
            LogicalPlan::ExplainAnalyze { plan, .. } => vec![plan.as_ref()],
            LogicalPlan::CreateTable { .. } => vec![],
            LogicalPlan::DropTable { .. } => vec![],
            LogicalPlan::Insert { input, .. } => vec![input.as_ref()],
            LogicalPlan::Delete { .. } => vec![],
            LogicalPlan::Update { .. } => vec![],
        }
    }

    /// Display the plan with indentation.
    pub fn display_indent(&self, indent: usize) -> String {
        let mut result = String::new();
        self.format_indent(&mut result, indent);
        result
    }

    fn format_indent(&self, f: &mut String, indent: usize) {
        let prefix = "  ".repeat(indent);
        match self {
            LogicalPlan::TableScan {
                table_ref,
                projection,
                filters,
                ..
            } => {
                f.push_str(&format!("{}TableScan: {}", prefix, table_ref));
                if let Some(proj) = projection {
                    f.push_str(&format!(" projection=[{:?}]", proj));
                }
                if !filters.is_empty() {
                    f.push_str(&format!(" filters=[{}]", filters.len()));
                }
                f.push('\n');
            }
            LogicalPlan::Projection { exprs, input, .. } => {
                f.push_str(&format!(
                    "{}Projection: {}\n",
                    prefix,
                    exprs
                        .iter()
                        .map(|e| e.name())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Filter { predicate, input } => {
                f.push_str(&format!("{}Filter: {}\n", prefix, predicate));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                input,
                ..
            } => {
                f.push_str(&format!(
                    "{}Aggregate: groupBy=[{}], aggrs=[{}]\n",
                    prefix,
                    group_by.iter().map(|e| e.name()).collect::<Vec<_>>().join(", "),
                    aggr_exprs.iter().map(|a| a.name()).collect::<Vec<_>>().join(", ")
                ));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Sort { exprs, input } => {
                f.push_str(&format!(
                    "{}Sort: [{}]\n",
                    prefix,
                    exprs.iter().map(|s| format!("{}", s)).collect::<Vec<_>>().join(", ")
                ));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Limit { skip, fetch, input } => {
                f.push_str(&format!("{}Limit: skip={}, fetch={:?}\n", prefix, skip, fetch));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
                ..
            } => {
                f.push_str(&format!(
                    "{}{}Join: on=[{}]\n",
                    prefix,
                    join_type,
                    on.iter()
                        .map(|(l, r)| format!("{} = {}", l, r))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                f.push_str(&format!("{}CrossJoin\n", prefix));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            LogicalPlan::SetOperation { left, right, op, all, .. } => {
                f.push_str(&format!(
                    "{}{:?}{}\n",
                    prefix,
                    op,
                    if *all { " ALL" } else { "" }
                ));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            LogicalPlan::SubqueryAlias { alias, input, .. } => {
                f.push_str(&format!("{}SubqueryAlias: {}\n", prefix, alias));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Distinct { input } => {
                f.push_str(&format!("{}Distinct\n", prefix));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Window { window_exprs, input, .. } => {
                f.push_str(&format!(
                    "{}Window: [{}]\n",
                    prefix,
                    window_exprs.iter().map(|e| e.name()).collect::<Vec<_>>().join(", ")
                ));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Values { values, .. } => {
                f.push_str(&format!("{}Values: {} row(s)\n", prefix, values.len()));
            }
            LogicalPlan::EmptyRelation { produce_one_row, .. } => {
                f.push_str(&format!(
                    "{}EmptyRelation: produce_one_row={}\n",
                    prefix, produce_one_row
                ));
            }
            LogicalPlan::Explain { plan, verbose } => {
                f.push_str(&format!("{}Explain: verbose={}\n", prefix, verbose));
                plan.format_indent(f, indent + 1);
            }
            LogicalPlan::ExplainAnalyze { plan, verbose } => {
                f.push_str(&format!("{}ExplainAnalyze: verbose={}\n", prefix, verbose));
                plan.format_indent(f, indent + 1);
            }
            LogicalPlan::CreateTable { name, if_not_exists, .. } => {
                f.push_str(&format!(
                    "{}CreateTable: {} if_not_exists={}\n",
                    prefix, name, if_not_exists
                ));
            }
            LogicalPlan::DropTable { name, if_exists } => {
                f.push_str(&format!(
                    "{}DropTable: {} if_exists={}\n",
                    prefix, name, if_exists
                ));
            }
            LogicalPlan::Insert { table_ref, input, .. } => {
                f.push_str(&format!("{}Insert: {}\n", prefix, table_ref));
                input.format_indent(f, indent + 1);
            }
            LogicalPlan::Delete { table_ref, predicate, .. } => {
                f.push_str(&format!("{}Delete: {}", prefix, table_ref));
                if let Some(p) = predicate {
                    f.push_str(&format!(" where={}", p));
                }
                f.push('\n');
            }
            LogicalPlan::Update { table_ref, assignments, predicate, .. } => {
                f.push_str(&format!(
                    "{}Update: {} set=[{}]",
                    prefix,
                    table_ref,
                    assignments
                        .iter()
                        .map(|(c, e)| format!("{} = {}", c, e))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                if let Some(p) = predicate {
                    f.push_str(&format!(" where={}", p));
                }
                f.push('\n');
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_indent(0))
    }
}

/// Builder for creating logical plans.
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan.
    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    /// Create a table scan.
    pub fn scan(table_ref: ResolvedTableRef, schema: Schema) -> Self {
        Self::scan_with_time_travel(table_ref, schema, None)
    }

    /// Create a table scan with time travel specification.
    pub fn scan_with_time_travel(
        table_ref: ResolvedTableRef,
        schema: Schema,
        time_travel: Option<TimeTravelSpec>,
    ) -> Self {
        Self {
            plan: LogicalPlan::TableScan {
                table_ref,
                projection: None,
                filters: vec![],
                schema,
                time_travel,
            },
        }
    }

    /// Create an empty relation.
    pub fn empty(produce_one_row: bool) -> Self {
        Self {
            plan: LogicalPlan::EmptyRelation {
                produce_one_row,
                schema: Schema::empty(),
            },
        }
    }

    /// Create a values plan.
    pub fn values(schema: Schema, values: Vec<Vec<LogicalExpr>>) -> Self {
        Self {
            plan: LogicalPlan::Values { schema, values },
        }
    }

    /// Add a projection.
    pub fn project(self, exprs: Vec<LogicalExpr>) -> Result<Self> {
        // Derive schema from expressions
        let schema = self.derive_projection_schema(&exprs)?;
        Ok(Self {
            plan: LogicalPlan::Projection {
                exprs,
                input: Arc::new(self.plan),
                schema,
            },
        })
    }

    /// Add a filter.
    pub fn filter(self, predicate: LogicalExpr) -> Self {
        Self {
            plan: LogicalPlan::Filter {
                predicate,
                input: Arc::new(self.plan),
            },
        }
    }

    /// Add an aggregate.
    pub fn aggregate(
        self,
        group_by: Vec<LogicalExpr>,
        aggr_exprs: Vec<AggregateExpr>,
    ) -> Result<Self> {
        let schema = self.derive_aggregate_schema(&group_by, &aggr_exprs)?;
        Ok(Self {
            plan: LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                input: Arc::new(self.plan),
                schema,
            },
        })
    }

    /// Add a sort.
    pub fn sort(self, exprs: Vec<SortExpr>) -> Self {
        Self {
            plan: LogicalPlan::Sort {
                exprs,
                input: Arc::new(self.plan),
            },
        }
    }

    /// Add a limit.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Self {
        Self {
            plan: LogicalPlan::Limit {
                skip,
                fetch,
                input: Arc::new(self.plan),
            },
        }
    }

    /// Add a join.
    pub fn join(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        on: Vec<(LogicalExpr, LogicalExpr)>,
    ) -> Result<Self> {
        let schema = self.derive_join_schema(&right, join_type)?;
        Ok(Self {
            plan: LogicalPlan::Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                join_type,
                on,
                filter: None,
                schema,
            },
        })
    }

    /// Add a cross join.
    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let schema = self.plan.schema().merge(right.schema());
        Ok(Self {
            plan: LogicalPlan::CrossJoin {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                schema,
            },
        })
    }

    /// Add a distinct operation.
    pub fn distinct(self) -> Self {
        Self {
            plan: LogicalPlan::Distinct {
                input: Arc::new(self.plan),
            },
        }
    }

    /// Add a subquery alias.
    pub fn alias(self, alias: impl Into<String>) -> Self {
        let schema = self.plan.schema().clone();
        Self {
            plan: LogicalPlan::SubqueryAlias {
                alias: alias.into(),
                input: Arc::new(self.plan),
                schema,
            },
        }
    }

    /// Build the final plan.
    pub fn build(self) -> LogicalPlan {
        self.plan
    }

    // Schema derivation helpers
    fn derive_projection_schema(&self, exprs: &[LogicalExpr]) -> Result<Schema> {
        use crate::types::Field;

        let input_schema = self.plan.schema();
        let fields: Vec<Field> = exprs
            .iter()
            .map(|e| {
                let data_type = e.data_type(input_schema);
                Field::new(e.name(), data_type, true)
            })
            .collect();
        Ok(Schema::new(fields))
    }

    fn derive_aggregate_schema(
        &self,
        group_by: &[LogicalExpr],
        aggr_exprs: &[AggregateExpr],
    ) -> Result<Schema> {
        use crate::types::{DataType, Field};

        let input_schema = self.plan.schema();
        let mut fields: Vec<Field> = group_by
            .iter()
            .map(|e| {
                let data_type = e.data_type(input_schema);
                Field::new(e.name(), data_type, true)
            })
            .collect();

        for agg in aggr_exprs {
            // Determine type based on aggregate function
            let data_type = match agg.func {
                AggregateFunc::Count | AggregateFunc::CountDistinct |
                AggregateFunc::ApproxCountDistinct => DataType::Int64,
                AggregateFunc::Sum | AggregateFunc::Avg |
                AggregateFunc::ApproxPercentile | AggregateFunc::ApproxMedian => DataType::Float64,
                AggregateFunc::Min | AggregateFunc::Max |
                AggregateFunc::First | AggregateFunc::Last => {
                    if let Some(arg) = agg.args.first() {
                        arg.data_type(input_schema)
                    } else {
                        DataType::Float64
                    }
                }
                AggregateFunc::ArrayAgg | AggregateFunc::StringAgg => DataType::Utf8,
            };
            fields.push(Field::new(agg.name(), data_type, true));
        }

        Ok(Schema::new(fields))
    }

    fn derive_join_schema(&self, right: &LogicalPlan, _join_type: JoinType) -> Result<Schema> {
        Ok(self.plan.schema().merge(right.schema()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};

    #[test]
    fn test_logical_plan_builder() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let table_ref = ResolvedTableRef::new("default", "main", "users");

        let plan = LogicalPlanBuilder::scan(table_ref, schema)
            .filter(LogicalExpr::column("id").gt(LogicalExpr::literal(10i32)))
            .project(vec![LogicalExpr::column("name")])
            .unwrap()
            .limit(0, Some(10))
            .build();

        let display = format!("{}", plan);
        assert!(display.contains("Limit"));
        assert!(display.contains("Projection"));
        assert!(display.contains("Filter"));
        assert!(display.contains("TableScan"));
    }
}
