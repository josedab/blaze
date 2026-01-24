//! Physical plan representation for execution.

use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;

use super::physical_expr::PhysicalExpr;
use super::logical_expr::AggregateFunc;
use super::logical_plan::{JoinType, TimeTravelSpec};

/// A physical plan node that can be executed.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Table scan
    Scan {
        /// Table name
        table_name: String,
        /// Projected columns (indices)
        projection: Option<Vec<usize>>,
        /// Output schema
        schema: Arc<ArrowSchema>,
        /// Filter expressions to push down
        filters: Vec<Arc<dyn PhysicalExpr>>,
        /// Time travel specification (for Delta Lake)
        time_travel: Option<TimeTravelSpec>,
    },

    /// Filter operation
    Filter {
        /// Filter predicate
        predicate: Arc<dyn PhysicalExpr>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Projection operation
    Projection {
        /// Projection expressions
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        /// Output schema
        schema: Arc<ArrowSchema>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Hash aggregate
    HashAggregate {
        /// Grouping expressions
        group_by: Vec<Arc<dyn PhysicalExpr>>,
        /// Aggregate expressions
        aggr_exprs: Vec<AggregateExpr>,
        /// Output schema
        schema: Arc<ArrowSchema>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Sort operation
    Sort {
        /// Sort expressions
        exprs: Vec<SortExpr>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Limit operation
    Limit {
        /// Number to skip
        skip: usize,
        /// Number to take
        fetch: Option<usize>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Hash join
    HashJoin {
        /// Left input
        left: Box<PhysicalPlan>,
        /// Right input
        right: Box<PhysicalPlan>,
        /// Join type
        join_type: JoinType,
        /// Left join keys
        left_keys: Vec<Arc<dyn PhysicalExpr>>,
        /// Right join keys
        right_keys: Vec<Arc<dyn PhysicalExpr>>,
        /// Output schema
        schema: Arc<ArrowSchema>,
    },

    /// Cross join
    CrossJoin {
        /// Left input
        left: Box<PhysicalPlan>,
        /// Right input
        right: Box<PhysicalPlan>,
        /// Output schema
        schema: Arc<ArrowSchema>,
    },

    /// Sort-merge join
    SortMergeJoin {
        /// Left input
        left: Box<PhysicalPlan>,
        /// Right input
        right: Box<PhysicalPlan>,
        /// Join type
        join_type: JoinType,
        /// Left join keys
        left_keys: Vec<Arc<dyn PhysicalExpr>>,
        /// Right join keys
        right_keys: Vec<Arc<dyn PhysicalExpr>>,
        /// Output schema
        schema: Arc<ArrowSchema>,
    },

    /// Union
    Union {
        /// Input plans
        inputs: Vec<PhysicalPlan>,
        /// Output schema
        schema: Arc<ArrowSchema>,
    },

    /// Values (inline data)
    Values {
        /// Schema
        schema: Arc<ArrowSchema>,
        /// Data batches
        data: Vec<arrow::record_batch::RecordBatch>,
    },

    /// Empty result
    Empty {
        /// Whether to produce one row
        produce_one_row: bool,
        /// Schema
        schema: Arc<ArrowSchema>,
    },

    /// Explain
    Explain {
        /// Plan to explain
        input: Box<PhysicalPlan>,
        /// Verbose mode
        verbose: bool,
        /// Schema
        schema: Arc<ArrowSchema>,
    },

    /// Window function
    Window {
        /// Window expressions
        window_exprs: Vec<WindowExpr>,
        /// Output schema
        schema: Arc<ArrowSchema>,
        /// Input plan
        input: Box<PhysicalPlan>,
    },

    /// Explain Analyze - executes and collects statistics
    ExplainAnalyze {
        /// Plan to execute and analyze
        input: Box<PhysicalPlan>,
        /// Verbose mode
        verbose: bool,
        /// Schema
        schema: Arc<ArrowSchema>,
    },

    /// Copy query results to a file
    Copy {
        /// Input plan
        input: Box<PhysicalPlan>,
        /// Target file path
        target: String,
        /// Output format
        format: CopyFormat,
        /// Output schema
        schema: Arc<ArrowSchema>,
    },
}

/// Output format for COPY TO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    /// Parquet format
    Parquet,
    /// CSV format
    Csv,
    /// JSON Lines format
    Json,
}

impl PhysicalPlan {
    /// Get the output schema of this plan.
    pub fn schema(&self) -> Arc<ArrowSchema> {
        match self {
            PhysicalPlan::Scan { schema, .. } => schema.clone(),
            PhysicalPlan::Filter { input, .. } => input.schema(),
            PhysicalPlan::Projection { schema, .. } => schema.clone(),
            PhysicalPlan::HashAggregate { schema, .. } => schema.clone(),
            PhysicalPlan::Sort { input, .. } => input.schema(),
            PhysicalPlan::Limit { input, .. } => input.schema(),
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

    /// Get the children of this plan.
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::Scan { .. } => vec![],
            PhysicalPlan::Filter { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Projection { input, .. } => vec![input.as_ref()],
            PhysicalPlan::HashAggregate { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Sort { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Limit { input, .. } => vec![input.as_ref()],
            PhysicalPlan::HashJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            PhysicalPlan::CrossJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            PhysicalPlan::SortMergeJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            PhysicalPlan::Union { inputs, .. } => inputs.iter().collect(),
            PhysicalPlan::Values { .. } => vec![],
            PhysicalPlan::Empty { .. } => vec![],
            PhysicalPlan::Explain { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Window { input, .. } => vec![input.as_ref()],
            PhysicalPlan::ExplainAnalyze { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Copy { input, .. } => vec![input.as_ref()],
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
            PhysicalPlan::Scan { table_name, projection, filters, .. } => {
                f.push_str(&format!("{}Scan: {}", prefix, table_name));
                if let Some(proj) = projection {
                    f.push_str(&format!(" projection=[{:?}]", proj));
                }
                if !filters.is_empty() {
                    f.push_str(&format!(" filters=[{}]", filters.len()));
                }
                f.push('\n');
            }
            PhysicalPlan::Filter { predicate, input } => {
                f.push_str(&format!("{}Filter: {}\n", prefix, predicate.name()));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::Projection { exprs, input, .. } => {
                let expr_names: Vec<_> = exprs.iter().map(|e| e.name().to_string()).collect();
                f.push_str(&format!("{}Projection: [{}]\n", prefix, expr_names.join(", ")));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::HashAggregate { group_by, aggr_exprs, input, .. } => {
                let group_names: Vec<_> = group_by.iter().map(|e| e.name().to_string()).collect();
                let aggr_names: Vec<_> = aggr_exprs.iter().map(|e| e.name()).collect();
                f.push_str(&format!(
                    "{}HashAggregate: groupBy=[{}], aggrs=[{}]\n",
                    prefix,
                    group_names.join(", "),
                    aggr_names.join(", ")
                ));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::Sort { exprs, input } => {
                let sort_names: Vec<_> = exprs.iter().map(|e| e.expr.name().to_string()).collect();
                f.push_str(&format!("{}Sort: [{}]\n", prefix, sort_names.join(", ")));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::Limit { skip, fetch, input } => {
                f.push_str(&format!("{}Limit: skip={}, fetch={:?}\n", prefix, skip, fetch));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::HashJoin { left, right, join_type, .. } => {
                f.push_str(&format!("{}HashJoin: {:?}\n", prefix, join_type));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            PhysicalPlan::CrossJoin { left, right, .. } => {
                f.push_str(&format!("{}CrossJoin\n", prefix));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            PhysicalPlan::SortMergeJoin { left, right, join_type, .. } => {
                f.push_str(&format!("{}SortMergeJoin: {:?}\n", prefix, join_type));
                left.format_indent(f, indent + 1);
                right.format_indent(f, indent + 1);
            }
            PhysicalPlan::Union { inputs, .. } => {
                f.push_str(&format!("{}Union\n", prefix));
                for input in inputs {
                    input.format_indent(f, indent + 1);
                }
            }
            PhysicalPlan::Values { data, .. } => {
                f.push_str(&format!("{}Values: {} batch(es)\n", prefix, data.len()));
            }
            PhysicalPlan::Empty { produce_one_row, .. } => {
                f.push_str(&format!("{}Empty: produce_one_row={}\n", prefix, produce_one_row));
            }
            PhysicalPlan::Explain { input, verbose, .. } => {
                f.push_str(&format!("{}Explain: verbose={}\n", prefix, verbose));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::Window { window_exprs, input, .. } => {
                let window_names: Vec<_> = window_exprs.iter().map(|e| e.name()).collect();
                f.push_str(&format!("{}Window: [{}]\n", prefix, window_names.join(", ")));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::ExplainAnalyze { input, verbose, .. } => {
                f.push_str(&format!("{}ExplainAnalyze: verbose={}\n", prefix, verbose));
                input.format_indent(f, indent + 1);
            }
            PhysicalPlan::Copy { input, target, format, .. } => {
                f.push_str(&format!("{}Copy: target='{}' format={:?}\n", prefix, target, format));
                input.format_indent(f, indent + 1);
            }
        }
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_indent(0))
    }
}

/// Physical aggregate expression.
#[derive(Debug)]
pub struct AggregateExpr {
    /// Aggregate function
    pub func: AggregateFunc,
    /// Input expressions (arguments)
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// Whether distinct
    pub distinct: bool,
    /// Alias for the aggregate
    pub alias: Option<String>,
}

impl AggregateExpr {
    /// Get a display name for this aggregate.
    pub fn name(&self) -> String {
        if let Some(alias) = &self.alias {
            alias.clone()
        } else {
            format!("{:?}", self.func)
        }
    }
}

/// Physical sort expression.
#[derive(Debug)]
pub struct SortExpr {
    /// Expression to sort by
    pub expr: Arc<dyn PhysicalExpr>,
    /// Ascending order
    pub ascending: bool,
    /// Nulls first
    pub nulls_first: bool,
}

impl SortExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, ascending: bool, nulls_first: bool) -> Self {
        Self { expr, ascending, nulls_first }
    }
}

/// Window function type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunction {
    /// ROW_NUMBER() - row number within partition
    RowNumber,
    /// RANK() - rank with gaps
    Rank,
    /// DENSE_RANK() - rank without gaps
    DenseRank,
    /// NTILE(n) - divide into n buckets
    Ntile,
    /// PERCENT_RANK() - relative rank (0 to 1)
    PercentRank,
    /// CUME_DIST() - cumulative distribution
    CumeDist,
    /// LAG(expr, offset, default) - value at offset before current row
    Lag,
    /// LEAD(expr, offset, default) - value at offset after current row
    Lead,
    /// FIRST_VALUE(expr) - first value in window
    FirstValue,
    /// LAST_VALUE(expr) - last value in window
    LastValue,
    /// NTH_VALUE(expr, n) - nth value in window
    NthValue,
    /// Aggregate as window function (SUM, AVG, COUNT, etc.)
    Aggregate(AggregateFunc),
}

impl std::fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowFunction::RowNumber => write!(f, "ROW_NUMBER"),
            WindowFunction::Rank => write!(f, "RANK"),
            WindowFunction::DenseRank => write!(f, "DENSE_RANK"),
            WindowFunction::Ntile => write!(f, "NTILE"),
            WindowFunction::PercentRank => write!(f, "PERCENT_RANK"),
            WindowFunction::CumeDist => write!(f, "CUME_DIST"),
            WindowFunction::Lag => write!(f, "LAG"),
            WindowFunction::Lead => write!(f, "LEAD"),
            WindowFunction::FirstValue => write!(f, "FIRST_VALUE"),
            WindowFunction::LastValue => write!(f, "LAST_VALUE"),
            WindowFunction::NthValue => write!(f, "NTH_VALUE"),
            WindowFunction::Aggregate(func) => write!(f, "{:?}", func),
        }
    }
}

/// Physical window expression.
#[derive(Debug)]
pub struct WindowExpr {
    /// The window function
    pub func: WindowFunction,
    /// Function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// PARTITION BY expressions
    pub partition_by: Vec<Arc<dyn PhysicalExpr>>,
    /// ORDER BY expressions
    pub order_by: Vec<SortExpr>,
    /// Alias for the result
    pub alias: Option<String>,
}

impl WindowExpr {
    /// Create a new window expression.
    pub fn new(
        func: WindowFunction,
        args: Vec<Arc<dyn PhysicalExpr>>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Vec<SortExpr>,
        alias: Option<String>,
    ) -> Self {
        Self {
            func,
            args,
            partition_by,
            order_by,
            alias,
        }
    }

    /// Get the display name for this window expression.
    pub fn name(&self) -> String {
        if let Some(alias) = &self.alias {
            alias.clone()
        } else {
            format!("{}", self.func)
        }
    }
}

/// Execution statistics for a plan node.
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Time spent in this node (nanoseconds)
    pub elapsed_nanos: u64,
    /// Number of rows processed
    pub rows_processed: usize,
    /// Number of batches processed
    pub batches_processed: usize,
    /// Number of rows output
    pub rows_output: usize,
    /// Peak memory usage in bytes
    pub peak_memory_bytes: usize,
    /// Bytes spilled to disk (if any)
    pub bytes_spilled: usize,
    /// Child statistics
    pub children: Vec<ExecutionStats>,
    /// Operator name
    pub operator_name: String,
    /// Additional metrics specific to the operator
    pub extra_metrics: Vec<(String, String)>,
}

impl ExecutionStats {
    /// Create new execution stats for an operator
    pub fn new(operator_name: &str) -> Self {
        Self {
            operator_name: operator_name.to_string(),
            ..Default::default()
        }
    }

    /// Add elapsed time
    pub fn add_elapsed(&mut self, nanos: u64) {
        self.elapsed_nanos += nanos;
    }

    /// Add rows processed
    pub fn add_rows_processed(&mut self, rows: usize) {
        self.rows_processed += rows;
    }

    /// Add an extra metric
    pub fn add_metric(&mut self, name: &str, value: &str) {
        self.extra_metrics.push((name.to_string(), value.to_string()));
    }

    /// Get total elapsed time including children
    pub fn total_elapsed_nanos(&self) -> u64 {
        self.elapsed_nanos + self.children.iter().map(|c| c.total_elapsed_nanos()).sum::<u64>()
    }

    /// Format as a tree for display
    pub fn format_tree(&self, indent: usize) -> String {
        let mut result = String::new();
        let prefix = "  ".repeat(indent);

        // Format elapsed time nicely
        let elapsed_ms = self.elapsed_nanos as f64 / 1_000_000.0;
        let elapsed_str = if elapsed_ms >= 1000.0 {
            format!("{:.2}s", elapsed_ms / 1000.0)
        } else if elapsed_ms >= 1.0 {
            format!("{:.2}ms", elapsed_ms)
        } else {
            format!("{:.0}µs", self.elapsed_nanos as f64 / 1000.0)
        };

        // Format memory nicely
        let memory_str = if self.peak_memory_bytes >= 1024 * 1024 * 1024 {
            format!("{:.2}GB", self.peak_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if self.peak_memory_bytes >= 1024 * 1024 {
            format!("{:.2}MB", self.peak_memory_bytes as f64 / (1024.0 * 1024.0))
        } else if self.peak_memory_bytes >= 1024 {
            format!("{:.2}KB", self.peak_memory_bytes as f64 / 1024.0)
        } else {
            format!("{}B", self.peak_memory_bytes)
        };

        result.push_str(&format!(
            "{}→ {} (time={}, rows_in={}, rows_out={}, batches={}, memory={})\n",
            prefix,
            self.operator_name,
            elapsed_str,
            self.rows_processed,
            self.rows_output,
            self.batches_processed,
            memory_str
        ));

        // Add extra metrics if any
        for (name, value) in &self.extra_metrics {
            result.push_str(&format!("{}  • {}: {}\n", prefix, name, value));
        }

        // Add children
        for child in &self.children {
            result.push_str(&child.format_tree(indent + 1));
        }

        result
    }
}
