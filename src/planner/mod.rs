//! Query planning for Blaze.
//!
//! This module provides logical and physical plan representations,
//! as well as the planner that converts SQL to executable plans.

mod logical_plan;
mod logical_expr;
mod physical_plan;
mod physical_expr;
mod physical_planner;
mod binder;
mod optimizer;

pub use logical_plan::{LogicalPlan, LogicalPlanBuilder, JoinType, TimeTravelSpec};
pub use logical_expr::{LogicalExpr, Column, AggregateExpr as LogicalAggregateExpr, WindowExpr, SortExpr as LogicalSortExpr, AggregateFunc, BinaryOp, UnaryOp};
pub use physical_plan::{PhysicalPlan, AggregateExpr, SortExpr, WindowExpr as PhysicalWindowExpr, WindowFunction, ExecutionStats, CopyFormat};
pub use physical_expr::{
    PhysicalExpr, ColumnExpr, LiteralExpr, BinaryExpr, BitwiseNotExpr, CastExpr,
    CaseExpr, BetweenExpr, LikeExpr, InListExpr, ScalarFunctionExpr,
    ScalarSubqueryExpr, ExistsExpr, InSubqueryExpr,
};
pub use physical_planner::{PhysicalPlanner, SubqueryExecutor};
pub use binder::{Binder, BindContext};
pub use optimizer::{Optimizer, OptimizerRule};
