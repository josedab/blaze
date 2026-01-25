//! Query planning for Blaze.
//!
//! This module provides logical and physical plan representations,
//! as well as the planner that converts SQL to executable plans.

mod binder;
mod logical_expr;
mod logical_plan;
mod optimizer;
pub mod physical_expr;
mod physical_plan;
mod physical_planner;

pub use binder::{BindContext, Binder};
pub use logical_expr::{
    AggregateExpr as LogicalAggregateExpr, AggregateFunc, BinaryOp, Column, LogicalExpr,
    SortExpr as LogicalSortExpr, UnaryOp, WindowExpr,
};
pub use logical_plan::{CopyOptions, JoinType, LogicalPlan, LogicalPlanBuilder, TimeTravelSpec};
pub use optimizer::{Optimizer, OptimizerRule};
pub use physical_expr::{
    BetweenExpr, BinaryExpr, BitwiseNotExpr, CaseExpr, CastExpr, ColumnExpr, ExistsExpr,
    InListExpr, InSubqueryExpr, LikeExpr, LiteralExpr, PhysicalExpr, ScalarFunctionExpr,
    ScalarSubqueryExpr,
};
pub use physical_plan::{
    AggregateExpr, CopyFormat, ExecutionStats, PhysicalPlan, SortExpr,
    WindowExpr as PhysicalWindowExpr, WindowFunction,
};
pub use physical_planner::{PhysicalPlanner, SubqueryExecutor};
