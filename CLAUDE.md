# CLAUDE.md - Blaze Query Engine

## Project Overview

Blaze is a high-performance, memory-safe embedded OLAP (Online Analytical Processing) query engine written in Rust. It provides SQL:2016 compliance with native Apache Arrow and Parquet integration.

## Quick Commands

```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run with release optimizations
cargo build --release

# Run clippy for linting
cargo clippy

# Format code
cargo fmt

# Run the CLI
cargo run
```

## Project Structure

```
src/
├── lib.rs              # Main library entry point, Connection API
├── main.rs             # CLI binary
├── error.rs            # Error types (BlazeError, Result)
├── catalog/            # Database catalog (tables, schemas, databases)
│   ├── mod.rs          # CatalogList, Catalog, Schema management
│   └── table.rs        # TableProvider trait
├── executor/           # Query execution engine
│   ├── mod.rs          # ExecutionContext, Executor
│   └── operators.rs    # Physical operators (HashJoin, Sort, Aggregate, Window, etc.)
├── planner/            # Query planning
│   ├── mod.rs          # Public exports
│   ├── logical_plan.rs # Logical plan representation
│   ├── logical_expr.rs # Logical expressions (Column, BinaryExpr, Aggregate, Window, etc.)
│   ├── physical_plan.rs # Physical plan representation
│   ├── physical_expr.rs # Physical expressions (ColumnExpr, LiteralExpr, CaseExpr, etc.)
│   ├── physical_planner.rs # Logical to physical plan conversion
│   ├── binder.rs       # SQL AST to logical plan binding
│   └── optimizer.rs    # Query optimization rules
├── sql/                # SQL parsing
│   └── parser.rs       # SQL parser using sqlparser-rs
├── storage/            # Data storage
│   ├── mod.rs          # Storage exports
│   ├── memory.rs       # In-memory tables
│   ├── csv.rs          # CSV file support
│   └── parquet.rs      # Parquet file support
└── types/              # Type system
    ├── mod.rs          # Type exports
    ├── datatype.rs     # DataType enum
    ├── schema.rs       # Schema, Field definitions
    └── value.rs        # ScalarValue for literals
```

## Key Concepts

### Query Execution Flow

```
SQL String
    ↓ (Parser)
AST (Statement)
    ↓ (Binder)
LogicalPlan
    ↓ (Optimizer)
Optimized LogicalPlan
    ↓ (PhysicalPlanner)
PhysicalPlan
    ↓ (Executor)
Vec<RecordBatch>
```

### Important Types

- **`Connection`** (`lib.rs`): Main API entry point for executing queries
- **`LogicalPlan`** (`planner/logical_plan.rs`): Tree of logical operations (Scan, Filter, Project, Join, Aggregate, etc.)
- **`PhysicalPlan`** (`planner/physical_plan.rs`): Executable plan with concrete operators
- **`PhysicalExpr`** trait (`planner/physical_expr.rs`): Expression evaluation interface
- **`RecordBatch`**: Arrow columnar data format (from `arrow` crate)

### Expression Types

Physical expressions in `planner/physical_expr.rs`:
- `ColumnExpr` - Column reference by index
- `LiteralExpr` - Constant values
- `BinaryExpr` - Binary operations (+, -, *, /, =, <, >, AND, OR, etc.)
- `CastExpr` - Type casting
- `CaseExpr` - CASE WHEN expressions
- `BetweenExpr` - BETWEEN low AND high
- `LikeExpr` - Pattern matching
- `InListExpr` - IN (value1, value2, ...)
- `ScalarFunctionExpr` - Functions like UPPER, LOWER, ABS, COALESCE

### Physical Operators

In `executor/operators.rs`:
- `HashJoinOperator` - Hash-based joins (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
- `CrossJoinOperator` - Cartesian product
- `SortOperator` - ORDER BY implementation
- `HashAggregateOperator` - GROUP BY with aggregates
- `WindowOperator` - Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)

## Code Conventions

### Error Handling

Use `BlazeError` and `Result<T>` from `error.rs`:

```rust
use crate::error::{BlazeError, Result};

fn my_function() -> Result<T> {
    // Use ? for propagation
    let value = something_fallible()?;

    // Create errors with helper methods
    Err(BlazeError::analysis("Column not found"))
    Err(BlazeError::not_implemented("Feature X"))
    Err(BlazeError::execution("Runtime error"))
}
```

### Adding New Physical Expressions

1. Add struct in `planner/physical_expr.rs`:
```rust
#[derive(Debug)]
pub struct MyExpr {
    // fields
}

impl PhysicalExpr for MyExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Implementation
    }
    fn name(&self) -> &str { "my_expr" }
}
```

2. Export in `planner/mod.rs`
3. Handle in `physical_planner.rs` `create_physical_expr()`

### Adding New Physical Operators

1. Add operator struct in `executor/operators.rs`
2. Add variant to `PhysicalPlan` in `planner/physical_plan.rs`
3. Update `schema()`, `children()`, `format_indent()` methods
4. Handle in `Executor::execute()` in `executor/mod.rs`
5. Handle in `PhysicalPlanner::create_physical_plan()`

## Testing

Tests are co-located with code using `#[cfg(test)]` modules:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature() {
        // Test implementation
    }
}
```

Run specific tests:
```bash
cargo test test_name
cargo test module::tests::
```

## Dependencies

Key dependencies in `Cargo.toml`:
- `arrow` - Apache Arrow columnar format
- `parquet` - Parquet file format
- `sqlparser` - SQL parsing
- `thiserror` - Error derive macros

## Current Capabilities

### Supported SQL Features
- SELECT with projections, aliases
- WHERE filtering
- JOIN (INNER, LEFT, RIGHT, FULL OUTER, CROSS, SEMI, ANTI)
- GROUP BY with aggregates (COUNT, SUM, AVG, MIN, MAX)
- ORDER BY, LIMIT, OFFSET
- CASE WHEN expressions
- BETWEEN, LIKE, IN operators
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- Scalar functions (UPPER, LOWER, ABS, COALESCE, CONCAT, TRIM, LENGTH)
- CTEs (WITH clause)
- UNION, INTERSECT, EXCEPT

### Storage Formats
- In-memory tables
- CSV files
- Parquet files

## Performance Considerations

- Uses Arrow's columnar format for vectorized processing
- Hash-based joins and aggregations
- Predicate pushdown optimization
- Constant folding optimization
- Memory manager for tracking allocations
