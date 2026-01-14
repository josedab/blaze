# ADR-0004: Two-Phase Planning (Logical and Physical Plans)

## Status

Accepted

## Context

Query planning transforms a bound query representation into an executable form. Two main approaches exist:

**Single-Plan Architecture**: One plan representation from binding through execution
- Simpler implementation
- Optimization rules must handle execution details
- Physical choices (e.g., hash join vs. sort-merge join) mixed with logical transformations
- Harder to retarget to different execution backends

**Two-Plan Architecture**: Separate logical and physical plan representations
- Logical plan: What to compute (abstract)
- Physical plan: How to compute (concrete)
- Clean separation of optimization from physical mapping
- Enables multiple physical backends from same logical plan

Query engines like Spark, Presto, and DataFusion use two-plan architectures. Simpler engines like SQLite use single-plan approaches.

## Decision

We implemented a **two-phase planning architecture** with distinct logical and physical plan types.

### Logical Plan

Represents the query in abstract, catalog-aware terms:

```rust
pub enum LogicalPlan {
    TableScan {
        table_ref: ResolvedTableRef,
        projection: Option<Vec<String>>,  // Column names
        filters: Vec<LogicalExpr>,
        schema: Schema,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<LogicalExpr>,  // Named expressions
        schema: Schema,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: LogicalExpr,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Vec<(LogicalExpr, LogicalExpr)>,
        join_type: JoinType,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
    },
    // ... Sort, Limit, Union, etc.
}
```

Key characteristics:
- References columns by **name** (symbolic)
- Uses **LogicalExpr** for expressions
- Carries **Schema** for type information
- Optimization-friendly (no physical details)

### Physical Plan

Represents concrete execution operations:

```rust
pub enum PhysicalPlan {
    Scan {
        table_name: String,
        projection: Option<Vec<usize>>,  // Column indices
        schema: Arc<ArrowSchema>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    },
    Projection {
        input: Box<PhysicalPlan>,
        expressions: Vec<Arc<dyn PhysicalExpr>>,  // Executable expressions
        schema: Arc<ArrowSchema>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Arc<dyn PhysicalExpr>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Vec<(usize, usize)>,  // Column indices
        join_type: JoinType,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Arc<dyn PhysicalExpr>>,
        aggregates: Vec<AggregateFunction>,
    },
    // ... Sort, Limit, Exchange, etc.
}
```

Key characteristics:
- References columns by **index** (concrete)
- Uses **PhysicalExpr** trait objects (executable)
- Carries **Arrow Schema** for execution
- Ready for direct execution

## Consequences

### Benefits

1. **Clean Optimization Phase**: Optimizer rules work with logical plans without physical concerns:
   ```rust
   // Filter pushdown operates on LogicalPlan
   fn push_filter_through_projection(plan: &LogicalPlan) -> LogicalPlan {
       // No need to consider HashJoin vs SortMergeJoin
   }
   ```

2. **Flexible Physical Mapping**: Same logical plan can map to different physical operators:
   ```rust
   // Logical Join → Physical HashJoin or SortMergeJoin
   LogicalPlan::Join { .. } => {
       if use_hash_join(&left_stats, &right_stats) {
           PhysicalPlan::HashJoin { .. }
       } else {
           PhysicalPlan::SortMergeJoin { .. }
       }
   }
   ```

3. **Multiple Backends**: Logical plan can target different physical backends:
   - CPU execution (current)
   - GPU execution (planned via `src/gpu/`)
   - Distributed execution (future)

4. **Prepared Statement Caching**: Cache at logical plan level, re-run physical planning per execution if needed.

5. **Plan Explanation**: Logical plan provides user-friendly EXPLAIN output; physical plan shows actual execution strategy.

6. **Type Safety**: Logical expressions carry type information; physical expressions are pre-validated.

### Costs

1. **Two Plan Hierarchies**: Must maintain parallel definitions for logical and physical nodes.

2. **Translation Code**: PhysicalPlanner (~1,500 lines) handles LogicalPlan → PhysicalPlan conversion.

3. **Expression Duplication**: LogicalExpr and PhysicalExpr hierarchies are similar but separate.

### Plan Translation Example

```rust
// Logical Plan (from binder)
LogicalPlan::Filter {
    input: Box::new(LogicalPlan::TableScan {
        table_ref: "users",
        projection: Some(vec!["id", "name"]),
        schema: Schema { fields: [id: Int64, name: Utf8] },
    }),
    predicate: LogicalExpr::BinaryOp {
        left: LogicalExpr::Column("id"),
        op: Gt,
        right: LogicalExpr::Literal(Int64(10)),
    },
}

// Physical Plan (from physical planner)
PhysicalPlan::Filter {
    input: Box::new(PhysicalPlan::Scan {
        table_name: "users",
        projection: Some(vec![0, 1]),  // Column indices
        schema: ArrowSchema { fields: [id: Int64, name: Utf8] },
    }),
    predicate: Arc::new(BinaryExpr {
        left: Arc::new(ColumnExpr { index: 0 }),
        op: Gt,
        right: Arc::new(LiteralExpr { value: Int64(10) }),
    }),
}
```

### Future Extensibility

The two-plan architecture enables:

1. **Cost-Based Physical Planning**: Choose operators based on statistics
   ```rust
   // Future: use cardinality estimates
   if estimated_right_rows < 10_000 {
       PhysicalPlan::HashJoin { build_side: Right, .. }
   } else {
       PhysicalPlan::SortMergeJoin { .. }
   }
   ```

2. **GPU Offloading**: Logical plan unchanged, physical planner emits GPU operators
   ```rust
   // Future GPU backend
   PhysicalPlan::GpuFilter { kernel: "filter_int64_gt", .. }
   ```

3. **Distributed Planning**: Physical planner adds exchange/shuffle operators
   ```rust
   // Future distributed backend
   PhysicalPlan::Exchange { partitioning: Hash(col0), .. }
   ```
