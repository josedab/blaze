# ADR-0003: Layered Query Processing Architecture

## Status

Accepted

## Context

Query engines must transform SQL text into executable operations and produce results. This transformation involves multiple complex steps:

1. Parsing SQL syntax into a structured representation
2. Resolving table and column references against a catalog
3. Type checking and validation
4. Query optimization
5. Physical execution planning
6. Actual data processing

Different architectural approaches exist:

**Monolithic Architecture**: Single module handles all stages
- Simpler initially
- Becomes unmaintainable as complexity grows
- Hard to test components in isolation
- Difficult to add new features (e.g., new optimizer rules)

**Layered Architecture**: Distinct modules with clear interfaces
- Each layer has single responsibility
- Layers can be tested independently
- New features can be added to specific layers
- Enables future extension (multiple physical backends)

## Decision

We structured Blaze as a **layered architecture** with the following distinct components:

```
┌─────────────────────────────────────────────────┐
│                   SQL Layer                      │
│              (src/sql/parser.rs)                 │
│         Converts SQL text → AST                  │
└─────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│                 Binding Layer                    │
│             (src/planner/binder.rs)              │
│    Resolves names, types → LogicalPlan          │
└─────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│              Optimization Layer                  │
│           (src/planner/optimizer.rs)             │
│  Transforms LogicalPlan → Optimized LogicalPlan │
└─────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│            Physical Planning Layer               │
│        (src/planner/physical_planner.rs)         │
│     Converts LogicalPlan → PhysicalPlan         │
└─────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│               Execution Layer                    │
│              (src/executor/)                     │
│     Executes PhysicalPlan → RecordBatches       │
└─────────────────────────────────────────────────┘
```

Supporting layers:
- **Catalog** (`src/catalog/`): Metadata about databases, schemas, tables
- **Storage** (`src/storage/`): File format readers/writers
- **Types** (`src/types/`): Type system definitions

## Consequences

### Benefits

1. **Separation of Concerns**: Each layer has a focused responsibility:
   - Parser: Syntax only (no semantics)
   - Binder: Name resolution and type checking
   - Optimizer: Plan improvement
   - Physical Planner: Operator selection
   - Executor: Data processing

2. **Independent Testing**: Each layer can be unit tested:
   ```rust
   // Test optimizer without parser
   let plan = LogicalPlanBuilder::scan(...).filter(...).build();
   let optimized = optimizer.optimize(&plan)?;
   assert!(matches!(optimized, LogicalPlan::Filter { .. }));
   ```

3. **Incremental Development**: New features target specific layers:
   - New SQL syntax → Parser + Binder
   - New optimization → Optimizer rules
   - New operator → Physical Planner + Executor

4. **Multiple Physical Backends**: The logical/physical separation enables:
   - CPU execution (current)
   - GPU execution (planned)
   - Distributed execution (future)

5. **Debuggability**: Problems can be localized to specific layers:
   - Parse error → SQL syntax issue
   - Binding error → Unknown column/table
   - Execution error → Data processing bug

6. **Prepared Statements**: Clean layer boundaries enable caching at logical plan level, skipping parse and bind for repeated queries.

### Costs

1. **More Code**: Layer abstractions require interface definitions and transformations between representations.

2. **Transformation Overhead**: Data structures are transformed at each boundary (AST → LogicalPlan → PhysicalPlan).

3. **Learning Curve**: Developers must understand multiple representations and their relationships.

### Layer Interfaces

**SQL → Binding**:
```rust
// Parser produces AST
let statements: Vec<Statement> = Parser::parse(sql)?;

// Binder consumes AST, produces LogicalPlan
let logical_plan = Binder::new(catalog).bind(statement)?;
```

**Binding → Optimization**:
```rust
// Optimizer transforms LogicalPlan
let optimized = Optimizer::default().optimize(&logical_plan)?;
```

**Optimization → Physical Planning**:
```rust
// PhysicalPlanner converts LogicalPlan to PhysicalPlan
let physical_plan = PhysicalPlanner::new().create_physical_plan(&optimized)?;
```

**Physical Planning → Execution**:
```rust
// Executor runs PhysicalPlan
let batches = ExecutionContext::new().execute(&physical_plan)?;
```

### Module Organization

```
src/
├── sql/
│   └── parser.rs          # SQL parsing (uses sqlparser-rs)
├── planner/
│   ├── binder.rs          # AST → LogicalPlan
│   ├── logical_plan.rs    # LogicalPlan definition
│   ├── logical_expr.rs    # Logical expressions
│   ├── optimizer.rs       # Optimization rules
│   ├── physical_planner.rs # LogicalPlan → PhysicalPlan
│   ├── physical_plan.rs   # PhysicalPlan definition
│   └── physical_expr.rs   # Physical expressions
├── executor/
│   ├── mod.rs             # ExecutionContext
│   └── operators.rs       # Physical operators
├── catalog/
│   ├── mod.rs             # CatalogList, Catalog
│   └── table.rs           # TableProvider trait
├── storage/
│   ├── memory.rs          # In-memory tables
│   ├── csv.rs             # CSV format
│   ├── parquet.rs         # Parquet format
│   └── delta.rs           # Delta Lake
└── types/
    ├── datatype.rs        # DataType enum
    ├── schema.rs          # Schema, Field
    └── value.rs           # ScalarValue
```
