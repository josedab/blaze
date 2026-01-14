---
sidebar_position: 1
---

# Architecture

Understanding Blaze's architecture helps you write efficient queries and extend the engine. This guide covers the query processing pipeline and core components.

## System Overview

Blaze follows a layered architecture with clear separation of concerns:

```mermaid
graph TB
    subgraph UI["User Interface"]
        API["Rust API"]
        PyAPI["Python API"]
        CLI["CLI"]
    end

    subgraph QP["Query Processing"]
        Parser["Parser"]
        Binder["Binder"]
        Optimizer["Optimizer"]
        PP["Physical Planner"]
        Parser --> Binder --> Optimizer --> PP
    end

    subgraph Exec["Execution"]
        VE["Vectorized Executor<br/>(Arrow-native)"]
    end

    subgraph Storage["Storage Layer"]
        Mem["Memory"]
        CSV["CSV"]
        Parquet["Parquet"]
        Delta["Delta Lake"]
    end

    UI --> QP
    QP --> Exec
    Exec --> Storage

    style UI fill:#ff6b35,stroke:#e85a24,color:#fff
    style QP fill:#1a365d,stroke:#16213e,color:#fff
    style Exec fill:#06d6a0,stroke:#05b085,color:#fff
    style Storage fill:#ffd166,stroke:#e5bc5c,color:#000
```

## Query Processing Pipeline

Every SQL query flows through a well-defined pipeline:

```mermaid
flowchart LR
    SQL["SQL String"] --> Parser
    Parser --> AST["AST"]
    AST --> Binder
    Binder --> LP["Logical Plan"]
    LP --> Optimizer
    Optimizer --> OLP["Optimized Plan"]
    OLP --> PP["Physical Planner"]
    PP --> PhysPlan["Physical Plan"]
    PhysPlan --> Executor
    Executor --> Results["RecordBatch[]"]

    style SQL fill:#f9f9f9,stroke:#333
    style Results fill:#06d6a0,stroke:#05b085
```

### 1. Parsing

The **Parser** converts SQL text into an Abstract Syntax Tree (AST).

```
SQL: "SELECT name FROM users WHERE age > 25"
         ↓
AST: SelectStatement {
       columns: [Column("name")],
       from: [Table("users")],
       where: BinaryExpr(Column("age"), Gt, Literal(25))
     }
```

**Location**: `src/sql/parser.rs`

### 2. Binding

The **Binder** transforms the AST into a logical plan by:

- Resolving table and column references via the Catalog
- Validating that columns exist and types are compatible
- Handling CTEs and subqueries
- Enforcing query depth limits (max 128 levels)

```mermaid
flowchart TB
    AST["AST"] --> Binder
    Binder --> Catalog["Catalog Lookup"]
    Catalog --> Validation["Type Validation"]
    Validation --> LP["Logical Plan"]

    subgraph LogicalPlan["Logical Plan Tree"]
        Filter["Filter: age > 25"]
        Proj["Projection: name"]
        Scan["TableScan: users"]
        Filter --> Proj --> Scan
    end

    LP --> LogicalPlan
```

**Location**: `src/planner/binder.rs`

### 3. Optimization

The **Optimizer** applies transformation rules to improve query performance:

| Rule | Effect |
|------|--------|
| **Predicate Pushdown** | Move filters closer to data sources |
| **Projection Pushdown** | Request only needed columns |
| **Constant Folding** | Evaluate constant expressions at plan time |
| **Simplify Expressions** | Simplify boolean logic |

```mermaid
flowchart LR
    subgraph Before["Before Optimization"]
        B1["Project"] --> B2["Filter"] --> B3["Scan"]
    end

    subgraph After["After Optimization"]
        A1["Project"] --> A3["Scan + Filter"]
    end

    Before -->|"Predicate Pushdown"| After

    style After fill:#06d6a0,stroke:#05b085
```

**Location**: `src/planner/optimizer.rs`

### 4. Physical Planning

The **Physical Planner** converts the logical plan into an executable physical plan:

- Columns are referenced by index (not name)
- Specific algorithms are chosen (e.g., HashJoin vs. MergeJoin)
- Expressions become executable `PhysicalExpr` implementations

**Location**: `src/planner/physical_planner.rs`

### 5. Execution

The **Executor** runs the physical plan and produces Arrow RecordBatches:

- Vectorized processing in batches (default 8,192 rows)
- SIMD-optimized operations where possible
- Memory tracking and limit enforcement

**Location**: `src/executor/mod.rs`

## Core Components

### Connection

The `Connection` is the primary entry point:

```rust
pub struct Connection {
    catalog_list: Arc<CatalogList>,      // Metadata
    execution_context: ExecutionContext,  // Runtime config
    optimizer: Optimizer,                  // Query optimizer
}
```

Key responsibilities:
- Parse and execute SQL
- Register tables and files
- Manage prepared statements

### Catalog System

Hierarchical metadata management:

```mermaid
graph TB
    CL["CatalogList"] --> C["Catalog: default"]
    C --> S["Schema: main"]
    S --> T1["Table: users"]
    S --> T2["Table: orders"]
    S --> T3["Table: products"]

    style CL fill:#ff6b35,stroke:#e85a24,color:#fff
    style C fill:#ffd166,stroke:#e5bc5c
    style S fill:#06d6a0,stroke:#05b085
```

Resolution order:
1. `table` → `default.main.table`
2. `schema.table` → `default.schema.table`
3. `catalog.schema.table` → fully qualified

### Logical Plan

Represents **what** to compute without specifying **how**:

```rust
pub enum LogicalPlan {
    TableScan { table_name: String, ... },
    Projection { exprs: Vec<LogicalExpr>, input: Box<LogicalPlan> },
    Filter { predicate: LogicalExpr, input: Box<LogicalPlan> },
    Aggregate { group_by: Vec<LogicalExpr>, aggr_exprs: Vec<LogicalExpr>, ... },
    Sort { exprs: Vec<SortExpr>, input: Box<LogicalPlan> },
    Limit { skip: usize, fetch: Option<usize>, input: Box<LogicalPlan> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, ... },
    Window { window_exprs: Vec<LogicalExpr>, input: Box<LogicalPlan> },
    // ... more variants
}
```

### Physical Plan

Executable plan with concrete algorithms:

```rust
pub enum PhysicalPlan {
    Scan { table: Arc<dyn TableProvider>, ... },
    Filter { predicate: Arc<dyn PhysicalExpr>, input: Box<PhysicalPlan> },
    Projection { exprs: Vec<Arc<dyn PhysicalExpr>>, input: Box<PhysicalPlan> },
    HashAggregate { ... },
    HashJoin { ... },
    Sort { ... },
    // ... more variants
}
```

### Physical Expressions

The `PhysicalExpr` trait defines evaluatable expressions:

```rust
pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    fn data_type(&self) -> DataType;
    fn name(&self) -> &str;
}
```

Implementations include:
- `ColumnExpr`: Reference columns by index
- `LiteralExpr`: Constant values
- `BinaryExpr`: Operations like `+`, `-`, `*`, `/`, `=`, `AND`, `OR`
- `CastExpr`: Type conversions
- `CaseExpr`: CASE WHEN expressions
- `ScalarFunctionExpr`: Built-in functions

## Data Flow Example

Here's how data flows through the system for a query:

```sql
SELECT name, SUM(amount)
FROM orders
WHERE status = 'completed'
GROUP BY name
ORDER BY SUM(amount) DESC
LIMIT 10
```

```mermaid
flowchart TB
    subgraph Step1["1. Parse"]
        SQL["SQL Query"]
    end

    subgraph Step2["2. Bind"]
        Limit["Limit(10)"]
        Sort["Sort(SUM DESC)"]
        Agg["Aggregate(GROUP BY name)"]
        Filter["Filter(status='completed')"]
        Scan["TableScan(orders)"]
        Limit --> Sort --> Agg --> Filter --> Scan
    end

    subgraph Step3["3. Optimize"]
        O1["Limit(10)"]
        O2["Sort"]
        O3["HashAggregate"]
        O4["Scan + Filter ✓"]
        O1 --> O2 --> O3 --> O4
    end

    subgraph Step4["4. Execute"]
        direction TB
        E5["Limit: Take 10"]
        E4["Sort: Order DESC"]
        E3["HashAggregate: Group & Sum"]
        E2["Filter: status check"]
        E1["Scan: Read batches"]
        E1 --> E2 --> E3 --> E4 --> E5
    end

    subgraph Step5["5. Result"]
        RB["Vec&lt;RecordBatch&gt;"]
    end

    Step1 --> Step2 --> Step3 --> Step4 --> Step5

    style Step3 fill:#06d6a0,stroke:#05b085
    style Step5 fill:#ffd166,stroke:#e5bc5c
```

## Memory Model

Blaze uses Arrow's columnar memory model:

```mermaid
graph LR
    subgraph RecordBatch
        Schema["Schema"]
        C1["Column 1<br/>(Int64Array)"]
        C2["Column 2<br/>(StringArray)"]
        C3["Column 3<br/>(Float64Array)"]
    end

    subgraph Memory["Memory Layout"]
        B1["Buffer 1"]
        B2["Buffer 2"]
        B3["Buffer 3"]
    end

    C1 --> B1
    C2 --> B2
    C3 --> B3

    style RecordBatch fill:#1a365d,stroke:#16213e,color:#fff
    style Memory fill:#ffd166,stroke:#e5bc5c
```

Benefits:
- Zero-copy data sharing
- Cache-efficient columnar access
- SIMD-friendly memory layout

## Thread Safety

- `Connection` is **not** thread-safe (each thread should have its own)
- RecordBatches are immutable and can be shared across threads
- Storage providers use interior mutability with proper synchronization

## Next Steps

- [Connection API](/docs/concepts/connection-api) - Working with connections
- [Query Execution](/docs/concepts/query-execution) - Execution details
- [Query Optimization](/docs/advanced/query-optimization) - Understanding the optimizer
