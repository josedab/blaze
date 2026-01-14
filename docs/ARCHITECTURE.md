# Blaze Query Engine Architecture

This document provides a comprehensive overview of the Blaze query engine architecture, including system design, component interactions, and data flow.

## Table of Contents

- [System Overview](#system-overview)
- [Query Execution Pipeline](#query-execution-pipeline)
- [Module Structure](#module-structure)
- [Core Components](#core-components)
  - [Connection API](#connection-api)
  - [SQL Parser](#sql-parser)
  - [Binder](#binder)
  - [Logical Planner](#logical-planner)
  - [Optimizer](#optimizer)
  - [Physical Planner](#physical-planner)
  - [Executor](#executor)
- [Storage Layer](#storage-layer)
- [Catalog System](#catalog-system)
- [Type System](#type-system)
- [Error Handling](#error-handling)
- [Advanced Features](#advanced-features)
- [Extension Points](#extension-points)

---

## System Overview

Blaze is a high-performance, memory-safe embedded OLAP query engine written in Rust. It provides SQL:2016 compliance with native Apache Arrow integration for vectorized execution.

```mermaid
graph TB
    subgraph "User Interface"
        CLI[CLI Binary]
        API[Rust API]
        PY[Python Bindings]
    end

    subgraph "Query Processing"
        CONN[Connection]
        PARSER[SQL Parser]
        BINDER[Binder]
        OPT[Optimizer]
        PPLANNER[Physical Planner]
        EXEC[Executor]
    end

    subgraph "Data Layer"
        CAT[Catalog System]
        STORAGE[Storage Providers]
        TYPES[Type System]
    end

    CLI --> CONN
    API --> CONN
    PY --> CONN
    CONN --> PARSER
    PARSER --> BINDER
    BINDER --> OPT
    OPT --> PPLANNER
    PPLANNER --> EXEC

    BINDER --> CAT
    EXEC --> STORAGE
    BINDER --> TYPES
    STORAGE --> TYPES
```

### Design Principles

1. **Vectorized Execution**: Process data in batches (default 8,192 rows) using Arrow's columnar format
2. **Two-Phase Planning**: Separate logical (what) from physical (how) planning
3. **Pluggable Storage**: Trait-based abstraction for multiple data formats
4. **Memory Safety**: Rust's ownership system ensures safe concurrent access
5. **Zero-Copy**: Arrow format enables efficient data sharing across components

---

## Query Execution Pipeline

Every SQL query flows through a well-defined pipeline from text to results:

```mermaid
flowchart LR
    SQL["SQL String"] --> PARSE["Parser"]
    PARSE --> AST["AST"]
    AST --> BIND["Binder"]
    BIND --> LP["Logical Plan"]
    LP --> OPT["Optimizer"]
    OPT --> OLP["Optimized Plan"]
    OLP --> PP["Physical Planner"]
    PP --> PHP["Physical Plan"]
    PHP --> EXEC["Executor"]
    EXEC --> RB["Vec&lt;RecordBatch&gt;"]

    style SQL fill:#e1f5fe
    style RB fill:#c8e6c9
```

### Pipeline Stages

| Stage | Input | Output | Location |
|-------|-------|--------|----------|
| **Parse** | SQL string | AST (Statement) | `sql/parser.rs` |
| **Bind** | AST + Catalog | LogicalPlan | `planner/binder.rs` |
| **Optimize** | LogicalPlan | Optimized LogicalPlan | `planner/optimizer.rs` |
| **Physical Plan** | LogicalPlan | PhysicalPlan | `planner/physical_planner.rs` |
| **Execute** | PhysicalPlan | Vec\<RecordBatch\> | `executor/mod.rs` |

### Example Query Flow

```sql
SELECT c.name, SUM(s.amount)
FROM sales s
JOIN customers c ON s.customer_id = c.id
GROUP BY c.name
ORDER BY SUM(s.amount) DESC
LIMIT 10
```

```mermaid
flowchart TB
    subgraph "1. Parse"
        SQL["SQL Text"] --> AST["SELECT Statement AST"]
    end

    subgraph "2. Bind"
        AST --> LP["LogicalPlan::Sort
        └─ Limit
           └─ Aggregate
              └─ Join
                 ├─ TableScan(sales)
                 └─ TableScan(customers)"]
    end

    subgraph "3. Optimize"
        LP --> OLP["Optimized LogicalPlan
        (predicate pushdown,
        projection pushdown)"]
    end

    subgraph "4. Physical Plan"
        OLP --> PP["Sort
        └─ Limit
           └─ HashAggregate
              └─ HashJoin
                 ├─ Scan(sales)
                 └─ Scan(customers)"]
    end

    subgraph "5. Execute"
        PP --> RB["Vec&lt;RecordBatch&gt;
        (Arrow columnar format)"]
    end
```

---

## Module Structure

```
src/
├── lib.rs                  # Main entry: Connection API
├── main.rs                 # CLI binary
├── error.rs                # Error types (BlazeError)
│
├── sql/                    # SQL Parsing
│   └── parser.rs           # sqlparser-rs wrapper
│
├── planner/                # Query Planning
│   ├── mod.rs              # Public exports
│   ├── binder.rs           # AST → LogicalPlan
│   ├── logical_plan.rs     # Logical operators
│   ├── logical_expr.rs     # Logical expressions
│   ├── optimizer.rs        # Rule-based optimizer
│   ├── physical_planner.rs # Logical → Physical
│   ├── physical_plan.rs    # Physical operators
│   └── physical_expr.rs    # Evaluatable expressions
│
├── executor/               # Query Execution
│   ├── mod.rs              # ExecutionContext
│   └── operators.rs        # Physical operator implementations
│
├── catalog/                # Metadata Management
│   ├── mod.rs              # CatalogList, Catalog, Schema
│   └── table.rs            # TableProvider trait
│
├── storage/                # Data Storage
│   ├── mod.rs              # Storage utilities
│   ├── memory.rs           # In-memory tables
│   ├── csv.rs              # CSV format
│   ├── parquet.rs          # Parquet format
│   └── delta.rs            # Delta Lake format
│
├── types/                  # Type System
│   ├── mod.rs              # Public exports
│   ├── datatype.rs         # DataType enum
│   ├── schema.rs           # Schema, Field
│   └── value.rs            # ScalarValue
│
└── optimizer/              # Advanced Optimization
    ├── cost_model.rs       # Cost estimation
    ├── statistics.rs       # Table statistics
    ├── cardinality.rs      # Cardinality estimation
    └── join_ordering.rs    # Join order optimization
```

---

## Core Components

### Connection API

The `Connection` is the primary user-facing API, managing database state and query execution.

```mermaid
classDiagram
    class Connection {
        -catalog_list: Arc~CatalogList~
        -execution_context: ExecutionContext
        -prepared_cache: LruCache
        +in_memory() Connection
        +with_config(config) Connection
        +query(sql) Vec~RecordBatch~
        +execute(sql) usize
        +prepare(sql) PreparedStatement
        +register_csv(name, path)
        +register_parquet(name, path)
        +list_tables() Vec~String~
    }

    class ConnectionConfig {
        +batch_size: usize
        +memory_limit: Option~usize~
        +num_threads: Option~usize~
    }

    class PreparedStatement {
        +sql: String
        +logical_plan: LogicalPlan
        +physical_plan: PhysicalPlan
        +parameter_types: Vec~DataType~
        +execute(params) Vec~RecordBatch~
    }

    Connection --> ConnectionConfig
    Connection --> PreparedStatement
```

**Key Methods**:

| Method | Purpose |
|--------|---------|
| `in_memory()` | Create in-memory database |
| `query(sql)` | Execute SELECT, return results |
| `execute(sql)` | Execute DDL/DML, return row count |
| `prepare(sql)` | Compile query for reuse |
| `register_csv(name, path)` | Register CSV as table |
| `register_parquet(name, path)` | Register Parquet as table |

---

### SQL Parser

Wraps `sqlparser-rs` to convert SQL text into AST.

**Location**: `src/sql/parser.rs`

```rust
pub struct Parser;

impl Parser {
    pub fn parse(&self, sql: &str) -> Result<Statement>
}
```

**Supported Statements**:
- `SELECT` (with JOINs, CTEs, subqueries, window functions)
- `INSERT`, `UPDATE`, `DELETE`
- `CREATE TABLE`, `DROP TABLE`
- `EXPLAIN`, `EXPLAIN ANALYZE`
- `COPY` (import/export)

---

### Binder

Transforms parsed AST into a `LogicalPlan` by resolving names and validating types.

**Location**: `src/planner/binder.rs`

```mermaid
flowchart LR
    AST["Statement AST"] --> BINDER["Binder"]
    CAT["CatalogList"] --> BINDER
    BINDER --> LP["LogicalPlan"]
    BINDER --> ERR["BlazeError
    (if validation fails)"]
```

**Responsibilities**:
1. Resolve table references via Catalog
2. Bind column references to schemas
3. Validate expression types
4. Handle CTEs and subqueries
5. Enforce depth limits (max 128 nesting levels)

---

### Logical Planner

Represents **what** to compute without specifying **how**.

**Location**: `src/planner/logical_plan.rs`, `src/planner/logical_expr.rs`

```mermaid
classDiagram
    class LogicalPlan {
        <<enumeration>>
        TableScan
        Projection
        Filter
        Aggregate
        Sort
        Limit
        Join
        CrossJoin
        Window
        Distinct
        SetOperation
        Values
        SubqueryAlias
    }

    class LogicalExpr {
        <<enumeration>>
        Column
        Literal
        BinaryExpr
        UnaryExpr
        Aggregate
        Window
        Case
        IsNull
        ScalarFunction
        Subquery
    }

    LogicalPlan --> LogicalExpr : contains
```

**Logical Plan Nodes**:

| Node | SQL Equivalent | Description |
|------|---------------|-------------|
| `TableScan` | `FROM table` | Source of data |
| `Projection` | `SELECT exprs` | Output columns |
| `Filter` | `WHERE cond` | Row filtering |
| `Aggregate` | `GROUP BY` | Grouping + aggregates |
| `Sort` | `ORDER BY` | Result ordering |
| `Limit` | `LIMIT n OFFSET m` | Row limiting |
| `Join` | `JOIN` | Table joins |
| `Window` | `OVER (...)` | Window functions |

---

### Optimizer

Transforms logical plans for better performance using rule-based transformations.

**Location**: `src/planner/optimizer.rs`, `src/optimizer/`

```mermaid
flowchart LR
    LP["LogicalPlan"] --> R1["SimplifyExpressions"]
    R1 --> R2["ConstantFolding"]
    R2 --> R3["PredicatePushdown"]
    R3 --> R4["ProjectionPushdown"]
    R4 --> R5["EliminateLimit"]
    R5 --> OLP["Optimized Plan"]

    style LP fill:#ffcdd2
    style OLP fill:#c8e6c9
```

**Optimization Rules**:

| Rule | Effect |
|------|--------|
| **SimplifyExpressions** | Simplify boolean logic |
| **ConstantFolding** | Evaluate constants at plan time |
| **PredicatePushdown** | Move filters closer to sources |
| **ProjectionPushdown** | Request only needed columns |
| **EliminateLimit** | Remove redundant limits |

**Cost-Based Components** (`src/optimizer/`):
- `CostModel`: CPU/IO cost estimation
- `Statistics`: Table/column statistics
- `CardinalityEstimator`: Row count prediction
- `JoinOrderOptimizer`: Multi-way join reordering

---

### Physical Planner

Converts logical plans to executable physical plans.

**Location**: `src/planner/physical_planner.rs`, `src/planner/physical_plan.rs`, `src/planner/physical_expr.rs`

```mermaid
classDiagram
    class PhysicalPlan {
        <<enumeration>>
        Scan
        Filter
        Projection
        HashAggregate
        Sort
        Limit
        HashJoin
        CrossJoin
        Window
        SetOperation
    }

    class PhysicalExpr {
        <<interface>>
        +evaluate(batch) ArrayRef
        +data_type() DataType
        +name() String
    }

    class ColumnExpr
    class LiteralExpr
    class BinaryExpr
    class CastExpr
    class CaseExpr
    class ScalarFunctionExpr

    PhysicalPlan --> PhysicalExpr
    PhysicalExpr <|-- ColumnExpr
    PhysicalExpr <|-- LiteralExpr
    PhysicalExpr <|-- BinaryExpr
    PhysicalExpr <|-- CastExpr
    PhysicalExpr <|-- CaseExpr
    PhysicalExpr <|-- ScalarFunctionExpr
```

**Key Differences from Logical Plan**:
- Columns referenced by **index** (not name)
- Expressions are **executable** (implement `evaluate()`)
- Specific algorithms chosen (e.g., `HashJoin` not just `Join`)
- Arrow schemas for execution

---

### Executor

Runs physical plans to produce results.

**Location**: `src/executor/mod.rs`, `src/executor/operators.rs`

```mermaid
flowchart TB
    subgraph "ExecutionContext"
        BS["batch_size: 8192"]
        MM["memory_manager"]
        CL["catalog_list"]
    end

    subgraph "Execution"
        PHP["PhysicalPlan"] --> EXEC["execute()"]
        EXEC --> |"dispatch"| OPS["Operators"]
    end

    subgraph "Operators"
        SCAN["TableScan"]
        FILTER["Filter"]
        PROJ["Projection"]
        HJ["HashJoin"]
        HA["HashAggregate"]
        SORT["Sort"]
        WIN["Window"]
    end

    OPS --> SCAN
    OPS --> FILTER
    OPS --> PROJ
    OPS --> HJ
    OPS --> HA
    OPS --> SORT
    OPS --> WIN

    SCAN --> RB["Vec&lt;RecordBatch&gt;"]
    FILTER --> RB
    PROJ --> RB
    HJ --> RB
    HA --> RB
    SORT --> RB
    WIN --> RB
```

**Physical Operators**:

| Operator | Algorithm | Complexity |
|----------|-----------|------------|
| `HashJoin` | Hash-based | O(n + m) |
| `HashAggregate` | Hash table | O(n) |
| `Sort` | In-memory sort | O(n log n) |
| `Filter` | Predicate evaluation | O(n) |
| `Projection` | Expression evaluation | O(n) |
| `Window` | Partitioned computation | O(n log n) |

---

## Storage Layer

Trait-based abstraction for multiple storage formats.

```mermaid
classDiagram
    class TableProvider {
        <<interface>>
        +schema() Schema
        +scan(projection, filters, limit) Vec~RecordBatch~
        +supports_filter_pushdown() bool
        +statistics() Option~Statistics~
    }

    class MemoryTable {
        -batches: Vec~RecordBatch~
        +append(batches)
        +replace(batches)
        +clear()
    }

    class CsvTable {
        -path: PathBuf
        -options: CsvOptions
    }

    class ParquetTable {
        -path: PathBuf
        -metadata: ParquetMetaData
    }

    class DeltaTable {
        -inner: DeltaTableInner
        +time_travel_to(version)
        +versions() Vec~i64~
    }

    TableProvider <|-- MemoryTable
    TableProvider <|-- CsvTable
    TableProvider <|-- ParquetTable
    TableProvider <|-- DeltaTable
```

**Storage Providers**:

| Provider | Format | Filter Pushdown | Statistics |
|----------|--------|-----------------|------------|
| `MemoryTable` | In-memory | No | Yes |
| `CsvTable` | CSV files | Yes | No |
| `ParquetTable` | Parquet files | Yes | Yes |
| `DeltaTable` | Delta Lake | Yes | Yes |

**Auto-Detection**: `read_file(path)` detects format by extension:
- `.csv`, `.tsv` → `CsvTable`
- `.parquet`, `.pq` → `ParquetTable`
- Directory with `_delta_log/` → `DeltaTable`

---

## Catalog System

Hierarchical metadata management.

```mermaid
flowchart TB
    CL["CatalogList"] --> C1["Catalog: default"]
    CL --> C2["Catalog: other"]

    C1 --> S1["Schema: main"]
    C1 --> S2["Schema: temp"]

    S1 --> T1["Table: users"]
    S1 --> T2["Table: orders"]

    style CL fill:#e3f2fd
    style C1 fill:#bbdefb
    style S1 fill:#90caf9
    style T1 fill:#64b5f6
    style T2 fill:#64b5f6
```

**Resolution Order**:
1. `table` → `default.main.table`
2. `schema.table` → `default.schema.table`
3. `catalog.schema.table` → fully qualified

**Components**:

| Component | Purpose |
|-----------|---------|
| `CatalogList` | Root container for catalogs |
| `Catalog` | Named container for schemas |
| `SchemaProvider` | Container for tables |
| `TableProvider` | Data source interface |

---

## Type System

SQL types mapped to Arrow types.

**Location**: `src/types/`

```mermaid
classDiagram
    class DataType {
        <<enumeration>>
        Null
        Boolean
        Int8, Int16, Int32, Int64
        UInt8, UInt16, UInt32, UInt64
        Float16, Float32, Float64
        Decimal128
        Utf8, LargeUtf8
        Binary, LargeBinary
        Date32, Date64
        Timestamp
        Time32, Time64
        Duration
        Interval
        List, Struct, Map
    }

    class Schema {
        -fields: Vec~Field~
        +field(name) Field
        +column_index(name) usize
        +to_arrow() ArrowSchema
    }

    class Field {
        +name: String
        +data_type: DataType
        +nullable: bool
    }

    class ScalarValue {
        <<enumeration>>
        Null
        Boolean(bool)
        Int64(i64)
        Float64(f64)
        Utf8(String)
        Timestamp(...)
    }

    Schema --> Field
    Field --> DataType
```

**Type Conversions**:
- `DataType::to_arrow()` → Arrow `DataType`
- `DataType::from_sql_type()` → Parse SQL type strings
- `Schema::to_arrow()` → Arrow `Schema`

---

## Error Handling

Comprehensive error types with context.

**Location**: `src/error.rs`

```rust
pub enum BlazeError {
    Parse { message: String, location: Option<Location> },
    Analysis(String),
    Plan(String),
    Optimization(String),
    Execution(String),
    Type { expected: String, actual: String },
    Schema { message: String },
    Catalog { name: String },
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
    ResourceExhausted { message: String },
    NotImplemented(String),
    InvalidArgument(String),
    Internal(String),
}
```

**Helper Methods**:
```rust
BlazeError::parse("unexpected token", Some(location))
BlazeError::analysis("column 'x' not found")
BlazeError::type_error("Int64", "Utf8")
BlazeError::not_implemented("LATERAL JOIN")
```

---

## Advanced Features

### Production Features

#### Lakehouse Support (`src/lakehouse/`)
- Delta Lake integration with time travel
- Iceberg format support
- Snapshot management

#### Python Bindings (`src/python.rs`)
- PyO3-based bindings
- Zero-copy PyArrow integration
- Pandas/Polars interoperability

#### Adaptive Query Execution (`src/adaptive/`)
- Runtime statistics collection
- Plan adjustments based on observed data
- Skew handling

### Experimental Features

> **Note**: These features are under active development and APIs may change.

#### Parallel Execution (`src/parallel/`) ⚠️ Experimental
- Exchange operators for data shuffling
- Worker pool for distributed computation
- Partition-based parallelism

#### GPU Acceleration (`src/gpu/`) ⚠️ Experimental
- CUDA kernel execution
- GPU memory management
- Offloading compute-intensive operations

### Planned Features

#### WebAssembly Support (`src/wasm/`) 📋 Planned
- Browser-based query execution
- Edge computing deployment
- JavaScript/TypeScript integration

---

## Extension Points

Blaze provides several extension mechanisms:

### Custom Storage Provider

```rust
impl TableProvider for MyStorage {
    fn schema(&self) -> &Schema { /* ... */ }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Custom implementation
    }
}

// Register
conn.register_table("my_table", Arc::new(MyStorage::new()))?;
```

### Custom Optimizer Rule

```rust
impl OptimizerRule for MyRule {
    fn name(&self) -> &str { "my_rule" }

    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Transform plan
    }
}
```

### Custom Physical Expression

```rust
impl PhysicalExpr for MyExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Evaluate expression
    }

    fn data_type(&self) -> ArrowDataType { /* ... */ }
    fn name(&self) -> &str { "my_expr" }
}
```

---

## Configuration

```rust
let conn = Connection::with_config(
    ConnectionConfig::new()
        .with_batch_size(8192)      // Vectorization batch size
        .with_memory_limit(1 << 30) // 1GB memory limit
        .with_num_threads(8)        // Parallel workers
)?;
```

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `batch_size` | 8,192 | 1 - 16M | Rows per batch |
| `memory_limit` | None | Any | Max memory usage |
| `num_threads` | None | 1+ | Parallel workers |

---

## Further Reading

- [ADR-0001: Rust as Implementation Language](adr/0001-rust-as-implementation-language.md)
- [ADR-0002: Apache Arrow as Native Data Format](adr/0002-apache-arrow-as-native-data-format.md)
- [ADR-0003: Layered Query Processing Architecture](adr/0003-layered-query-processing-architecture.md)
- [ADR-0004: Two-Phase Planning](adr/0004-two-phase-planning.md)
- [ADR-0005: Trait-Based Pluggable Storage](adr/0005-trait-based-pluggable-storage.md)
- [ADR-0006: Batch-Oriented Vectorized Execution](adr/0006-batch-oriented-vectorized-execution.md)
- [ADR-0007: Hash-Based Join and Aggregation](adr/0007-hash-based-join-aggregation.md)
- [ADR-0008: Rule-Based Optimizer](adr/0008-rule-based-optimizer.md)
