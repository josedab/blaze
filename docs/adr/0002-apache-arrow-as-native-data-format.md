# ADR-0002: Apache Arrow as Native Data Format

## Status

Accepted

## Context

A query engine needs an internal data representation for processing query results. The choice of data format affects:

- **Performance**: Memory layout impacts CPU cache utilization and SIMD vectorization
- **Interoperability**: Data exchange with external systems (Python, Spark, other tools)
- **Memory Efficiency**: Overhead per row/column affects memory consumption
- **Implementation Complexity**: Custom formats require more code to maintain

Traditional options include:

1. **Row-oriented formats**: Each row stored contiguously (e.g., MySQL, PostgreSQL wire format)
   - Good for OLTP (accessing all columns of few rows)
   - Poor for OLAP (scanning few columns of many rows)

2. **Custom columnar format**: Design our own column representation
   - Maximum flexibility
   - No ecosystem interoperability
   - Significant implementation burden

3. **Apache Arrow**: Standardized columnar memory format
   - Designed for analytics workloads
   - Language-agnostic specification
   - Growing ecosystem adoption (Pandas, Spark, DuckDB, Polars)

## Decision

We adopted **Apache Arrow** as Blaze's native in-memory data format.

All query processing operates on Arrow's `RecordBatch` type:
- Tables are represented as `Vec<RecordBatch>`
- Operators consume and produce `RecordBatch` instances
- Results are returned to users as Arrow format

We use the official `arrow-rs` crate (version 57.x) maintained by the Apache Arrow project.

## Consequences

### Benefits

1. **Optimized for Analytics**: Arrow's columnar layout enables:
   - Sequential memory access patterns (cache-friendly)
   - SIMD vectorization for batch operations
   - Efficient compression (similar values adjacent)
   - O(1) column projection (just pointer arithmetic)

2. **Zero-Copy Interoperability**: Arrow's standardized format enables data exchange without serialization:
   - Python: PyArrow tables share memory with Rust
   - Spark: Arrow-based data exchange
   - Other Arrow-native tools: Direct memory sharing

3. **Rich Type System**: Arrow provides:
   - Primitive types (integers, floats, booleans)
   - Variable-length types (strings, binary)
   - Nested types (lists, structs, maps)
   - Temporal types (dates, timestamps, intervals)
   - Dictionary encoding for categorical data

4. **Ecosystem Integration**:
   - Parquet read/write uses Arrow as intermediate format
   - PyArrow provides seamless Python DataFrame conversion
   - Polars and Pandas 2.0 use Arrow backend

5. **Battle-Tested Implementation**: The `arrow-rs` crate is:
   - Maintained by Apache Arrow committers
   - Used in production by DataFusion, Ballista, Polars
   - Well-tested with extensive CI

6. **Null Handling**: Arrow's validity bitmaps efficiently represent NULL values without boxing overhead.

### Costs

1. **Memory Layout Constraints**: Arrow's specification is fixed; we cannot optimize layout for specific query patterns.

2. **Learning Curve**: Developers must understand Arrow's memory model, array types, and APIs.

3. **Dependency Weight**: Arrow crate brings significant compile-time and binary size overhead (~10MB release binary contribution).

4. **Version Coupling**: Major Arrow version upgrades may require code changes. We must track `arrow-rs` releases.

### Implications

- **Data Flow**: All operators work with `RecordBatch` inputs and outputs
- **Type System**: Blaze's `DataType` enum maps bidirectionally to Arrow types
- **Memory Management**: Arrow's buffer allocation used throughout
- **Batch Size**: Configurable rows per batch (default 8,192) balances memory and vectorization
- **Storage Integration**: CSV, Parquet, and Delta readers produce Arrow batches directly
- **Python Bindings**: Results convert to PyArrow tables for Python interop

### Example Flow

```
CSV File → CsvTable::scan() → Vec<RecordBatch>
                                    ↓
                            Filter Operator
                                    ↓
                            Vec<RecordBatch>
                                    ↓
                            Project Operator
                                    ↓
                            Vec<RecordBatch>
                                    ↓
                            PyArrow Table (Python)
```

Every stage uses Arrow's `RecordBatch` as the data currency.
