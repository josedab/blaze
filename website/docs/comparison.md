---
sidebar_position: 2
---

# Why Blaze?

How Blaze compares to other data processing solutions and when to choose it.

## Comparison Overview

| Feature | Blaze | DuckDB | SQLite | Polars | DataFusion |
|---------|-------|--------|--------|--------|------------|
| **Language** | Rust | C++ | C | Rust | Rust |
| **Memory Safety** | Yes | No | No | Yes | Yes |
| **Primary Use Case** | Embedded OLAP | Embedded OLAP | Embedded OLTP | DataFrames | Query Framework |
| **Arrow Native** | Yes | Yes | No | Yes | Yes |
| **Parquet Support** | Yes | Yes | Extension | Yes | Yes |
| **Python Bindings** | Yes | Yes | Yes | Yes | Yes |
| **SQL Support** | SQL:2016 | PostgreSQL-like | SQLite dialect | SQL-like | SQL:2016 |
| **Embeddable** | Yes | Yes | Yes | Yes | Yes |
| **No Dependencies** | Yes | No | Yes | No | No |

## Detailed Comparisons

### Blaze vs DuckDB

**DuckDB** is an excellent embedded OLAP database written in C++.

**Choose Blaze when:**
- Memory safety is critical for your application
- You want native Rust integration without FFI overhead
- You need a lightweight, dependency-free solution
- You're building a Rust application and want consistent tooling

**Choose DuckDB when:**
- You need maximum query performance (DuckDB is highly optimized)
- You need advanced features (persistent storage, transactions)
- You're working primarily in Python or R
- You need the most mature feature set

```rust
// Blaze - Native Rust, zero-copy Arrow
let conn = Connection::in_memory()?;
let batches = conn.query("SELECT * FROM users")?;
// batches are Arrow RecordBatches - no conversion needed

// DuckDB from Rust requires FFI bindings
```

### Blaze vs SQLite

**SQLite** is the world's most deployed database, optimized for OLTP workloads.

**Choose Blaze when:**
- You're doing analytical queries (aggregations, window functions)
- You're processing columnar data (Parquet, Arrow)
- You need high-throughput batch processing
- You want vectorized execution

**Choose SQLite when:**
- You need persistent storage with ACID transactions
- You're doing transactional workloads (many small reads/writes)
- You need the broadest ecosystem support
- You need row-level operations

```sql
-- Blaze excels at analytical queries
SELECT
    region,
    SUM(amount) as total,
    AVG(amount) as average,
    ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) as rank
FROM sales
GROUP BY region;

-- SQLite excels at transactional queries
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
SELECT * FROM users WHERE id = 42;
```

### Blaze vs Polars

**Polars** is a high-performance DataFrame library written in Rust.

**Choose Blaze when:**
- You prefer SQL over DataFrame APIs
- You need to integrate with existing SQL-based systems
- You want a query engine (not a DataFrame library)
- You're building a data platform with SQL interface

**Choose Polars when:**
- You prefer DataFrame/method-chaining APIs
- You need lazy evaluation with query optimization
- You want the most feature-rich DataFrame library
- You need streaming/out-of-core processing

```rust
// Blaze - SQL interface
let results = conn.query("
    SELECT name, SUM(amount)
    FROM orders
    GROUP BY name
")?;

// Polars - DataFrame interface
let df = df.lazy()
    .group_by(["name"])
    .agg([col("amount").sum()])
    .collect()?;
```

### Blaze vs DataFusion

**DataFusion** is a query framework for building custom query engines.

**Choose Blaze when:**
- You want a ready-to-use query engine
- You need simple, batteries-included setup
- You want minimal configuration
- You're building applications, not query engines

**Choose DataFusion when:**
- You're building a custom query engine
- You need maximum extensibility
- You want to customize every component
- You're building a database product

```rust
// Blaze - Simple, ready to use
let conn = Connection::in_memory()?;
conn.register_csv("data", "file.csv")?;
let results = conn.query("SELECT * FROM data")?;

// DataFusion - Framework for building custom engines
let ctx = SessionContext::new();
ctx.register_csv("data", "file.csv", CsvReadOptions::new()).await?;
let df = ctx.sql("SELECT * FROM data").await?;
let results = df.collect().await?;
```

## When to Choose Blaze

### Ideal Use Cases

1. **Rust Applications with Analytics**
   - Native integration, no FFI
   - Consistent error handling with `Result<T>`
   - Memory safety guarantees

2. **Embedded Analytics**
   - Desktop applications with data analysis
   - CLI tools processing data files
   - Serverless functions with data queries

3. **Data Pipelines**
   - ETL/ELT processing
   - Data transformation with SQL
   - Parquet/CSV file processing

4. **Learning & Prototyping**
   - Clean, readable codebase
   - Educational query engine
   - Prototype for custom solutions

### Not Ideal For

1. **Transactional Workloads**
   - Use SQLite, PostgreSQL, or MySQL instead
   - Blaze has no transaction support

2. **Persistent Storage**
   - Blaze is primarily in-memory
   - Use DuckDB or SQLite for persistence

3. **Maximum Performance**
   - DuckDB has more optimization
   - Consider Polars for pure DataFrame work

4. **Distributed Queries**
   - Blaze is single-node only
   - Use Spark, Trino, or ClickHouse for distributed

## Feature Matrix

### SQL Features

| Feature | Blaze | DuckDB | SQLite |
|---------|-------|--------|--------|
| SELECT/FROM/WHERE | Yes | Yes | Yes |
| JOIN (all types) | Yes | Yes | Yes |
| GROUP BY | Yes | Yes | Yes |
| Window Functions | Yes | Yes | Yes |
| CTEs (WITH) | Yes | Yes | Yes |
| Subqueries | Yes | Yes | Yes |
| UNION/INTERSECT/EXCEPT | Yes | Yes | Yes |
| CASE expressions | Yes | Yes | Yes |
| CREATE TABLE | No | Yes | Yes |
| INSERT/UPDATE/DELETE | No | Yes | Yes |
| Transactions | No | Yes | Yes |
| Indexes | No | Yes | Yes |

### Data Formats

| Format | Blaze | DuckDB | Polars |
|--------|-------|--------|--------|
| CSV | Read | Read/Write | Read/Write |
| Parquet | Read | Read/Write | Read/Write |
| Arrow | Native | Native | Native |
| JSON | No | Yes | Yes |
| Excel | No | Yes | Yes |
| Delta Lake | Yes | Yes | Yes |

### Language Bindings

| Language | Blaze | DuckDB | SQLite |
|----------|-------|--------|--------|
| Rust | Native | FFI | FFI |
| Python | PyO3 | Native | Native |
| JavaScript | No | Yes | Yes |
| Java | No | Yes | Yes |
| Go | No | Yes | Yes |

## Migration Guide

### From DuckDB

```rust
// DuckDB (via duckdb-rs)
let conn = duckdb::Connection::open_in_memory()?;
conn.execute_batch("CREATE TABLE t (x INT)")?;
let mut stmt = conn.prepare("SELECT * FROM t")?;

// Blaze
let conn = blaze::Connection::in_memory()?;
conn.register_csv("t", "data.csv")?;  // Or register Arrow data
let results = conn.query("SELECT * FROM t")?;
```

### From SQLite

```rust
// SQLite (via rusqlite)
let conn = rusqlite::Connection::open_in_memory()?;
let mut stmt = conn.prepare("SELECT * FROM users")?;
let rows = stmt.query_map([], |row| { /* ... */ })?;

// Blaze
let conn = blaze::Connection::in_memory()?;
// Results are Arrow RecordBatches
let batches = conn.query("SELECT * FROM users")?;
```

### From Polars

```rust
// Polars
let df = CsvReader::from_path("data.csv")?.finish()?;
let filtered = df.filter(&df["age"].gt(25))?;

// Blaze
let conn = Connection::in_memory()?;
conn.register_csv("data", "data.csv")?;
let results = conn.query("SELECT * FROM data WHERE age > 25")?;
```

## Summary

Blaze is the right choice when you need:

- **Memory-safe** embedded analytics in Rust
- **SQL interface** for querying data
- **Arrow/Parquet** native support
- **Lightweight** solution with minimal dependencies
- **Educational** or easily understandable query engine

Consider alternatives when you need:

- Persistent storage and transactions (DuckDB, SQLite)
- Maximum query performance (DuckDB)
- DataFrame API (Polars)
- Custom query engine framework (DataFusion)
- Distributed queries (Spark, Trino)
