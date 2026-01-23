# Blaze Query Engine

A high-performance, memory-safe embedded OLAP query engine written in Rust with SQL:2016 compliance and native Apache Arrow/Parquet integration.

## Features

- **Memory Safety**: Built in Rust for guaranteed memory safety
- **Vectorized Execution**: Columnar processing with SIMD optimization
- **Native Arrow/Parquet**: First-class support for modern data formats
- **Embeddable**: In-process database with zero network overhead
- **SQL Support**: SELECT, JOIN, GROUP BY, window functions, CTEs, and more
- **Prepared Statements**: Safe parameterized queries with `$1`, `$2` syntax
- **Python Bindings**: Use Blaze from Python with PyO3 integration

## Quick Start

### Registering Data from Files

The most common way to use Blaze is to register external files as tables:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register CSV and Parquet files as tables
    conn.register_csv("sales", "data/sales.csv")?;
    conn.register_parquet("customers", "data/customers.parquet")?;

    // Query across files
    let results = conn.query("
        SELECT c.name, SUM(s.amount) as total
        FROM sales s
        JOIN customers c ON s.customer_id = c.id
        GROUP BY c.name
        ORDER BY total DESC
    ")?;

    for batch in &results {
        println!("Got {} rows", batch.num_rows());
    }

    Ok(())
}
```

### Registering Arrow Data

You can also register Arrow RecordBatches directly:

```rust
use blaze::{Connection, Result};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create Arrow data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Register as a queryable table
    conn.register_batches("users", vec![batch])?;

    // Query the data
    let results = conn.query("SELECT * FROM users WHERE id > 1")?;

    Ok(())
}
```

### In-Memory Tables with DDL

For dynamic table creation and modification, use in-memory tables:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create a table (in-memory only)
    conn.execute("CREATE TABLE users (id INT, name VARCHAR, active BOOLEAN)")?;

    // Insert data (in-memory tables only)
    conn.execute("INSERT INTO users VALUES (1, 'Alice', true)")?;
    conn.execute("INSERT INTO users VALUES (2, 'Bob', false)")?;

    // Query the data
    let results = conn.query("SELECT * FROM users WHERE active = true")?;

    Ok(())
}
```

> **Note**: `INSERT`, `UPDATE`, and `DELETE` statements only work with in-memory tables created via `CREATE TABLE`. For CSV/Parquet files, data is read-only.

### Prepared Statements

Use prepared statements for safe, efficient parameterized queries:

```rust
use blaze::{Connection, ScalarValue, PreparedStatementCache, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;

    // Prepare a query with parameters
    let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

    // Execute with different parameter values
    let results1 = stmt.execute(&[ScalarValue::Int64(Some(1))])?;
    let results2 = stmt.execute(&[ScalarValue::Int64(Some(2))])?;

    // For frequently-used queries, use the statement cache
    let cache = PreparedStatementCache::new(100);
    let stmt = conn.prepare_cached("SELECT * FROM users WHERE id = $1", &cache)?;

    Ok(())
}
```

## CLI Usage

```bash
cargo run

blaze> .tables
blaze> .read sales data/sales.csv
blaze> SELECT * FROM sales LIMIT 10;
blaze> exit
```

## Building

```bash
cargo build            # Debug build
cargo build --release  # Release build
cargo test             # Run tests
cargo clippy           # Linting
```

## Examples

See the `examples/` directory for usage examples:

```bash
cargo run --example basic_queries
```

## Supported SQL Features

### Queries
- SELECT with projections and aliases
- WHERE filtering with complex predicates
- JOIN: INNER, LEFT, RIGHT, FULL OUTER, CROSS, SEMI, ANTI
- GROUP BY with aggregates: COUNT, COUNT(DISTINCT), SUM, AVG, MIN, MAX
- ORDER BY, LIMIT, OFFSET
- UNION, INTERSECT, EXCEPT
- CTEs (WITH clause)
- Subqueries (scalar, EXISTS, IN)
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE

### Expressions
- Arithmetic: +, -, *, /, %
- Comparison: =, <>, <, >, <=, >=
- Logical: AND, OR, NOT
- CASE WHEN expressions
- BETWEEN, LIKE, IN operators
- IS NULL, IS NOT NULL
- CAST expressions
- Scalar functions: UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, CONCAT, SUBSTRING, ABS, ROUND, CEIL, FLOOR, COALESCE, NULLIF

### DDL/DML (In-Memory Tables Only)
- CREATE TABLE
- DROP TABLE
- INSERT INTO ... VALUES
- INSERT INTO ... SELECT
- UPDATE ... SET ... WHERE
- DELETE FROM ... WHERE

## Configuration

```rust
use blaze::{Connection, ConnectionConfig};

let config = ConnectionConfig::new()
    .with_batch_size(4096)           // Vectorized batch size (1 to 16M)
    .with_memory_limit(1024 * 1024)  // Memory limit in bytes
    .with_num_threads(4);            // Parallel execution threads

let conn = Connection::with_config(config)?;
```

## Known Limitations

See [LIMITATIONS.md](LIMITATIONS.md) for a detailed list.

### Summary

- **DML operations** (INSERT, UPDATE, DELETE) only work with in-memory tables
- **Cross join cardinality** limited to 10 million rows
- **Query nesting depth** limited to 128 levels
- Only the first statement in multi-statement queries is executed

### Type Support

Hash joins, sort-merge joins, and comparisons support these types:
- Boolean
- Int8, Int16, Int32, Int64
- UInt8, UInt16, UInt32, UInt64
- Float32, Float64
- Utf8 (String)
- Date32, Date64
- Timestamp (Second, Millisecond, Microsecond, Nanosecond)
- Decimal128

Decimal types are not yet supported as join keys.

### Performance Notes

- Filter pushdown is implemented for in-memory tables; CSV/Parquet apply filters post-scan
- Table statistics include row counts, byte sizes, and null counts per column
- Memory limits are enforced when configured via `ConnectionConfig::with_memory_limit()`

## Project Structure

```
src/
├── lib.rs           # Connection API and main entry point
├── main.rs          # CLI binary
├── prepared.rs      # Prepared statements and caching
├── catalog/         # Table and schema management
├── executor/        # Query execution engine
├── planner/         # Query planning and optimization
├── sql/             # SQL parsing
├── storage/         # CSV, Parquet, in-memory tables
├── types/           # Type system
├── optimizer/       # Query optimization rules
├── parallel/        # Parallel execution
├── simd/            # SIMD optimizations
└── python/          # Python bindings (PyO3)
```

## Documentation

For comprehensive documentation, see the [Blaze Documentation Website](./website/).

## Contributing

See `CLAUDE.md` for detailed development guidelines including code conventions, testing patterns, and architecture overview.

## License

MIT License
