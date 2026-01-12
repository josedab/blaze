# Blaze Query Engine

A high-performance, memory-safe embedded OLAP query engine written in Rust with SQL:2016 compliance and native Apache Arrow/Parquet integration.

## Features

- **Memory Safety**: Built in Rust for guaranteed memory safety
- **Vectorized Execution**: Columnar processing with SIMD optimization
- **Native Arrow/Parquet**: First-class support for modern data formats
- **Embeddable**: In-process database with zero network overhead
- **SQL Support**: SELECT, JOIN, GROUP BY, window functions, CTEs, and more

## Quick Start

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    // Create an in-memory database
    let conn = Connection::in_memory()?;

    // Create a table
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;

    // Query data
    let results = conn.query("SELECT * FROM users")?;

    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}
```

## Reading External Files

```rust
use blaze::Connection;

let conn = Connection::in_memory()?;

// Register CSV and Parquet files as tables
conn.register_csv("sales", "data/sales.csv")?;
conn.register_parquet("customers", "data/customers.parquet")?;

// Query across files
let results = conn.query("
    SELECT c.name, SUM(s.amount)
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    GROUP BY c.name
")?;
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

## Supported SQL Features

### Queries
- SELECT with projections and aliases
- WHERE filtering with complex predicates
- JOIN: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- GROUP BY with aggregates: COUNT, SUM, AVG, MIN, MAX
- ORDER BY, LIMIT, OFFSET
- UNION, INTERSECT, EXCEPT
- CTEs (WITH clause)
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD

### Expressions
- Arithmetic: +, -, *, /
- Comparison: =, <>, <, >, <=, >=
- Logical: AND, OR, NOT
- CASE WHEN expressions
- BETWEEN, LIKE, IN operators
- Scalar functions: UPPER, LOWER, ABS, COALESCE, CONCAT, TRIM, LENGTH

### DDL
- CREATE TABLE
- DROP TABLE

## Known Limitations

### Not Yet Implemented

The following features are planned but not yet functional:

- **Subqueries**: Scalar subqueries, EXISTS, and IN subqueries return errors
- **DML Operations**: INSERT, UPDATE, DELETE are not implemented
- **SortMergeJoin**: Only HashJoin is currently supported

### Type Support

Hash joins and comparisons support these types:
- Boolean
- Int8, Int16, Int32, Int64
- UInt32, UInt64
- Float32, Float64
- Utf8 (String)
- Date32, Date64
- Timestamp (Second, Millisecond, Microsecond, Nanosecond)

Decimal types are not yet supported as join keys.

### Performance Notes

- Filter pushdown is implemented for in-memory tables; CSV/Parquet apply filters post-scan
- Table statistics include row counts, byte sizes, and null counts per column
- Memory limits are enforced when configured via `ConnectionConfig::with_memory_limit()`

### Other Limitations

- Only the first statement in multi-statement queries is executed

## Project Structure

```
src/
├── lib.rs           # Connection API
├── main.rs          # CLI binary
├── catalog/         # Table and schema management
├── executor/        # Query execution engine
├── planner/         # Query planning and optimization
├── sql/             # SQL parsing
├── storage/         # CSV, Parquet, in-memory tables
└── types/           # Type system
```

## Contributing

See `CLAUDE.md` for detailed development guidelines including code conventions, testing patterns, and architecture overview.

## License

[Add your license here]
