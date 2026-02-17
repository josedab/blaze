# Blaze Query Engine

[![CI](https://github.com/blaze-db/blaze/actions/workflows/ci.yml/badge.svg)](https://github.com/blaze-db/blaze/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/blaze-db/blaze/branch/main/graph/badge.svg)](https://codecov.io/gh/blaze-db/blaze)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A high-performance, memory-safe embedded OLAP query engine written in Rust with SQL:2016 compliance and native Apache Arrow/Parquet integration.

## Features

- **Memory Safety**: Built in Rust for guaranteed memory safety
- **Vectorized Execution**: Columnar processing with SIMD optimization
- **Native Arrow/Parquet**: First-class support for modern data formats
- **Embeddable**: In-process database with zero network overhead
- **SQL Support**: SELECT, JOIN, GROUP BY, window functions, CTEs, and more
- **Prepared Statements**: Safe parameterized queries with `$1`, `$2` syntax
- **Multi-Platform**: Rust library, CLI, WASM, Node.js, Python, and C FFI bindings
- **COPY TO**: Export query results to Parquet, CSV, or JSON files

### Feature Maturity

| Feature | Status | Feature Flag |
|---------|--------|-------------|
| Core SQL Engine | ✅ Stable | _(default)_ |
| CSV/Parquet Storage | ✅ Stable | _(default)_ |
| In-Memory Tables | ✅ Stable | _(default)_ |
| Prepared Statements | ✅ Stable | _(default)_ |
| Window Functions | ✅ Stable | _(default)_ |
| Connection Pooling | ✅ Stable | _(default)_ |
| WASM Bindings | ✅ Stable | _(default)_ |
| SIMD Optimization | ⚠️ Experimental | `simd` |
| Arrow Flight / Flight SQL | ⚠️ Experimental | `flight` |
| Time-Series Extensions | ⚠️ Experimental | `timeseries` |
| Streaming Queries | ⚠️ Experimental | `streaming` |
| Adaptive Query Execution | ⚠️ Experimental | `adaptive` |
| Lakehouse (Delta Lake) | ⚠️ Experimental | `lakehouse` |
| Federated Queries | 🚧 Preview | `federation` |
| GPU Acceleration | 🚧 Preview | `gpu` |
| Natural Language Queries | 🚧 Preview | `nlq` |
| Learned Optimizer | 🚧 Preview | `learned-optimizer` |

**Legend**: ✅ Production-ready • ⚠️ API may change • 🚧 Not feature-complete

> **⚠️ Experimental & Preview Features**: Features marked ⚠️ or 🚧 are **not production-ready**.
> APIs will change, implementations may be incomplete, and bugs are expected.
> Enable them with `--features <flag>` for evaluation only. Core features (✅ Stable) are safe to depend on.

## Quick Start

### Prerequisites

- **Rust 1.92.0+** — Install via [rustup](https://rustup.rs/)
- Build takes ~10s (debug) or ~4-6min (release)
- Tests run in ~0.3s (900+ tests)

```bash
# Clone and verify everything works
git clone <repo-url> && cd blaze
make setup    # First-time: installs toolchain, pre-commit hook, builds, tests
```

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
# Interactive REPL
cargo run

blaze> .tables
blaze> .read sales data/sales.csv
blaze> SELECT * FROM sales LIMIT 10;
blaze> exit

# Execute SQL file
cargo run -- script.sql

# Execute query directly
cargo run -- -c "SELECT 1 + 1 AS result"

# Output formats
cargo run -- -c "SELECT * FROM users" --format json
cargo run -- -c "SELECT * FROM users" --format csv
cargo run -- -c "SELECT * FROM users" --format table

# Output to file
cargo run -- -c "SELECT * FROM users" --format csv --output results.csv
```

### Session Commands

```
.help              Show available commands
.tables            List all tables
.schema <table>    Show table schema
.read <name> <f>   Load CSV file as table
.timer on|off      Toggle query timing
.mode csv|json     Set output format
.output <file>     Redirect output to file
```

## Building

```bash
cargo build            # Debug build (~10s)
cargo build --release  # Release build (~4-6min)
cargo test             # Run tests (~0.2s, 900+ tests)
cargo clippy           # Linting
```

## Development

For contributors, see [CONTRIBUTING.md](CONTRIBUTING.md) for a full guide.

```bash
# First-time setup (installs pre-commit hook, verifies toolchain)
make setup

# Daily workflow
make              # Format code + run tests
make quick        # Fast check: syntax + tests
make fix          # Auto-fix formatting + clippy issues
make watch        # Re-run checks on file changes (requires cargo-watch)
make test-all     # Test all feature flags
```

## Benchmarks

Run the benchmark suite:

```bash
cargo bench
```

Benchmarks include:
- Table scans at various sizes
- Filter operations (equality, range, string)
- Aggregations (COUNT, SUM, AVG, GROUP BY)
- Sorting (single/multi-column, ASC/DESC)
- Joins (INNER, LEFT at various sizes)
- Window functions (ROW_NUMBER, RANK, LAG, running sum)
- Complex queries (CTEs, subqueries)

## Language Bindings

### WASM (Browser/Node.js)

```bash
npm install @blaze-sql/wasm
```

```typescript
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

await initBlaze();
const db = new BlazeDB();
db.registerJson('users', [{ id: 1, name: 'Alice' }]);
const result = db.query('SELECT * FROM users');
```

See [`wasm/README.md`](wasm/README.md) for full documentation.

### Node.js

```bash
npm install @blaze-sql/node
```

```typescript
import { BlazeDB } from '@blaze-sql/node';

const db = await BlazeDB.create();
await db.registerCsv('users', './users.csv');
const result = db.query('SELECT * FROM users');
```

See [`node/README.md`](node/README.md) for full documentation.

### C FFI

Build with FFI support:

```bash
cargo build --release --features c-ffi
```

```c
#include "blaze.h"

BlazeConnection* conn = blaze_connection_new();
BlazeResult* result = blaze_query(conn, "SELECT 1 + 1");
printf("Rows: %lld\n", blaze_result_row_count(result));
blaze_result_free(result);
blaze_connection_free(conn);
```

See [`include/blaze.h`](include/blaze.h) for the full C API.

## Examples

See the `examples/` directory for usage examples:

```bash
cargo run --example basic_queries
cargo run --example advanced_queries
```

- `basic_queries.rs` - Simple queries, joins, aggregations
- `advanced_queries.rs` - Window functions, CTEs, subqueries, set operations
- `python_notebook.ipynb` - PyArrow/Pandas/Polars integration
- `browser_playground.html` - Interactive web-based SQL playground

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `target/` is very large (>10GB) | Run `cargo clean` to reclaim disk space. The target directory can grow to 15-20GB with all features, benchmarks, and examples. Use `cargo clean --release` to only remove release artifacts. |
| Tests fail after checkout | Run `make setup` to ensure correct toolchain |
| Type mismatch errors in queries | Use float literals (`80000.0`) when comparing to `FLOAT`/`DOUBLE` columns |

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

### Scalar Functions
- **String**: UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, CONCAT, SUBSTRING, REPLACE, LEFT, RIGHT, LPAD, RPAD, REVERSE, SPLIT_PART, REGEXP_MATCH, REGEXP_REPLACE
- **Math**: ABS, ROUND, CEIL, FLOOR, SQRT, POWER, MOD
- **Date/Time**: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_SUB, DATE_DIFF, TO_DATE, TO_TIMESTAMP, CURRENT_DATE, CURRENT_TIMESTAMP
- **Utility**: COALESCE, NULLIF, IFNULL, NVL, GREATEST, LEAST

### Window Functions
- Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST
- Value: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- Aggregate: SUM, AVG, COUNT, MIN, MAX (with OVER clause)

### Data Export (COPY TO)
```sql
-- Export to Parquet
COPY (SELECT * FROM users) TO 'output.parquet';

-- Export to CSV
COPY (SELECT * FROM users) TO 'output.csv' WITH (FORMAT CSV, HEADER true);

-- Export to JSON
COPY (SELECT * FROM users) TO 'output.json' WITH (FORMAT JSON);
```

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
├── ffi/             # C FFI bindings
├── wasm/            # WASM bindings
└── python/          # Python bindings (PyO3)

wasm/                # @blaze-sql/wasm npm package
node/                # @blaze-sql/node npm package
include/             # C header files
benches/             # Criterion benchmarks
examples/            # Usage examples
```

## Documentation

For comprehensive documentation, see the [Blaze Documentation Website](./website/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, workflow, and conventions. For architecture details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## License

MIT License
