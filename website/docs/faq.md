---
sidebar_position: 8
---

# Frequently Asked Questions

Common questions about Blaze and their answers.

## General

### What is Blaze?

Blaze is a high-performance, memory-safe embedded OLAP (Online Analytical Processing) query engine written in Rust. It provides SQL:2016 compliance with native Apache Arrow and Parquet integration.

### Who should use Blaze?

Blaze is ideal for:
- **Rust developers** building applications with data analytics
- **Data engineers** processing Parquet/CSV files
- **Application developers** embedding SQL analytics in their software
- **Python data scientists** (via PyO3 bindings) who want fast SQL processing

### Is Blaze a database?

Blaze is a **query engine**, not a full database. It doesn't provide:
- Persistent storage (data lives in memory during execution)
- Transactions (ACID properties)
- Indexes (relies on scan + filter)
- User management or access control

Think of Blaze as a way to run SQL queries over data you load into memory.

### How does Blaze compare to DuckDB?

Both are embedded OLAP engines. Key differences:

| Aspect | Blaze | DuckDB |
|--------|-------|--------|
| Language | Rust | C++ |
| Memory safety | Guaranteed | Manual |
| Maturity | Newer | More mature |
| Features | Core SQL | Extensive |
| Persistence | No | Yes |

Choose Blaze for Rust-native integration and memory safety. Choose DuckDB for maximum features and performance.

See [Why Blaze?](/docs/comparison) for detailed comparisons.

## Installation

### How do I install Blaze?

**Rust:**
```toml
[dependencies]
blaze = "0.1"
```

**Python:**
```bash
pip install blaze-db
```

See [Installation](/docs/getting-started/installation) for complete instructions.

### What are the system requirements?

- **Rust:** 1.70 or later
- **OS:** Linux, macOS, Windows
- **Memory:** Depends on your data size
- **Dependencies:** None (pure Rust, no C/C++ dependencies)

### Can I use Blaze in WebAssembly?

Currently, Blaze doesn't officially support WebAssembly. Some dependencies may not compile to WASM. This is on the roadmap for future versions.

## Usage

### How do I load data?

```rust
use blaze::Connection;

let conn = Connection::in_memory()?;

// CSV
conn.register_csv("users", "users.csv")?;

// Parquet
conn.register_parquet("events", "events.parquet")?;

// Arrow data
conn.register_batches("data", record_batches)?;
```

### How do I run a query?

```rust
let results = conn.query("SELECT * FROM users WHERE age > 25")?;

// results is Vec<RecordBatch>
for batch in results {
    println!("Rows: {}", batch.num_rows());
}
```

### Can I use prepared statements?

Yes! Blaze supports prepared statements with parameter binding:

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE age > $1 AND city = $2")?;
let results = stmt.execute(&[&25i64, &"NYC"])?;
```

See [Prepared Statements](/docs/guides/prepared-statements) for details.

### How do I handle NULLs?

Use standard SQL NULL handling:

```sql
-- Check for NULL
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;

-- Provide defaults
SELECT COALESCE(email, 'no-email') FROM users;

-- Replace specific values with NULL
SELECT NULLIF(status, 'unknown') FROM users;
```

### How do I use window functions?

```sql
SELECT
    name,
    amount,
    ROW_NUMBER() OVER (ORDER BY amount DESC) as rank,
    SUM(amount) OVER (PARTITION BY department) as dept_total
FROM sales;
```

See [Window Functions](/docs/guides/window-functions) for comprehensive examples.

## Performance

### How much data can Blaze handle?

Blaze operates in-memory, so the limit is your available RAM. For reference:
- **1 GB RAM:** ~10-50 million rows (depending on columns)
- **16 GB RAM:** ~100-500 million rows
- **64 GB RAM:** ~1+ billion rows

Use memory limits to prevent OOM:

```rust
let config = ConnectionConfig::new()
    .with_memory_limit(8 * 1024 * 1024 * 1024);  // 8 GB
```

### How do I optimize query performance?

1. **Use appropriate batch size:**
```rust
ConnectionConfig::new().with_batch_size(8192);  // Default is good for most cases
```

2. **Select only needed columns:**
```sql
-- Good
SELECT name, email FROM users;

-- Bad (reads all columns)
SELECT * FROM users;
```

3. **Filter early:**
```sql
-- Filters are pushed down
SELECT * FROM large_table WHERE date = '2024-01-15';
```

4. **Use Parquet for large datasets:**
   - Columnar format = efficient column selection
   - Built-in compression
   - Row group statistics enable skipping

### Why is my query slow?

Common causes:

1. **Large cross joins:** Limited to 10M rows
2. **No filter pushdown:** Filters on derived columns
3. **All columns selected:** `SELECT *` reads everything
4. **Large string operations:** String functions on big datasets

See [Query Optimization](/docs/advanced/query-optimization) for tips.

### Is Blaze faster than X?

Performance depends on the workload:
- **Simple queries:** Similar to other engines
- **Complex analytics:** Generally fast due to vectorized execution
- **Memory-bound:** Depends on your RAM

For benchmarks, see our [benchmark suite](https://github.com/blaze-db/blaze/tree/main/benchmarks).

## Error Handling

### What errors can Blaze return?

```rust
use blaze::BlazeError;

match conn.query(sql) {
    Ok(results) => { /* success */ }
    Err(BlazeError::Parse { message, .. }) => { /* SQL syntax error */ }
    Err(BlazeError::Analysis(msg)) => { /* Semantic error */ }
    Err(BlazeError::Catalog { name }) => { /* Table not found */ }
    Err(BlazeError::Type { expected, actual }) => { /* Type mismatch */ }
    Err(BlazeError::Execution(msg)) => { /* Runtime error */ }
    Err(BlazeError::ResourceExhausted { .. }) => { /* Memory limit */ }
    Err(e) => { /* Other error */ }
}
```

See [Error Handling](/docs/reference/error-handling) for complete reference.

### How do I debug query plans?

Use EXPLAIN:

```sql
EXPLAIN SELECT * FROM users WHERE age > 25;
```

Output:
```
Filter: age > 25
  TableScan: users
```

### Why is my table not found?

```rust
// Error: BlazeError::Catalog { name: "users" }
conn.query("SELECT * FROM users")?;
```

Check:
1. Did you register the table? `conn.register_csv("users", "...")?`
2. Is the name spelled correctly?
3. Is the connection the same instance?

## Python Integration

### How do I use Blaze from Python?

```python
import blaze

conn = blaze.Connection()
conn.register_parquet("events", "events.parquet")
result = conn.query("SELECT * FROM events LIMIT 10")

# Convert to pandas
df = result.to_pandas()

# Convert to PyArrow
table = result.to_pyarrow()
```

### Can I use Blaze with pandas DataFrames?

Yes! Convert DataFrames to dictionaries and register:

```python
import pandas as pd
import blaze

df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

conn = blaze.Connection()
conn.from_dict("my_table", df.to_dict(orient='list'))
result = conn.query("SELECT * FROM my_table WHERE a > 1")
```

See [Python Integration](/docs/guides/python-integration) for complete guide.

## Extending Blaze

### Can I add custom functions?

Yes, by implementing `PhysicalExpr`:

```rust
impl PhysicalExpr for MyFunction {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Your implementation
    }
    fn name(&self) -> &str { "my_function" }
}
```

See [Extending Blaze](/docs/advanced/extending-blaze).

### Can I add custom data sources?

Yes, implement `TableProvider`:

```rust
impl TableProvider for MyDataSource {
    fn schema(&self) -> &Schema { &self.schema }
    fn scan(&self, projection: Option<&[usize]>, ...) -> Result<Vec<RecordBatch>> {
        // Read from your source
    }
    fn as_any(&self) -> &dyn Any { self }
}
```

See [Custom Storage Providers](/docs/advanced/custom-storage-providers).

## Troubleshooting

### Memory usage keeps growing

1. Check for unbounded queries (no LIMIT)
2. Set memory limits:
```rust
ConnectionConfig::new().with_memory_limit(1024 * 1024 * 1024);
```
3. Process data in batches
4. Drop results when done

### Queries hang or timeout

1. Check for large cross joins
2. Add LIMIT for exploration
3. Ensure files are accessible
4. Check for complex nested queries

### Type errors in queries

```sql
-- Error: Type mismatch
SELECT 'text' + 123;

-- Fix: Explicit cast
SELECT CAST(123 AS VARCHAR);
-- or
SELECT CONCAT('text', CAST(123 AS VARCHAR));
```

## Getting Help

### Where can I get support?

1. **Documentation:** You're here!
2. **GitHub Issues:** [Report bugs](https://github.com/blaze-db/blaze/issues)
3. **Discussions:** [Ask questions](https://github.com/blaze-db/blaze/discussions)

### How do I report a bug?

1. Check existing issues
2. Create minimal reproduction
3. Include: Blaze version, OS, error message, query
4. Submit at [GitHub Issues](https://github.com/blaze-db/blaze/issues/new)

### How can I contribute?

See [Contributing](/docs/contributing) for guidelines. We welcome:
- Bug reports
- Feature requests
- Documentation improvements
- Code contributions
