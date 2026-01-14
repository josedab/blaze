---
sidebar_position: 2
---

# Connection API

The `Connection` is the primary interface for interacting with Blaze. This guide covers creating connections, configuring options, and using the API.

## Creating Connections

### In-Memory Connection

The simplest way to get started:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Ready to use
    conn.execute("CREATE TABLE test (id INT)")?;

    Ok(())
}
```

### Connection with Configuration

Customize behavior with `ConnectionConfig`:

```rust
use blaze::{Connection, ConnectionConfig, Result};

fn main() -> Result<()> {
    let config = ConnectionConfig::new()
        .with_batch_size(4096)               // Rows per batch
        .with_memory_limit(1024 * 1024 * 512) // 512 MB limit
        .with_num_threads(4);                 // Parallel workers

    let conn = Connection::with_config(config)?;

    Ok(())
}
```

## Configuration Options

| Option | Default | Range | Description |
|--------|---------|-------|-------------|
| `batch_size` | 8,192 | 1 - 16,777,216 | Rows processed per batch |
| `memory_limit` | None | Any | Maximum memory usage in bytes |
| `num_threads` | None | 1+ | Parallel execution threads |

### Batch Size

Controls vectorization granularity. Larger batches improve throughput but use more memory:

```rust
// Smaller batches for memory-constrained environments
let config = ConnectionConfig::new().with_batch_size(1024);

// Larger batches for maximum throughput
let config = ConnectionConfig::new().with_batch_size(16384);
```

### Memory Limit

Prevents runaway queries from exhausting system memory:

```rust
// Limit to 1 GB
let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 1024);
```

When exceeded, queries fail with a `ResourceExhausted` error.

## Query Methods

### `query()` - Execute SELECT

Returns results as `Vec<RecordBatch>`:

```rust
let results = conn.query("SELECT * FROM users WHERE age > 25")?;

for batch in results {
    println!("Got {} rows", batch.num_rows());
    // Process batch...
}
```

### `execute()` - Execute DDL/DML

For statements that don't return data (CREATE, INSERT, UPDATE, DELETE):

```rust
// CREATE TABLE
conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;

// INSERT returns row count
let inserted = conn.execute("INSERT INTO users VALUES (1, 'Alice')")?;
println!("Inserted {} rows", inserted);

// UPDATE returns affected rows
let updated = conn.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;

// DELETE returns deleted rows
let deleted = conn.execute("DELETE FROM users WHERE id = 1")?;
```

## Table Management

### Register Tables

#### From Arrow RecordBatches

```rust
use arrow::record_batch::RecordBatch;

let batches: Vec<RecordBatch> = /* ... */;
conn.register_batches("my_table", batches)?;
```

#### From CSV Files

```rust
conn.register_csv("sales", "data/sales.csv")?;

// With options
use blaze::storage::CsvOptions;
let opts = CsvOptions::default().with_delimiter(b';');
conn.register_csv_with_options("data", "file.csv", opts)?;
```

#### From Parquet Files

```rust
conn.register_parquet("transactions", "data/transactions.parquet")?;
```

#### Custom Table Provider

```rust
use blaze::catalog::TableProvider;
use std::sync::Arc;

let provider: Arc<dyn TableProvider> = /* ... */;
conn.register_table("custom", provider)?;
```

### List Tables

```rust
let tables = conn.list_tables();
for name in tables {
    println!("Table: {}", name);
}
```

### Get Table Schema

```rust
if let Some(schema) = conn.table_schema("users") {
    for field in schema.fields() {
        println!("{}: {:?}", field.name(), field.data_type());
    }
}
```

### Deregister Tables

```rust
conn.deregister_table("old_table")?;
```

## Prepared Statements

Prepare queries for repeated execution with different parameters:

```rust
use blaze::ScalarValue;

// Prepare once
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

// Execute multiple times
let results1 = stmt.execute(&[ScalarValue::Int64(Some(1))])?;
let results2 = stmt.execute(&[ScalarValue::Int64(Some(2))])?;
```

### Cached Prepared Statements

Use a cache to avoid re-parsing frequently used queries:

```rust
use blaze::PreparedStatementCache;

// Create cache with capacity for 100 statements
let cache = PreparedStatementCache::new(100);

// First call parses and caches
let stmt1 = conn.prepare_cached("SELECT $1 + $2", &cache)?;

// Second call reuses cached plan
let stmt2 = conn.prepare_cached("SELECT $1 + $2", &cache)?;
```

## Working with Results

Query results are returned as `Vec<RecordBatch>`:

```rust
use arrow::util::pretty::print_batches;
use arrow::array::{Int64Array, StringArray};

let results = conn.query("SELECT id, name FROM users")?;

// Print formatted table
print_batches(&results)?;

// Access column data
for batch in &results {
    // Get column by index
    let id_col = batch.column(0);
    let ids = id_col.as_any().downcast_ref::<Int64Array>().unwrap();

    let name_col = batch.column(1);
    let names = name_col.as_any().downcast_ref::<StringArray>().unwrap();

    for i in 0..batch.num_rows() {
        println!("id={}, name={}", ids.value(i), names.value(i));
    }
}
```

## Error Handling

Blaze uses the `BlazeError` type for all errors:

```rust
use blaze::{BlazeError, Result};

fn run_query(conn: &Connection, sql: &str) -> Result<()> {
    match conn.query(sql) {
        Ok(results) => {
            println!("Query returned {} batches", results.len());
            Ok(())
        }
        Err(BlazeError::Parse { message, .. }) => {
            eprintln!("SQL syntax error: {}", message);
            Err(BlazeError::parse(message, None))
        }
        Err(BlazeError::Analysis(msg)) => {
            eprintln!("Query analysis error: {}", msg);
            Err(BlazeError::analysis(msg))
        }
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
            Err(e)
        }
    }
}
```

## Connection Lifecycle

```rust
fn main() -> Result<()> {
    // Create connection
    let conn = Connection::in_memory()?;

    // Set up tables
    conn.execute("CREATE TABLE data (value INT)")?;

    // Run queries
    conn.execute("INSERT INTO data VALUES (1), (2), (3)")?;
    let results = conn.query("SELECT SUM(value) FROM data")?;

    // Connection is dropped at end of scope
    // In-memory data is freed
    Ok(())
}
```

## Best Practices

### Reuse Connections

Creating connections has overhead. Reuse a single connection for multiple queries:

```rust
// Good - reuse connection
let conn = Connection::in_memory()?;
for query in queries {
    conn.query(&query)?;
}

// Avoid - creating new connection per query
for query in queries {
    let conn = Connection::in_memory()?;
    conn.query(&query)?;
}
```

### Use Prepared Statements for Repeated Queries

```rust
// Good - prepare once, execute many
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
for id in user_ids {
    let results = stmt.execute(&[ScalarValue::Int64(Some(id))])?;
}

// Less efficient - parsing each time
for id in user_ids {
    let query = format!("SELECT * FROM users WHERE id = {}", id);
    conn.query(&query)?;
}
```

### Set Memory Limits in Production

```rust
// Always set memory limits in production
let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 1024); // 1 GB

let conn = Connection::with_config(config)?;
```

## Next Steps

- [Query Execution](/docs/concepts/query-execution) - How queries are executed
- [Prepared Statements](/docs/guides/prepared-statements) - Advanced prepared statement usage
- [Error Handling](/docs/reference/error-handling) - Complete error reference
