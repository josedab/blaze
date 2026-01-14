---
sidebar_position: 1
---

# Working with Files

Blaze can query data directly from CSV, Parquet, and Delta Lake files. This guide covers file operations in detail.

## CSV Files

### Basic Usage

Register a CSV file as a queryable table:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register CSV file
    conn.register_csv("sales", "data/sales.csv")?;

    // Query it like any table
    let results = conn.query("
        SELECT product, SUM(quantity) as total_sold
        FROM sales
        GROUP BY product
    ")?;

    Ok(())
}
```

### CSV Options

Configure parsing behavior:

```rust
use blaze::{Connection, Result};
use blaze::storage::CsvOptions;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    let options = CsvOptions::default()
        .with_delimiter(b';')       // Semicolon separator
        .with_has_header(true)      // First row is header
        .with_quote(b'"')           // Quote character
        .with_null_value("NA");     // Treat "NA" as NULL

    conn.register_csv_with_options("data", "file.csv", options)?;

    Ok(())
}
```

### CSV Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` | Field separator |
| `has_header` | `true` | First row contains column names |
| `quote` | `"` | Character for quoting fields |
| `escape` | `None` | Escape character within quotes |
| `null_value` | `""` | String interpreted as NULL |
| `comment` | `None` | Character that starts comments |

### Type Inference

Blaze infers column types from CSV data:

| Detected Pattern | Inferred Type |
|------------------|---------------|
| Integer values | Int64 |
| Decimal numbers | Float64 |
| `true`/`false` | Boolean |
| Everything else | Utf8 (String) |

## Parquet Files

### Basic Usage

Parquet is the recommended format for large datasets:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register Parquet file
    conn.register_parquet("events", "data/events.parquet")?;

    // Query with projection pushdown
    let results = conn.query("
        SELECT event_type, COUNT(*)
        FROM events
        WHERE timestamp > '2024-01-01'
        GROUP BY event_type
    ")?;

    Ok(())
}
```

### Parquet Options

```rust
use blaze::{Connection, Result};
use blaze::storage::ParquetOptions;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    let options = ParquetOptions::default()
        .with_row_groups(vec![0, 1, 2]);  // Read specific row groups

    conn.register_parquet_with_options("data", "file.parquet", options)?;

    Ok(())
}
```

### Parquet Benefits

- **Columnar storage**: Only reads needed columns
- **Compression**: Efficient storage and fast I/O
- **Statistics**: Enables predicate pushdown
- **Schema**: Preserves exact types

### Projection Pushdown

Blaze reads only the columns you query:

```rust
// Only reads 'name' and 'email' columns from file
let results = conn.query("SELECT name, email FROM large_table")?;
```

### Predicate Pushdown

Filters can be pushed to the Parquet reader:

```rust
// Predicate applied during file scan
let results = conn.query("
    SELECT * FROM events
    WHERE date = '2024-01-15'
")?;
```

## Delta Lake

### Basic Usage

Query Delta Lake tables with ACID guarantees:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register Delta Lake table (directory path)
    conn.register_delta("transactions", "data/transactions_delta")?;

    // Query as normal table
    let results = conn.query("
        SELECT account_id, SUM(amount)
        FROM transactions
        GROUP BY account_id
    ")?;

    Ok(())
}
```

### Time Travel

Query historical versions of data:

```rust
// Register specific version
conn.register_delta_version("old_data", "data/table_delta", 5)?;

// Compare versions
let current = conn.query("SELECT COUNT(*) FROM transactions")?;
let historical = conn.query("SELECT COUNT(*) FROM old_data")?;
```

### Delta Lake Features

- **ACID transactions**: Consistent reads
- **Time travel**: Query historical data
- **Schema evolution**: Handle schema changes
- **Partition pruning**: Skip irrelevant data

## Querying Multiple Files

### Join Files

Join data across different file formats:

```rust
conn.register_parquet("orders", "data/orders.parquet")?;
conn.register_csv("products", "data/products.csv")?;

let results = conn.query("
    SELECT p.name, SUM(o.quantity) as total_ordered
    FROM orders o
    JOIN products p ON o.product_id = p.id
    GROUP BY p.name
    ORDER BY total_ordered DESC
")?;
```

### Union Files

Combine data from multiple files:

```rust
// Register multiple CSV files
conn.register_csv("jan_sales", "data/sales_jan.csv")?;
conn.register_csv("feb_sales", "data/sales_feb.csv")?;

// Union them
let results = conn.query("
    SELECT * FROM jan_sales
    UNION ALL
    SELECT * FROM feb_sales
")?;
```

## File Path Patterns

### Absolute Paths

```rust
conn.register_csv("data", "/home/user/data/file.csv")?;
```

### Relative Paths

```rust
conn.register_csv("data", "data/file.csv")?;  // Relative to working directory
```

### Home Directory

```rust
let path = dirs::home_dir().unwrap().join("data/file.parquet");
conn.register_parquet("data", path)?;
```

## Performance Tips

### Use Parquet for Large Files

Parquet provides significant advantages:

```rust
// Parquet: Fast columnar reads
conn.register_parquet("large_data", "data.parquet")?;

// CSV: Slower, reads entire file
conn.register_csv("large_data", "data.csv")?;
```

### Project Only Needed Columns

```rust
// Good: Only reads 2 columns
conn.query("SELECT id, name FROM large_table")?;

// Slow: Reads all columns
conn.query("SELECT * FROM large_table")?;
```

### Filter Early

```rust
// Good: Filter pushdown reduces data read
conn.query("SELECT * FROM events WHERE date = '2024-01-15'")?;
```

### Partition Data

For large datasets, use partitioned Parquet or Delta Lake:

```
data/
├── year=2023/
│   ├── month=01/
│   │   └── data.parquet
│   └── month=02/
│       └── data.parquet
└── year=2024/
    └── month=01/
        └── data.parquet
```

## Error Handling

### File Not Found

```rust
match conn.register_csv("data", "nonexistent.csv") {
    Ok(_) => println!("Registered"),
    Err(e) => eprintln!("Error: {}", e),  // File not found error
}
```

### Invalid Format

```rust
// Registering CSV as Parquet will fail
match conn.register_parquet("data", "file.csv") {
    Ok(_) => println!("Registered"),
    Err(e) => eprintln!("Invalid Parquet file: {}", e),
}
```

### Schema Mismatch

When joining files with incompatible schemas:

```rust
// Ensure join columns have compatible types
let results = conn.query("
    SELECT * FROM file_a a
    JOIN file_b b ON CAST(a.id AS VARCHAR) = b.id
")?;
```

## Next Steps

- [Memory Management](/docs/guides/memory-management) - Handle large files
- [SQL Reference](/docs/reference/sql) - Query syntax
- [Query Optimization](/docs/advanced/query-optimization) - Performance tuning
