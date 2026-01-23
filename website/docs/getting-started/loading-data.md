---
sidebar_position: 3
---

# Loading Data

Blaze supports multiple ways to load data into your database. This guide covers all available options.

## In-Memory Tables

Create tables directly with SQL and insert data:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create table
    conn.execute("
        CREATE TABLE events (
            id INT,
            event_type VARCHAR,
            timestamp VARCHAR,
            value DOUBLE
        )
    ")?;

    // Insert single row
    conn.execute("INSERT INTO events VALUES (1, 'click', '2024-01-15', 1.5)")?;

    // Insert multiple rows
    conn.execute("
        INSERT INTO events VALUES
        (2, 'view', '2024-01-15', 2.0),
        (3, 'click', '2024-01-16', 1.8),
        (4, 'purchase', '2024-01-16', 99.99)
    ")?;

    Ok(())
}
```

## CSV Files

Register CSV files as queryable tables:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Basic CSV registration (auto-detects schema)
    conn.register_csv("sales", "data/sales.csv")?;

    // Query the CSV file
    let results = conn.query("SELECT * FROM sales LIMIT 10")?;

    Ok(())
}
```

### CSV Options

Configure CSV parsing with custom options:

```rust
use blaze::{Connection, Result};
use blaze::storage::CsvOptions;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    let options = CsvOptions::default()
        .with_delimiter(b';')           // Semicolon delimiter
        .with_has_header(true)          // First row is header
        .with_quote(b'"');              // Quote character

    conn.register_csv_with_options("data", "file.csv", options)?;

    Ok(())
}
```

Available CSV options:

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` | Field separator character |
| `has_header` | `true` | Whether first row contains column names |
| `quote` | `"` | Quote character for fields |
| `escape` | `None` | Escape character |
| `null_value` | `""` | String to interpret as NULL |

## Parquet Files

Register Parquet files for efficient columnar queries:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register Parquet file
    conn.register_parquet("transactions", "data/transactions.parquet")?;

    // Parquet files often have many columns - project only what you need
    let results = conn.query("
        SELECT user_id, amount, timestamp
        FROM transactions
        WHERE amount > 100
    ")?;

    Ok(())
}
```

### Parquet Options

Configure Parquet reading:

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

## Arrow RecordBatches

Register Arrow RecordBatches directly for zero-copy integration:

```rust
use blaze::{Connection, Result};
use arrow::array::{Int64Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));

    // Create batch
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.0), None])),
        ],
    )?;

    // Register as table
    conn.register_batches("scores", vec![batch])?;

    // Query normally
    let results = conn.query("
        SELECT name, score
        FROM scores
        WHERE score IS NOT NULL
        ORDER BY score DESC
    ")?;

    Ok(())
}
```

## Delta Lake Tables

Query Delta Lake tables with ACID guarantees and time travel:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register Delta Lake table
    conn.register_delta("events", "data/events_delta")?;

    // Query as normal table
    let results = conn.query("SELECT * FROM events WHERE date = '2024-01-15'")?;

    // Time travel to specific version
    conn.register_delta_version("events_v1", "data/events_delta", 1)?;

    Ok(())
}
```

## Custom Table Providers

Implement the `TableProvider` trait for custom data sources:

```rust
use blaze::{Connection, Result};
use blaze::catalog::TableProvider;
use blaze::types::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

struct MyDataSource {
    schema: Schema,
    // Your data source fields
}

impl TableProvider for MyDataSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn blaze::planner::PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Implement data reading logic
        todo!()
    }
}

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register custom provider
    // conn.register_table("custom", Arc::new(my_source))?;

    Ok(())
}
```

## Data Type Mapping

When loading data, Blaze maps types as follows:

| Source Format | SQL Type | Arrow Type |
|---------------|----------|------------|
| CSV integer | INT | Int64 |
| CSV decimal | DOUBLE | Float64 |
| CSV text | VARCHAR | Utf8 |
| Parquet INT32 | INT | Int32 |
| Parquet INT64 | BIGINT | Int64 |
| Parquet FLOAT | REAL | Float32 |
| Parquet DOUBLE | DOUBLE | Float64 |
| Parquet STRING | VARCHAR | Utf8 |
| Parquet BOOLEAN | BOOLEAN | Boolean |
| Parquet DATE | DATE | Date32 |
| Parquet TIMESTAMP | TIMESTAMP | Timestamp |

## Best Practices

### Use Parquet for Large Datasets

Parquet provides better compression and columnar access patterns:

```rust
// Parquet only reads columns you need
let results = conn.query("SELECT name, amount FROM large_table")?;
// Only 'name' and 'amount' columns are read from disk
```

### Project Only Required Columns

Always specify the columns you need rather than `SELECT *`:

```rust
// Good - only reads two columns
conn.query("SELECT user_id, amount FROM transactions")?;

// Avoid - reads all columns even if unused
conn.query("SELECT * FROM transactions")?;
```

### Use Filters Early

Push filters down to reduce data volume:

```rust
// Good - filter is applied during scan
conn.query("SELECT * FROM events WHERE date = '2024-01-15'")?;

// Less efficient - processes all data then filters
conn.query("SELECT * FROM events")?;  // Then filter in application
```

## Next Steps

- [Working with Files](/docs/guides/working-with-files) - Advanced file operations
- [Memory Management](/docs/guides/memory-management) - Control memory usage
- [SQL Reference](/docs/reference/sql) - Complete SQL syntax
