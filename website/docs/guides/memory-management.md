---
sidebar_position: 5
---

# Memory Management

Blaze processes data in memory using Apache Arrow's columnar format. This guide covers memory configuration, monitoring, and optimization.

## Memory Architecture

Blaze uses Arrow's memory model:

```
┌───────────────────────────────────────────────────┐
│                   Connection                       │
│  ┌─────────────────────────────────────────────┐  │
│  │             Memory Manager                   │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │  │
│  │  │ Tracked │  │  Limit  │  │  Allocator  │ │  │
│  │  │  Usage  │  │ Enforcer│  │   (Arrow)   │ │  │
│  │  └─────────┘  └─────────┘  └─────────────┘ │  │
│  └─────────────────────────────────────────────┘  │
│                        ↓                          │
│  ┌─────────────────────────────────────────────┐  │
│  │              RecordBatches                   │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │  │
│  │  │ Column1 │  │ Column2 │  │   Column3   │ │  │
│  │  │ (Array) │  │ (Array) │  │   (Array)   │ │  │
│  │  └─────────┘  └─────────┘  └─────────────┘ │  │
│  └─────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────┘
```

## Configuration

### Setting Memory Limits

Prevent runaway queries from exhausting memory:

```rust
use blaze::{Connection, ConnectionConfig, Result};

fn main() -> Result<()> {
    let config = ConnectionConfig::new()
        .with_memory_limit(1024 * 1024 * 1024); // 1 GB

    let conn = Connection::with_config(config)?;

    // Queries that exceed limit will fail with ResourceExhausted error
    Ok(())
}
```

### Memory Limit Behavior

When the memory limit is exceeded:

1. Current allocation is rejected
2. Query fails with `ResourceExhausted` error
3. Existing data remains intact
4. Connection is still usable for smaller queries

```rust
use blaze::{Connection, ConnectionConfig, BlazeError};

fn main() {
    let config = ConnectionConfig::new()
        .with_memory_limit(100 * 1024 * 1024); // 100 MB

    let conn = Connection::with_config(config).unwrap();
    conn.register_parquet("huge", "very_large_file.parquet").unwrap();

    match conn.query("SELECT * FROM huge") {
        Ok(results) => println!("Got {} batches", results.len()),
        Err(BlazeError::ResourceExhausted { message }) => {
            eprintln!("Memory limit exceeded: {}", message);
            // Connection still usable for smaller queries
        }
        Err(e) => eprintln!("Other error: {}", e),
    }
}
```

### Batch Size

Control memory granularity with batch size:

```rust
let config = ConnectionConfig::new()
    .with_batch_size(4096);  // Smaller batches = lower peak memory
```

| Batch Size | Peak Memory | Throughput |
|------------|-------------|------------|
| 1,024 | Lower | Lower |
| 8,192 (default) | Balanced | Balanced |
| 32,768 | Higher | Higher |

## Memory-Efficient Queries

### Project Only Needed Columns

```sql
-- Good: reads only 2 columns
SELECT id, name FROM large_table

-- Bad: reads all columns
SELECT * FROM large_table
```

Memory impact: 50-column table × 1M rows
- `SELECT *`: ~400 MB
- `SELECT id, name`: ~16 MB

### Use LIMIT for Exploration

```sql
-- Good: limits result size
SELECT * FROM huge_table LIMIT 1000

-- Bad: returns everything
SELECT * FROM huge_table
```

### Filter Early

```sql
-- Good: filter reduces intermediate data
SELECT * FROM events WHERE date = '2024-01-15'

-- Bad: process everything then filter
-- (though optimizer may push down filters)
```

### Aggregate Instead of Returning All Rows

```sql
-- Good: returns small aggregated result
SELECT category, COUNT(*), AVG(price)
FROM products
GROUP BY category

-- Bad: returns millions of rows
SELECT * FROM products
```

## Memory-Intensive Operations

### Sorting

Sorting requires holding all data in memory:

```sql
-- High memory: sorts entire result
SELECT * FROM large_table ORDER BY value

-- Better: sort on server with LIMIT
SELECT * FROM large_table ORDER BY value LIMIT 1000
```

### Hash Joins

Hash joins build a hash table from one side:

```sql
-- Put smaller table on right (build side)
SELECT * FROM big_table b
JOIN small_lookup l ON b.key = l.key

-- Optimizer usually chooses correctly, but order matters for hints
```

### Aggregations

Hash aggregations use memory proportional to distinct groups:

```sql
-- Low memory: few groups
SELECT region, SUM(sales) FROM data GROUP BY region

-- High memory: many groups (e.g., user_id)
SELECT user_id, SUM(activity) FROM events GROUP BY user_id
```

### Window Functions

Window functions may buffer entire partitions:

```sql
-- Memory per partition
SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date)
FROM events

-- Large partitions = high memory
```

## Monitoring Memory Usage

### In Code

```rust
use blaze::{Connection, ConnectionConfig};

let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 512);

let conn = Connection::with_config(config)?;

// Memory is tracked automatically
// Exceeding limit returns ResourceExhausted error
```

### Result Size Estimation

Estimate memory before querying:

```rust
// Check row count first
let count_result = conn.query("SELECT COUNT(*) FROM large_table")?;

// Estimate based on schema
let schema = conn.table_schema("large_table").unwrap();
let estimated_row_size: usize = schema.fields().iter()
    .map(|f| estimate_field_size(f.data_type()))
    .sum();

// If estimated_rows * estimated_row_size > memory_limit, use LIMIT
```

## Best Practices

### 1. Always Set Memory Limits in Production

```rust
// Production configuration
let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 1024)  // 1 GB
    .with_batch_size(8192);

let conn = Connection::with_config(config)?;
```

### 2. Use Appropriate Batch Sizes

```rust
// Memory-constrained environment
let config = ConnectionConfig::new()
    .with_batch_size(2048)
    .with_memory_limit(256 * 1024 * 1024); // 256 MB

// High-throughput environment
let config = ConnectionConfig::new()
    .with_batch_size(16384)
    .with_memory_limit(4 * 1024 * 1024 * 1024); // 4 GB
```

### 3. Process Results Incrementally

```rust
let results = conn.query("SELECT * FROM medium_table")?;

for batch in results {
    // Process batch
    process_batch(&batch)?;
    // Batch memory freed after iteration
}
// All batch memory freed here
```

### 4. Use Streaming for Large Results

```rust
// Instead of loading everything
let all_results = conn.query("SELECT * FROM huge_table")?;

// Process in chunks
let mut offset = 0;
let chunk_size = 10000;

loop {
    let chunk = conn.query(&format!(
        "SELECT * FROM huge_table LIMIT {} OFFSET {}",
        chunk_size, offset
    ))?;

    if chunk.is_empty() || chunk[0].num_rows() == 0 {
        break;
    }

    process_chunk(&chunk)?;
    offset += chunk_size;
}
```

### 5. Profile Memory Usage

```rust
// Test queries with small limits first
let test_config = ConnectionConfig::new()
    .with_memory_limit(100 * 1024 * 1024); // 100 MB

let test_conn = Connection::with_config(test_config)?;

match test_conn.query("SELECT * FROM table LIMIT 10000") {
    Ok(_) => println!("Query fits in 100 MB"),
    Err(_) => println!("Query needs more than 100 MB"),
}
```

## Troubleshooting

### ResourceExhausted Errors

**Problem**: Query fails with memory error

**Solutions**:
1. Increase memory limit
2. Add LIMIT clause
3. Project fewer columns
4. Add filters to reduce data
5. Use smaller batch size

### Slow Performance with Small Batches

**Problem**: Query is slow despite adequate memory

**Solutions**:
1. Increase batch size
2. Check for unnecessary sorting
3. Ensure predicate pushdown is working

### Out of Memory (OOM) Kills

**Problem**: Process killed by OS

**Solutions**:
1. Set memory limit below system memory
2. Leave headroom for OS and other processes
3. Monitor system memory during queries

## Next Steps

- [Configuration](/docs/reference/configuration) - All configuration options
- [Query Optimization](/docs/advanced/query-optimization) - Performance tuning
- [Error Handling](/docs/reference/error-handling) - Error types
