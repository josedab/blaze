---
sidebar_position: 9
---

# Troubleshooting

This guide helps you diagnose and resolve common issues when using Blaze.

## Diagnostic Tools

### EXPLAIN for Query Analysis

Use `EXPLAIN` to understand how Blaze executes your query:

```sql
EXPLAIN SELECT * FROM users WHERE age > 25 AND city = 'NYC';
```

Output:
```
Filter: age > 25 AND city = 'NYC'
  TableScan: users
```

This helps identify:
- Whether filters are being pushed down
- Join order and strategy
- Projection columns

### Memory Tracking

Monitor memory usage programmatically:

```rust
use blaze::{Connection, ConnectionConfig};

let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 1024);  // 1 GB

let conn = Connection::with_config(config)?;

// Execute query and check if approaching limit
match conn.query("SELECT * FROM large_table") {
    Ok(results) => { /* success */ }
    Err(BlazeError::ResourceExhausted { limit, requested }) => {
        eprintln!("Memory limit exceeded: requested {} bytes, limit is {} bytes",
                  requested, limit);
    }
    Err(e) => { /* other error */ }
}
```

## Common Issues

### Table Not Found

**Error:**
```
BlazeError::Catalog { name: "users" }
```

**Causes and Solutions:**

1. **Table not registered:**
   ```rust
   // Ensure you register before querying
   conn.register_csv("users", "path/to/users.csv")?;
   conn.query("SELECT * FROM users")?;
   ```

2. **Typo in table name:**
   ```sql
   -- Wrong
   SELECT * FROM user;

   -- Correct
   SELECT * FROM users;
   ```

3. **Case sensitivity:**
   Table names are case-sensitive. `Users` and `users` are different tables.

4. **Connection scope:**
   Tables are registered per connection. Different connections don't share tables.
   ```rust
   let conn1 = Connection::in_memory()?;
   conn1.register_csv("data", "data.csv")?;

   let conn2 = Connection::in_memory()?;
   // conn2 does NOT have "data" table
   ```

### Column Not Found

**Error:**
```
BlazeError::Analysis("Column 'user_name' not found in any table")
```

**Causes and Solutions:**

1. **Typo in column name:**
   ```sql
   -- Check actual column names
   SELECT * FROM users LIMIT 1;
   ```

2. **Missing table alias:**
   ```sql
   -- Wrong (ambiguous)
   SELECT name FROM users u JOIN orders o ON u.id = o.user_id;

   -- Correct
   SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id;
   ```

3. **Column from wrong table in JOIN:**
   ```sql
   -- Wrong: customer_name is in customers, not orders
   SELECT customer_name FROM orders;

   -- Correct
   SELECT c.customer_name
   FROM orders o
   JOIN customers c ON o.customer_id = c.id;
   ```

### Type Mismatch

**Error:**
```
BlazeError::Type { expected: "Int64", actual: "Utf8" }
```

**Causes and Solutions:**

1. **Comparing incompatible types:**
   ```sql
   -- Wrong: comparing string to integer
   SELECT * FROM users WHERE id = '123';

   -- Correct
   SELECT * FROM users WHERE id = 123;
   -- or
   SELECT * FROM users WHERE id = CAST('123' AS BIGINT);
   ```

2. **Arithmetic on strings:**
   ```sql
   -- Wrong
   SELECT price + '10' FROM products;

   -- Correct
   SELECT price + 10 FROM products;
   SELECT price + CAST('10' AS DOUBLE) FROM products;
   ```

3. **Aggregate type requirements:**
   ```sql
   -- Wrong: SUM on strings
   SELECT SUM(name) FROM users;

   -- Correct
   SELECT SUM(amount) FROM orders;
   ```

### Parse Errors

**Error:**
```
BlazeError::Parse { message: "Expected ...", line: 1, column: 15 }
```

**Common causes:**

1. **Missing quotes around strings:**
   ```sql
   -- Wrong
   SELECT * FROM users WHERE name = Alice;

   -- Correct
   SELECT * FROM users WHERE name = 'Alice';
   ```

2. **Mismatched parentheses:**
   ```sql
   -- Wrong
   SELECT * FROM users WHERE (age > 25 AND city = 'NYC';

   -- Correct
   SELECT * FROM users WHERE (age > 25 AND city = 'NYC');
   ```

3. **Reserved word as identifier:**
   ```sql
   -- Wrong (ORDER is reserved)
   SELECT order FROM orders;

   -- Correct
   SELECT "order" FROM orders;
   ```

4. **Missing comma in SELECT:**
   ```sql
   -- Wrong
   SELECT id name email FROM users;

   -- Correct
   SELECT id, name, email FROM users;
   ```

### Memory Issues

**Error:**
```
BlazeError::ResourceExhausted { limit: 1073741824, requested: 1500000000 }
```

**Solutions:**

1. **Increase memory limit:**
   ```rust
   let config = ConnectionConfig::new()
       .with_memory_limit(4 * 1024 * 1024 * 1024);  // 4 GB
   ```

2. **Reduce query scope:**
   ```sql
   -- Add filters to reduce data
   SELECT * FROM events
   WHERE date >= '2024-01-01'
   LIMIT 100000;
   ```

3. **Select fewer columns:**
   ```sql
   -- Only select what you need
   SELECT id, name FROM large_table;
   -- Instead of
   SELECT * FROM large_table;
   ```

4. **Process in batches:**
   ```rust
   // Process date ranges separately
   for month in 1..=12 {
       let sql = format!(
           "SELECT * FROM events WHERE EXTRACT(MONTH FROM date) = {}",
           month
       );
       let results = conn.query(&sql)?;
       process_batch(results);
   }
   ```

### Slow Queries

**Symptoms:** Query takes longer than expected

**Diagnosis:**

1. **Check query plan:**
   ```sql
   EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id WHERE a.status = 'active';
   ```

2. **Identify bottlenecks:**
   - Full table scans without filters
   - Large cross joins
   - Complex string operations

**Solutions:**

1. **Add selective filters:**
   ```sql
   -- Filter high-selectivity columns first
   SELECT * FROM events
   WHERE event_type = 'click'  -- Filters 95% of rows
     AND date = '2024-01-15';
   ```

2. **Avoid large cross joins:**
   ```sql
   -- Wrong: creates massive intermediate result
   SELECT * FROM table_a CROSS JOIN table_b;

   -- Better: use JOIN with condition
   SELECT * FROM table_a a
   JOIN table_b b ON a.id = b.a_id;
   ```

3. **Use appropriate data types:**
   ```sql
   -- Comparing integers is faster than strings
   WHERE user_id = 123  -- Fast
   WHERE user_id = '123'  -- Slower (requires conversion)
   ```

4. **Pre-aggregate when possible:**
   ```sql
   -- Aggregate before joining
   SELECT c.name, totals.amount
   FROM customers c
   JOIN (
       SELECT customer_id, SUM(amount) as amount
       FROM orders
       GROUP BY customer_id
   ) totals ON c.id = totals.customer_id;
   ```

### File Loading Issues

**Error loading CSV:**
```
BlazeError::IO(std::io::Error { ... })
```

**Solutions:**

1. **Check file path:**
   ```rust
   // Use absolute path for clarity
   conn.register_csv("data", "/full/path/to/data.csv")?;
   ```

2. **Check file permissions:**
   ```bash
   ls -la /path/to/data.csv
   ```

3. **Verify file format:**
   ```rust
   // Specify CSV options if non-standard
   let options = CsvReadOptions::new()
       .with_delimiter(b';')  // Semicolon delimiter
       .with_has_header(false);
   conn.register_csv_with_options("data", "data.csv", options)?;
   ```

**Error loading Parquet:**
```
BlazeError::Parquet(...)
```

**Solutions:**

1. **Verify file is valid Parquet:**
   ```bash
   # Using parquet-tools
   parquet-tools schema file.parquet
   ```

2. **Check for nested types:**
   Blaze has limited support for deeply nested types. Flatten if possible.

3. **Ensure file isn't corrupted:**
   Try reading with another tool (pyarrow, pandas) first.

### Join Issues

**Error:**
```
BlazeError::NotImplemented("Decimal type not supported as join key")
```

**Solutions:**

1. **Cast decimal columns:**
   ```sql
   SELECT * FROM a
   JOIN b ON CAST(a.price AS DOUBLE) = CAST(b.price AS DOUBLE);
   ```

2. **Use supported join key types:**
   - Integers (Int8 through Int64, UInt32, UInt64)
   - Floats (Float32, Float64)
   - Strings (Utf8)
   - Dates and Timestamps
   - Boolean

**Cross join limit exceeded:**
```
BlazeError::Execution("Cross join would produce more than 10000000 rows")
```

**Solutions:**

1. **Add filters to reduce row count:**
   ```sql
   SELECT * FROM a
   CROSS JOIN b
   WHERE a.category = 'active' AND b.type = 'product';
   ```

2. **Use INNER JOIN with condition instead:**
   ```sql
   SELECT * FROM a
   JOIN b ON a.some_column = b.some_column;
   ```

### Window Function Issues

**Error:**
```
BlazeError::NotImplemented("Window function NTILE is not supported")
```

**Workarounds:**

For `NTILE`:
```sql
-- Approximate NTILE using ROW_NUMBER
SELECT
    *,
    CEILING(ROW_NUMBER() OVER (ORDER BY value) * 4.0 / COUNT(*) OVER ()) as quartile
FROM data;
```

For `PERCENT_RANK`:
```sql
-- Calculate manually
SELECT
    *,
    (RANK() OVER (ORDER BY value) - 1.0) / (COUNT(*) OVER () - 1) as percent_rank
FROM data;
```

## Python-Specific Issues

### Import Errors

```python
ImportError: No module named 'blaze'
```

**Solutions:**

1. **Install the package:**
   ```bash
   pip install blaze-db
   ```

2. **Build from source:**
   ```bash
   pip install maturin
   maturin develop --features python
   ```

### Type Conversion Errors

```python
TypeError: cannot convert 'numpy.int64' to Rust i64
```

**Solutions:**

1. **Convert numpy types:**
   ```python
   import numpy as np

   # Convert numpy types to Python types
   value = int(np.int64(42))
   ```

2. **Use Python native types in dictionaries:**
   ```python
   conn.from_dict("data", {
       'id': [int(x) for x in numpy_array],
       'value': [float(x) for x in float_array]
   })
   ```

### Memory Issues in Python

```python
blaze.ResourceExhaustedError: Memory limit exceeded
```

**Solutions:**

1. **Set memory limit:**
   ```python
   conn = blaze.Connection(memory_limit=4 * 1024 * 1024 * 1024)
   ```

2. **Process in chunks:**
   ```python
   offset = 0
   chunk_size = 100000

   while True:
       result = conn.query(f"""
           SELECT * FROM large_table
           LIMIT {chunk_size} OFFSET {offset}
       """)
       if result.num_rows() == 0:
           break
       process(result.to_pandas())
       offset += chunk_size
   ```

## Debug Checklist

When encountering issues, work through this checklist:

1. **Verify setup:**
   - [ ] Blaze version is correct
   - [ ] All dependencies installed
   - [ ] Files exist and are readable

2. **Check query:**
   - [ ] SQL syntax is valid
   - [ ] Table names are correct
   - [ ] Column names exist
   - [ ] Types are compatible

3. **Review resources:**
   - [ ] Memory limit is appropriate
   - [ ] File paths are correct
   - [ ] Data is in expected format

4. **Inspect execution:**
   - [ ] Use EXPLAIN to check plan
   - [ ] Add LIMIT for testing
   - [ ] Simplify query to isolate issue

5. **Gather info for bug report:**
   - [ ] Blaze version
   - [ ] Operating system
   - [ ] Minimal reproduction
   - [ ] Full error message

## Getting Help

If you can't resolve an issue:

1. **Search existing issues:** [GitHub Issues](https://github.com/blaze-db/blaze/issues)
2. **Ask a question:** [GitHub Discussions](https://github.com/blaze-db/blaze/discussions)
3. **Report a bug:** [Create Issue](https://github.com/blaze-db/blaze/issues/new)

When reporting, include:
- Blaze version (`cargo tree -p blaze` or `pip show blaze-db`)
- Operating system and version
- Minimal code to reproduce
- Complete error message
- What you expected vs. what happened
