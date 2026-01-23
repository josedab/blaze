---
sidebar_position: 7
---

# Known Limitations

This page documents current limitations and unimplemented features in Blaze. Understanding these will help you determine if Blaze is right for your use case.

## Query Features

### Multi-Statement Queries

Only the first statement in a query string is executed. Semicolon-separated statements after the first are ignored.

```sql
-- Only the first SELECT is executed
SELECT * FROM t1; SELECT * FROM t2  -- Second query is ignored
```

**Workaround:** Execute statements separately:

```rust
conn.query("SELECT * FROM t1")?;
conn.query("SELECT * FROM t2")?;
```

### Query Nesting Depth

Query nesting is limited to **128 levels** to prevent stack overflow. Deeply nested queries will fail with an error.

```sql
-- Very deep nesting may fail
SELECT * FROM (
    SELECT * FROM (
        SELECT * FROM (
            -- ... 128+ levels deep
        )
    )
)
```

## Join Limitations

### Cross Join Cardinality

Cross joins are limited to a maximum of **10 million rows** (10,000,000) to prevent accidental cartesian product explosions.

```sql
-- Will fail if left_table * right_table > 10,000,000 rows
SELECT * FROM left_table CROSS JOIN right_table
```

**Workaround:** Add filters or use explicit joins with conditions.

### Supported Join Key Types

The following types work as join keys:

| Supported | Not Supported |
|-----------|---------------|
| Boolean | Decimal |
| Int8, Int16, Int32, Int64 | Binary |
| UInt32, UInt64 | List/Array |
| Float32, Float64 | Struct |
| Utf8 (String) | |
| Date32, Date64 | |
| Timestamp (all units) | |

## Type System

### Decimal Support

Decimal types have limited support:

- Cannot be used as join keys
- Cannot be used in GROUP BY
- Some scalar functions may not support decimal inputs

**Workaround:** Cast decimals to Float64 for joins/grouping:

```sql
SELECT a.id, b.id
FROM table_a a
JOIN table_b b ON CAST(a.decimal_col AS DOUBLE) = CAST(b.decimal_col AS DOUBLE)
```

### Date/Time Functions

| Supported | Not Supported |
|-----------|---------------|
| `DATE_TRUNC`: year, month, day, hour, minute, second | Full timezone support |
| `EXTRACT`: year, month, day, hour, minute, second, dow, doy, week | `TO_DATE`, `TO_TIMESTAMP` |
| `CURRENT_DATE`, `CURRENT_TIMESTAMP` | `DATE_ADD`, `DATE_SUB` |

## Window Functions

### Supported Functions

```sql
ROW_NUMBER()
RANK()
DENSE_RANK()
LAG(column, offset, default)
LEAD(column, offset, default)
FIRST_VALUE(column)
LAST_VALUE(column)
-- Aggregate functions as window functions
SUM(...) OVER (...)
COUNT(...) OVER (...)
AVG(...) OVER (...)
MIN(...) OVER (...)
MAX(...) OVER (...)
```

### Not Supported

```sql
NTH_VALUE()      -- Not implemented
NTILE()          -- Not implemented
PERCENT_RANK()   -- Not implemented
CUME_DIST()      -- Not implemented
```

### Frame Specifications

Window frame specifications (`ROWS BETWEEN`, `RANGE BETWEEN`) have limited support. Default frames are used.

## Scalar Functions

### Implemented Functions

**String:**
- `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`
- `LENGTH`, `CONCAT`, `SUBSTRING`

**Math:**
- `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`

**Null Handling:**
- `COALESCE`, `NULLIF`

**Date/Time:**
- `CURRENT_DATE`, `CURRENT_TIMESTAMP`/`NOW`
- `EXTRACT`/`DATE_PART`, `DATE_TRUNC`

### Not Implemented

```sql
REPLACE(string, from, to)
SPLIT(string, delimiter)
REGEXP_MATCH, REGEXP_REPLACE
TO_DATE(string, format)
TO_TIMESTAMP(string, format)
DATE_ADD, DATE_SUB
GREATEST(a, b, ...)
LEAST(a, b, ...)
```

## Storage Limitations

### CSV Files

| Limitation | Description |
|------------|-------------|
| Encoding | UTF-8 only |
| Type Inference | Limited automatic type detection |
| Memory | Entire file read into memory |
| Write Support | Read-only |

### Parquet Files

| Limitation | Description |
|------------|-------------|
| Write Support | Read-only |
| Row Groups | All read into memory |
| Nested Types | Limited support |

### File Paths

- Must be valid UTF-8
- Must be accessible from the current process
- No glob patterns (register files individually)

## Performance Considerations

### Filter Pushdown

| Data Source | Filter Pushdown |
|-------------|-----------------|
| In-memory tables | Full support |
| CSV files | Applied after scan |
| Parquet files | Row group pruning only |

### Statistics

- In-memory tables: Full statistics (row count, distinct values, null counts)
- CSV files: Limited (computed on load)
- Parquet files: From file metadata only

### Memory Management

Memory limits are enforced via `ConnectionConfig::with_memory_limit()`:

```rust
let config = ConnectionConfig::new()
    .with_memory_limit(512 * 1024 * 1024);  // 512 MB
```

When the limit is exceeded:
1. New allocations are rejected
2. Current query fails with `BlazeError::ResourceExhausted`
3. Connection remains usable for smaller queries
4. Memory is tracked at batch level, not per-row

## API Limitations

### Thread Safety

`Connection` is **not thread-safe**. Each thread should create its own connection.

```rust
// Wrong - sharing connection across threads
let conn = Connection::in_memory()?;
let handle1 = thread::spawn(move || conn.query("..."));  // Error!

// Correct - each thread has its own connection
let handle1 = thread::spawn(|| {
    let conn = Connection::in_memory()?;
    conn.query("...")
});
```

### Batch Size Limits

Batch size must be between **1** and **16,777,216** rows.

```rust
// Valid
ConnectionConfig::new().with_batch_size(8192);

// Panics
ConnectionConfig::new().with_batch_size(0);
ConnectionConfig::new().with_batch_size(20_000_000);
```

## SQL Compatibility

### DDL Statements

Data Definition Language is supported for in-memory tables:

```sql
-- Supported
CREATE TABLE users (id INT, name VARCHAR);
DROP TABLE users;

-- Not supported
ALTER TABLE users ADD COLUMN email VARCHAR;
CREATE INDEX idx ON users(id);
```

You can also use programmatic table registration for file-based tables:

```rust
conn.register_csv("users", "users.csv")?;
conn.register_parquet("events", "events.parquet")?;
conn.register_batches("data", record_batches)?;
```

### DML Statements

Data Manipulation Language is supported **only for in-memory tables** created via `CREATE TABLE`:

```sql
-- Supported for in-memory tables
CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice');
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

DML is **not supported** for external data sources (CSV, Parquet, Delta Lake files):

```rust
// This won't work - CSV files are read-only
conn.register_csv("users", "users.csv")?;
conn.execute("INSERT INTO users VALUES (2, 'Carol')")?;  // Error!
```

### Prepared Statements

Prepared statements support positional parameters (`$1`, `$2`, etc.):

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let results = stmt.execute(&[ScalarValue::Int64(Some(42))])?;
```

Limitations:
- Named parameters (`:name`, `@name`) are not supported
- The `?` placeholder syntax is not supported
- Type inference for parameters is limited; explicit casts may be needed

### Transactions

No transaction support:

```sql
-- Not supported
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;
```

## CLI Limitations

The CLI (`cargo run`) is a simple REPL for testing and development. It is **not intended for production use**.

- No command history persistence
- No multi-line query support
- No result pagination
- No output formatting options

## Workarounds Summary

| Limitation | Workaround |
|------------|------------|
| Multi-statement queries | Execute separately |
| Deep nesting | Flatten with CTEs |
| Large cross joins | Add filters |
| Decimal join keys | Cast to Float64 |
| Missing functions | Implement custom expressions |
| Thread safety | One connection per thread |
| DML on external files | Use in-memory tables, then export |
| Persistence | Export to Parquet, reload |

## Roadmap

The following features are planned for future releases:

- [ ] Decimal type support in joins and GROUP BY
- [ ] Additional window functions (NTH_VALUE, NTILE, PERCENT_RANK, CUME_DIST)
- [ ] Parquet write support
- [ ] Transaction support
- [ ] Thread-safe connection pool
- [ ] More date/time functions (DATE_ADD, DATE_SUB, TO_DATE, TO_TIMESTAMP)
- [ ] Additional scalar functions (REPLACE, SPLIT, REGEXP_*, GREATEST, LEAST)
- [ ] Filter pushdown for CSV and Parquet files
- [ ] Named parameters in prepared statements

See [GitHub Issues](https://github.com/blaze-db/blaze/issues) for the latest roadmap.
