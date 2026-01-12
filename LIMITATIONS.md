# Blaze Query Engine - Known Limitations

This document lists current limitations and unimplemented features in Blaze.

## Query Features

### Query Placeholders

Query placeholders (`$1`, `$2`, `?`) are not supported. Instead, use string formatting to substitute values:

```rust
// Not supported:
// conn.query("SELECT * FROM users WHERE id = $1")

// Do this instead:
let id = 42;
conn.query(&format!("SELECT * FROM users WHERE id = {}", id))?;
```

### Multi-Statement Queries

Only the first statement in a query string is executed. Semicolon-separated statements after the first are ignored.

```rust
// Only the first SELECT is executed
conn.query("SELECT * FROM t1; SELECT * FROM t2")?;
```

### Query Nesting Depth

Query nesting is limited to 128 levels to prevent stack overflow. Deeply nested queries will fail with an error.

## Join Limitations

### Cross Join Cardinality

Cross joins are limited to a maximum of 10 million rows (10,000,000) to prevent accidental cartesian product explosions. Larger cross joins will return an error.

### Join Key Types

The following types are supported as join keys:
- Boolean
- Int8, Int16, Int32, Int64
- UInt32, UInt64
- Float32, Float64
- Utf8 (String)
- Date32, Date64
- Timestamp (all time units)

**Not supported as join keys:**
- Decimal types
- Binary types
- List/Array types
- Struct types

## Type System

### Decimal Support

Decimal types have limited support:
- Cannot be used as join keys
- Cannot be used in GROUP BY
- Some scalar functions may not support decimal inputs

### Date/Time Functions

Date/time functions have the following limitations:
- `DATE_TRUNC` supports: year, month, day, hour, minute, second
- `EXTRACT` supports: year, month, day, hour, minute, second, dow (day of week), doy (day of year), week
- Timezone-aware operations are limited

## Performance Considerations

### Filter Pushdown

Filter pushdown is fully implemented for in-memory tables. For CSV and Parquet files, filters are applied after the scan.

### Statistics

Table statistics (row counts, distinct values, null counts) are computed for in-memory tables. CSV and Parquet file statistics may be limited depending on file metadata.

### Memory Management

Memory limits are enforced via `ConnectionConfig::with_memory_limit()`. When the limit is exceeded:
- New allocations are rejected
- Current query fails with an error
- Memory is tracked at the batch level, not per-row

## Window Functions

### Supported Functions

- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG()`
- `LEAD()`
- `FIRST_VALUE()`
- `LAST_VALUE()`
- Aggregate functions as window functions (SUM, COUNT, AVG, etc.)

### Not Supported

- `NTH_VALUE()`
- `NTILE()`
- `PERCENT_RANK()`
- `CUME_DIST()`

### Frame Specifications

Window frame specifications (ROWS BETWEEN, RANGE BETWEEN) have limited support.

## Storage

### CSV Files

- UTF-8 encoding only
- Limited type inference
- Large files are read entirely into memory

### Parquet Files

- Reading only (no write support)
- All row groups are read into memory

### File Paths

File paths must be valid UTF-8 and accessible from the current process.

## Scalar Functions

### Implemented

- String: `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `CONCAT`, `SUBSTRING`
- Math: `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`
- Null handling: `COALESCE`, `NULLIF`
- Date/Time: `CURRENT_DATE`, `CURRENT_TIMESTAMP`/`NOW`, `EXTRACT`/`DATE_PART`, `DATE_TRUNC`

### Not Implemented

- `REPLACE`
- `SPLIT`
- `REGEXP_*` functions
- `TO_DATE`, `TO_TIMESTAMP` (parsing)
- `DATE_ADD`, `DATE_SUB`
- `GREATEST`, `LEAST`

## Operators

### Bitwise Operators

Bitwise operators (`&`, `|`, `^`, `~`) are supported for integer types only.

## Error Handling

### Error Messages

Some internal errors may expose implementation details. Error messages are continuously being improved.

### Recovery

Failed queries do not roll back any state changes (there are no transactions).

## API

### Thread Safety

`Connection` is not thread-safe. Each thread should create its own connection.

### Batch Size

Batch size must be between 1 and 16,777,216 rows.

## CLI

The CLI (`cargo run`) is a simple REPL for testing. It is not intended for production use.

## Future Improvements

The following features are planned for future releases:

- [ ] Prepared statements with parameter binding
- [ ] Decimal type support in joins
- [ ] Additional window functions
- [ ] Parquet write support
- [ ] Transaction support
- [ ] Thread-safe connection pool
- [ ] More comprehensive date/time functions
