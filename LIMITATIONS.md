# Blaze Query Engine - Known Limitations

This document lists current limitations and unimplemented features in Blaze.

## Query Features

### DML Operations (INSERT, UPDATE, DELETE)

DML operations are only supported for in-memory tables created via `CREATE TABLE`. External data sources (CSV, Parquet files) are read-only.

```rust
// In-memory tables support full DML
conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;
conn.execute("INSERT INTO users VALUES (1, 'Alice')")?;
conn.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;
conn.execute("DELETE FROM users WHERE id = 1")?;

// CSV/Parquet files are read-only
conn.register_csv("sales", "data/sales.csv")?;
// conn.execute("DELETE FROM sales WHERE ..."); // ERROR: Not supported
```

### Multi-Statement Queries

Only the first statement in a query string is executed. Semicolon-separated statements after the first are ignored.

```rust
// Only the first SELECT is executed
conn.query("SELECT * FROM t1; SELECT * FROM t2")?;
```

### Prepared Statements

Prepared statements support positional parameters (`$1`, `$2`, etc.) but have some limitations:

- Named parameters (`:name`, `@name`) are not supported
- The `?` placeholder syntax is not supported (use `$1`, `$2`, etc.)
- Type inference for parameters is limited; explicit casts may be needed

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

Filter pushdown is implemented for in-memory tables, CSV files, and Parquet files.

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
- `NTILE()`
- `PERCENT_RANK()`
- `CUME_DIST()`
- `NTH_VALUE()`
- Aggregate functions as window functions (SUM, COUNT, AVG, etc.)

### Not Supported

- Named window references (`WINDOW w AS (...)`)

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

- String: `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `CONCAT`, `SUBSTRING`, `REPLACE`, `REVERSE`, `LPAD`, `RPAD`, `SPLIT_PART`, `LEFT`, `RIGHT`, `INITCAP`, `REPEAT`, `TRANSLATE`, `ASCII`, `CHR`, `POSITION`/`STRPOS`, `STARTS_WITH`, `ENDS_WITH`
- Math: `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `POWER`/`POW`, `SQRT`, `EXP`, `LN`, `LOG`, `SIGN`, `MOD`/`MODULO`
- Regex: `REGEXP_MATCH`, `REGEXP_REPLACE`, `REGEXP_EXTRACT`
- Hash: `MD5`, `SHA256`/`SHA2`
- Null handling: `COALESCE`, `NULLIF`, `IFNULL`/`NVL`, `GREATEST`, `LEAST`
- Date/Time: `CURRENT_DATE`, `CURRENT_TIMESTAMP`/`NOW`, `EXTRACT`/`DATE_PART`, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DATE_ADD`/`DATEADD`, `DATE_SUB`/`DATESUB`, `DATE_DIFF`/`DATEDIFF`

### Not Yet Implemented

- `SPLIT` (use `SPLIT_PART` with index)
- Full `REGEXP_*` family with flags

## Operators

### Bitwise Operators

Bitwise operators (`&`, `|`, `^`, `~`) are supported for integer types only.

## Error Handling

### Error Messages

Some internal errors may expose implementation details. Error messages are continuously being improved.

### Recovery

Full transaction support is available via SQL `BEGIN`/`COMMIT`/`ROLLBACK` statements, programmatic `begin_transaction`/`commit_transaction`/`rollback_transaction` API, and `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` for fine-grained control. Full MVCC snapshot isolation is provided by the `TransactionManager`.

## API

### Thread Safety

`Connection` is not thread-safe. Each thread should create its own connection.

### Batch Size

Batch size must be between 1 and 16,777,216 rows.

## CLI

The CLI (`cargo run`) is a simple REPL for testing. It is not intended for production use.

## Future Improvements

The following features are planned for future releases:

- [ ] Decimal type support in joins and GROUP BY
- [ ] Parquet write support
- [x] SQL-level transaction statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- [ ] Thread-safe connection pool
- [x] Date/time functions (DATE_ADD, DATE_SUB, TO_DATE, TO_TIMESTAMP)
- [x] Additional scalar functions (REPLACE, REGEXP_*, GREATEST, LEAST)
- [x] Filter pushdown for CSV and Parquet files
- [ ] `SPLIT` function (currently use `SPLIT_PART` with index)
- [ ] Full `REGEXP_*` family with flags support
