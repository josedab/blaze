---
sidebar_position: 10
---

# Changelog

All notable changes to Blaze are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### SQL Functions
- **String Functions**: REPLACE, LEFT, RIGHT, LPAD, RPAD, REVERSE, SPLIT_PART, REGEXP_MATCH, REGEXP_REPLACE
- **Date/Time Functions**: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_SUB, DATE_DIFF, TO_DATE, TO_TIMESTAMP
- **Utility Functions**: GREATEST, LEAST, IFNULL, NVL
- **Window Functions**: NTILE, PERCENT_RANK, CUME_DIST, NTH_VALUE

#### CLI Enhancements
- SQL file execution: `blaze script.sql`
- Direct query execution: `blaze -c "SELECT 1"`
- Output formats: `--format json|csv|table|arrow`
- Output to file: `--output results.csv`
- Session commands: `.timer`, `.mode`, `.output`

#### COPY TO Syntax
- Export query results to Parquet: `COPY (SELECT ...) TO 'file.parquet'`
- Export to CSV: `COPY (SELECT ...) TO 'file.csv' WITH (FORMAT CSV)`
- Export to JSON: `COPY (SELECT ...) TO 'file.json' WITH (FORMAT JSON)`

#### Language Bindings
- **WASM Package** (`@blaze-sql/wasm`): Run Blaze in the browser
- **Node.js Package** (`@blaze-sql/node`): Node.js wrapper with filesystem integration
- **C FFI**: C header file and FFI bindings for embedding in other languages

#### Developer Experience
- Comprehensive benchmark suite with Criterion
- Python Jupyter notebook example with PyArrow/Pandas/Polars integration
- Interactive browser playground
- Advanced query examples (window functions, CTEs, subqueries)
- Error message suggestions using edit distance ("Did you mean...?")

#### Documentation
- Documentation website with Docusaurus
- Comprehensive troubleshooting guide
- Production best practices guide
- Community section on landing page

### Changed
- Updated Quick Start guide with clearer examples for file registration and Arrow data
- Improved README with accurate feature descriptions and prepared statements examples
- Enhanced ARCHITECTURE.md with prepared statements documentation
- Updated LIMITATIONS.md to accurately document DDL/DML support (in-memory tables only)
- Clarified that INSERT/UPDATE/DELETE only work with in-memory tables

### Fixed
- Fixed documentation inconsistencies about CREATE TABLE and DML support
- Fixed LIMITATIONS.md incorrectly stating that prepared statements aren't supported

## [0.1.0] - 2025-01-15

### Added

#### Core Features
- **SQL Parser** with SQL:2016 compliance
  - SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
  - JOIN support: INNER, LEFT, RIGHT, FULL OUTER, CROSS, SEMI, ANTI
  - Subqueries and CTEs (WITH clause)
  - Set operations: UNION, INTERSECT, EXCEPT

- **Query Planner**
  - Logical plan generation
  - Physical plan generation
  - Rule-based query optimization
    - Predicate pushdown
    - Projection pushdown
    - Constant folding
    - Expression simplification

- **Query Executor**
  - Vectorized batch execution
  - Hash-based joins
  - Hash-based aggregations
  - Sorting with multiple keys
  - Window functions

- **Expression System**
  - Arithmetic expressions: `+`, `-`, `*`, `/`, `%`
  - Comparison expressions: `=`, `<>`, `<`, `>`, `<=`, `>=`
  - Logical expressions: `AND`, `OR`, `NOT`
  - CASE expressions
  - BETWEEN expressions
  - LIKE pattern matching
  - IN list expressions
  - CAST expressions
  - IS NULL / IS NOT NULL

- **Aggregate Functions**
  - COUNT, COUNT(DISTINCT)
  - SUM, AVG
  - MIN, MAX

- **Window Functions**
  - ROW_NUMBER
  - RANK, DENSE_RANK
  - LAG, LEAD
  - FIRST_VALUE, LAST_VALUE
  - Aggregate functions as window functions

- **Scalar Functions**
  - String: UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, CONCAT, SUBSTRING
  - Math: ABS, ROUND, CEIL, FLOOR
  - Null: COALESCE, NULLIF
  - Date: CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT, DATE_TRUNC

- **Storage**
  - In-memory tables with Arrow RecordBatch
  - CSV file reading
  - Parquet file reading
  - Delta Lake support (via delta-rs)

- **Connection API**
  - `Connection::in_memory()` - create in-memory connection
  - `Connection::with_config()` - create with configuration
  - `Connection::query()` - execute SQL query
  - `Connection::execute()` - execute SQL statement
  - `Connection::register_csv()` - register CSV file
  - `Connection::register_parquet()` - register Parquet file
  - `Connection::register_batches()` - register Arrow data
  - `Connection::register_table()` - register custom table provider

- **Prepared Statements**
  - `Connection::prepare()` - prepare SQL statement
  - `PreparedStatement::execute()` - execute with parameters
  - Parameter binding for common types
  - Statement caching

- **Configuration**
  - `ConnectionConfig` for connection settings
  - Batch size configuration
  - Memory limit configuration
  - Thread count configuration
  - CSV options (delimiter, header, quote, escape)
  - Parquet options (row groups)

- **Python Bindings**
  - PyO3-based Python module
  - Connection management
  - Query execution
  - Arrow integration
  - Pandas DataFrame support

- **Type System**
  - Boolean
  - Int8, Int16, Int32, Int64
  - UInt8, UInt16, UInt32, UInt64
  - Float32, Float64
  - Utf8 (String)
  - Date32, Date64
  - Timestamp (with time units)
  - Decimal128

- **Error Handling**
  - Comprehensive `BlazeError` type
  - Detailed error messages
  - Error location tracking for parse errors

#### Developer Experience
- CLI REPL for interactive queries
- EXPLAIN for query plan inspection
- Comprehensive test suite
- Example code

### Known Limitations

See [Limitations](/docs/limitations) for current known limitations.

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.1.0 | 2025-01-15 | Initial release |

## Upgrading

### From Pre-release to 0.1.0

If you were using a pre-release version:

```rust
// Old API (pre-release)
let conn = Connection::new();

// New API (0.1.0)
let conn = Connection::in_memory()?;
```

## Deprecations

No deprecations in the current version.

## Security Updates

No security updates in the current version.

---

## Future Roadmap

Features planned for future releases:

### Short Term
- [x] Additional window functions (NTILE, PERCENT_RANK, CUME_DIST) - **Completed**
- [x] More scalar functions (REPLACE, REGEXP_*) - **Completed**
- [ ] Improved Parquet filter pushdown

### Medium Term
- [x] Parquet write support (via COPY TO) - **Completed**
- [ ] Persistent storage option
- [ ] Thread-safe connection pool
- [x] WebAssembly support - **Completed**

### Long Term
- [ ] Distributed query execution
- [ ] Transaction support
- [ ] Automatic indexing
- [ ] Query caching

## Contributing

See [Contributing](/docs/contributing) for how to contribute to Blaze.

## License

Blaze is released under the MIT License.
