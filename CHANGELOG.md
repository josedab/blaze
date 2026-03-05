# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GROUPING SETS, CUBE, ROLLUP with GROUPING() bitmask function
- Correlated subquery support: EXISTS, NOT EXISTS, IN, NOT IN, scalar subqueries
- LATERAL join support with correlated column references
- INTERSECT and EXCEPT set operations with ALL variant
- String concatenation via `||` operator
- ORDER BY and GROUP BY positional column references (ORDER BY 1, GROUP BY 1)
- COUNT(DISTINCT) with type-specific hashing (integer, float, string)
- STRING_AGG aggregate with configurable separator
- ARRAY_AGG aggregate returning Arrow ListArray
- Decimal-preserving SUM/MIN/MAX aggregates (i128 accumulation, Decimal128 output)
- Integer-preserving SUM (i128 accumulation, Int64 output for lossless precision)
- COPY FROM JSON format support
- information_schema.schemata virtual table
- Schema-qualified table scan (information_schema tables now return real data)
- Arrow IPC result format for FFI (`blaze_query_arrow()`)
- Arrow Flight TCP server with JSON command protocol
- Federation HTTP and S3 remote data source providers
- Multi-statement execution in Connection::execute()
- HAVING clause support for nested expressions (NOT, BETWEEN, CASE, IS NULL)
- Benchmark memory tracking with peak measurement and regression detection
- Division by zero protection for integer arithmetic
- SQL-standard NULL comparison behavior (WHERE x = NULL returns 0 rows)
- CAST error messages with source/target type information

### Changed
- README: moved implemented features from Roadmap to Also Available section
- LIMITATIONS.md: separated completed items from planned features
- GPU module: documented as CPU simulation mode
- Flight module: documented as handler-level (not standalone gRPC server)
- Vector embeddings: moved from Coming Soon to Also Available

### Fixed
- SUM/AVG/MIN/MAX accumulators now handle Decimal128 input correctly
- Physical planner now wires SubqueryExecutor in all code paths
- information_schema virtual tables now visible via schema-qualified scan
- INTERSECT/EXCEPT no longer silently converted to UNION

### Previously Added (unreleased features from prior development)
- Vector embeddings: `VECTOR(n)` data type, distance functions (L2, cosine, dot product), brute-force k-NN search
- Production streaming SQL: temporal joins, deduplication, backpressure control, stream checkpointing
- Enhanced LSP support: signature help, code actions, document symbols, SQL formatting, semantic validation
- Cloud-native storage: partition discovery, parallel fetch configuration, multi-cloud support (S3/GCS/Azure)
- Auto-tuning optimizer: A/B plan testing, model drift detection, cardinality feedback loops
- Federated query enhancements: cost-based routing, source capability modeling, federation metrics
- WASM optimization: build profiles, size tracking, feature gating, lazy module loading
- Observability & profiling: query profiler with history, bottleneck analysis, OpenTelemetry export, Prometheus metrics
- Incremental view maintenance: refresh triggers, priority refresh queue, refresh metrics, consistency checking
- Time-series enhancements: delta-of-delta compression, high-throughput ingestion buffer, interpolation engine

## [0.1.0] - 2026-01-12

### Added
- Core SQL engine with SQL:2016 compliance
- SELECT with projections, aliases, WHERE filtering
- JOIN support: INNER, LEFT, RIGHT, FULL OUTER, CROSS, SEMI, ANTI
- GROUP BY with aggregates: COUNT, SUM, AVG, MIN, MAX
- ORDER BY, LIMIT, OFFSET
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, FIRST_VALUE, LAST_VALUE
- CTEs (WITH clause) including recursive CTEs
- Set operations: UNION, INTERSECT, EXCEPT
- CASE WHEN, BETWEEN, LIKE, IN expressions
- 40+ scalar functions (UPPER, LOWER, ABS, COALESCE, CONCAT, TRIM, LENGTH, etc.)
- Prepared statements with positional parameters ($1, $2)
- Transaction support (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- Storage: in-memory tables, CSV files, Parquet files
- Apache Arrow native data format with zero-copy processing
- Connection pooling and prepared statement caching
- Multi-platform bindings: WASM, Node.js (N-API), Python (PyO3), C FFI
- CLI with interactive REPL
- Query optimization: rule-based and cost-based optimizer
- Query profiling with JSON, DOT, Mermaid output
- User-defined functions (UDFs)

### Experimental Features (feature-gated)
- Adaptive query execution (`--features adaptive`)
- GPU acceleration (`--features gpu`)
- Arrow Flight protocol (`--features flight`)
- Natural language queries (`--features nlq`)
- SIMD optimizations (`--features simd`)
- External table federation (`--features federation`)
- Delta Lake / Iceberg support (`--features lakehouse`)
- Streaming execution (`--features streaming`)
- Time-series functions (`--features timeseries`)
- AI-powered optimizer (`--features learned-optimizer`)

[Unreleased]: https://github.com/blaze-db/blaze/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/blaze-db/blaze/releases/tag/v0.1.0
