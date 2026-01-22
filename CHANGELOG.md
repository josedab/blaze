# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
