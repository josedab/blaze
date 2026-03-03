# Performance Tuning Guide

## Batch Size

The default batch size is 8,192 rows. Larger batches improve throughput for large scans but use more memory.

```rust
let config = ConnectionConfig::new().with_batch_size(16384);
let conn = Connection::with_config(config)?;
```

## Memory Limits

Set memory limits to prevent OOM on large queries:

```rust
let config = ConnectionConfig::new().with_memory_limit(512 * 1024 * 1024); // 512 MB
```

## Query Optimization

### EXPLAIN your queries

```sql
EXPLAIN SELECT * FROM large_table WHERE id > 1000;
EXPLAIN ANALYZE SELECT COUNT(*) FROM orders GROUP BY status;
```

### Filter pushdown
Blaze pushes filters down to storage for CSV, Parquet, and in-memory tables. Write selective WHERE clauses to reduce data scanned.

### Parquet column pruning
SELECT only the columns you need — Blaze pushes projections to the Parquet reader.

### Statistics
Run `ANALYZE TABLE <name>` to compute table statistics for better query plans.

## Parallel Execution

Enable parallel execution for large datasets:

```rust
let config = ConnectionConfig::new().with_num_threads(4);
```

## Caching

Query result caching is enabled by default. Control it with:
- `/*+ NO_CACHE */` hint to bypass cache
- `conn.clear_cache()` to clear all cached results
- `conn.cache_stats()` to monitor hit rates
