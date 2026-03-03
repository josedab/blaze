# Observability Guide

## Query Profiling

Use `EXPLAIN ANALYZE` to inspect execution statistics for a query:

```sql
EXPLAIN ANALYZE SELECT * FROM large_table WHERE id > 1000;
```

The output includes row counts, execution time, and operator-level metrics.

## Logging

Blaze uses the `tracing` framework. Control log verbosity via the
`RUST_LOG` environment variable:

```bash
# General debug logging
RUST_LOG=blaze=debug cargo run

# Trace-level logging for the executor
RUST_LOG=blaze::executor=trace cargo run
```

## Cache Monitoring

Inspect the prepared-statement and query-plan caches:

```sql
EXPLAIN CACHE;
```

## Resource Governor

Monitor resource usage per-tenant with the `ResourceGovernor`:

```rust
use blaze::resource_governor::{ResourceGovernor, GovernorConfig};

let governor = ResourceGovernor::new(GovernorConfig::default());
// … execute queries …
let stats = governor.usage("tenant-1");
```

## Future: OpenTelemetry

OpenTelemetry export is planned for a future release.
