---
sidebar_position: 3
---

# Configuration

Complete reference for Blaze configuration options.

## Connection Configuration

Configure connections using `ConnectionConfig`:

```rust
use blaze::{Connection, ConnectionConfig};

let config = ConnectionConfig::new()
    .with_batch_size(8192)
    .with_memory_limit(1024 * 1024 * 1024)
    .with_num_threads(4);

let conn = Connection::with_config(config)?;
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | 8,192 | Rows per batch for vectorized execution |
| `memory_limit` | None (unlimited) | Maximum memory usage in bytes |
| `num_threads` | None (single-threaded) | Parallel execution workers |

## Batch Size

Controls how many rows are processed per batch.

```rust
// Smaller batches - lower memory, lower throughput
let config = ConnectionConfig::new().with_batch_size(1024);

// Default - balanced
let config = ConnectionConfig::new().with_batch_size(8192);

// Larger batches - higher memory, higher throughput
let config = ConnectionConfig::new().with_batch_size(32768);
```

### Guidelines

| Environment | Recommended Batch Size |
|-------------|------------------------|
| Memory-constrained (< 256 MB) | 1,024 - 2,048 |
| Standard (256 MB - 4 GB) | 4,096 - 8,192 |
| High-throughput (> 4 GB) | 16,384 - 65,536 |

### Valid Range

- **Minimum**: 1
- **Maximum**: 16,777,216 (16 million)

```rust
// Panics: batch_size = 0
ConnectionConfig::new().with_batch_size(0);

// Panics: batch_size > 16,777,216
ConnectionConfig::new().with_batch_size(20_000_000);
```

## Memory Limit

Maximum memory that can be allocated for query execution.

```rust
// 512 MB limit
let config = ConnectionConfig::new()
    .with_memory_limit(512 * 1024 * 1024);

// 4 GB limit
let config = ConnectionConfig::new()
    .with_memory_limit(4 * 1024 * 1024 * 1024);
```

### Behavior When Exceeded

1. Allocation request is rejected
2. Query fails with `BlazeError::ResourceExhausted`
3. Connection remains usable for smaller queries
4. Existing allocations are preserved

```rust
match conn.query("SELECT * FROM huge_table") {
    Ok(results) => { /* process */ },
    Err(BlazeError::ResourceExhausted { message }) => {
        eprintln!("Memory limit exceeded: {}", message);
    },
    Err(e) => { /* other error */ },
}
```

### Recommended Limits

| Use Case | Recommended Limit |
|----------|------------------|
| Embedded/Edge | 64 - 256 MB |
| Desktop application | 512 MB - 2 GB |
| Server/Backend | 2 - 8 GB |
| Data processing | 8 - 32 GB |

## Thread Configuration

Number of parallel workers for query execution.

```rust
// Single-threaded (default)
let config = ConnectionConfig::new();

// 4 parallel workers
let config = ConnectionConfig::new().with_num_threads(4);

// Match CPU cores
let config = ConnectionConfig::new()
    .with_num_threads(num_cpus::get());
```

### Considerations

| Scenario | Threads |
|----------|---------|
| Simple queries | 1 (default) |
| Large aggregations | 2-4 |
| Heavy analytics | CPU count |
| Shared environment | CPU count / 2 |

## CSV Options

Configure CSV file parsing:

```rust
use blaze::storage::CsvOptions;

let options = CsvOptions::default()
    .with_delimiter(b',')
    .with_has_header(true)
    .with_quote(b'"')
    .with_escape(Some(b'\\'))
    .with_null_value("NA");

conn.register_csv_with_options("data", "file.csv", options)?;
```

### Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` (comma) | Field separator byte |
| `has_header` | `true` | First row contains column names |
| `quote` | `"` | Quote character for fields |
| `escape` | `None` | Escape character within quoted fields |
| `null_value` | `""` (empty) | String to interpret as NULL |
| `comment` | `None` | Character that starts comment lines |

### Examples

#### TSV Files (Tab-Separated)

```rust
let options = CsvOptions::default()
    .with_delimiter(b'\t');
```

#### Semicolon-Separated (European)

```rust
let options = CsvOptions::default()
    .with_delimiter(b';');
```

#### Custom NULL Handling

```rust
let options = CsvOptions::default()
    .with_null_value("NULL")
    .with_null_value("N/A");
```

#### No Header Row

```rust
let options = CsvOptions::default()
    .with_has_header(false);
```

## Parquet Options

Configure Parquet file reading:

```rust
use blaze::storage::ParquetOptions;

let options = ParquetOptions::default()
    .with_row_groups(vec![0, 1, 2]);

conn.register_parquet_with_options("data", "file.parquet", options)?;
```

### Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `row_groups` | All | Specific row groups to read |

### Examples

#### Read Specific Row Groups

```rust
// Read only first 3 row groups
let options = ParquetOptions::default()
    .with_row_groups(vec![0, 1, 2]);
```

## Environment Variables

Blaze respects these environment variables:

| Variable | Description |
|----------|-------------|
| `BLAZE_BATCH_SIZE` | Default batch size |
| `BLAZE_MEMORY_LIMIT` | Default memory limit (bytes) |
| `RUST_LOG` | Logging level (via tracing) |

### Example Usage

```bash
# Set batch size
export BLAZE_BATCH_SIZE=4096

# Set memory limit to 1 GB
export BLAZE_MEMORY_LIMIT=1073741824

# Enable debug logging
export RUST_LOG=blaze=debug
```

## Configuration Patterns

### Production Configuration

```rust
let config = ConnectionConfig::new()
    .with_batch_size(8192)
    .with_memory_limit(2 * 1024 * 1024 * 1024)  // 2 GB
    .with_num_threads(4);
```

### Development Configuration

```rust
let config = ConnectionConfig::new()
    .with_batch_size(1024)  // Smaller for quick iteration
    .with_memory_limit(256 * 1024 * 1024);  // 256 MB
```

### High-Throughput Configuration

```rust
let config = ConnectionConfig::new()
    .with_batch_size(65536)  // Large batches
    .with_memory_limit(16 * 1024 * 1024 * 1024)  // 16 GB
    .with_num_threads(num_cpus::get());
```

### Memory-Constrained Configuration

```rust
let config = ConnectionConfig::new()
    .with_batch_size(512)
    .with_memory_limit(64 * 1024 * 1024);  // 64 MB
```

## Statement Cache Configuration

Configure the prepared statement cache:

```rust
use blaze::PreparedStatementCache;

// Small cache for simple applications
let cache = PreparedStatementCache::new(10);

// Large cache for applications with many queries
let cache = PreparedStatementCache::new(1000);
```

### Sizing Guidelines

| Application Type | Cache Size |
|-----------------|------------|
| Simple script | 10-20 |
| Web application | 50-100 |
| Complex analytics | 100-500 |
| Multi-tenant system | 500-1000+ |
