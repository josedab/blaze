# ADR-0005: Trait-Based Pluggable Storage Layer

## Status

Accepted

## Context

A query engine must read data from various sources. Different storage formats have different characteristics:

| Format | Characteristics |
|--------|-----------------|
| In-Memory | Fastest, volatile, full control |
| CSV | Human-readable, schema-less, slow parsing |
| Parquet | Columnar, compressed, predicate pushdown |
| Delta Lake | ACID transactions, time travel, schema evolution |
| JSON | Semi-structured, nested data |
| Remote (S3, HDFS) | Network latency, parallel fetch |

Approaches to multi-format support:

**Hard-coded Formats**: Each format built into execution engine
- Tight coupling between executor and storage
- Adding formats requires modifying core code
- Testing requires actual files for each format

**Plugin Architecture**: Dynamic loading of format handlers
- Maximum flexibility
- Complex plugin API
- Runtime overhead from dynamic dispatch

**Trait-Based Abstraction**: Interface trait with static implementations
- Clean abstraction without runtime plugin complexity
- Compile-time type safety
- Easy to add new implementations

## Decision

We defined a **TableProvider trait** as the abstraction for all data sources:

```rust
pub trait TableProvider: Debug + Send + Sync {
    /// Return the schema of this table
    fn schema(&self) -> &Schema;

    /// Scan the table, optionally with projection and filters
    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>>;

    /// Check if this provider supports filter pushdown
    fn supports_filter_pushdown(&self) -> bool {
        false  // Conservative default
    }

    /// Return statistics about this table (optional)
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    /// Allow downcasting for format-specific operations
    fn as_any(&self) -> &dyn std::any::Any;
}
```

Implementations:
- `MemoryTable`: In-memory record batches
- `CsvTable`: CSV file reader
- `ParquetTable`: Parquet file reader with pushdown
- `DeltaTable`: Delta Lake tables with time travel

## Consequences

### Benefits

1. **Uniform Interface**: Executor treats all sources identically:
   ```rust
   fn execute_scan(&self, table: &dyn TableProvider) -> Result<Vec<RecordBatch>> {
       // Same code for CSV, Parquet, Delta, Memory
       table.scan(projection, filters, limit)
   }
   ```

2. **Easy Format Addition**: New formats only implement the trait:
   ```rust
   pub struct JsonTable { /* fields */ }

   impl TableProvider for JsonTable {
       fn schema(&self) -> &Schema { /* ... */ }
       fn scan(&self, ...) -> Result<Vec<RecordBatch>> { /* ... */ }
   }
   ```

3. **Optional Optimizations**: Providers opt-in to capabilities:
   ```rust
   impl TableProvider for ParquetTable {
       fn supports_filter_pushdown(&self) -> bool { true }

       fn scan(&self, projection, filters, limit) -> Result<Vec<RecordBatch>> {
           // Use Parquet predicate pushdown
           self.reader.with_row_filter(filters).read()
       }
   }
   ```

4. **Testing Flexibility**: Test execution with MemoryTable:
   ```rust
   #[test]
   fn test_filter_operator() {
       let table = MemoryTable::new(schema, test_batches);
       let result = executor.execute_scan(&table, ...)?;
       assert_eq!(result.len(), expected);
   }
   ```

5. **Catalog Integration**: Tables registered by trait object:
   ```rust
   catalog.register_table("users", Arc::new(ParquetTable::open(path)?));
   catalog.register_table("cache", Arc::new(MemoryTable::new(schema, data)));
   ```

6. **Statistics for Optimization**: Providers can report statistics:
   ```rust
   impl TableProvider for ParquetTable {
       fn statistics(&self) -> Option<Statistics> {
           Some(Statistics {
               num_rows: Some(self.metadata.num_rows()),
               column_statistics: self.compute_column_stats(),
           })
       }
   }
   ```

### Costs

1. **Trait Object Overhead**: Virtual dispatch for `scan()` calls. Negligible for OLAP (scan dominates).

2. **Common Code Duplication**: Some logic repeated across implementations (e.g., schema validation).

3. **Limited Format-Specific Features**: Trait is lowest common denominator. Format-specific features require downcasting:
   ```rust
   // Delta-specific time travel
   if let Some(delta) = table.as_any().downcast_ref::<DeltaTable>() {
       delta.time_travel_to_version(5)?;
   }
   ```

### Current Implementations

**MemoryTable** (`storage/memory.rs`):
- Holds `Vec<RecordBatch>` in memory
- Supports append, replace, clear operations
- Computes statistics on demand
- Used for CREATE TABLE results and intermediate data

**CsvTable** (`storage/csv.rs`):
- Reads CSV files using Arrow's CSV reader
- Configurable delimiter, header, schema inference
- No filter pushdown (full scan always)

**ParquetTable** (`storage/parquet.rs`):
- Reads Parquet files with column projection
- Implements filter pushdown via row group filtering
- Reports statistics from Parquet metadata

**DeltaTable** (`storage/delta.rs`):
- Reads Delta Lake tables
- Time travel by version number or timestamp
- ACID write support (append, overwrite)
- Automatic log replay for consistency

### Auto-Detection Pattern

The storage module provides format auto-detection:

```rust
pub fn read_file(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    let path = path.as_ref();

    // Check for Delta table (directory with _delta_log)
    if path.is_dir() && path.join("_delta_log").exists() {
        return Ok(Arc::new(DeltaTable::open(path)?));
    }

    // Check file extension
    match path.extension().and_then(|e| e.to_str()) {
        Some("csv") | Some("tsv") => Ok(Arc::new(CsvTable::open(path)?)),
        Some("parquet") | Some("pq") => Ok(Arc::new(ParquetTable::open(path)?)),
        _ => Err(BlazeError::invalid_argument("Unsupported format")),
    }
}
```

This allows users to call `read_file("data.parquet")` without specifying format.

### Future Extensions

The trait-based design enables:

1. **Remote Storage**: S3Table, GcsTable with async scan
2. **Federated Queries**: PostgresTable wrapping external databases
3. **Streaming Sources**: KafkaTable for real-time data
4. **Caching Layer**: CachedTable wrapping other providers
