# ADR-0012: Delta Lake Native Integration for Lakehouse Support

## Status

Accepted

## Context

Modern data architectures increasingly adopt the "lakehouse" pattern—combining data lake storage (cheap, scalable) with data warehouse features (ACID, schema enforcement). Delta Lake is a leading lakehouse format:

**Delta Lake Features**:
- ACID transactions on object storage
- Time travel (query historical versions)
- Schema enforcement and evolution
- Unified batch and streaming
- Audit history

**Integration Approaches**:

| Approach | Description | Trade-offs |
|----------|-------------|------------|
| External connector | Call Delta Lake service/library | Network overhead, dependency |
| Format reader only | Read Parquet files + log parsing | Limited features, complex |
| Native integration | Direct protocol implementation | Full control, maintenance burden |
| Rust library (delta-rs) | Rust-native Delta Lake | Good fit, active development |

## Decision

We integrated Delta Lake using the **delta-rs** Rust library, implementing a native `DeltaTable` storage provider:

### DeltaTable Implementation

```rust
// src/storage/delta.rs
use deltalake::DeltaTable as DeltaTableInner;

pub struct DeltaTable {
    inner: DeltaTableInner,
    schema: Schema,
}

impl DeltaTable {
    /// Open a Delta table from a path (local or cloud)
    pub async fn open(path: impl AsRef<str>) -> Result<Self> {
        let table = deltalake::open_table(path.as_ref()).await
            .map_err(|e| BlazeError::storage(format!("Delta open failed: {}", e)))?;

        let schema = Self::convert_schema(table.schema().unwrap())?;

        Ok(Self { inner: table, schema })
    }

    /// Open a specific version (time travel)
    pub async fn open_version(path: impl AsRef<str>, version: i64) -> Result<Self> {
        let table = deltalake::open_table_with_version(path.as_ref(), version).await
            .map_err(|e| BlazeError::storage(format!("Delta version open failed: {}", e)))?;

        let schema = Self::convert_schema(table.schema().unwrap())?;

        Ok(Self { inner: table, schema })
    }

    /// Load table at a specific timestamp
    pub async fn open_at_timestamp(path: impl AsRef<str>, timestamp: DateTime<Utc>) -> Result<Self> {
        let mut table = deltalake::open_table(path.as_ref()).await?;
        table.load_with_datetime(timestamp).await?;

        let schema = Self::convert_schema(table.schema().unwrap())?;

        Ok(Self { inner: table, schema })
    }
}
```

### TableProvider Implementation

```rust
impl TableProvider for DeltaTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Get active files from Delta log
        let files = self.inner.get_files_iter()
            .map_err(|e| BlazeError::storage(e.to_string()))?;

        // Convert filters to Delta's predicate format for pushdown
        let delta_filters = self.convert_filters(filters);

        // Read Parquet files with projection and filter pushdown
        let mut batches = Vec::new();
        let mut rows_read = 0;

        for file in files {
            if let Some(limit) = limit {
                if rows_read >= limit {
                    break;
                }
            }

            let parquet_path = self.resolve_file_path(&file);
            let batch = self.read_parquet_file(
                &parquet_path,
                projection,
                &delta_filters,
                limit.map(|l| l - rows_read),
            )?;

            rows_read += batch.num_rows();
            batches.push(batch);
        }

        Ok(batches)
    }

    fn supports_filter_pushdown(&self) -> bool {
        true  // Delta supports predicate pushdown via Parquet
    }

    fn statistics(&self) -> Option<Statistics> {
        // Delta maintains statistics in transaction log
        let stats = self.inner.get_stats();
        Some(Statistics {
            num_rows: stats.num_records,
            total_byte_size: Some(stats.total_size_bytes),
            column_statistics: self.extract_column_stats(&stats),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
```

### Time Travel Support

```rust
impl DeltaTable {
    /// Get available versions
    pub fn versions(&self) -> Result<Vec<i64>> {
        Ok(self.inner.get_versions()?)
    }

    /// Get version at timestamp
    pub fn version_at(&self, timestamp: DateTime<Utc>) -> Result<i64> {
        Ok(self.inner.version_at_timestamp(timestamp)?)
    }

    /// Reload to different version
    pub fn time_travel_to(&mut self, version: i64) -> Result<()> {
        self.inner.load_version(version)?;
        self.schema = Self::convert_schema(self.inner.schema().unwrap())?;
        Ok(())
    }
}

// SQL extension for time travel
// SELECT * FROM events VERSION AS OF 5
// SELECT * FROM events TIMESTAMP AS OF '2024-01-01'
```

### Write Support

```rust
impl DeltaTable {
    /// Append data to table
    pub async fn append(&mut self, batches: Vec<RecordBatch>) -> Result<()> {
        let mut writer = deltalake::writer::RecordBatchWriter::for_table(&self.inner)?;

        for batch in batches {
            writer.write(batch).await?;
        }

        writer.flush_and_commit(&mut self.inner).await?;
        Ok(())
    }

    /// Overwrite table data
    pub async fn overwrite(&mut self, batches: Vec<RecordBatch>) -> Result<()> {
        let mut writer = deltalake::writer::RecordBatchWriter::for_table(&self.inner)?;
        writer.with_save_mode(deltalake::writer::SaveMode::Overwrite);

        for batch in batches {
            writer.write(batch).await?;
        }

        writer.flush_and_commit(&mut self.inner).await?;
        Ok(())
    }
}
```

## Consequences

### Benefits

1. **ACID Transactions**: Concurrent reads/writes are safe:
   ```rust
   // Writer
   let mut table = DeltaTable::open("s3://bucket/events").await?;
   table.append(new_batches).await?;
   // Commit is atomic

   // Reader (sees consistent snapshot)
   let table = DeltaTable::open("s3://bucket/events").await?;
   let batches = table.scan(None, &[], None)?;
   // Always sees complete transaction or none
   ```

2. **Time Travel**: Query historical data:
   ```sql
   -- Query yesterday's data
   SELECT * FROM events VERSION AS OF 42

   -- Compare versions
   SELECT
       (SELECT COUNT(*) FROM events VERSION AS OF 42) as yesterday,
       (SELECT COUNT(*) FROM events) as today
   ```

   ```rust
   // Programmatic time travel
   let current = DeltaTable::open(path).await?;
   let yesterday = DeltaTable::open_version(path, current.version() - 1).await?;

   let diff = compare_tables(&current, &yesterday)?;
   ```

3. **Schema Enforcement**: Invalid writes rejected:
   ```rust
   // Table schema: id: Int64, name: Utf8
   let invalid_batch = RecordBatch::new(
       schema![("id", Utf8), ("name", Int64)],  // Wrong types
       // ...
   );

   table.append(vec![invalid_batch]).await;
   // Error: Schema mismatch
   ```

4. **Cloud Storage Support**: Works with S3, Azure, GCS:
   ```rust
   // Local filesystem
   let local = DeltaTable::open("./data/events").await?;

   // Amazon S3
   let s3 = DeltaTable::open("s3://bucket/events").await?;

   // Azure Blob Storage
   let azure = DeltaTable::open("az://container/events").await?;

   // Google Cloud Storage
   let gcs = DeltaTable::open("gs://bucket/events").await?;
   ```

5. **Efficient Updates**: Only changed files rewritten:
   ```
   Transaction log:
   v0: add file1.parquet, file2.parquet
   v1: add file3.parquet  (append)
   v2: remove file1.parquet, add file1_v2.parquet  (update)

   Storage:
   - file1.parquet (orphaned after vacuum)
   - file1_v2.parquet
   - file2.parquet
   - file3.parquet
   - _delta_log/
     - 00000000000000000000.json
     - 00000000000000000001.json
     - 00000000000000000002.json
   ```

6. **Statistics for Optimization**: Column-level stats in log:
   ```rust
   impl DeltaTable {
       fn statistics(&self) -> Option<Statistics> {
           // Stats from Delta log - no file scanning needed
           let stats = self.inner.get_stats();
           Some(Statistics {
               num_rows: stats.num_records,
               column_statistics: vec![
                   ColumnStatistics {
                       null_count: Some(stats.null_counts["id"]),
                       min_value: Some(stats.min_values["id"].clone()),
                       max_value: Some(stats.max_values["id"].clone()),
                       distinct_count: None,
                   },
                   // ...
               ],
           })
       }
   }
   ```

### Costs

1. **Async Requirement**: Delta operations are async:
   ```rust
   // Must use async runtime
   #[tokio::main]
   async fn main() -> Result<()> {
       let table = DeltaTable::open(path).await?;
       // ...
   }

   // Or block in sync context
   let table = tokio::runtime::Runtime::new()?
       .block_on(DeltaTable::open(path))?;
   ```

2. **Additional Dependency**: delta-rs adds compile time and binary size:
   ```toml
   [dependencies]
   deltalake = { version = "0.17", features = ["s3", "azure", "gcs"] }
   # Adds ~5-10MB to binary, significant compile time
   ```

3. **Log Parsing Overhead**: First open parses transaction log:
   ```
   Table with 1000 versions:
   - Parse 1000 JSON log files
   - Build file list
   - ~100-500ms cold open

   Mitigation: Checkpoint files reduce log parsing
   ```

4. **Cloud Credentials**: Remote storage requires configuration:
   ```rust
   // AWS credentials from environment or config
   std::env::set_var("AWS_ACCESS_KEY_ID", "...");
   std::env::set_var("AWS_SECRET_ACCESS_KEY", "...");

   let table = DeltaTable::open("s3://bucket/table").await?;
   ```

### Integration with Query Engine

**Registration**:
```rust
// Register Delta table in catalog
let table = DeltaTable::open("s3://warehouse/sales").await?;
conn.register_table("sales", Arc::new(table))?;

// Query like any other table
let results = conn.query("
    SELECT region, SUM(amount)
    FROM sales
    WHERE date >= '2024-01-01'
    GROUP BY region
")?;
```

**Filter Pushdown**:
```sql
SELECT * FROM events WHERE date = '2024-01-15' AND user_id > 1000
```

```
Execution:
1. Delta log: Find files with date='2024-01-15' (partition pruning)
2. Parquet reader: Apply user_id > 1000 at row group level
3. Scan: Only read matching row groups from matching files
```

**Predicate Conversion**:
```rust
fn convert_to_delta_filter(expr: &dyn PhysicalExpr) -> Option<DeltaFilter> {
    match expr.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) if binary.op == Operator::Eq => {
            let col = binary.left.as_any().downcast_ref::<ColumnExpr>()?;
            let lit = binary.right.as_any().downcast_ref::<LiteralExpr>()?;
            Some(DeltaFilter::Eq(col.name.clone(), lit.value.clone()))
        }
        // ... other operators
        _ => None,
    }
}
```

### Comparison with Alternatives

| Feature | Raw Parquet | Delta Lake | Iceberg |
|---------|-------------|------------|---------|
| ACID transactions | No | Yes | Yes |
| Time travel | No | Yes | Yes |
| Schema evolution | Manual | Yes | Yes |
| Partition evolution | No | Limited | Yes |
| Rust support | Native | delta-rs | Limited |
| Adoption | Widespread | Growing | Growing |

### Future Enhancements

1. **Merge Support**: UPDATE/DELETE/MERGE operations:
   ```sql
   MERGE INTO target
   USING source
   ON target.id = source.id
   WHEN MATCHED THEN UPDATE SET value = source.value
   WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
   ```

2. **Optimize/Vacuum**: Table maintenance commands:
   ```sql
   OPTIMIZE events  -- Compact small files
   VACUUM events RETAIN 168 HOURS  -- Remove old versions
   ```

3. **Change Data Feed**: Stream changes between versions:
   ```rust
   let changes = table.changes_between(version_start, version_end)?;
   for change in changes {
       match change.operation {
           Operation::Insert => /* ... */,
           Operation::Update { before, after } => /* ... */,
           Operation::Delete => /* ... */,
       }
   }
   ```
