---
sidebar_position: 1
---

# Custom Storage Providers

Blaze's storage layer is extensible via the `TableProvider` trait. This guide shows how to create custom data sources.

## TableProvider Trait

The core trait for data sources:

```rust
pub trait TableProvider: Send + Sync {
    /// Returns the schema of this table
    fn schema(&self) -> &Schema;

    /// Scan the table and return data
    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>>;

    /// Check if this provider supports filter pushdown
    fn supports_filter_pushdown(&self) -> bool {
        false
    }

    /// Get table statistics for query optimization
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    /// For downcasting
    fn as_any(&self) -> &dyn Any;
}
```

## Basic Implementation

### Simple Static Data

```rust
use blaze::{Result, Schema, Field, DataType};
use blaze::catalog::TableProvider;
use blaze::planner::PhysicalExpr;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, StringArray};
use std::sync::Arc;
use std::any::Any;

struct StaticTable {
    schema: Schema,
    data: Vec<RecordBatch>,
}

impl StaticTable {
    fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(schema.to_arrow());

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        ).unwrap();

        Self {
            schema,
            data: vec![batch],
        }
    }
}

impl TableProvider for StaticTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = self.data.clone();

        // Apply projection
        if let Some(indices) = projection {
            result = result.into_iter()
                .map(|batch| batch.project(indices).unwrap())
                .collect();
        }

        // Apply limit
        if let Some(limit) = limit {
            let mut count = 0;
            result = result.into_iter()
                .take_while(|batch| {
                    if count >= limit {
                        return false;
                    }
                    count += batch.num_rows();
                    true
                })
                .collect();
        }

        Ok(result)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

### Register Custom Provider

```rust
use blaze::Connection;
use std::sync::Arc;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create and register custom table
    let table = Arc::new(StaticTable::new());
    conn.register_table("custom_data", table)?;

    // Query it
    let results = conn.query("SELECT * FROM custom_data")?;

    Ok(())
}
```

## Advanced Implementation

### With Filter Pushdown

```rust
use blaze::planner::PhysicalExpr;

struct FilterableTable {
    schema: Schema,
    data: Vec<Record>,
}

impl TableProvider for FilterableTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn supports_filter_pushdown(&self) -> bool {
        true  // Enable filter pushdown
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Apply filters during data generation
        let filtered_data: Vec<Record> = self.data.iter()
            .filter(|record| {
                // Evaluate filters (simplified)
                self.matches_filters(record, filters)
            })
            .cloned()
            .collect();

        // Convert to RecordBatch and apply projection/limit
        self.to_batches(filtered_data, projection, limit)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

### With Statistics

```rust
use blaze::optimizer::Statistics;

impl TableProvider for StatisticsTable {
    fn statistics(&self) -> Option<Statistics> {
        Some(Statistics {
            num_rows: Some(1_000_000),
            total_byte_size: Some(100_000_000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Some(0),
                    distinct_count: Some(1_000_000),
                    min_value: Some(ScalarValue::Int64(Some(1))),
                    max_value: Some(ScalarValue::Int64(Some(1_000_000))),
                },
                // ... more columns
            ],
        })
    }

    // ... other methods
}
```

## Real-World Examples

### HTTP API Data Source

```rust
use reqwest::blocking::Client;
use serde::Deserialize;

struct ApiTable {
    schema: Schema,
    endpoint: String,
    client: Client,
}

impl ApiTable {
    fn new(endpoint: &str) -> Self {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Utf8, false),
        ]);

        Self {
            schema,
            endpoint: endpoint.to_string(),
            client: Client::new(),
        }
    }

    fn fetch_data(&self, limit: Option<usize>) -> Result<Vec<ApiRecord>> {
        let url = match limit {
            Some(n) => format!("{}?limit={}", self.endpoint, n),
            None => self.endpoint.clone(),
        };

        let response: Vec<ApiRecord> = self.client
            .get(&url)
            .send()?
            .json()?;

        Ok(response)
    }
}

impl TableProvider for ApiTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let data = self.fetch_data(limit)?;
        self.to_batches(data, projection)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

### Database Backend

```rust
use rusqlite::Connection as SqliteConn;

struct SqliteTable {
    schema: Schema,
    table_name: String,
    db_path: String,
}

impl TableProvider for SqliteTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn supports_filter_pushdown(&self) -> bool {
        true
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let conn = SqliteConn::open(&self.db_path)?;

        // Build SQL with pushdown
        let columns = match projection {
            Some(indices) => indices
                .iter()
                .map(|i| self.schema.field(*i).name())
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };

        let where_clause = self.filters_to_sql(filters);
        let limit_clause = limit.map(|n| format!("LIMIT {}", n)).unwrap_or_default();

        let sql = format!(
            "SELECT {} FROM {} {} {}",
            columns, self.table_name, where_clause, limit_clause
        );

        // Execute and convert to Arrow
        self.execute_and_convert(&conn, &sql)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

### Computed/Virtual Table

```rust
struct FibonacciTable {
    schema: Schema,
    max_n: i64,
}

impl FibonacciTable {
    fn new(max_n: i64) -> Self {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("fib", DataType::Int64, false),
        ]);

        Self { schema, max_n }
    }

    fn compute_fibonacci(&self, limit: Option<usize>) -> Vec<(i64, i64)> {
        let mut result = Vec::new();
        let mut a = 0i64;
        let mut b = 1i64;

        for n in 0..self.max_n {
            if let Some(l) = limit {
                if result.len() >= l {
                    break;
                }
            }

            result.push((n, a));

            let next = a + b;
            a = b;
            b = next;
        }

        result
    }
}

impl TableProvider for FibonacciTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let data = self.compute_fibonacci(limit);

        let arrow_schema = Arc::new(self.schema.to_arrow());
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(data.iter().map(|(n, _)| *n).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(data.iter().map(|(_, f)| *f).collect::<Vec<_>>())),
            ],
        )?;

        let mut result = vec![batch];

        if let Some(indices) = projection {
            result = result.into_iter()
                .map(|batch| batch.project(indices).unwrap())
                .collect();
        }

        Ok(result)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

## Best Practices

### 1. Implement Projection Pushdown

Only read columns that are requested:

```rust
fn scan(
    &self,
    projection: Option<&[usize]>,
    // ...
) -> Result<Vec<RecordBatch>> {
    // Read only projected columns from source
    let columns_to_read = projection
        .map(|p| p.iter().map(|i| self.schema.field(*i).name()).collect())
        .unwrap_or_else(|| self.schema.field_names());

    self.read_columns(&columns_to_read)
}
```

### 2. Honor Limit Early

Apply limit at the data source when possible:

```rust
fn scan(
    &self,
    _projection: Option<&[usize]>,
    _filters: &[Arc<dyn PhysicalExpr>],
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // Fetch only required rows
    let data = self.fetch_with_limit(limit)?;
    // ...
}
```

### 3. Provide Accurate Statistics

Help the optimizer make better decisions:

```rust
fn statistics(&self) -> Option<Statistics> {
    // Compute or cache statistics
    Some(Statistics {
        num_rows: Some(self.row_count()),
        total_byte_size: Some(self.byte_size()),
        column_statistics: self.compute_column_stats(),
    })
}
```

### 4. Handle Errors Gracefully

```rust
fn scan(/* ... */) -> Result<Vec<RecordBatch>> {
    match self.fetch_data() {
        Ok(data) => self.to_batches(data),
        Err(e) => Err(BlazeError::execution(format!(
            "Failed to fetch data from {}: {}",
            self.source_name, e
        ))),
    }
}
```

## Next Steps

- [Query Optimization](/docs/advanced/query-optimization) - How statistics affect planning
- [Extending Blaze](/docs/advanced/extending-blaze) - Other extension points
- [API Reference](/docs/reference/api) - Complete API documentation
