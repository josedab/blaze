# ADR-0011: Prepared Statement Cache for Query Reuse

## Status

Accepted

## Context

Applications often execute the same SQL query multiple times with different parameter values:

```python
# Pattern: Same query structure, different values
for user_id in user_ids:
    conn.query(f"SELECT * FROM orders WHERE user_id = {user_id}")
```

Each execution incurs:
1. **Parsing**: Convert SQL text to AST
2. **Binding**: Resolve names, type-check
3. **Optimization**: Apply transformation rules
4. **Physical Planning**: Create executable plan

For repeated queries, steps 1-4 produce identical results—only the parameter values differ.

**Approaches to Query Reuse**:

| Approach | Description | Overhead |
|----------|-------------|----------|
| No caching | Parse every time | Full cost per query |
| Statement cache | Cache by SQL text | Hash lookup |
| Prepared statements | Explicit prepare/execute | User manages lifecycle |
| Plan cache | Cache optimized plans | Hash + validation |

## Decision

We implemented **prepared statements with LRU caching**:

### Prepared Statement API

```rust
pub struct PreparedStatement {
    sql: String,
    logical_plan: LogicalPlan,
    physical_plan: PhysicalPlan,
    parameter_types: Vec<DataType>,
    schema: Arc<Schema>,
}

impl Connection {
    /// Prepare a SQL statement for repeated execution
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>> {
        // Check cache first
        {
            let mut cache = self.prepared_cache.lock().unwrap();
            if let Some(stmt) = cache.get(sql) {
                return Ok(Arc::clone(stmt));
            }
        }

        // Full compilation pipeline
        let statement = self.parser.parse(sql)?;
        let logical_plan = self.binder.bind(&statement)?;
        let optimized = self.optimizer.optimize(&logical_plan)?;
        let physical_plan = self.physical_planner.plan(&optimized)?;

        let prepared = Arc::new(PreparedStatement {
            sql: sql.to_string(),
            logical_plan: optimized.clone(),
            physical_plan,
            parameter_types: extract_parameter_types(&optimized),
            schema: optimized.schema(),
        });

        // Add to cache
        {
            let mut cache = self.prepared_cache.lock().unwrap();
            cache.put(sql.to_string(), Arc::clone(&prepared));
        }

        Ok(prepared)
    }

    /// Execute a prepared statement with parameters
    pub fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[ScalarValue],
    ) -> Result<Vec<RecordBatch>> {
        // Validate parameter count and types
        if params.len() != stmt.parameter_types.len() {
            return Err(BlazeError::invalid_argument(format!(
                "Expected {} parameters, got {}",
                stmt.parameter_types.len(),
                params.len()
            )));
        }

        // Bind parameters to physical plan
        let bound_plan = self.bind_parameters(&stmt.physical_plan, params)?;

        // Execute
        self.executor.execute(&bound_plan)
    }
}
```

### LRU Cache Implementation

```rust
use lru::LruCache;
use std::num::NonZeroUsize;

pub struct Connection {
    // ... other fields
    prepared_cache: Mutex<LruCache<String, Arc<PreparedStatement>>>,
}

impl Connection {
    pub fn new() -> Result<Self> {
        Ok(Self {
            // Default: 100 prepared statements
            prepared_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(100).unwrap()
            )),
            // ...
        })
    }

    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.prepared_cache = Mutex::new(LruCache::new(
            NonZeroUsize::new(size).unwrap_or(NonZeroUsize::new(1).unwrap())
        ));
        self
    }
}
```

### Parameter Binding

```rust
/// Placeholder for parameterized queries
#[derive(Debug, Clone)]
pub struct Parameter {
    pub index: usize,  // $1, $2, etc.
    pub data_type: DataType,
}

impl PhysicalExpr for Parameter {
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        // Parameters must be bound before execution
        Err(BlazeError::execution("Unbound parameter"))
    }
}

/// Bind concrete values to parameter placeholders
fn bind_parameters(
    plan: &PhysicalPlan,
    params: &[ScalarValue],
) -> Result<PhysicalPlan> {
    plan.transform_expressions(|expr| {
        if let Some(param) = expr.as_any().downcast_ref::<Parameter>() {
            let value = params.get(param.index)
                .ok_or_else(|| BlazeError::invalid_argument(
                    format!("Missing parameter ${}", param.index + 1)
                ))?;

            // Replace parameter with literal
            Ok(Arc::new(LiteralExpr::new(value.clone())))
        } else {
            Ok(expr.clone())
        }
    })
}
```

## Consequences

### Benefits

1. **Amortized Compilation Cost**: Parse/optimize once, execute many times:
   ```
   Without caching (1000 executions):
   - 1000 × (parse + bind + optimize + plan + execute)
   - Total: ~1000ms parse/plan + execution time

   With prepared statement:
   - 1 × (parse + bind + optimize + plan) + 1000 × (bind_params + execute)
   - Total: ~1ms parse/plan + execution time

   Speedup: ~1000x for compilation overhead
   ```

2. **Automatic Cache Management**: LRU eviction handles memory:
   ```rust
   // Most recently used statements stay cached
   // Least recently used evicted when full
   let cache_size = 100;  // Configurable

   // Common queries stay hot
   conn.prepare("SELECT * FROM users WHERE id = $1")?;  // Cached
   conn.prepare("SELECT * FROM orders WHERE user_id = $1")?;  // Cached
   // ... 98 more unique queries ...
   conn.prepare("SELECT * FROM rare_table")?;  // Evicts oldest
   ```

3. **Type Safety**: Parameter types checked at prepare time:
   ```rust
   let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
   // stmt.parameter_types = [DataType::Int64]

   // Type mismatch caught early
   conn.execute_prepared(&stmt, &[ScalarValue::Utf8("hello")])?;
   // Error: Parameter $1 expects Int64, got Utf8
   ```

4. **Schema Stability**: Result schema known before execution:
   ```rust
   let stmt = conn.prepare("SELECT id, name FROM users")?;

   // Schema available immediately
   println!("Columns: {:?}", stmt.schema().fields());
   // [Field { name: "id", type: Int64 }, Field { name: "name", type: Utf8 }]
   ```

5. **Transparent Caching**: Same API with or without explicit prepare:
   ```rust
   // Explicit prepare (user manages)
   let stmt = conn.prepare(sql)?;
   conn.execute_prepared(&stmt, &params)?;

   // Implicit (cache checked internally)
   conn.query_with_params(sql, &params)?;  // Uses cache
   ```

### Costs

1. **Memory Overhead**: Cached plans consume memory:
   ```rust
   // Each PreparedStatement holds:
   // - SQL string: ~100 bytes typical
   // - LogicalPlan: ~1-10KB typical
   // - PhysicalPlan: ~1-10KB typical
   // - Schema: ~100 bytes typical

   // 100 cached statements ≈ 1-2MB
   ```

2. **Cache Invalidation**: Schema changes require cache clear:
   ```rust
   // Table schema changed
   conn.execute("ALTER TABLE users ADD COLUMN email TEXT")?;

   // Cached plans may be invalid
   conn.clear_prepared_cache();  // Must clear manually
   ```

3. **Parameter Limitations**: Not all queries can be parameterized:
   ```sql
   -- Works: Value parameters
   SELECT * FROM users WHERE id = $1

   -- Doesn't work: Table names, column names
   SELECT * FROM $1 WHERE $2 = $3  -- Can't parameterize identifiers
   ```

### Usage Patterns

**Explicit Preparation**:
```rust
// Prepare once
let get_user = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let get_orders = conn.prepare("SELECT * FROM orders WHERE user_id = $1")?;

// Execute many times
for user_id in user_ids {
    let user = conn.execute_prepared(&get_user, &[user_id.into()])?;
    let orders = conn.execute_prepared(&get_orders, &[user_id.into()])?;
    process(user, orders);
}
```

**Batch Processing**:
```rust
let insert_stmt = conn.prepare("INSERT INTO results VALUES ($1, $2, $3)")?;

for record in records {
    conn.execute_prepared(&insert_stmt, &[
        record.id.into(),
        record.name.into(),
        record.value.into(),
    ])?;
}
```

**Python Bindings**:
```python
# Prepare
stmt = conn.prepare("SELECT * FROM events WHERE date = $1 AND type = $2")

# Execute with different parameters
for date in dates:
    for event_type in event_types:
        result = stmt.execute([date, event_type])
        process(result)
```

### Cache Configuration

```rust
/// Connection configuration
pub struct ConnectionConfig {
    /// Maximum number of prepared statements to cache
    pub prepared_cache_size: usize,

    /// Whether to automatically cache all queries
    pub auto_cache: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            prepared_cache_size: 100,
            auto_cache: true,
        }
    }
}
```

### Performance Characteristics

| Operation | Without Cache | With Cache (Cold) | With Cache (Hot) |
|-----------|--------------|-------------------|------------------|
| Parse | 50-200μs | 50-200μs | 0 (skipped) |
| Bind | 20-100μs | 20-100μs | 0 (skipped) |
| Optimize | 50-500μs | 50-500μs | 0 (skipped) |
| Plan | 30-200μs | 30-200μs | 0 (skipped) |
| Param bind | - | 5-20μs | 5-20μs |
| Execute | varies | varies | varies |
| **Total overhead** | 150-1000μs | 155-1020μs | 5-20μs |

For a query executed 1000 times:
- Without cache: 150-1000ms overhead
- With cache: ~0.15-1ms + 5-20ms = 5-21ms overhead
- **Speedup: 10-50x** for compilation overhead
