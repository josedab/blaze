# ADR-0006: Batch-Oriented Vectorized Execution Model

## Status

Accepted

## Context

Query execution models vary in how they process data:

**Tuple-at-a-Time (Volcano Model)**:
- Process one row per operator call
- Simple implementation
- Poor CPU cache utilization
- Function call overhead per row
- Used by: Traditional databases (PostgreSQL, MySQL)

**Compiled/JIT Execution**:
- Generate code for entire query pipeline
- Eliminates interpretation overhead
- Complex implementation
- Long compilation times for complex queries
- Used by: HyPer, Spark (Tungsten)

**Vectorized/Batch Execution**:
- Process batches of rows (hundreds to thousands)
- Amortizes function call overhead
- Cache-friendly memory access patterns
- SIMD-friendly operations
- Moderate implementation complexity
- Used by: DuckDB, ClickHouse, Velox, MonetDB

For OLAP workloads scanning millions of rows, vectorized execution offers the best balance of performance and implementation complexity.

## Decision

We implemented **batch-oriented vectorized execution** where:

1. Data flows through operators in **batches** (configurable, default 8,192 rows)
2. Each batch is an Arrow `RecordBatch` (columnar format)
3. Operators process entire batches at once
4. Results are `Vec<RecordBatch>`

```rust
pub struct ExecutionContext {
    batch_size: usize,  // Rows per batch (default: 8192)
    memory_manager: MemoryManager,
    catalog_list: Arc<CatalogList>,
}

impl ExecutionContext {
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            PhysicalPlan::Filter { input, predicate } => {
                let batches = self.execute(input)?;
                batches.into_iter()
                    .map(|batch| self.apply_filter(&batch, predicate))
                    .collect()
            }
            // ... other operators
        }
    }
}
```

## Consequences

### Benefits

1. **CPU Cache Efficiency**: Processing 8K rows of a column keeps data in L2/L3 cache:
   ```rust
   // Good: Sequential access through column
   for i in 0..batch.num_rows() {
       result[i] = column_a[i] + column_b[i];  // Cache-friendly
   }

   // Bad (tuple-at-a-time): Random access patterns
   for row in rows {
       result.push(row.column_a + row.column_b);  // Cache misses
   }
   ```

2. **SIMD Vectorization**: Arrow's columnar layout enables SIMD operations:
   ```rust
   // Arrow's compute kernels use SIMD internally
   arrow::compute::add(&column_a, &column_b)  // Uses AVX2/NEON
   ```

3. **Amortized Overhead**: Function call and virtual dispatch costs spread over batch:
   ```
   Tuple-at-a-time: 1M rows × (call + process) = 1M function calls
   Batch (8K): 1M rows ÷ 8K = 125 function calls
   ```

4. **Memory Predictability**: Fixed batch sizes enable memory budgeting:
   ```rust
   let estimated_memory = batch_size * row_width * num_columns;
   if !memory_manager.try_reserve(estimated_memory) {
       return Err(BlazeError::ResourceExhausted);
   }
   ```

5. **Natural Arrow Integration**: Arrow's `RecordBatch` is already batch-oriented, so data flows naturally through operators.

### Costs

1. **Not Ideal for Small Results**: Single-row queries (e.g., point lookups) have overhead of batch infrastructure.

2. **Memory Footprint**: Must hold entire batch in memory (mitigated by streaming between batches).

3. **Batch Boundary Handling**: Some operations (sorting, grouping) must handle data spanning batches.

### Batch Size Selection

Default batch size: **8,192 rows**

Rationale:
- **8K × 8 bytes (Int64) = 64KB** fits in L2 cache (typically 256KB-1MB)
- Power of 2 for alignment
- Large enough to amortize overhead
- Small enough for responsive streaming

Configurable range: 1 to 16,777,216 (16M) rows

```rust
impl ConnectionConfig {
    const MIN_BATCH_SIZE: usize = 1;
    const MAX_BATCH_SIZE: usize = 16_777_216;

    pub fn with_batch_size(mut self, size: usize) -> Self {
        assert!(size >= Self::MIN_BATCH_SIZE && size <= Self::MAX_BATCH_SIZE);
        self.batch_size = size;
        self
    }
}
```

### Operator Implementation Pattern

All operators follow the batch-in, batch-out pattern:

```rust
// Filter: Process batch, return filtered batch
fn execute_filter(
    &self,
    batch: &RecordBatch,
    predicate: &dyn PhysicalExpr,
) -> Result<RecordBatch> {
    // Evaluate predicate on entire batch → boolean array
    let mask = predicate.evaluate(batch)?;
    let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();

    // Filter entire batch using mask
    filter_record_batch(batch, mask)
}

// Projection: Evaluate expressions on batch
fn execute_projection(
    &self,
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = exprs
        .iter()
        .map(|expr| expr.evaluate(batch))  // Evaluate on entire batch
        .collect::<Result<_>>()?;

    RecordBatch::try_new(schema, columns)
}
```

### Memory Management Integration

Batch sizes interact with memory limits:

```rust
impl MemoryManager {
    pub fn try_reserve(&self, size: usize) -> bool {
        let current = self.used.load(Ordering::SeqCst);
        let budget = self.budget.load(Ordering::SeqCst);

        if current + size > budget {
            return false;  // Would exceed limit
        }

        // Atomic CAS to reserve
        self.used.compare_exchange(current, current + size, ...)
    }
}
```

Queries exceeding memory limits fail with `ResourceExhausted` error rather than causing OOM.

### Comparison with Alternatives

| Aspect | Tuple-at-a-time | Vectorized (Blaze) | Compiled |
|--------|----------------|---------------------|----------|
| Implementation | Simple | Moderate | Complex |
| Function calls | Per row | Per batch | Eliminated |
| Cache efficiency | Poor | Good | Excellent |
| SIMD utilization | None | Natural | Requires codegen |
| Compilation time | None | None | Per query |
| Debugging | Easy | Easy | Hard |
