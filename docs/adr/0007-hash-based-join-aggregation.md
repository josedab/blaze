# ADR-0007: Hash-Based Join and Aggregation Operators

## Status

Accepted

## Context

Joins and aggregations are the most computationally expensive operations in analytical queries. Algorithm choice significantly impacts performance:

**Join Algorithms**:

| Algorithm | Time Complexity | Memory | Best For |
|-----------|----------------|--------|----------|
| Nested Loop | O(n × m) | O(1) | Small tables, non-equi joins |
| Sort-Merge | O(n log n + m log m) | O(n + m) | Pre-sorted data, range joins |
| Hash Join | O(n + m) | O(min(n,m)) | Equi-joins, unsorted data |

**Aggregation Algorithms**:

| Algorithm | Time Complexity | Memory | Best For |
|-----------|----------------|--------|----------|
| Sort-based | O(n log n) | O(n) | Large groups, spilling |
| Hash-based | O(n) | O(groups) | Many groups, fits memory |

For OLAP workloads with large fact tables joined to dimension tables, hash-based algorithms typically perform best.

## Decision

We chose **hash-based algorithms** as the primary implementation for joins and aggregations:

### Hash Join

```rust
pub struct HashJoinOperator {
    left: Box<PhysicalPlan>,
    right: Box<PhysicalPlan>,
    on: Vec<(usize, usize)>,  // (left_col, right_col) pairs
    join_type: JoinType,      // Inner, Left, Right, Full, Semi, Anti
}

impl HashJoinOperator {
    fn execute(&self, ctx: &ExecutionContext) -> Result<Vec<RecordBatch>> {
        // 1. Build phase: Hash the right (build) side
        let right_batches = ctx.execute(&self.right)?;
        let hash_table = self.build_hash_table(&right_batches)?;

        // 2. Probe phase: Stream left side against hash table
        let left_batches = ctx.execute(&self.left)?;
        let result = self.probe_hash_table(&left_batches, &hash_table)?;

        Ok(result)
    }
}
```

### Hash Aggregate

```rust
pub struct HashAggregateOperator {
    input: Box<PhysicalPlan>,
    group_by: Vec<Arc<dyn PhysicalExpr>>,
    aggregates: Vec<AggregateFunction>,
}

impl HashAggregateOperator {
    fn execute(&self, ctx: &ExecutionContext) -> Result<Vec<RecordBatch>> {
        let batches = ctx.execute(&self.input)?;

        // Hash table: group_key → accumulator_state
        let mut groups: HashMap<GroupKey, AccumulatorState> = HashMap::new();

        for batch in batches {
            let keys = self.evaluate_group_keys(&batch)?;
            for (row_idx, key) in keys.iter().enumerate() {
                let state = groups.entry(key.clone()).or_insert_with(|| {
                    self.create_accumulator_state()
                });
                self.update_accumulators(state, &batch, row_idx)?;
            }
        }

        self.finalize_groups(groups)
    }
}
```

## Consequences

### Benefits

1. **Linear Time Complexity**: Hash join is O(n + m) vs O(n × m) for nested loop:
   ```
   1M rows × 10K rows:
   - Nested loop: 10 billion comparisons
   - Hash join: 1M + 10K ≈ 1M operations
   ```

2. **Cache-Efficient Hash Tables**: Using `hashbrown` (Swiss tables):
   - Open addressing for cache locality
   - SIMD probing for parallel key comparison
   - Low memory overhead

3. **Streaming Probe Side**: Build hash table from smaller side, stream larger side:
   ```rust
   // Build on dimension table (small)
   let hash_table = build_hash_table(&dimension_table);

   // Stream fact table (large) - no need to hold in memory
   for batch in fact_table_batches {
       let matches = probe_hash_table(&batch, &hash_table);
       yield matches;
   }
   ```

4. **Multiple Join Types**: Hash join naturally supports:
   - INNER JOIN: Output matched pairs
   - LEFT JOIN: Output all left + matched right
   - RIGHT JOIN: Output matched left + all right
   - FULL JOIN: Output all from both sides
   - SEMI JOIN: Output left rows with any match
   - ANTI JOIN: Output left rows with no match

5. **Composite Keys**: Hash on multiple columns efficiently:
   ```rust
   fn compute_hash(columns: &[&ArrayRef], row: usize) -> u64 {
       let mut hasher = ahash::AHasher::default();
       for col in columns {
           hash_array_value(col, row, &mut hasher);
       }
       hasher.finish()
   }
   ```

### Costs

1. **Memory Requirement**: Build side must fit in memory. For very large builds, would need:
   - Partitioned hash join (spill to disk)
   - Grace hash join variant
   - Currently: fail with ResourceExhausted if too large

2. **Hash Collisions**: Poor hash functions or skewed data cause performance degradation. Mitigated by:
   - Using `ahash` (high-quality, fast hasher)
   - Monitoring chain lengths

3. **Not Optimal for Pre-Sorted Data**: If both sides are already sorted on join keys, sort-merge join would be faster. We provide `SortMergeJoinOperator` as alternative.

### Supported Aggregates

Hash aggregate supports common OLAP aggregates:

| Function | Description |
|----------|-------------|
| COUNT(*) | Row count |
| COUNT(col) | Non-null count |
| SUM(col) | Sum of values |
| AVG(col) | Average (sum/count) |
| MIN(col) | Minimum value |
| MAX(col) | Maximum value |
| COUNT(DISTINCT col) | Distinct count (via nested hash set) |

### Build Side Selection

For hash join, the smaller table should be the build side:

```rust
fn select_build_side(left_stats: &Statistics, right_stats: &Statistics) -> BuildSide {
    match (left_stats.num_rows, right_stats.num_rows) {
        (Some(l), Some(r)) if l < r => BuildSide::Left,
        (Some(l), Some(r)) if r < l => BuildSide::Right,
        _ => BuildSide::Right,  // Default: right side is build
    }
}
```

### Alternative: Sort-Merge Join

Provided for cases where hash join is suboptimal:

```rust
pub struct SortMergeJoinOperator {
    left: Box<PhysicalPlan>,
    right: Box<PhysicalPlan>,
    on: Vec<(usize, usize)>,
    join_type: JoinType,
}
```

Use cases:
- Data already sorted on join keys
- Memory-constrained environments
- Range joins (future)

### Hash Table Implementation

Using `hashbrown::HashMap` with `ahash`:

```rust
use hashbrown::HashMap;
use ahash::RandomState;

type JoinHashTable = HashMap<u64, Vec<(usize, usize)>, RandomState>;
//                   hash   → (batch_idx, row_idx)
```

Benefits of hashbrown:
- Swiss table implementation (cache-efficient)
- SIMD-accelerated operations
- Lower memory overhead than std HashMap
- Rust's standard library HashMap is built on hashbrown

### Memory Tracking

Hash tables are tracked by memory manager:

```rust
fn build_hash_table(&self, batches: &[RecordBatch]) -> Result<JoinHashTable> {
    let estimated_size = estimate_hash_table_size(batches);

    if !self.memory_manager.try_reserve(estimated_size) {
        return Err(BlazeError::ResourceExhausted {
            message: "Hash table exceeds memory budget".into(),
        });
    }

    // Build hash table...
}
```
