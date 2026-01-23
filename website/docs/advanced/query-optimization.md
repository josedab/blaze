---
sidebar_position: 2
---

# Query Optimization

Blaze includes a rule-based query optimizer that transforms logical plans for better performance. This guide explains how optimization works and how to write efficient queries.

## Optimization Pipeline

```
Logical Plan
     ↓
┌─────────────────────────┐
│   Simplify Expressions  │
└─────────────────────────┘
     ↓
┌─────────────────────────┐
│    Constant Folding     │
└─────────────────────────┘
     ↓
┌─────────────────────────┐
│   Predicate Pushdown    │
└─────────────────────────┘
     ↓
┌─────────────────────────┐
│  Projection Pushdown    │
└─────────────────────────┘
     ↓
┌─────────────────────────┐
│    Eliminate Limit      │
└─────────────────────────┘
     ↓
Optimized Logical Plan
```

## Optimization Rules

### Simplify Expressions

Simplifies boolean and arithmetic expressions:

```sql
-- Before optimization
WHERE 1 = 1 AND status = 'active'

-- After optimization
WHERE status = 'active'

-- Before
WHERE NOT (NOT active)

-- After
WHERE active

-- Before
WHERE x OR TRUE

-- After
WHERE TRUE  -- (or eliminated entirely)
```

### Constant Folding

Evaluates constant expressions at plan time:

```sql
-- Before optimization
SELECT 1 + 2 + 3 AS value

-- After optimization
SELECT 6 AS value

-- Before
WHERE date > '2024-01-01' AND date > '2024-02-01'

-- After
WHERE date > '2024-02-01'
```

### Predicate Pushdown

Moves filters closer to data sources:

```sql
-- Original query
SELECT * FROM (
    SELECT * FROM orders
    JOIN customers ON orders.customer_id = customers.id
) AS joined
WHERE joined.status = 'completed'

-- After pushdown
SELECT * FROM (
    SELECT * FROM orders WHERE status = 'completed'
    JOIN customers ON orders.customer_id = customers.id
) AS joined
```

Benefits:
- Reduces data volume early
- Enables filter pushdown to storage (e.g., Parquet)
- Reduces join input sizes

### Projection Pushdown

Requests only needed columns:

```sql
-- Query
SELECT name, email FROM (
    SELECT * FROM users
) AS u

-- After pushdown
SELECT name, email FROM (
    SELECT name, email FROM users  -- Only reads needed columns
) AS u
```

Benefits:
- Reduces I/O for columnar formats (Parquet)
- Reduces memory usage
- Speeds up processing

### Eliminate Limit

Removes redundant LIMIT clauses:

```sql
-- Before
SELECT * FROM (
    SELECT * FROM users LIMIT 10
) LIMIT 100

-- After (outer limit is redundant)
SELECT * FROM (
    SELECT * FROM users LIMIT 10
)
```

## Cost-Based Optimization

Blaze also includes cost-based optimization components:

### Statistics

Table statistics inform optimization decisions:

```rust
pub struct Statistics {
    pub num_rows: Option<usize>,
    pub total_byte_size: Option<usize>,
    pub column_statistics: Vec<ColumnStatistics>,
}

pub struct ColumnStatistics {
    pub null_count: Option<usize>,
    pub distinct_count: Option<usize>,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
}
```

### Cardinality Estimation

Estimates output rows for each operation:

```
TableScan: num_rows from statistics
Filter: num_rows * selectivity
Join: left_rows * right_rows * selectivity
Aggregate: estimated_groups
```

### Join Ordering

Optimizes multi-way join order:

```sql
-- Query with 3-way join
SELECT * FROM a
JOIN b ON a.id = b.a_id
JOIN c ON b.id = c.b_id

-- Optimizer considers different orders:
-- (a ⋈ b) ⋈ c
-- (a ⋈ c) ⋈ b  (if applicable)
-- (b ⋈ c) ⋈ a
-- And chooses based on estimated cost
```

## Writing Efficient Queries

### Use Selective Filters

```sql
-- Good: Highly selective filter first
SELECT * FROM events
WHERE event_type = 'purchase'  -- Filters 99% of rows
  AND user_id = 123

-- Optimizer will apply event_type filter first
```

### Avoid `SELECT *`

```sql
-- Good: Request only needed columns
SELECT id, name, email FROM users

-- Bad: Reads all columns
SELECT * FROM users
```

### Use LIMIT for Exploration

```sql
-- Good: Limits data early
SELECT * FROM large_table LIMIT 100

-- Bad: Processes everything
SELECT * FROM large_table
```

### Filter Before Join

```sql
-- Good: Filter reduces join input
SELECT * FROM orders o
JOIN (
    SELECT * FROM customers WHERE region = 'US'
) c ON o.customer_id = c.id

-- Also good (optimizer will push down):
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.region = 'US'
```

### Use EXISTS Instead of IN for Large Subqueries

```sql
-- Better for large subqueries
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
)

-- Can be slower for large subquery results
SELECT * FROM customers
WHERE id IN (SELECT customer_id FROM orders)
```

### Aggregate Early

```sql
-- Good: Aggregate before join
SELECT c.name, o.total_amount
FROM customers c
JOIN (
    SELECT customer_id, SUM(amount) as total_amount
    FROM orders
    GROUP BY customer_id
) o ON c.id = o.customer_id

-- Less efficient: Join then aggregate
SELECT c.name, SUM(o.amount)
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.name
```

## Analyzing Query Plans

### EXPLAIN

View the logical plan:

```sql
EXPLAIN SELECT * FROM users WHERE age > 25
```

Output:
```
Filter: age > 25
  TableScan: users
```

### EXPLAIN ANALYZE

View execution statistics:

```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE age > 25
```

Output:
```
Filter: age > 25 (rows=1000, time=5ms)
  TableScan: users (rows=5000, time=10ms)
```

## Common Optimization Patterns

### Index-like Optimization

While Blaze doesn't have traditional indexes, filter pushdown achieves similar benefits for Parquet:

```sql
-- Parquet with sorted data and row group statistics
-- This can skip entire row groups
SELECT * FROM sorted_events
WHERE timestamp > '2024-01-15'
```

### Materialization

For repeated subqueries, consider materializing:

```sql
-- Instead of repeating complex subquery
WITH expensive_calc AS (
    SELECT id, complex_function(data) as result
    FROM large_table
)
SELECT * FROM expensive_calc WHERE result > 100
UNION ALL
SELECT * FROM expensive_calc WHERE result < 10
```

### Partition Pruning

For partitioned data, include partition columns in filters:

```sql
-- With data partitioned by date
SELECT * FROM events
WHERE date = '2024-01-15'  -- Only reads one partition
  AND event_type = 'click'
```

## Optimization Limitations

### No Automatic Indexing

Blaze doesn't create indexes. Performance depends on:
- Filter pushdown to storage
- Columnar format benefits
- Statistics for planning

### Single-Node Execution

Current optimizer assumes single-node execution. Future versions may include:
- Distributed query planning
- Data shuffling optimization
- Partition-aware joins

### Limited Statistics

Statistics accuracy affects optimization quality. For best results:
- Use storage formats with metadata (Parquet)
- Implement `statistics()` for custom providers

## Next Steps

- [Custom Storage Providers](/docs/advanced/custom-storage-providers) - Provide statistics
- [Memory Management](/docs/guides/memory-management) - Resource optimization
- [Configuration](/docs/reference/configuration) - Performance tuning
