---
sidebar_position: 3
---

# Query Execution

This guide explains how Blaze executes queries, from SQL text to result batches.

## Execution Model

Blaze uses a **pull-based, vectorized execution model**:

- **Pull-based**: Operators request data from their children
- **Vectorized**: Data is processed in batches, not row-by-row
- **Arrow-native**: All data uses Apache Arrow's columnar format

```
┌──────────────────────────────────────────────────┐
│                    Application                    │
│                        ↑                          │
│                   Vec<RecordBatch>                │
├──────────────────────────────────────────────────┤
│                      Limit                        │
│                        ↑                          │
├──────────────────────────────────────────────────┤
│                       Sort                        │
│                        ↑                          │
├──────────────────────────────────────────────────┤
│                  HashAggregate                    │
│                        ↑                          │
├──────────────────────────────────────────────────┤
│                      Filter                       │
│                        ↑                          │
├──────────────────────────────────────────────────┤
│                   TableScan                       │
│                        ↑                          │
├──────────────────────────────────────────────────┤
│                     Storage                       │
└──────────────────────────────────────────────────┘
```

## Physical Operators

### Scan

Reads data from storage providers:

```sql
SELECT * FROM users
```

- Supports projection pushdown (reads only needed columns)
- Supports filter pushdown (applies predicates during scan)
- Returns data in batches

### Filter

Evaluates predicates and filters rows:

```sql
SELECT * FROM users WHERE age > 25 AND status = 'active'
```

- Evaluates predicate expression on each batch
- Uses boolean mask to filter rows
- Complexity: O(n)

### Projection

Computes output expressions:

```sql
SELECT name, age * 2 AS double_age FROM users
```

- Evaluates each expression on input batches
- Creates new batches with computed columns
- Complexity: O(n * m) where m is number of expressions

### HashAggregate

Performs aggregation using hash tables:

```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department
```

- Builds hash table keyed by GROUP BY columns
- Maintains aggregation state per group
- Complexity: O(n)

Supported aggregates:
- `COUNT(*)`
- `COUNT(column)`
- `SUM(column)`
- `AVG(column)`
- `MIN(column)`
- `MAX(column)`

### HashJoin

Implements hash-based joins:

```sql
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
```

- Builds hash table from smaller (build) side
- Probes hash table with larger (probe) side
- Complexity: O(n + m)

Supported join types:
- `INNER JOIN`
- `LEFT OUTER JOIN`
- `RIGHT OUTER JOIN`
- `FULL OUTER JOIN`
- `CROSS JOIN`
- `LEFT SEMI JOIN`
- `LEFT ANTI JOIN`

### Sort

Orders results:

```sql
SELECT * FROM products ORDER BY price DESC, name ASC
```

- Collects all input batches
- Performs in-memory sort
- Complexity: O(n log n)

### Limit

Restricts output rows:

```sql
SELECT * FROM events LIMIT 100 OFFSET 50
```

- Passes through batches until limit reached
- Handles offset by skipping rows
- Complexity: O(1) after offset

### Window

Computes window functions:

```sql
SELECT name, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
FROM employees
```

- Partitions data by PARTITION BY columns
- Orders within partitions
- Computes window function for each row

Supported window functions:
- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG(expr, offset, default)`
- `LEAD(expr, offset, default)`
- `FIRST_VALUE(expr)`
- `LAST_VALUE(expr)`
- Aggregate functions: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`

## Expression Evaluation

Physical expressions implement the `PhysicalExpr` trait:

```rust
pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}
```

### Column Reference

Access columns by index:

```rust
// Column at index 0
ColumnExpr { index: 0 }
```

### Literals

Constant values:

```rust
// Integer literal 42
LiteralExpr { value: ScalarValue::Int64(Some(42)) }
```

### Binary Expressions

Arithmetic and comparison:

```rust
// a + b
BinaryExpr {
    left: ColumnExpr { index: 0 },
    op: Operator::Plus,
    right: ColumnExpr { index: 1 },
}

// price > 100
BinaryExpr {
    left: ColumnExpr { index: 2 },
    op: Operator::Gt,
    right: LiteralExpr { value: ScalarValue::Int64(Some(100)) },
}
```

### CASE Expressions

```sql
CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END
```

Evaluates conditions in order, returns first matching result.

### Scalar Functions

Built-in functions:

```sql
SELECT UPPER(name), ABS(value), COALESCE(a, b, c)
```

## Batch Processing

Data flows through operators in batches:

```rust
// Batch size: 8192 rows (default)
struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<Arc<dyn Array>>,  // Columnar data
}
```

Benefits of batch processing:
- **CPU efficiency**: Amortizes function call overhead
- **Cache efficiency**: Columnar access patterns
- **SIMD**: Vectorized operations on arrays

## Memory Management

### Batch Size Impact

| Batch Size | Memory | Throughput | Latency |
|------------|--------|------------|---------|
| Small (1K) | Lower | Lower | Lower |
| Default (8K) | Balanced | Balanced | Balanced |
| Large (64K) | Higher | Higher | Higher |

### Memory Tracking

Blaze tracks memory at the batch level:

```rust
let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 512); // 512 MB

let conn = Connection::with_config(config)?;
```

When the limit is exceeded:
1. Current allocation is rejected
2. Query fails with `ResourceExhausted` error
3. Existing allocations are preserved

## Query Examples

### Simple Scan with Filter

```sql
SELECT name, age FROM users WHERE age > 30
```

Execution:
```
1. Scan: Read all columns from 'users'
2. Filter: Apply age > 30 predicate
3. Project: Select name, age columns
4. Return: Vec<RecordBatch>
```

### Aggregation

```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department
```

Execution:
```
1. Scan: Read department, salary from 'employees'
2. HashAggregate:
   - Key: department
   - Accumulators: count, sum(salary), count(salary)
3. Project: department, count, sum/count as avg
4. Return: Vec<RecordBatch>
```

### Join with Aggregation

```sql
SELECT c.name, SUM(o.amount)
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.name
ORDER BY SUM(o.amount) DESC
LIMIT 10
```

Execution:
```
1. Scan orders: Read customer_id, amount, status
2. Filter: status = 'completed'
3. Scan customers: Read id, name
4. HashJoin: customers.id = orders.customer_id
5. HashAggregate: GROUP BY name, SUM(amount)
6. Sort: ORDER BY sum DESC
7. Limit: Take 10 rows
8. Return: Vec<RecordBatch>
```

## Performance Considerations

### Predicate Pushdown

Filters applied during scan reduce data volume early:

```sql
-- Filter pushed to Parquet scan
SELECT * FROM large_table WHERE date = '2024-01-15'
```

### Projection Pushdown

Request only needed columns:

```sql
-- Only reads 'name' and 'email' columns
SELECT name, email FROM users
```

### Join Order

Smaller tables should be the build side of hash joins:

```sql
-- Small lookup table as build side
SELECT * FROM large_facts f
JOIN small_dimensions d ON f.dim_id = d.id
```

## Next Steps

- [Query Optimization](/docs/advanced/query-optimization) - Optimizer rules
- [Memory Management](/docs/guides/memory-management) - Memory configuration
- [Configuration](/docs/reference/configuration) - Tuning options
