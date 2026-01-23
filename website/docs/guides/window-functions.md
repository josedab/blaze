---
sidebar_position: 2
---

# Window Functions

Window functions perform calculations across a set of rows related to the current row. Unlike aggregate functions, window functions don't collapse rows into a single output.

## Basic Syntax

```sql
function_name(args) OVER (
    [PARTITION BY partition_expression]
    [ORDER BY sort_expression [ASC|DESC]]
    [frame_clause]
)
```

## Ranking Functions

### ROW_NUMBER

Assigns a unique sequential number to each row:

```sql
SELECT
    name,
    department,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM employees
```

Result:
| name | department | salary | rank |
|------|------------|--------|------|
| Alice | Engineering | 150000 | 1 |
| Bob | Sales | 120000 | 2 |
| Charlie | Engineering | 110000 | 3 |

### RANK

Assigns rank with gaps for ties:

```sql
SELECT
    name,
    score,
    RANK() OVER (ORDER BY score DESC) as rank
FROM contestants
```

Result:
| name | score | rank |
|------|-------|------|
| Alice | 95 | 1 |
| Bob | 95 | 1 |
| Charlie | 90 | 3 |

### DENSE_RANK

Assigns rank without gaps:

```sql
SELECT
    name,
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) as rank
FROM contestants
```

Result:
| name | score | rank |
|------|-------|------|
| Alice | 95 | 1 |
| Bob | 95 | 1 |
| Charlie | 90 | 2 |

## PARTITION BY

Divide results into partitions:

```sql
SELECT
    name,
    department,
    salary,
    ROW_NUMBER() OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) as dept_rank
FROM employees
```

Result:
| name | department | salary | dept_rank |
|------|------------|--------|-----------|
| Alice | Engineering | 150000 | 1 |
| Charlie | Engineering | 110000 | 2 |
| Bob | Sales | 120000 | 1 |
| Diana | Sales | 100000 | 2 |

## Value Functions

### LAG

Access previous row values:

```sql
SELECT
    date,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY date) as prev_revenue,
    revenue - LAG(revenue, 1) OVER (ORDER BY date) as change
FROM daily_sales
```

Result:
| date | revenue | prev_revenue | change |
|------|---------|--------------|--------|
| 2024-01-01 | 1000 | NULL | NULL |
| 2024-01-02 | 1200 | 1000 | 200 |
| 2024-01-03 | 1100 | 1200 | -100 |

### LEAD

Access following row values:

```sql
SELECT
    date,
    revenue,
    LEAD(revenue, 1) OVER (ORDER BY date) as next_revenue
FROM daily_sales
```

Result:
| date | revenue | next_revenue |
|------|---------|--------------|
| 2024-01-01 | 1000 | 1200 |
| 2024-01-02 | 1200 | 1100 |
| 2024-01-03 | 1100 | NULL |

### LAG/LEAD with Default

Specify a default value for NULL results:

```sql
SELECT
    date,
    revenue,
    LAG(revenue, 1, 0) OVER (ORDER BY date) as prev_revenue
FROM daily_sales
```

### FIRST_VALUE and LAST_VALUE

Get first/last value in the window:

```sql
SELECT
    name,
    department,
    salary,
    FIRST_VALUE(name) OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) as highest_paid
FROM employees
```

## Aggregate Window Functions

Use aggregate functions as window functions:

```sql
SELECT
    date,
    revenue,
    SUM(revenue) OVER (ORDER BY date) as running_total,
    AVG(revenue) OVER (ORDER BY date) as running_avg,
    COUNT(*) OVER (ORDER BY date) as row_count
FROM daily_sales
```

Result:
| date | revenue | running_total | running_avg | row_count |
|------|---------|---------------|-------------|-----------|
| 2024-01-01 | 1000 | 1000 | 1000.0 | 1 |
| 2024-01-02 | 1200 | 2200 | 1100.0 | 2 |
| 2024-01-03 | 1100 | 3300 | 1100.0 | 3 |

## Common Use Cases

### Top N per Group

Get top 3 products per category by sales:

```sql
WITH ranked AS (
    SELECT
        category,
        product,
        sales,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY sales DESC
        ) as rank
    FROM products
)
SELECT category, product, sales
FROM ranked
WHERE rank <= 3
```

### Running Totals

Calculate cumulative sum:

```sql
SELECT
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as cumulative_amount
FROM transactions
```

### Moving Average

Calculate 7-day moving average:

```sql
SELECT
    date,
    revenue,
    AVG(revenue) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM daily_metrics
```

### Year-over-Year Comparison

```sql
SELECT
    month,
    year,
    revenue,
    LAG(revenue, 12) OVER (ORDER BY year, month) as prev_year_revenue,
    revenue - LAG(revenue, 12) OVER (ORDER BY year, month) as yoy_change
FROM monthly_sales
```

### Percentile Ranking

Calculate each row's percentile:

```sql
SELECT
    name,
    score,
    CAST(RANK() OVER (ORDER BY score) AS DOUBLE) /
    CAST(COUNT(*) OVER () AS DOUBLE) * 100 as percentile
FROM students
```

### Gap Detection

Find gaps in sequential IDs:

```sql
SELECT
    id,
    LAG(id) OVER (ORDER BY id) as prev_id,
    id - LAG(id) OVER (ORDER BY id) as gap
FROM records
WHERE id - LAG(id) OVER (ORDER BY id) > 1
```

## Multiple Window Functions

Use multiple window functions in one query:

```sql
SELECT
    name,
    department,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as overall_rank,
    ROW_NUMBER() OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) as dept_rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM employees
```

## Named Windows

Define reusable window specifications:

```sql
SELECT
    date,
    revenue,
    SUM(revenue) OVER w as running_total,
    AVG(revenue) OVER w as running_avg,
    COUNT(*) OVER w as row_count
FROM daily_sales
WINDOW w AS (ORDER BY date)
```

## Performance Considerations

### Partition Size

Large partitions require more memory:

```rust
// Set memory limit for large window operations
let config = ConnectionConfig::new()
    .with_memory_limit(1024 * 1024 * 1024);
```

### Index Order

Window functions with ORDER BY benefit from sorted data:

```sql
-- Efficient: data is naturally sorted by date
SELECT date, SUM(x) OVER (ORDER BY date) FROM time_series

-- May require sort: random order
SELECT id, SUM(x) OVER (ORDER BY value) FROM random_data
```

## Limitations

Current limitations in Blaze window functions:

- `NTH_VALUE()` is not supported
- `NTILE()` is not supported
- `PERCENT_RANK()` is not supported
- `CUME_DIST()` is not supported
- Frame specifications have limited support

## Next Steps

- [SQL Reference](/docs/reference/sql) - Complete SQL syntax
- [Query Execution](/docs/concepts/query-execution) - How windows are processed
- [Query Optimization](/docs/advanced/query-optimization) - Performance tuning
