---
sidebar_position: 1
---

# SQL Reference

Complete reference for Blaze's SQL dialect, which is SQL:2016 compliant.

## Statements

### SELECT

```sql
SELECT [DISTINCT] select_list
FROM table_reference
[WHERE condition]
[GROUP BY grouping_elements]
[HAVING condition]
[ORDER BY sort_specification]
[LIMIT count [OFFSET offset]]
```

#### Examples

```sql
-- Basic select
SELECT * FROM users

-- With projection
SELECT name, age FROM users

-- With alias
SELECT name AS user_name, age * 2 AS double_age FROM users

-- Distinct values
SELECT DISTINCT category FROM products
```

### INSERT

```sql
INSERT INTO table_name [(column_list)]
VALUES (value_list) [, (value_list)...]

INSERT INTO table_name [(column_list)]
SELECT ...
```

#### Examples

```sql
-- Single row
INSERT INTO users VALUES (1, 'Alice', 30)

-- Multiple rows
INSERT INTO users VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35)

-- With column list
INSERT INTO users (name, age) VALUES ('Alice', 30)

-- From select
INSERT INTO archive SELECT * FROM users WHERE created_at < '2023-01-01'
```

### UPDATE

```sql
UPDATE table_name
SET column = value [, column = value...]
[WHERE condition]
```

#### Examples

```sql
-- Update all rows
UPDATE users SET status = 'inactive'

-- Conditional update
UPDATE users SET status = 'active' WHERE last_login > '2024-01-01'

-- Multiple columns
UPDATE products SET price = price * 1.1, updated_at = NOW()
```

### DELETE

```sql
DELETE FROM table_name
[WHERE condition]
```

#### Examples

```sql
-- Delete all
DELETE FROM temp_data

-- Conditional delete
DELETE FROM users WHERE status = 'deleted'
```

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column_name data_type [constraints],
    ...
)
```

#### Examples

```sql
CREATE TABLE users (
    id INT,
    name VARCHAR,
    email VARCHAR,
    age INT,
    created_at TIMESTAMP
)
```

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name
```

### COPY TO

Export query results to files in various formats:

```sql
COPY (query) TO 'path/to/file'
COPY (query) TO 'path/to/file' WITH (FORMAT format_name [, options...])
```

#### Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| `PARQUET` | `.parquet` | Columnar format, best for analytics |
| `CSV` | `.csv` | Comma-separated values |
| `JSON` | `.json` | JSON Lines format |

#### Examples

```sql
-- Export to Parquet (default, auto-detected from extension)
COPY (SELECT * FROM sales WHERE year = 2024) TO 'sales_2024.parquet'

-- Export to CSV with header
COPY (SELECT * FROM users) TO 'users.csv' WITH (FORMAT CSV, HEADER true)

-- Export to JSON
COPY (SELECT id, name, email FROM customers) TO 'customers.json' WITH (FORMAT JSON)

-- Export aggregated results
COPY (
    SELECT region, SUM(amount) as total
    FROM sales
    GROUP BY region
    ORDER BY total DESC
) TO 'regional_summary.parquet'
```

#### Options

| Option | Values | Description |
|--------|--------|-------------|
| `FORMAT` | `PARQUET`, `CSV`, `JSON` | Output format |
| `HEADER` | `true`, `false` | Include header row (CSV only) |
| `DELIMITER` | character | Field delimiter (CSV only, default `,`) |

## Data Types

| SQL Type | Description | Arrow Type |
|----------|-------------|------------|
| `BOOLEAN` | True/false | Boolean |
| `TINYINT` | 8-bit integer | Int8 |
| `SMALLINT` | 16-bit integer | Int16 |
| `INT`, `INTEGER` | 32-bit integer | Int32 |
| `BIGINT` | 64-bit integer | Int64 |
| `REAL`, `FLOAT` | 32-bit float | Float32 |
| `DOUBLE` | 64-bit float | Float64 |
| `VARCHAR`, `STRING`, `TEXT` | Variable string | Utf8 |
| `DATE` | Date (no time) | Date32 |
| `TIMESTAMP` | Date and time | Timestamp |
| `DECIMAL(p,s)` | Fixed precision | Decimal128 |

## Expressions

### Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `a + b` |
| `-` | Subtraction | `a - b` |
| `*` | Multiplication | `a * b` |
| `/` | Division | `a / b` |
| `%` | Modulo | `a % b` |

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `a = b` |
| `<>`, `!=` | Not equal | `a <> b` |
| `<` | Less than | `a < b` |
| `>` | Greater than | `a > b` |
| `<=` | Less or equal | `a <= b` |
| `>=` | Greater or equal | `a >= b` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Logical and | `a AND b` |
| `OR` | Logical or | `a OR b` |
| `NOT` | Logical not | `NOT a` |

### Special Operators

```sql
-- BETWEEN
WHERE age BETWEEN 18 AND 65

-- IN
WHERE status IN ('active', 'pending', 'review')

-- LIKE (pattern matching)
WHERE name LIKE 'A%'      -- Starts with A
WHERE name LIKE '%son'    -- Ends with son
WHERE name LIKE '%ann%'   -- Contains ann

-- IS NULL / IS NOT NULL
WHERE email IS NOT NULL

-- CASE
CASE
    WHEN age < 18 THEN 'minor'
    WHEN age < 65 THEN 'adult'
    ELSE 'senior'
END
```

## Joins

### INNER JOIN

Returns matching rows from both tables:

```sql
SELECT * FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
```

### LEFT JOIN

Returns all rows from left table, matching rows from right:

```sql
SELECT * FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
```

### RIGHT JOIN

Returns all rows from right table, matching rows from left:

```sql
SELECT * FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.id
```

### FULL OUTER JOIN

Returns all rows from both tables:

```sql
SELECT * FROM table_a a
FULL OUTER JOIN table_b b ON a.id = b.a_id
```

### CROSS JOIN

Cartesian product of both tables:

```sql
SELECT * FROM colors
CROSS JOIN sizes
```

### SEMI JOIN

Returns rows from left where match exists in right:

```sql
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
```

### ANTI JOIN

Returns rows from left where no match in right:

```sql
SELECT * FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
```

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count distinct values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

```sql
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary
FROM employees
GROUP BY department
```

## Window Functions

### Ranking Functions

```sql
ROW_NUMBER() OVER (ORDER BY column)
RANK() OVER (ORDER BY column)
DENSE_RANK() OVER (ORDER BY column)
NTILE(n) OVER (ORDER BY column)            -- Divide into n buckets
PERCENT_RANK() OVER (ORDER BY column)      -- Relative rank (0-1)
CUME_DIST() OVER (ORDER BY column)         -- Cumulative distribution
```

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Unique sequential number for each row |
| `RANK()` | Rank with gaps for ties |
| `DENSE_RANK()` | Rank without gaps for ties |
| `NTILE(n)` | Divide rows into n equal buckets |
| `PERCENT_RANK()` | (rank - 1) / (count - 1), range 0-1 |
| `CUME_DIST()` | Fraction of rows ≤ current row |

### Value Functions

```sql
LAG(column, offset, default) OVER (ORDER BY column)
LEAD(column, offset, default) OVER (ORDER BY column)
FIRST_VALUE(column) OVER (ORDER BY column)
LAST_VALUE(column) OVER (ORDER BY column)
NTH_VALUE(column, n) OVER (ORDER BY column)
```

| Function | Description |
|----------|-------------|
| `LAG(col, n, default)` | Value n rows before current |
| `LEAD(col, n, default)` | Value n rows after current |
| `FIRST_VALUE(col)` | First value in window frame |
| `LAST_VALUE(col)` | Last value in window frame |
| `NTH_VALUE(col, n)` | Nth value in window frame |

### Aggregate Windows

```sql
SUM(column) OVER (ORDER BY date)           -- Running total
AVG(column) OVER (PARTITION BY group)      -- Group average
COUNT(*) OVER ()                           -- Total count
```

### Window Examples

```sql
-- Quartile assignment
SELECT name, score,
    NTILE(4) OVER (ORDER BY score DESC) as quartile
FROM students

-- Percentile ranking
SELECT name, salary,
    PERCENT_RANK() OVER (ORDER BY salary) as percentile
FROM employees

-- Compare to nth value
SELECT product, revenue,
    NTH_VALUE(product, 1) OVER (ORDER BY revenue DESC) as top_product
FROM sales
```

## Scalar Functions

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER('hello')` → `'HELLO'` |
| `LOWER(s)` | Lowercase | `LOWER('HELLO')` → `'hello'` |
| `LENGTH(s)` | String length | `LENGTH('hello')` → `5` |
| `TRIM(s)` | Remove whitespace | `TRIM('  hi  ')` → `'hi'` |
| `LTRIM(s)` | Left trim | `LTRIM('  hi')` → `'hi'` |
| `RTRIM(s)` | Right trim | `RTRIM('hi  ')` → `'hi'` |
| `CONCAT(a, b, ...)` | Concatenate | `CONCAT('a', 'b')` → `'ab'` |
| `SUBSTRING(s, start, len)` | Extract substring | `SUBSTRING('hello', 2, 3)` → `'ell'` |
| `REPLACE(s, from, to)` | Replace occurrences | `REPLACE('hello', 'l', 'L')` → `'heLLo'` |
| `LEFT(s, n)` | First n characters | `LEFT('hello', 3)` → `'hel'` |
| `RIGHT(s, n)` | Last n characters | `RIGHT('hello', 3)` → `'llo'` |
| `LPAD(s, len, pad)` | Left pad to length | `LPAD('hi', 5, '0')` → `'000hi'` |
| `RPAD(s, len, pad)` | Right pad to length | `RPAD('hi', 5, '0')` → `'hi000'` |
| `REVERSE(s)` | Reverse string | `REVERSE('hello')` → `'olleh'` |
| `SPLIT_PART(s, delim, n)` | Get nth split part | `SPLIT_PART('a,b,c', ',', 2)` → `'b'` |
| `REGEXP_MATCH(s, pattern)` | Regex match | `REGEXP_MATCH('hello', 'e.+o')` → `true` |
| `REGEXP_REPLACE(s, pat, rep)` | Regex replace | `REGEXP_REPLACE('hello', '[aeiou]', 'X')` → `'hXllX'` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(x)` | Absolute value | `ABS(-5)` → `5` |
| `ROUND(x, d)` | Round to d decimals | `ROUND(3.14159, 2)` → `3.14` |
| `CEIL(x)` | Ceiling | `CEIL(3.2)` → `4` |
| `FLOOR(x)` | Floor | `FLOOR(3.8)` → `3` |

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `CURRENT_DATE` | Current date |
| `CURRENT_TIMESTAMP`, `NOW()` | Current timestamp |
| `EXTRACT(field FROM date)` | Extract date part |
| `DATE_TRUNC(unit, date)` | Truncate to unit |
| `YEAR(date)` | Extract year |
| `MONTH(date)` | Extract month (1-12) |
| `DAY(date)` | Extract day of month |
| `HOUR(timestamp)` | Extract hour (0-23) |
| `MINUTE(timestamp)` | Extract minute (0-59) |
| `SECOND(timestamp)` | Extract second (0-59) |
| `DATE_ADD(date, interval)` | Add interval to date |
| `DATE_SUB(date, interval)` | Subtract interval from date |
| `DATE_DIFF(date1, date2)` | Days between dates |
| `TO_DATE(string, format)` | Parse string to date |
| `TO_TIMESTAMP(string, format)` | Parse string to timestamp |

```sql
-- Extract parts
SELECT EXTRACT(YEAR FROM created_at) as year
SELECT YEAR(created_at), MONTH(created_at), DAY(created_at)

-- Truncate to day
SELECT DATE_TRUNC('day', timestamp) as day

-- Current time
SELECT CURRENT_TIMESTAMP as now

-- Date arithmetic
SELECT DATE_ADD(order_date, 30) as due_date
SELECT DATE_DIFF(end_date, start_date) as duration_days

-- Parse dates
SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD')
SELECT TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH:MI:SS')
```

### Utility Functions

| Function | Description |
|----------|-------------|
| `GREATEST(a, b, ...)` | Maximum of values |
| `LEAST(a, b, ...)` | Minimum of values |
| `IFNULL(value, default)` | Return default if null |
| `NVL(value, default)` | Return default if null (Oracle-style) |

```sql
-- Get max/min across columns
SELECT GREATEST(score1, score2, score3) as best_score
SELECT LEAST(price1, price2) as lowest_price

-- Handle nulls
SELECT IFNULL(nickname, 'Anonymous') as display_name
SELECT NVL(commission, 0) as commission
```

### Null Functions

| Function | Description |
|----------|-------------|
| `COALESCE(a, b, ...)` | First non-null value |
| `NULLIF(a, b)` | NULL if a = b |

```sql
-- Default value for NULL
SELECT COALESCE(nickname, name, 'Anonymous') as display_name

-- Convert empty to NULL
SELECT NULLIF(status, '') as status
```

## Common Table Expressions (CTEs)

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name
```

### Example

```sql
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as total
    FROM sales
    GROUP BY DATE_TRUNC('month', sale_date)
),
ranked AS (
    SELECT
        month,
        total,
        RANK() OVER (ORDER BY total DESC) as rank
    FROM monthly_sales
)
SELECT * FROM ranked WHERE rank <= 3
```

## Set Operations

### UNION

Combine results (removes duplicates):

```sql
SELECT name FROM customers
UNION
SELECT name FROM suppliers
```

### UNION ALL

Combine results (keeps duplicates):

```sql
SELECT name FROM table_a
UNION ALL
SELECT name FROM table_b
```

### INTERSECT

Rows in both queries:

```sql
SELECT id FROM active_users
INTERSECT
SELECT id FROM premium_users
```

### EXCEPT

Rows in first but not second:

```sql
SELECT id FROM all_users
EXCEPT
SELECT id FROM banned_users
```

## Subqueries

### Scalar Subquery

```sql
SELECT name, (SELECT AVG(salary) FROM employees) as company_avg
FROM employees
```

### IN Subquery

```sql
SELECT * FROM orders
WHERE customer_id IN (SELECT id FROM vip_customers)
```

### EXISTS Subquery

```sql
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.id
    AND o.amount > 1000
)
```

### Correlated Subquery

```sql
SELECT *,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count
FROM customers c
```

## Query Clauses

### ORDER BY

```sql
ORDER BY column [ASC|DESC] [NULLS FIRST|LAST]
```

```sql
-- Ascending (default)
ORDER BY name

-- Descending
ORDER BY created_at DESC

-- Multiple columns
ORDER BY department, salary DESC

-- Nulls handling
ORDER BY value DESC NULLS LAST
```

### LIMIT and OFFSET

```sql
LIMIT count [OFFSET offset]
```

```sql
-- First 10 rows
LIMIT 10

-- Rows 11-20
LIMIT 10 OFFSET 10

-- Skip first 5
OFFSET 5
```

### GROUP BY

```sql
GROUP BY column [, column...]
```

```sql
-- Single column
GROUP BY department

-- Multiple columns
GROUP BY region, department

-- With HAVING
GROUP BY department
HAVING COUNT(*) > 5
```

## Reserved Words

The following are reserved words in Blaze SQL:

```
ALL, AND, AS, ASC, BETWEEN, BY, CASE, CAST, CREATE, CROSS,
DELETE, DESC, DISTINCT, DROP, ELSE, END, EXCEPT, EXISTS,
FALSE, FROM, FULL, GROUP, HAVING, IF, IN, INNER, INSERT,
INTERSECT, INTO, IS, JOIN, LEFT, LIKE, LIMIT, NOT, NULL,
OFFSET, ON, OR, ORDER, OUTER, RIGHT, SELECT, SET, TABLE,
THEN, TRUE, UNION, UPDATE, VALUES, WHEN, WHERE, WITH
```

Use double quotes to escape reserved words as identifiers:

```sql
SELECT "order", "group" FROM "table"
```
