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
```

### Value Functions

```sql
LAG(column, offset, default) OVER (ORDER BY column)
LEAD(column, offset, default) OVER (ORDER BY column)
FIRST_VALUE(column) OVER (ORDER BY column)
LAST_VALUE(column) OVER (ORDER BY column)
```

### Aggregate Windows

```sql
SUM(column) OVER (ORDER BY date)           -- Running total
AVG(column) OVER (PARTITION BY group)      -- Group average
COUNT(*) OVER ()                           -- Total count
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

```sql
-- Extract parts
SELECT EXTRACT(YEAR FROM created_at) as year

-- Truncate to day
SELECT DATE_TRUNC('day', timestamp) as day

-- Current time
SELECT CURRENT_TIMESTAMP as now
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
