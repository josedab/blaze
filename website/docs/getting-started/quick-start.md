---
sidebar_position: 2
---

# Quick Start

Get up and running with Blaze in under 5 minutes. This guide walks you through creating a connection, loading data, and running queries.

## Prerequisites

Make sure you have [installed Blaze](/docs/getting-started/installation) in your project:

```bash
cargo add blaze
```

## Create a Connection

Every interaction with Blaze starts with a `Connection`. Create an in-memory connection:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    // Create an in-memory database
    let conn = Connection::in_memory()?;

    Ok(())
}
```

## Register Data

Blaze doesn't use traditional `CREATE TABLE` or `INSERT` statements. Instead, you register data sources directly:

### From Arrow RecordBatches

```rust
use blaze::{Connection, Result};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    // Create data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![30, 25, 35])),
        ],
    )?;

    // Register as a table
    conn.register_batches("users", vec![batch])?;

    Ok(())
}
```

### From Files

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register CSV file
    conn.register_csv("customers", "data/customers.csv")?;

    // Register Parquet file (preferred for large datasets)
    conn.register_parquet("orders", "data/orders.parquet")?;

    Ok(())
}
```

## Query Data

Run SELECT queries and iterate over results:

```rust
use blaze::{Connection, Result};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create and register data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![30, 25, 35])),
        ],
    )?;

    conn.register_batches("users", vec![batch])?;

    // Query all users
    let results = conn.query("SELECT * FROM users")?;

    for batch in &results {
        println!("Got {} rows", batch.num_rows());
    }

    // Query with filtering and ordering
    let results = conn.query("
        SELECT name, age
        FROM users
        WHERE age > 25
        ORDER BY age DESC
    ")?;

    Ok(())
}
```

## Complete Example

Here's a complete example demonstrating common operations:

```rust
use blaze::{Connection, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Create connection
    let conn = Connection::in_memory()?;

    // 2. Create products table
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let products = RecordBatch::try_new(
        products_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Desk"])),
            Arc::new(Float64Array::from(vec![999.99, 29.99, 299.99])),
            Arc::new(StringArray::from(vec!["Electronics", "Electronics", "Furniture"])),
        ],
    )?;

    conn.register_batches("products", vec![products])?;

    // 3. Create sales table
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
        Field::new("quantity", DataType::Int64, false),
    ]));

    let sales = RecordBatch::try_new(
        sales_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(Int64Array::from(vec![5, 20, 3])),
        ],
    )?;

    conn.register_batches("sales", vec![sales])?;

    // 4. Run analytical queries

    // Simple aggregation
    let results = conn.query("
        SELECT category, COUNT(*) as product_count
        FROM products
        GROUP BY category
    ")?;
    println!("Products by category:");
    print_batches(&results)?;

    // Join with aggregation
    let results = conn.query("
        SELECT
            p.name,
            SUM(s.quantity) as total_sold,
            SUM(CAST(s.quantity AS DOUBLE) * p.price) as revenue
        FROM products p
        JOIN sales s ON p.id = s.product_id
        GROUP BY p.name
        ORDER BY revenue DESC
    ")?;
    println!("\nSales summary:");
    print_batches(&results)?;

    Ok(())
}
```

Output:
```
Products by category:
+-------------+---------------+
| category    | product_count |
+-------------+---------------+
| Electronics | 2             |
| Furniture   | 1             |
+-------------+---------------+

Sales summary:
+--------+------------+---------+
| name   | total_sold | revenue |
+--------+------------+---------+
| Laptop | 8          | 7999.92 |
| Mouse  | 20         | 599.80  |
+--------+------------+---------+
```

## Working with Files

Blaze shines when querying files directly:

```rust
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register files as tables
    conn.register_csv("customers", "data/customers.csv")?;
    conn.register_parquet("orders", "data/orders.parquet")?;

    // Query across both files with a JOIN
    let results = conn.query("
        SELECT
            c.name,
            COUNT(o.id) as order_count,
            SUM(o.amount) as total_spent
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name
        ORDER BY total_spent DESC
    ")?;

    Ok(())
}
```

## SQL Features

Blaze supports a rich set of SQL features:

```sql
-- Window functions
SELECT name, amount,
       ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
FROM sales;

-- CTEs (Common Table Expressions)
WITH top_customers AS (
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 10
)
SELECT c.name, tc.total
FROM top_customers tc
JOIN customers c ON tc.customer_id = c.id;

-- Aggregations
SELECT category,
       COUNT(*) as count,
       AVG(price) as avg_price,
       MIN(price) as min_price,
       MAX(price) as max_price
FROM products
GROUP BY category;

-- CASE expressions
SELECT name,
       CASE
           WHEN price > 500 THEN 'premium'
           WHEN price > 100 THEN 'standard'
           ELSE 'budget'
       END as tier
FROM products;
```

## Next Steps

Now that you've run your first queries:

- [Loading Data](/docs/getting-started/loading-data) - Learn all the ways to import data
- [SQL Reference](/docs/reference/sql) - Complete SQL syntax reference
- [Working with Files](/docs/guides/working-with-files) - Query Parquet, CSV, and Delta Lake
- [Python Integration](/docs/guides/python-integration) - Use Blaze from Python
