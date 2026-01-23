---
sidebar_position: 3
---

# Prepared Statements

Prepared statements allow you to parse and plan a query once, then execute it multiple times with different parameter values. This improves performance for repeated queries.

## Basic Usage

### Prepare and Execute

```rust
use blaze::{Connection, ScalarValue, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    conn.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)")?;
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")?;

    // Prepare the query (parses and plans once)
    let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

    // Execute with different parameters
    let results1 = stmt.execute(&[ScalarValue::Int64(Some(1))])?;
    let results2 = stmt.execute(&[ScalarValue::Int64(Some(2))])?;

    Ok(())
}
```

### Parameter Placeholders

Use `$1`, `$2`, etc. for positional parameters:

```rust
// Single parameter
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

// Multiple parameters
let stmt = conn.prepare("
    SELECT * FROM users
    WHERE age BETWEEN $1 AND $2
    AND status = $3
")?;

let results = stmt.execute(&[
    ScalarValue::Int64(Some(20)),  // $1
    ScalarValue::Int64(Some(30)),  // $2
    ScalarValue::Utf8(Some("active".to_string())),  // $3
])?;
```

## Parameter Types

Blaze supports various parameter types via `ScalarValue`:

```rust
use blaze::ScalarValue;

// Integer types
ScalarValue::Int64(Some(42))
ScalarValue::Int32(Some(42))

// Floating point
ScalarValue::Float64(Some(3.14))
ScalarValue::Float32(Some(3.14))

// String
ScalarValue::Utf8(Some("hello".to_string()))

// Boolean
ScalarValue::Boolean(Some(true))

// NULL (any type)
ScalarValue::Null

// Optional values
ScalarValue::Int64(None)  // NULL Int64
ScalarValue::Utf8(None)   // NULL String
```

## Statement Cache

For frequently used queries, use the statement cache:

```rust
use blaze::{Connection, PreparedStatementCache, ScalarValue, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Create cache with capacity for 100 statements
    let cache = PreparedStatementCache::new(100);

    // First call parses, plans, and caches
    let stmt1 = conn.prepare_cached("SELECT * FROM users WHERE id = $1", &cache)?;

    // Subsequent calls reuse the cached plan
    let stmt2 = conn.prepare_cached("SELECT * FROM users WHERE id = $1", &cache)?;

    // Different query creates new cache entry
    let stmt3 = conn.prepare_cached("SELECT * FROM orders WHERE user_id = $1", &cache)?;

    Ok(())
}
```

### Cache Benefits

| Operation | Without Cache | With Cache |
|-----------|--------------|------------|
| Parse SQL | Every call | First call only |
| Create logical plan | Every call | First call only |
| Create physical plan | Every call | Every call* |
| Execute | Every call | Every call |

*Physical plan is created per execution to handle parameter binding.

## Query Parameter Information

Inspect prepared statement parameters:

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1 AND age > $2")?;

// Get parameter information
for param in stmt.parameters() {
    println!("Parameter {}: {:?}", param.index, param.data_type);
}
```

## Use Cases

### Batch Processing

Process multiple records with the same query pattern:

```rust
let stmt = conn.prepare("INSERT INTO events VALUES ($1, $2, $3)")?;

for event in events {
    stmt.execute(&[
        ScalarValue::Int64(Some(event.id)),
        ScalarValue::Utf8(Some(event.type_.clone())),
        ScalarValue::Utf8(Some(event.timestamp.clone())),
    ])?;
}
```

### User-Provided Values

Safely handle user input without SQL injection:

```rust
fn find_user(conn: &Connection, user_id: i64) -> Result<Vec<RecordBatch>> {
    let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
    stmt.execute(&[ScalarValue::Int64(Some(user_id))])
}

// Safe from SQL injection
let results = find_user(&conn, user_provided_id)?;
```

### Search with Optional Filters

Build queries with optional conditions:

```rust
fn search_products(
    conn: &Connection,
    category: Option<&str>,
    min_price: Option<f64>,
    max_price: Option<f64>,
) -> Result<Vec<RecordBatch>> {
    // Build query dynamically based on provided filters
    let mut conditions = Vec::new();
    let mut params = Vec::new();

    if let Some(cat) = category {
        conditions.push(format!("category = ${}", params.len() + 1));
        params.push(ScalarValue::Utf8(Some(cat.to_string())));
    }

    if let Some(min) = min_price {
        conditions.push(format!("price >= ${}", params.len() + 1));
        params.push(ScalarValue::Float64(Some(min)));
    }

    if let Some(max) = max_price {
        conditions.push(format!("price <= ${}", params.len() + 1));
        params.push(ScalarValue::Float64(Some(max)));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let sql = format!("SELECT * FROM products {}", where_clause);
    let stmt = conn.prepare(&sql)?;
    stmt.execute(&params)
}
```

## Error Handling

### Parameter Count Mismatch

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1 AND age = $2")?;

// Error: expected 2 parameters, got 1
match stmt.execute(&[ScalarValue::Int64(Some(1))]) {
    Ok(_) => {},
    Err(e) => eprintln!("Parameter error: {}", e),
}
```

### Type Mismatch

```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;

// id column is INT, but we're passing a string
// This may work with implicit casting, or may error
let results = stmt.execute(&[ScalarValue::Utf8(Some("1".to_string()))])?;
```

## Performance Tips

### Reuse Prepared Statements

```rust
// Good: prepare once, execute many
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
for id in &[1, 2, 3, 4, 5] {
    stmt.execute(&[ScalarValue::Int64(Some(*id))])?;
}

// Less efficient: prepare each time
for id in &[1, 2, 3, 4, 5] {
    let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
    stmt.execute(&[ScalarValue::Int64(Some(*id))])?;
}
```

### Use Statement Cache for Global Queries

```rust
// Application-level cache
lazy_static! {
    static ref STMT_CACHE: PreparedStatementCache = PreparedStatementCache::new(100);
}

fn get_user(conn: &Connection, id: i64) -> Result<Vec<RecordBatch>> {
    let stmt = conn.prepare_cached("SELECT * FROM users WHERE id = $1", &STMT_CACHE)?;
    stmt.execute(&[ScalarValue::Int64(Some(id))])
}
```

### Batch Parameters When Possible

```rust
// Instead of multiple single-row queries
for id in user_ids {
    conn.query(&format!("SELECT * FROM users WHERE id = {}", id))?;
}

// Use IN clause with single query
let ids = user_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",");
conn.query(&format!("SELECT * FROM users WHERE id IN ({})", ids))?;
```

## Limitations

- Parameters cannot be used for:
  - Table names: `SELECT * FROM $1` (invalid)
  - Column names: `SELECT $1 FROM users` (invalid)
  - SQL keywords: `SELECT * FROM users $1 BY id` (invalid)

- Parameters work for:
  - Values in WHERE clauses
  - Values in INSERT statements
  - Literals in expressions

## Next Steps

- [Connection API](/docs/concepts/connection-api) - Connection methods
- [SQL Reference](/docs/reference/sql) - SQL syntax
- [Error Handling](/docs/reference/error-handling) - Error types
