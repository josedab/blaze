---
sidebar_position: 4
---

# Error Handling

Blaze uses a comprehensive error type system to provide detailed information about failures.

## Error Type

All Blaze operations return `Result<T, BlazeError>`:

```rust
use blaze::{Connection, BlazeError, Result};

fn example() -> Result<()> {
    let conn = Connection::in_memory()?;
    conn.execute("INVALID SQL")?;  // Returns Err(BlazeError::Parse { ... })
    Ok(())
}
```

## Error Variants

### Parse Error

SQL syntax errors:

```rust
BlazeError::Parse {
    message: String,
    location: Option<Location>,
}
```

**When it occurs:**
- Invalid SQL syntax
- Unexpected tokens
- Unterminated strings

**Example:**
```rust
match conn.query("SELECTT * FROM users") {
    Err(BlazeError::Parse { message, location }) => {
        eprintln!("Syntax error: {}", message);
        if let Some(loc) = location {
            eprintln!("At line {}, column {}", loc.line, loc.column);
        }
    },
    _ => {},
}
```

### Analysis Error

Semantic errors during query analysis:

```rust
BlazeError::Analysis(String)
```

**When it occurs:**
- Unknown table name
- Unknown column name
- Invalid expression types
- Ambiguous column references

**Example:**
```rust
match conn.query("SELECT unknown_column FROM users") {
    Err(BlazeError::Analysis(msg)) => {
        eprintln!("Query analysis failed: {}", msg);
    },
    _ => {},
}
```

### Plan Error

Query planning failures:

```rust
BlazeError::Plan(String)
```

**When it occurs:**
- Invalid join conditions
- Unsupported query patterns
- Planning phase errors

### Optimization Error

Optimization phase failures:

```rust
BlazeError::Optimization(String)
```

**When it occurs:**
- Optimizer rule failures
- Invalid optimization transformations

### Execution Error

Runtime execution errors:

```rust
BlazeError::Execution(String)
```

**When it occurs:**
- Runtime computation errors
- Division by zero
- Overflow in arithmetic
- Function evaluation errors

**Example:**
```rust
match conn.query("SELECT 1 / 0") {
    Err(BlazeError::Execution(msg)) => {
        eprintln!("Execution error: {}", msg);
    },
    _ => {},
}
```

### Type Error

Type mismatch errors:

```rust
BlazeError::Type {
    expected: String,
    actual: String,
}
```

**When it occurs:**
- Incompatible types in comparison
- Invalid cast operations
- Function argument type mismatches

**Example:**
```rust
match conn.query("SELECT 'text' + 123") {
    Err(BlazeError::Type { expected, actual }) => {
        eprintln!("Type error: expected {}, got {}", expected, actual);
    },
    _ => {},
}
```

### Schema Error

Schema-related errors:

```rust
BlazeError::Schema { message: String }
```

**When it occurs:**
- Schema mismatch in UNION
- Invalid schema definition
- Column count mismatch

### Catalog Error

Catalog operation errors:

```rust
BlazeError::Catalog { name: String }
```

**When it occurs:**
- Table not found
- Database not found
- Schema not found
- Table already exists

**Example:**
```rust
match conn.query("SELECT * FROM nonexistent_table") {
    Err(BlazeError::Catalog { name }) => {
        eprintln!("Table '{}' not found", name);
    },
    _ => {},
}
```

### Resource Exhausted

Memory or resource limit exceeded:

```rust
BlazeError::ResourceExhausted { message: String }
```

**When it occurs:**
- Memory limit exceeded
- Too many open files
- Query too complex

**Example:**
```rust
let config = ConnectionConfig::new()
    .with_memory_limit(100 * 1024 * 1024);  // 100 MB

let conn = Connection::with_config(config)?;

match conn.query("SELECT * FROM huge_table") {
    Err(BlazeError::ResourceExhausted { message }) => {
        eprintln!("Resource limit: {}", message);
        // Can still use connection for smaller queries
    },
    _ => {},
}
```

### Not Implemented

Unsupported feature:

```rust
BlazeError::NotImplemented(String)
```

**When it occurs:**
- Unsupported SQL feature
- Unimplemented function
- Unsupported data type combination

**Example:**
```rust
match conn.query("SELECT MEDIAN(value) FROM data") {
    Err(BlazeError::NotImplemented(feature)) => {
        eprintln!("Feature not supported: {}", feature);
    },
    _ => {},
}
```

### Invalid Argument

Invalid function arguments:

```rust
BlazeError::InvalidArgument(String)
```

**When it occurs:**
- Invalid parameter values
- Out of range arguments
- Empty required values

### Internal Error

Unexpected internal errors:

```rust
BlazeError::Internal(String)
```

**When it occurs:**
- Internal assertion failures
- Unexpected state
- Should never happen (indicates a bug)

### External Errors

Errors from external libraries:

```rust
BlazeError::Arrow(arrow::error::ArrowError)
BlazeError::Parquet(parquet::errors::ParquetError)
BlazeError::Io(std::io::Error)
```

## Error Handling Patterns

### Basic Error Handling

```rust
use blaze::{Connection, BlazeError, Result};

fn run_query(conn: &Connection, sql: &str) -> Result<()> {
    match conn.query(sql) {
        Ok(results) => {
            println!("Query returned {} batches", results.len());
            Ok(())
        }
        Err(e) => {
            eprintln!("Query failed: {}", e);
            Err(e)
        }
    }
}
```

### Detailed Error Handling

```rust
fn handle_query_error(error: BlazeError) {
    match error {
        BlazeError::Parse { message, location } => {
            eprintln!("SQL syntax error: {}", message);
            if let Some(loc) = location {
                eprintln!("  at line {}, column {}", loc.line, loc.column);
            }
        }
        BlazeError::Analysis(msg) => {
            eprintln!("Query analysis error: {}", msg);
            eprintln!("  Check table and column names");
        }
        BlazeError::Catalog { name } => {
            eprintln!("Object '{}' not found", name);
            eprintln!("  Available tables: ...");
        }
        BlazeError::Type { expected, actual } => {
            eprintln!("Type mismatch: expected {}, got {}", expected, actual);
        }
        BlazeError::ResourceExhausted { message } => {
            eprintln!("Resource limit exceeded: {}", message);
            eprintln!("  Consider using LIMIT or increasing memory");
        }
        BlazeError::NotImplemented(feature) => {
            eprintln!("Feature not supported: {}", feature);
        }
        _ => {
            eprintln!("Error: {}", error);
        }
    }
}
```

### Error Recovery

```rust
fn query_with_fallback(conn: &Connection) -> Result<Vec<RecordBatch>> {
    // Try primary query
    match conn.query("SELECT * FROM primary_table") {
        Ok(results) => return Ok(results),
        Err(BlazeError::Catalog { .. }) => {
            // Table doesn't exist, try fallback
        }
        Err(e) => return Err(e),  // Other errors propagate
    }

    // Try fallback table
    conn.query("SELECT * FROM fallback_table")
}
```

### Retry Pattern

```rust
fn query_with_retry(
    conn: &Connection,
    sql: &str,
    max_retries: u32,
) -> Result<Vec<RecordBatch>> {
    let mut retries = 0;

    loop {
        match conn.query(sql) {
            Ok(results) => return Ok(results),
            Err(BlazeError::ResourceExhausted { .. }) if retries < max_retries => {
                retries += 1;
                eprintln!("Memory pressure, retry {}/{}", retries, max_retries);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Helper Methods

`BlazeError` provides helper methods for creating errors:

```rust
// Parse error
BlazeError::parse("unexpected token", Some(location))

// Analysis error
BlazeError::analysis("column 'x' not found")

// Type error
BlazeError::type_error("Int64", "Utf8")

// Execution error
BlazeError::execution("division by zero")

// Not implemented
BlazeError::not_implemented("LATERAL JOIN")

// Catalog error
BlazeError::catalog("table 'users' not found")

// Schema error
BlazeError::schema("column count mismatch")

// Invalid argument
BlazeError::invalid_argument("batch_size must be positive")
```

## Converting Errors

### From Arrow Errors

```rust
use arrow::error::ArrowError;

let arrow_err = ArrowError::InvalidArgumentError("msg".to_string());
let blaze_err: BlazeError = arrow_err.into();
```

### From IO Errors

```rust
let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
let blaze_err: BlazeError = io_err.into();
```

## Display and Debug

```rust
let error = BlazeError::analysis("column not found");

// Display (user-friendly)
println!("{}", error);  // "Analysis error: column not found"

// Debug (detailed)
println!("{:?}", error);  // BlazeError::Analysis("column not found")
```

## Error Source Chain

`BlazeError` implements `std::error::Error` with source chain:

```rust
use std::error::Error;

fn print_error_chain(error: &dyn Error) {
    eprintln!("Error: {}", error);

    let mut source = error.source();
    while let Some(s) = source {
        eprintln!("  Caused by: {}", s);
        source = s.source();
    }
}
```
