# User-Defined Functions Cookbook

## Registering a Scalar UDF

```rust
use blaze::{Connection, ScalarValue};

let conn = Connection::in_memory()?;

// Register a function that doubles an integer
conn.register_udf(
    "double",
    vec![blaze::DataType::Int64],
    blaze::DataType::Int64,
    |args| {
        if let Some(ScalarValue::Int64(Some(v))) = args.first() {
            Ok(ScalarValue::Int64(Some(v * 2)))
        } else {
            Ok(ScalarValue::Int64(None))
        }
    },
)?;

let results = conn.query("SELECT double(42)")?;
// Returns: 84
```

## String UDF

```rust
conn.register_udf(
    "shout",
    vec![blaze::DataType::Utf8],
    blaze::DataType::Utf8,
    |args| {
        if let Some(ScalarValue::Utf8(Some(s))) = args.first() {
            Ok(ScalarValue::Utf8(Some(format!("{}!!!", s.to_uppercase()))))
        } else {
            Ok(ScalarValue::Utf8(None))
        }
    },
)?;

let results = conn.query("SELECT shout('hello')")?;
// Returns: "HELLO!!!"
```
