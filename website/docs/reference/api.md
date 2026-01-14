---
sidebar_position: 2
---

# API Reference

Complete reference for Blaze's Rust API.

## Connection

The main entry point for database operations.

### Creating Connections

#### `Connection::in_memory()`

Create an in-memory database:

```rust
pub fn in_memory() -> Result<Self>
```

**Example:**
```rust
let conn = Connection::in_memory()?;
```

#### `Connection::with_config()`

Create with custom configuration:

```rust
pub fn with_config(config: ConnectionConfig) -> Result<Self>
```

**Example:**
```rust
let config = ConnectionConfig::new()
    .with_batch_size(4096)
    .with_memory_limit(1024 * 1024 * 1024);

let conn = Connection::with_config(config)?;
```

### Query Methods

#### `query()`

Execute a SELECT query and return results:

```rust
pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>>
```

**Parameters:**
- `sql`: SQL query string

**Returns:** Vector of Arrow RecordBatches

**Example:**
```rust
let results = conn.query("SELECT * FROM users WHERE age > 25")?;
for batch in results {
    println!("Got {} rows", batch.num_rows());
}
```

#### `execute()`

Execute DDL/DML statement:

```rust
pub fn execute(&self, sql: &str) -> Result<usize>
```

**Parameters:**
- `sql`: SQL statement (CREATE, INSERT, UPDATE, DELETE)

**Returns:** Number of affected rows

**Example:**
```rust
conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;
let inserted = conn.execute("INSERT INTO users VALUES (1, 'Alice')")?;
```

### Table Registration

#### `register_csv()`

Register a CSV file as a table:

```rust
pub fn register_csv(&self, name: &str, path: impl AsRef<Path>) -> Result<()>
```

**Parameters:**
- `name`: Table name
- `path`: Path to CSV file

**Example:**
```rust
conn.register_csv("sales", "data/sales.csv")?;
```

#### `register_csv_with_options()`

Register CSV with custom parsing options:

```rust
pub fn register_csv_with_options(
    &self,
    name: &str,
    path: impl AsRef<Path>,
    options: CsvOptions,
) -> Result<()>
```

**Example:**
```rust
let options = CsvOptions::default()
    .with_delimiter(b';')
    .with_has_header(true);

conn.register_csv_with_options("data", "file.csv", options)?;
```

#### `register_parquet()`

Register a Parquet file as a table:

```rust
pub fn register_parquet(&self, name: &str, path: impl AsRef<Path>) -> Result<()>
```

**Example:**
```rust
conn.register_parquet("events", "data/events.parquet")?;
```

#### `register_batches()`

Register Arrow RecordBatches as a table:

```rust
pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()>
```

**Example:**
```rust
let batches = vec![/* RecordBatch instances */];
conn.register_batches("my_data", batches)?;
```

#### `register_table()`

Register a custom TableProvider:

```rust
pub fn register_table(
    &self,
    name: &str,
    table: Arc<dyn TableProvider>,
) -> Result<()>
```

**Example:**
```rust
let provider: Arc<dyn TableProvider> = Arc::new(MyCustomProvider::new());
conn.register_table("custom", provider)?;
```

### Table Management

#### `list_tables()`

List all registered tables:

```rust
pub fn list_tables(&self) -> Vec<String>
```

**Example:**
```rust
for table in conn.list_tables() {
    println!("Table: {}", table);
}
```

#### `table_schema()`

Get a table's schema:

```rust
pub fn table_schema(&self, name: &str) -> Option<Schema>
```

**Example:**
```rust
if let Some(schema) = conn.table_schema("users") {
    for field in schema.fields() {
        println!("{}: {:?}", field.name(), field.data_type());
    }
}
```

#### `deregister_table()`

Remove a table from the catalog:

```rust
pub fn deregister_table(&self, name: &str) -> Result<()>
```

**Example:**
```rust
conn.deregister_table("temp_table")?;
```

### Prepared Statements

#### `prepare()`

Prepare a statement for repeated execution:

```rust
pub fn prepare(&self, sql: &str) -> Result<PreparedStatement>
```

**Example:**
```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
let results = stmt.execute(&[ScalarValue::Int64(Some(1))])?;
```

#### `prepare_cached()`

Prepare using the statement cache:

```rust
pub fn prepare_cached(
    &self,
    sql: &str,
    cache: &PreparedStatementCache,
) -> Result<PreparedStatement>
```

**Example:**
```rust
let cache = PreparedStatementCache::new(100);
let stmt = conn.prepare_cached("SELECT $1 + $2", &cache)?;
```

## ConnectionConfig

Configuration for database connections.

### Methods

#### `new()`

Create default configuration:

```rust
pub fn new() -> Self
```

#### `with_batch_size()`

Set the batch size (1 to 16,777,216):

```rust
pub fn with_batch_size(mut self, batch_size: usize) -> Self
```

#### `with_memory_limit()`

Set maximum memory usage in bytes:

```rust
pub fn with_memory_limit(mut self, limit: usize) -> Self
```

#### `with_num_threads()`

Set number of parallel workers:

```rust
pub fn with_num_threads(mut self, num_threads: usize) -> Self
```

**Example:**
```rust
let config = ConnectionConfig::new()
    .with_batch_size(8192)
    .with_memory_limit(1024 * 1024 * 1024)
    .with_num_threads(4);
```

## PreparedStatement

A prepared SQL statement for repeated execution.

### Methods

#### `execute()`

Execute with parameter values:

```rust
pub fn execute(&self, params: &[ScalarValue]) -> Result<Vec<RecordBatch>>
```

**Example:**
```rust
let stmt = conn.prepare("SELECT * FROM users WHERE id = $1 AND status = $2")?;
let results = stmt.execute(&[
    ScalarValue::Int64(Some(1)),
    ScalarValue::Utf8(Some("active".to_string())),
])?;
```

#### `parameters()`

Get parameter information:

```rust
pub fn parameters(&self) -> &[ParameterInfo]
```

## PreparedStatementCache

LRU cache for prepared statements.

### Methods

#### `new()`

Create cache with specified capacity:

```rust
pub fn new(capacity: usize) -> Self
```

**Example:**
```rust
let cache = PreparedStatementCache::new(100);
```

## ScalarValue

Represents a single value for parameters and literals.

### Variants

```rust
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    // ... and more
}
```

**Examples:**
```rust
ScalarValue::Int64(Some(42))
ScalarValue::Utf8(Some("hello".to_string()))
ScalarValue::Boolean(Some(true))
ScalarValue::Null
ScalarValue::Float64(None)  // NULL float
```

## Schema

Represents a table schema.

### Methods

#### `new()`

Create from field list:

```rust
pub fn new(fields: Vec<Field>) -> Self
```

#### `fields()`

Get all fields:

```rust
pub fn fields(&self) -> &[Field]
```

#### `field()`

Get field by name:

```rust
pub fn field(&self, name: &str) -> Option<&Field>
```

#### `column_index()`

Get column index by name:

```rust
pub fn column_index(&self, name: &str) -> Option<usize>
```

## Field

Represents a column definition.

### Methods

#### `new()`

Create a new field:

```rust
pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self
```

#### `name()`

Get field name:

```rust
pub fn name(&self) -> &str
```

#### `data_type()`

Get data type:

```rust
pub fn data_type(&self) -> &DataType
```

#### `is_nullable()`

Check if nullable:

```rust
pub fn is_nullable(&self) -> bool
```

## DataType

Represents SQL data types.

### Variants

```rust
pub enum DataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp(TimeUnit, Option<String>),
    Time32(TimeUnit),
    Time64(TimeUnit),
    Decimal128(u8, i8),
    List(Box<Field>),
    Struct(Vec<Field>),
    // ... and more
}
```

## Storage Options

### CsvOptions

```rust
impl CsvOptions {
    pub fn default() -> Self
    pub fn with_delimiter(self, delimiter: u8) -> Self
    pub fn with_has_header(self, has_header: bool) -> Self
    pub fn with_quote(self, quote: u8) -> Self
    pub fn with_escape(self, escape: Option<u8>) -> Self
    pub fn with_null_value(self, null_value: &str) -> Self
}
```

### ParquetOptions

```rust
impl ParquetOptions {
    pub fn default() -> Self
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self
}
```

## Re-exports

The following types are re-exported from `blaze`:

```rust
pub use blaze::{
    Connection,
    ConnectionConfig,
    BlazeError,
    Result,
    DataType,
    Field,
    Schema,
    ScalarValue,
    PreparedStatement,
    PreparedStatementCache,
    ParameterInfo,
};
```

## Prelude

Import common types:

```rust
use blaze::prelude::*;
```

Includes:
- `Connection`
- `ConnectionConfig`
- `BlazeError`
- `Result`
- `DataType`
- `Field`
- `Schema`
- `ScalarValue`
