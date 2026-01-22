# ADR-0009: Embedded In-Process Architecture

## Status

Accepted

## Context

Database systems can be deployed in different architectural patterns:

**Client-Server Architecture**:
- Separate database server process
- Network communication (TCP/IP, Unix sockets)
- Multi-client support
- Requires installation, configuration, administration
- Examples: PostgreSQL, MySQL, Oracle

**Embedded Architecture**:
- Database runs within application process
- Direct function calls, no network overhead
- Single application ownership
- Zero configuration deployment
- Examples: SQLite, DuckDB, RocksDB

**Hybrid Architecture**:
- Embeddable but can also run as server
- Flexibility at cost of complexity
- Examples: H2, Derby

For analytical workloads in data science and application embedding scenarios, the deployment friction of client-server databases is often prohibitive.

## Decision

We chose an **embedded in-process architecture** where Blaze runs entirely within the host application:

```rust
// The Connection is the entire database
pub struct Connection {
    catalog_list: Arc<CatalogList>,
    execution_context: ExecutionContext,
    prepared_cache: Mutex<LruCache<String, Arc<PreparedStatement>>>,
}

impl Connection {
    /// Create new in-memory database
    pub fn new() -> Result<Self> {
        Ok(Self {
            catalog_list: Arc::new(CatalogList::new()),
            execution_context: ExecutionContext::new(/* ... */),
            prepared_cache: Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())),
        })
    }

    /// Execute SQL directly
    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // No network round-trip - direct execution
        let statement = self.parser.parse(sql)?;
        let logical_plan = self.binder.bind(&statement)?;
        let optimized = self.optimizer.optimize(&logical_plan)?;
        let physical = self.physical_planner.plan(&optimized)?;
        self.executor.execute(&physical)
    }
}
```

### No Server Process

```rust
// Application code - no server needed
fn main() -> Result<()> {
    // Database is just a Rust struct
    let conn = Connection::new()?;

    // Load data
    conn.register_parquet("sales", "data/sales.parquet")?;

    // Query directly - no network
    let results = conn.query("
        SELECT region, SUM(amount) as total
        FROM sales
        GROUP BY region
    ")?;

    // Database destroyed when conn drops
    Ok(())
}
```

### Direct Memory Access

```rust
// Data stays in process memory
let batches = conn.query("SELECT * FROM users")?;

// Zero-copy access to Arrow arrays
for batch in &batches {
    let names: &StringArray = batch
        .column(0)
        .as_any()
        .downcast_ref()
        .unwrap();

    // Direct memory access - no serialization
    for name in names.iter() {
        println!("{:?}", name);
    }
}
```

## Consequences

### Benefits

1. **Zero Deployment Friction**: No installation, configuration, or administration:
   ```rust
   // Add dependency to Cargo.toml
   // blaze = "0.1"

   // Use immediately
   let conn = Connection::new()?;
   ```

2. **Minimal Latency**: No network overhead for queries:
   ```
   Client-Server: parse → serialize → network → deserialize → execute → serialize → network → deserialize
   Embedded:      parse → execute → return reference

   Simple query latency:
   - PostgreSQL: ~0.5-2ms (network + protocol overhead)
   - Blaze:      ~10-50μs (function call)
   ```

3. **Memory Efficiency**: Results stay in process memory:
   ```rust
   // No serialization/deserialization
   let batches = conn.query("SELECT * FROM large_table")?;

   // Data is already Arrow format - use directly
   arrow::compute::sum(batches[0].column(0))
   ```

4. **Simple Concurrency Model**: Rust's ownership handles thread safety:
   ```rust
   // Connection is Send + Sync
   let conn = Arc::new(Connection::new()?);

   // Safe concurrent queries
   let handles: Vec<_> = (0..4).map(|i| {
       let conn = Arc::clone(&conn);
       std::thread::spawn(move || {
           conn.query(&format!("SELECT * FROM t WHERE id = {}", i))
       })
   }).collect();
   ```

5. **Portable Distribution**: Single binary includes everything:
   ```bash
   # Rust application with embedded Blaze
   $ cargo build --release
   $ ls -la target/release/myapp
   -rwxr-xr-x 1 user user 15M Jan 1 00:00 myapp

   # No external dependencies to install
   $ ./myapp  # Just works
   ```

6. **Testing Simplicity**: In-memory database for tests:
   ```rust
   #[test]
   fn test_query_logic() {
       let conn = Connection::new().unwrap();
       conn.execute("CREATE TABLE test (id INT, value TEXT)").unwrap();
       conn.execute("INSERT INTO test VALUES (1, 'a')").unwrap();

       let result = conn.query("SELECT * FROM test").unwrap();
       assert_eq!(result[0].num_rows(), 1);
       // Database cleaned up automatically
   }
   ```

### Costs

1. **Single Process Limitation**: Cannot share database across processes:
   ```rust
   // Process A
   let conn_a = Connection::new()?;
   conn_a.register_parquet("data", "shared.parquet")?;

   // Process B - cannot access conn_a's catalog
   let conn_b = Connection::new()?;  // Separate database
   ```

2. **Memory Bound by Process**: Database limited to process address space:
   ```rust
   // All data must fit in process memory
   // For very large datasets, need external storage + streaming
   ```

3. **No Built-in Replication**: High availability requires external solution.

4. **Process Crash = Data Loss**: In-memory data not persistent by default:
   ```rust
   // Mitigation: Persist to Parquet/Delta periodically
   conn.execute("COPY users TO 'backup/users.parquet'")?;
   ```

### Comparison with Alternatives

| Aspect | Embedded (Blaze) | Client-Server |
|--------|-----------------|---------------|
| Deployment | Zero config | Install + configure |
| Query latency | μs | ms (network) |
| Memory efficiency | Optimal | Serialization overhead |
| Multi-process | No | Yes |
| High availability | Manual | Built-in options |
| Scaling | Vertical only | Horizontal possible |

### Use Cases

**Ideal For**:
- Data science notebooks and scripts
- Application-embedded analytics
- ETL pipelines
- CLI tools
- Unit testing
- Single-user applications

**Not Ideal For**:
- Multi-user web applications
- Microservices with shared state
- High-availability requirements
- Cross-process data sharing

### Integration Patterns

**Python Data Science**:
```python
import pyblaze

# No server to start
conn = pyblaze.connect()
conn.register_parquet("events", "s3://bucket/events.parquet")

# Query returns PyArrow
result = conn.query("SELECT date, COUNT(*) FROM events GROUP BY date")
df = result.to_pandas()  # Zero-copy where possible
```

**Rust Application Embedding**:
```rust
struct MyApplication {
    db: Connection,
    // ... other fields
}

impl MyApplication {
    fn new() -> Result<Self> {
        Ok(Self {
            db: Connection::new()?,
        })
    }

    fn analytics(&self) -> Result<Report> {
        let data = self.db.query("SELECT ...")?;
        // Process directly
    }
}
```

**WebAssembly**:
```javascript
// Browser - no server needed
import init, { Connection } from 'blaze-wasm';

await init();
const conn = new Connection();
conn.registerCsv('data', csvString);
const result = conn.query('SELECT * FROM data');
```
