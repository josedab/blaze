---
sidebar_position: 6
---

# Production Best Practices

This guide covers best practices for deploying Blaze in production environments, including configuration, monitoring, error handling, and performance optimization.

## Configuration for Production

### Memory Management

Always set explicit memory limits to prevent out-of-memory errors:

```rust
use blaze::{Connection, ConnectionConfig};

let config = ConnectionConfig::new()
    // Set based on available system memory
    // Leave headroom for OS and other processes
    .with_memory_limit(available_memory * 0.7)
    // Optimize batch size for your workload
    .with_batch_size(8192);

let conn = Connection::with_config(config)?;
```

**Guidelines:**
- **Memory limit:** 70-80% of available RAM
- **Batch size:** 4096-16384 for most workloads
  - Smaller batches: Lower memory, higher overhead
  - Larger batches: Higher memory, better throughput

### Thread Configuration

Configure threads based on your hardware:

```rust
let config = ConnectionConfig::new()
    // Use physical cores, not hyperthreads for CPU-bound work
    .with_num_threads(num_cpus::get_physical());
```

**Guidelines:**
- **Dedicated analytics server:** Use all physical cores
- **Shared server:** Limit to 50% of cores
- **Container/pod:** Match container CPU limit

## Connection Management

### Connection Lifecycle

Blaze connections are **not thread-safe**. Manage them appropriately:

```rust
// Pattern 1: Connection per request
fn handle_request(sql: &str) -> Result<Vec<RecordBatch>> {
    let conn = create_connection()?;  // Fast - no I/O
    conn.register_parquet("data", DATA_PATH)?;  // Cache this if possible
    conn.query(sql)
}

// Pattern 2: Thread-local connections
thread_local! {
    static CONN: RefCell<Connection> = RefCell::new(
        create_connection().expect("Failed to create connection")
    );
}

fn handle_request(sql: &str) -> Result<Vec<RecordBatch>> {
    CONN.with(|conn| {
        conn.borrow().query(sql)
    })
}
```

### Reusing Table Registrations

If querying the same tables repeatedly, reuse registrations:

```rust
pub struct QueryService {
    conn: Connection,
}

impl QueryService {
    pub fn new() -> Result<Self> {
        let conn = Connection::in_memory()?;
        // Register tables once at startup
        conn.register_parquet("customers", "data/customers.parquet")?;
        conn.register_parquet("orders", "data/orders.parquet")?;
        conn.register_parquet("products", "data/products.parquet")?;
        Ok(Self { conn })
    }

    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.conn.query(sql)
    }
}
```

## Error Handling

### Comprehensive Error Handling

Handle all error variants appropriately:

```rust
use blaze::{BlazeError, Result};

pub fn execute_query(conn: &Connection, sql: &str) -> Result<QueryResult> {
    match conn.query(sql) {
        Ok(batches) => Ok(QueryResult::Success(batches)),

        // User errors - return helpful message
        Err(BlazeError::Parse { message, line, column }) => {
            log::warn!("SQL parse error at {}:{}: {}", line, column, message);
            Ok(QueryResult::UserError(format!(
                "Syntax error at line {}, column {}: {}",
                line, column, message
            )))
        }

        Err(BlazeError::Analysis(msg)) => {
            log::warn!("Query analysis error: {}", msg);
            Ok(QueryResult::UserError(format!("Query error: {}", msg)))
        }

        Err(BlazeError::Catalog { name }) => {
            log::warn!("Table not found: {}", name);
            Ok(QueryResult::UserError(format!("Table '{}' not found", name)))
        }

        Err(BlazeError::Type { expected, actual }) => {
            log::warn!("Type mismatch: expected {}, got {}", expected, actual);
            Ok(QueryResult::UserError(format!(
                "Type error: expected {}, got {}",
                expected, actual
            )))
        }

        // Resource errors - may need intervention
        Err(BlazeError::ResourceExhausted { limit, requested }) => {
            log::error!(
                "Memory limit exceeded: requested {}, limit {}",
                requested, limit
            );
            metrics::increment_counter!("blaze.memory_exceeded");
            Ok(QueryResult::ResourceError(
                "Query exceeded memory limit. Try a smaller query.".to_string()
            ))
        }

        // System errors - log and alert
        Err(BlazeError::IO(e)) => {
            log::error!("IO error: {}", e);
            metrics::increment_counter!("blaze.io_errors");
            Err(BlazeError::IO(e))
        }

        Err(e) => {
            log::error!("Unexpected error: {}", e);
            metrics::increment_counter!("blaze.unexpected_errors");
            Err(e)
        }
    }
}
```

### Graceful Degradation

Implement fallbacks for non-critical features:

```rust
pub fn get_user_analytics(user_id: i64) -> Result<Analytics> {
    let conn = get_connection()?;

    // Primary query with full analytics
    let sql = format!(r#"
        SELECT
            COUNT(*) as total_events,
            COUNT(DISTINCT event_type) as unique_events,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        FROM events
        WHERE user_id = {}
    "#, user_id);

    match conn.query(&sql) {
        Ok(results) => extract_analytics(results),

        // Fallback to simpler query if memory constrained
        Err(BlazeError::ResourceExhausted { .. }) => {
            log::warn!("Using simplified analytics for user {}", user_id);

            let simple_sql = format!(
                "SELECT COUNT(*) as total_events FROM events WHERE user_id = {} LIMIT 100000",
                user_id
            );

            let results = conn.query(&simple_sql)?;
            extract_simple_analytics(results)
        }

        Err(e) => Err(e),
    }
}
```

## Query Safety

### Input Validation

Always validate and sanitize user input:

```rust
use blaze::PreparedStatement;

// GOOD: Use prepared statements
pub fn get_user_by_id(conn: &Connection, user_id: i64) -> Result<Vec<RecordBatch>> {
    let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
    stmt.execute(&[&user_id])
}

// GOOD: Validate input before building queries
pub fn get_users_by_status(conn: &Connection, status: &str) -> Result<Vec<RecordBatch>> {
    // Whitelist allowed values
    let valid_statuses = ["active", "inactive", "pending"];
    if !valid_statuses.contains(&status) {
        return Err(BlazeError::invalid_argument(
            format!("Invalid status: {}", status)
        ));
    }

    conn.query(&format!("SELECT * FROM users WHERE status = '{}'", status))
}

// BAD: Never directly interpolate user input
pub fn unsafe_query(conn: &Connection, user_input: &str) -> Result<Vec<RecordBatch>> {
    // DANGEROUS - SQL injection risk!
    conn.query(&format!("SELECT * FROM users WHERE name = '{}'", user_input))
}
```

### Query Timeouts

Implement timeouts for long-running queries:

```rust
use std::time::{Duration, Instant};
use std::sync::mpsc;
use std::thread;

pub fn query_with_timeout(
    conn: &Connection,
    sql: &str,
    timeout: Duration,
) -> Result<Vec<RecordBatch>> {
    let (tx, rx) = mpsc::channel();
    let sql = sql.to_string();

    // Note: This requires a thread-safe connection or connection-per-thread
    let handle = thread::spawn(move || {
        let conn = Connection::in_memory().unwrap();
        // Register tables...
        let result = conn.query(&sql);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(_) => {
            // Query timed out
            // Note: The query will continue in background until completion
            log::warn!("Query timed out after {:?}", timeout);
            Err(BlazeError::execution("Query timed out"))
        }
    }
}
```

### Resource Limits

Enforce limits at the application level:

```rust
pub struct QueryLimits {
    pub max_rows: usize,
    pub max_query_length: usize,
    pub allowed_tables: HashSet<String>,
}

impl QueryLimits {
    pub fn validate(&self, sql: &str) -> Result<()> {
        // Check query length
        if sql.len() > self.max_query_length {
            return Err(BlazeError::invalid_argument(
                format!("Query too long: {} chars (max {})",
                        sql.len(), self.max_query_length)
            ));
        }

        // Basic check for dangerous patterns
        let sql_upper = sql.to_uppercase();
        if sql_upper.contains("DROP") || sql_upper.contains("DELETE") {
            return Err(BlazeError::invalid_argument(
                "DDL/DML statements not allowed"
            ));
        }

        Ok(())
    }
}

pub fn safe_query(
    conn: &Connection,
    sql: &str,
    limits: &QueryLimits,
) -> Result<Vec<RecordBatch>> {
    limits.validate(sql)?;

    // Add LIMIT if not present
    let sql = if !sql.to_uppercase().contains("LIMIT") {
        format!("{} LIMIT {}", sql, limits.max_rows)
    } else {
        sql.to_string()
    };

    conn.query(&sql)
}
```

## Monitoring

### Key Metrics to Track

```rust
use metrics::{counter, histogram, gauge};

pub fn execute_with_metrics(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>> {
    let start = Instant::now();

    counter!("blaze.queries.total").increment(1);

    let result = conn.query(sql);

    let duration = start.elapsed();
    histogram!("blaze.query.duration_seconds").record(duration.as_secs_f64());

    match &result {
        Ok(batches) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            histogram!("blaze.query.rows_returned").record(rows as f64);
            counter!("blaze.queries.success").increment(1);
        }
        Err(e) => {
            counter!("blaze.queries.errors").increment(1);
            counter!(
                "blaze.queries.errors.by_type",
                "type" => error_type(e)
            ).increment(1);
        }
    }

    result
}

fn error_type(e: &BlazeError) -> &'static str {
    match e {
        BlazeError::Parse { .. } => "parse",
        BlazeError::Analysis(_) => "analysis",
        BlazeError::Catalog { .. } => "catalog",
        BlazeError::Type { .. } => "type",
        BlazeError::Execution(_) => "execution",
        BlazeError::ResourceExhausted { .. } => "resource",
        BlazeError::IO(_) => "io",
        _ => "other",
    }
}
```

### Health Checks

Implement health checks for load balancers:

```rust
pub fn health_check() -> HealthStatus {
    // Check connection creation
    let conn = match Connection::in_memory() {
        Ok(c) => c,
        Err(e) => return HealthStatus::Unhealthy(format!("Connection failed: {}", e)),
    };

    // Check basic query execution
    match conn.query("SELECT 1 as health_check") {
        Ok(_) => HealthStatus::Healthy,
        Err(e) => HealthStatus::Unhealthy(format!("Query failed: {}", e)),
    }
}

// For web servers
async fn health_endpoint() -> impl Responder {
    match health_check() {
        HealthStatus::Healthy => HttpResponse::Ok().json(json!({"status": "healthy"})),
        HealthStatus::Unhealthy(msg) => {
            HttpResponse::ServiceUnavailable().json(json!({
                "status": "unhealthy",
                "message": msg
            }))
        }
    }
}
```

## Performance in Production

### Data Loading Strategy

Load data efficiently at startup:

```rust
pub struct DataService {
    conn: Connection,
    last_refresh: Instant,
    refresh_interval: Duration,
}

impl DataService {
    pub fn new(config: DataConfig) -> Result<Self> {
        let conn = Connection::in_memory()?;

        // Load static reference data
        conn.register_parquet("products", &config.products_path)?;
        conn.register_parquet("categories", &config.categories_path)?;

        // Load frequently updated data
        conn.register_parquet("inventory", &config.inventory_path)?;

        Ok(Self {
            conn,
            last_refresh: Instant::now(),
            refresh_interval: Duration::from_secs(300),  // 5 minutes
        })
    }

    pub fn maybe_refresh(&mut self) -> Result<()> {
        if self.last_refresh.elapsed() > self.refresh_interval {
            // Refresh only volatile data
            self.conn.register_parquet("inventory", INVENTORY_PATH)?;
            self.last_refresh = Instant::now();
            log::info!("Refreshed inventory data");
        }
        Ok(())
    }
}
```

### Query Optimization

Apply production query patterns:

```rust
// Pattern 1: Paginated queries for large results
pub fn get_orders_paginated(
    conn: &Connection,
    page: usize,
    page_size: usize,
) -> Result<Vec<RecordBatch>> {
    let offset = page * page_size;
    conn.query(&format!(
        "SELECT * FROM orders ORDER BY created_at DESC LIMIT {} OFFSET {}",
        page_size, offset
    ))
}

// Pattern 2: Pre-computed aggregations
pub fn setup_materialized_views(conn: &Connection) -> Result<()> {
    // Create summary table for common queries
    let daily_summary = conn.query(r#"
        SELECT
            DATE_TRUNC('day', created_at) as date,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM orders
        GROUP BY DATE_TRUNC('day', created_at)
    "#)?;

    conn.register_batches("daily_order_summary", daily_summary)?;
    Ok(())
}

// Pattern 3: Filtered table registration
pub fn register_active_users(conn: &Connection) -> Result<()> {
    // Instead of filtering in every query, create filtered table
    let active_users = conn.query(
        "SELECT * FROM users WHERE status = 'active' AND last_login > CURRENT_DATE - 30"
    )?;

    conn.register_batches("active_users", active_users)?;
    Ok(())
}
```

### Caching Layer

Implement caching for expensive queries:

```rust
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

pub struct QueryCache {
    cache: HashMap<u64, CacheEntry>,
    ttl: Duration,
    max_entries: usize,
}

struct CacheEntry {
    results: Vec<RecordBatch>,
    created_at: Instant,
}

impl QueryCache {
    pub fn get_or_execute(
        &mut self,
        conn: &Connection,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let key = self.hash_query(sql);

        // Check cache
        if let Some(entry) = self.cache.get(&key) {
            if entry.created_at.elapsed() < self.ttl {
                return Ok(entry.results.clone());
            }
        }

        // Execute query
        let results = conn.query(sql)?;

        // Cache results
        if self.cache.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.cache.insert(key, CacheEntry {
            results: results.clone(),
            created_at: Instant::now(),
        });

        Ok(results)
    }

    fn hash_query(&self, sql: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }

    fn evict_oldest(&mut self) {
        if let Some((&oldest_key, _)) = self.cache
            .iter()
            .min_by_key(|(_, e)| e.created_at)
        {
            self.cache.remove(&oldest_key);
        }
    }
}
```

## Deployment Patterns

### Microservice Integration

```rust
// REST API example with Actix-web
use actix_web::{web, App, HttpServer, HttpResponse};

struct AppState {
    conn: Connection,
}

async fn query_endpoint(
    data: web::Data<AppState>,
    query: web::Json<QueryRequest>,
) -> HttpResponse {
    // Validate query
    if let Err(e) = validate_query(&query.sql) {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: e.to_string(),
        });
    }

    // Execute with timeout
    match data.conn.query(&query.sql) {
        Ok(batches) => {
            let json_results = batches_to_json(batches);
            HttpResponse::Ok().json(json_results)
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: e.to_string(),
            })
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conn = Connection::in_memory().expect("Failed to create connection");
    // Register tables...

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState { conn: conn.clone() }))
            .route("/query", web::post().to(query_endpoint))
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
```

### Container Deployment

```dockerfile
# Dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/my-service /usr/local/bin/
CMD ["my-service"]
```

```yaml
# kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: blaze-analytics
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: analytics
        image: my-service:latest
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        env:
        - name: BLAZE_MEMORY_LIMIT
          value: "3221225472"  # 3GB (75% of limit)
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
```

## Checklist

Before deploying to production:

- [ ] Memory limits configured appropriately
- [ ] Error handling covers all error types
- [ ] Input validation implemented
- [ ] Query timeouts in place
- [ ] Monitoring and metrics configured
- [ ] Health checks implemented
- [ ] Logging configured
- [ ] Resource limits validated
- [ ] Load testing completed
- [ ] Graceful shutdown implemented
- [ ] Backup/recovery plan documented

## Next Steps

- [Memory Management](/docs/guides/memory-management) - Detailed memory configuration
- [Query Optimization](/docs/advanced/query-optimization) - Performance tuning
- [Error Handling](/docs/reference/error-handling) - Complete error reference
- [Configuration](/docs/reference/configuration) - All configuration options
