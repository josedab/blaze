---
sidebar_position: 6
---

# Data Export (COPY TO)

Blaze supports exporting query results to files in various formats using the `COPY TO` syntax.

## Basic Syntax

```sql
COPY (query) TO 'path/to/file'
COPY (query) TO 'path/to/file' WITH (FORMAT format_name [, options...])
```

## Supported Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| `PARQUET` | `.parquet` | Columnar format, best for analytics |
| `CSV` | `.csv` | Comma-separated values |
| `JSON` | `.json` | JSON Lines format |

## Parquet Export

Parquet is the recommended format for analytical workloads:

```sql
-- Export to Parquet (format auto-detected from extension)
COPY (SELECT * FROM sales WHERE year = 2024) TO 'sales_2024.parquet'

-- Explicit format specification
COPY (SELECT * FROM users) TO 'users.parquet' WITH (FORMAT PARQUET)
```

### Parquet Benefits

- **Columnar storage**: Efficient for analytical queries
- **Compression**: Automatic compression reduces file size
- **Schema preservation**: Types are preserved exactly
- **Fast reads**: Blaze can read Parquet files efficiently

### Example: Export Aggregated Data

```sql
COPY (
    SELECT
        region,
        product_category,
        SUM(quantity) as total_qty,
        SUM(revenue) as total_revenue,
        AVG(unit_price) as avg_price
    FROM sales
    GROUP BY region, product_category
    ORDER BY total_revenue DESC
) TO 'regional_summary.parquet'
```

## CSV Export

CSV is useful for interoperability with spreadsheets and other tools:

```sql
-- Basic CSV export
COPY (SELECT * FROM users) TO 'users.csv' WITH (FORMAT CSV)

-- With header row
COPY (SELECT * FROM users) TO 'users.csv' WITH (FORMAT CSV, HEADER true)

-- Custom delimiter
COPY (SELECT * FROM users) TO 'users.tsv' WITH (FORMAT CSV, DELIMITER E'\t')
```

### CSV Options

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `HEADER` | `true`, `false` | `false` | Include header row |
| `DELIMITER` | character | `,` | Field separator |

### Example: Export for Excel

```sql
COPY (
    SELECT
        order_id,
        customer_name,
        FORMAT(order_date, 'YYYY-MM-DD') as order_date,
        ROUND(total_amount, 2) as amount
    FROM orders
    WHERE status = 'completed'
) TO 'completed_orders.csv' WITH (FORMAT CSV, HEADER true)
```

## JSON Export

JSON Lines format exports each row as a JSON object:

```sql
-- Export to JSON
COPY (SELECT id, name, email FROM customers) TO 'customers.json' WITH (FORMAT JSON)
```

Output format (JSON Lines):

```json
{"id":1,"name":"Alice","email":"alice@example.com"}
{"id":2,"name":"Bob","email":"bob@example.com"}
{"id":3,"name":"Carol","email":"carol@example.com"}
```

### Example: Export Nested Data

```sql
COPY (
    SELECT
        customer_id,
        customer_name,
        order_count,
        total_spent,
        last_order_date
    FROM customer_summary
    ORDER BY total_spent DESC
    LIMIT 100
) TO 'top_customers.json' WITH (FORMAT JSON)
```

## Auto-Detection

When no format is specified, Blaze detects the format from the file extension:

```sql
-- Auto-detects Parquet
COPY (SELECT * FROM users) TO 'users.parquet'

-- Auto-detects CSV
COPY (SELECT * FROM users) TO 'users.csv'

-- Auto-detects JSON
COPY (SELECT * FROM users) TO 'users.json'
```

## Use Cases

### Data Pipeline Export

Export processed data for downstream systems:

```sql
-- Daily aggregation for reporting
COPY (
    SELECT
        DATE_TRUNC('day', event_time) as date,
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM events
    WHERE event_time >= CURRENT_DATE - 1
      AND event_time < CURRENT_DATE
    GROUP BY DATE_TRUNC('day', event_time), event_type
) TO '/data/daily_events.parquet'
```

### Data Archival

Archive historical data:

```sql
-- Archive old orders
COPY (
    SELECT * FROM orders
    WHERE order_date < '2023-01-01'
) TO 'archive/orders_2022.parquet'
```

### Analytics Output

Export analysis results:

```sql
-- Customer segmentation
COPY (
    SELECT
        CASE
            WHEN total_orders >= 100 THEN 'VIP'
            WHEN total_orders >= 10 THEN 'Regular'
            ELSE 'New'
        END as segment,
        COUNT(*) as customer_count,
        AVG(lifetime_value) as avg_ltv
    FROM customer_metrics
    GROUP BY 1
) TO 'customer_segments.json' WITH (FORMAT JSON)
```

### Backup and Restore

Export entire tables:

```sql
-- Full table export
COPY (SELECT * FROM users) TO 'backup/users.parquet'
COPY (SELECT * FROM orders) TO 'backup/orders.parquet'
```

To restore, register the Parquet files:

```rust
conn.register_parquet("users", "backup/users.parquet")?;
conn.register_parquet("orders", "backup/orders.parquet")?;
```

## Rust API

You can also export data programmatically:

```rust
use blaze::{Connection, Result};

fn export_data() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Execute COPY TO via SQL
    conn.execute("COPY (SELECT * FROM users) TO 'users.parquet'")?;

    Ok(())
}
```

## Performance Tips

1. **Use Parquet for large exports**: Parquet compresses well and is fast to write
2. **Project only needed columns**: `SELECT a, b` reduces output size
3. **Filter early**: Use WHERE to reduce rows before export
4. **Order for compression**: `ORDER BY` can improve Parquet compression
5. **Partition large exports**: Split by date or region into multiple files

## Limitations

- COPY TO creates new files; it cannot append to existing files
- Output paths are relative to the current working directory
- File permissions depend on the process user
- JSON format is JSON Lines (one JSON object per line), not a JSON array
