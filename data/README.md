# Sample Data

This directory contains sample data files for use with Blaze examples and the README.

## Files

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `sales.csv` | CSV | 20 | Product sales with categories, amounts, and regions |
| `customers.csv` | CSV | 10 | Customer profiles with names, emails, and cities |
| `customers.parquet` | Parquet | 10 | Same customer data in Parquet format |

## Regenerating Parquet Files

```bash
cargo run --example generate_sample_data
```

## Usage

```rust
use blaze::Connection;

let conn = Connection::in_memory()?;
conn.register_csv("sales", "data/sales.csv")?;
conn.register_parquet("customers", "data/customers.parquet")?;

let results = conn.query("
    SELECT * FROM sales
    ORDER BY amount DESC
    LIMIT 5
")?;
```
