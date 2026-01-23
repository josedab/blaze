---
sidebar_position: 4
---

# Python Integration

Blaze provides Python bindings via PyO3, enabling seamless integration with the Python data ecosystem including PyArrow, Pandas, and Polars.

## Installation

Install the Python package:

```bash
pip install blaze-db
```

Or build from source:

```bash
git clone https://github.com/blaze-db/blaze.git
cd blaze
pip install maturin
maturin develop --features python
```

## Basic Usage

### Create a Connection

```python
import blaze

# Create in-memory connection
conn = blaze.Connection()

# Register data from Python dictionaries
conn.from_dict("users", {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35]
})

# Query data
results = conn.query("SELECT * FROM users WHERE age > 25")
print(results.to_pandas())
```

### Query Files

```python
import blaze

conn = blaze.Connection()

# Register files as tables
conn.register_csv("sales", "data/sales.csv")
conn.register_parquet("customers", "data/customers.parquet")

# Query across files
results = conn.query("""
    SELECT c.name, SUM(s.amount) as total
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    GROUP BY c.name
    ORDER BY total DESC
""")
```

## PyArrow Integration

### Get Results as Arrow Table

```python
import blaze
import pyarrow as pa

conn = blaze.Connection()
conn.register_parquet("data", "file.parquet")

# Query returns a QueryResult
result = conn.query("SELECT * FROM data WHERE value > 100")

# Access result metadata
print(f"Rows: {result.num_rows()}")
print(f"Columns: {result.column_names()}")

# Convert to PyArrow Table for further processing
table = result.to_pyarrow()
print(f"Schema: {table.schema}")
```

### Register Data from Python

```python
import blaze

conn = blaze.Connection()

# Register data using from_dict
conn.from_dict("scores", {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'score': [95.5, 87.0, 92.3]
})

# Query it
result = conn.query("SELECT name, score FROM scores WHERE score > 90")
```

## Pandas Integration

### Query to DataFrame

```python
import blaze
import pandas as pd

conn = blaze.Connection()
conn.register_parquet("events", "data/events.parquet")

# Query and convert to DataFrame
result = conn.query("""
    SELECT event_type, COUNT(*) as count
    FROM events
    GROUP BY event_type
""")

df = result.to_pandas()
print(df)
```

### Register DataFrame

```python
import blaze
import pandas as pd

conn = blaze.Connection()

# Create DataFrame
df = pd.DataFrame({
    'product': ['A', 'B', 'C'],
    'price': [10.0, 20.0, 30.0],
    'quantity': [100, 50, 75]
})

# Register using from_dict (convert DataFrame to dict)
conn.from_dict("products", df.to_dict(orient='list'))

# Query
result = conn.query("SELECT product, price * quantity as value FROM products")
print(result.to_pandas())
```

## Polars Integration

### Query to Polars DataFrame

```python
import blaze
import polars as pl

conn = blaze.Connection()
conn.register_parquet("data", "file.parquet")

# Query and convert to Polars
result = conn.query("SELECT * FROM data LIMIT 1000")
df = pl.from_arrow(result.to_pyarrow())

print(df)
```

### Register Polars DataFrame

```python
import blaze
import polars as pl

conn = blaze.Connection()

# Create Polars DataFrame
df = pl.DataFrame({
    'id': [1, 2, 3],
    'value': [10.5, 20.3, 30.1]
})

# Register using from_dict (convert DataFrame to dict)
conn.from_dict("values", df.to_dict())

# Query
result = conn.query("SELECT id, value * 2 as doubled FROM values")
```

## Configuration

### Memory Limits

```python
import blaze

# Set memory limit (1 GB)
conn = blaze.Connection(memory_limit=1024 * 1024 * 1024)

# Query large dataset
try:
    result = conn.query("SELECT * FROM huge_table")
except blaze.ResourceExhaustedError as e:
    print(f"Memory limit exceeded: {e}")
```

### Batch Size

```python
import blaze

# Configure batch size
conn = blaze.Connection(batch_size=4096)
```

## Error Handling

```python
import blaze

conn = blaze.Connection()

try:
    conn.query("SELECT * FROM nonexistent_table")
except blaze.CatalogError as e:
    print(f"Table not found: {e}")

try:
    conn.query("INVALID SQL")
except blaze.ParseError as e:
    print(f"Syntax error: {e}")

try:
    conn.query("SELECT * FROM users WHERE invalid_column = 1")
except blaze.AnalysisError as e:
    print(f"Query error: {e}")
```

## Use Cases

### Data Exploration

```python
import blaze

conn = blaze.Connection()
conn.register_parquet("logs", "data/application_logs.parquet")

# Quick data profiling
profile = conn.query("""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT user_id) as unique_users,
        MIN(timestamp) as earliest,
        MAX(timestamp) as latest
    FROM logs
""")
print(profile.to_pandas())

# Sample data
sample = conn.query("SELECT * FROM logs LIMIT 10")
print(sample.to_pandas())
```

### ETL Pipeline

```python
import blaze
import pyarrow.parquet as pq

conn = blaze.Connection()

# Load source data
conn.register_csv("raw_data", "input/raw.csv")

# Transform with SQL
result = conn.query("""
    SELECT
        user_id,
        DATE_TRUNC('day', timestamp) as date,
        SUM(amount) as daily_total,
        COUNT(*) as transaction_count
    FROM raw_data
    WHERE status = 'completed'
    GROUP BY user_id, DATE_TRUNC('day', timestamp)
""")

# Save as Parquet (convert to PyArrow table first)
pq.write_table(result.to_pyarrow(), "output/aggregated.parquet")
```

### Analytics Dashboard Backend

```python
import blaze
from flask import Flask, jsonify

app = Flask(__name__)
conn = blaze.Connection()
conn.register_parquet("sales", "data/sales.parquet")

@app.route("/api/sales/summary")
def sales_summary():
    result = conn.query("""
        SELECT
            region,
            SUM(amount) as total,
            COUNT(*) as count
        FROM sales
        GROUP BY region
        ORDER BY total DESC
    """)
    return jsonify(result.to_pandas().to_dict(orient='records'))

@app.route("/api/sales/top-products")
def top_products():
    result = conn.query("""
        SELECT product, SUM(quantity) as total_sold
        FROM sales
        GROUP BY product
        ORDER BY total_sold DESC
        LIMIT 10
    """)
    return jsonify(result.to_pandas().to_dict(orient='records'))
```

### Jupyter Notebook

```python
# Cell 1: Setup
import blaze
import pandas as pd
import matplotlib.pyplot as plt

conn = blaze.Connection()
conn.register_parquet("data", "analysis_data.parquet")

# Cell 2: Explore schema
schema = conn.query("SELECT * FROM data LIMIT 0")
print(schema.schema)

# Cell 3: Analyze
df = conn.query("""
    SELECT category, AVG(value) as avg_value
    FROM data
    GROUP BY category
""").to_pandas()

# Cell 4: Visualize
df.plot(kind='bar', x='category', y='avg_value')
plt.show()
```

## Performance Tips

### Use Parquet Files

```python
# Parquet is much faster than CSV
conn.register_parquet("data", "file.parquet")  # Preferred
conn.register_csv("data", "file.csv")           # Slower
```

### Project Only Needed Columns

```python
# Good: only reads needed columns
result = conn.query("SELECT id, name FROM large_table")

# Slow: reads all columns
result = conn.query("SELECT * FROM large_table")
```

### Filter Early

```python
# Good: filter in SQL
result = conn.query("SELECT * FROM data WHERE date = '2024-01-15'")

# Less efficient: filter in Python
result = conn.query("SELECT * FROM data")
df = result.to_pandas()
df = df[df['date'] == '2024-01-15']
```

## Next Steps

- [Working with Files](/docs/guides/working-with-files) - File formats
- [SQL Reference](/docs/reference/sql) - SQL syntax
- [Memory Management](/docs/guides/memory-management) - Memory configuration
