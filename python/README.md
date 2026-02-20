# PyBlaze

Python bindings for the [Blaze](https://github.com/blaze-db/blaze) embedded OLAP query engine. Provides SQL:2016 compliance with native Apache Arrow and Parquet integration.

## Features

- **Full SQL Support**: SELECT, JOIN, GROUP BY, window functions, CTEs, and more
- **Native Arrow/Parquet**: Read CSV, Parquet, and Delta Lake files directly
- **Zero-Copy Integration**: Convert results to PyArrow, Pandas, or Polars with minimal overhead
- **Prepared Statements**: Safe parameterized queries with `$1`, `$2` syntax
- **In-Process**: No server required — runs entirely in your Python process

## Installation

```bash
pip install pyblaze
```

### Building from Source

PyBlaze uses [Maturin](https://www.maturin.rs/) to build the Rust extension:

```bash
pip install maturin
maturin develop --release
```

## Quick Start

```python
import pyblaze

# Create a connection
conn = pyblaze.connect()

# Create and populate a table
conn.execute("CREATE TABLE users (id INT, name VARCHAR, active BOOLEAN)")
conn.execute("INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false)")

# Query the data
result = conn.query("SELECT * FROM users WHERE active = true")
print(result.to_dicts())
# [{'id': 1, 'name': 'Alice', 'active': True}]
```

## Usage

### Registering Files

```python
conn = pyblaze.connect()

# CSV files
conn.register_csv("sales", "data/sales.csv")

# Parquet files
conn.register_parquet("events", "data/events.parquet")

# Query across files
result = conn.query("""
    SELECT s.product, COUNT(*) as cnt
    FROM sales s
    GROUP BY s.product
    ORDER BY cnt DESC
""")
```

### Reading Files Directly

```python
# Read without registering
result = pyblaze.read_csv("data/sales.csv")
result = pyblaze.read_parquet("data/events.parquet")
result = pyblaze.read_delta("data/my_table", version=3)
```

### Working with Results

```python
result = conn.query("SELECT id, name FROM users")

# Row-oriented (list of dicts)
rows = result.to_dicts()

# Column-oriented (dict of lists)
cols = result.to_dict()

# Metadata
print(result.num_rows())
print(result.num_columns())
print(result.column_names())
```

### PyArrow / Pandas / Polars Integration

```python
result = conn.query("SELECT * FROM users")

# To PyArrow Table
arrow_table = result.to_arrow()

# To Pandas DataFrame
df = result.to_pandas()

# To Polars DataFrame
pl_df = result.to_polars()
```

### Prepared Statements

```python
stmt = conn.prepare("SELECT * FROM users WHERE id = $1")
result = stmt.execute(1)
```

### Creating Tables from Python Data

```python
conn.from_dict("metrics", {
    "timestamp": [1, 2, 3],
    "value": [10.5, 20.3, 15.7],
})
result = conn.query("SELECT * FROM metrics")
```

### Configuration

```python
conn = pyblaze.connect(
    batch_size=4096,          # Vectorized batch size
    memory_limit=512 * 1024 * 1024,  # 512 MB memory limit
)
```

## API Reference

### Module Functions

| Function | Description |
|----------|-------------|
| `connect(**kwargs)` | Create a new in-memory connection |
| `read_csv(path, **kwargs)` | Read a CSV file into a `QueryResult` |
| `read_parquet(path)` | Read a Parquet file into a `QueryResult` |
| `read_delta(path, **kwargs)` | Read a Delta Lake table into a `QueryResult` |

### `Connection`

| Method | Description |
|--------|-------------|
| `query(sql)` | Execute a query and return results |
| `execute(sql)` | Execute a statement, return rows affected |
| `register_csv(name, path)` | Register a CSV file as a table |
| `register_parquet(name, path)` | Register a Parquet file as a table |
| `list_tables()` | List all registered tables |
| `table_schema(name)` | Get a table's schema |
| `prepare(sql)` | Create a prepared statement |
| `from_dict(name, data)` | Create a table from a Python dict |

### `QueryResult`

| Method | Description |
|--------|-------------|
| `num_rows()` | Number of rows |
| `num_columns()` | Number of columns |
| `column_names()` | List of column names |
| `to_dicts()` | Convert to list of dicts (row-oriented) |
| `to_dict()` | Convert to dict of lists (column-oriented) |
| `to_arrow()` | Convert to PyArrow Table |
| `to_pandas()` | Convert to Pandas DataFrame |
| `to_polars()` | Convert to Polars DataFrame |

## License

MIT
