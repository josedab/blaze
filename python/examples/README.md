# Python Binding Examples

## Quickstart

```bash
pip install pyblaze
python quickstart.py
```

### What the example covers

- Creating an in-memory connection
- Creating tables with `CREATE TABLE`
- Inserting data with `INSERT INTO`
- Running `SELECT` queries with aggregations
- `JOIN` queries across tables
- Window functions (`RANK() OVER`)
- Reading results as Python dictionaries

### Loading files

```python
import pyblaze

# Load CSV directly
conn = pyblaze.read_csv("path/to/data.csv")
result = conn.query("SELECT * FROM data LIMIT 10")

# Load Parquet
conn = pyblaze.read_parquet("path/to/data.parquet")
```

### Prepared statements

```python
conn = pyblaze.connect()
stmt = conn.prepare("SELECT * FROM users WHERE id = $1")
result = stmt.execute([42])
```
