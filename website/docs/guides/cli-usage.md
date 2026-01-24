---
sidebar_position: 1
---

# CLI Usage

Blaze includes a powerful command-line interface for interactive SQL queries and script execution.

## Installation

```bash
# Build from source
cargo build --release

# The binary is at target/release/blaze
./target/release/blaze
```

## Interactive REPL

Start the interactive REPL by running `blaze` without arguments:

```bash
blaze
```

You'll see the interactive prompt:

```
Blaze SQL Engine v0.1.0
Type 'exit' or press Ctrl+D to quit.

blaze>
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.tables` | List all registered tables |
| `.schema <table>` | Show table schema |
| `.read <name> <file>` | Load CSV file as table |
| `.timer on\|off` | Toggle query timing |
| `.mode csv\|json\|table` | Set output format |
| `.output <file>` | Redirect output to file |
| `.output stdout` | Reset output to console |
| `exit` | Exit the REPL |

### Example Session

```
blaze> .read users data/users.csv
Loaded 'users' from data/users.csv

blaze> .tables
users

blaze> .schema users
Table: users
  id: Int64
  name: Utf8
  email: Utf8
  created_at: Timestamp

blaze> SELECT * FROM users LIMIT 3;
+----+-------+------------------+---------------------+
| id | name  | email            | created_at          |
+----+-------+------------------+---------------------+
| 1  | Alice | alice@email.com  | 2024-01-15 10:30:00 |
| 2  | Bob   | bob@email.com    | 2024-01-16 14:20:00 |
| 3  | Carol | carol@email.com  | 2024-01-17 09:15:00 |
+----+-------+------------------+---------------------+
3 rows returned

blaze> .timer on
Query timing enabled

blaze> SELECT COUNT(*) FROM users;
+----------+
| COUNT(*) |
+----------+
| 1000     |
+----------+
Query executed in 2.3ms

blaze> exit
```

## Direct Query Execution

Execute a single query without entering the REPL:

```bash
# Simple query
blaze -c "SELECT 1 + 1 AS result"

# Query with file data
blaze -c "SELECT * FROM 'data/users.csv' LIMIT 10"
```

## SQL File Execution

Execute SQL from a file:

```bash
# Execute a SQL script
blaze script.sql

# With output format
blaze script.sql --format json
```

Example `script.sql`:

```sql
-- Load data
-- Note: Use .read command in REPL for CSV files

-- Run analytics
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as orders,
    SUM(amount) as revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

## Output Formats

Control how results are displayed:

```bash
# Table format (default)
blaze -c "SELECT * FROM users" --format table

# JSON output
blaze -c "SELECT * FROM users" --format json

# CSV output
blaze -c "SELECT * FROM users" --format csv

# Arrow IPC format (for programmatic use)
blaze -c "SELECT * FROM users" --format arrow
```

### JSON Output Example

```bash
blaze -c "SELECT id, name FROM users LIMIT 2" --format json
```

```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

### CSV Output Example

```bash
blaze -c "SELECT id, name FROM users LIMIT 2" --format csv
```

```csv
id,name
1,Alice
2,Bob
```

## Output to File

Redirect results to a file:

```bash
# Save results to CSV
blaze -c "SELECT * FROM users" --format csv --output users.csv

# Save results to JSON
blaze -c "SELECT * FROM analytics" --format json --output report.json
```

## Working with Multiple Files

You can query multiple data files by registering them:

```bash
blaze << 'EOF'
.read customers data/customers.csv
.read orders data/orders.parquet

SELECT
    c.name,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 10;
EOF
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BLAZE_BATCH_SIZE` | Vectorized batch size | 8192 |
| `BLAZE_MEMORY_LIMIT` | Max memory in bytes | unlimited |
| `BLAZE_THREADS` | Number of worker threads | auto |

Example:

```bash
BLAZE_BATCH_SIZE=4096 BLAZE_MEMORY_LIMIT=1073741824 blaze script.sql
```

## Query History

The REPL maintains command history across sessions:

- **Up/Down arrows**: Navigate history
- **Ctrl+R**: Reverse search history
- History is saved to `~/.blaze_history`

## Error Handling

When errors occur, Blaze provides helpful suggestions:

```
blaze> SELECT nmae FROM users;
Error: Column 'nmae' not found in table 'users'
Did you mean: 'name'?
Available columns: id, name, email, created_at
```

## Performance Tips

1. **Use LIMIT during exploration**: Avoid fetching millions of rows interactively
2. **Enable timing**: Use `.timer on` to identify slow queries
3. **Use Parquet for large files**: Parquet is more efficient than CSV
4. **Project only needed columns**: `SELECT a, b` is faster than `SELECT *`

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | SQL error |
| 2 | File not found |
| 3 | Parse error |

Use exit codes in scripts:

```bash
blaze script.sql && echo "Success" || echo "Failed"
```
