# Coming from DuckDB

## Quick Comparison

| Feature | DuckDB | Blaze |
|---------|--------|-------|
| Language | C++ | Rust |
| In-process | ✅ | ✅ |
| SQL Dialect | PostgreSQL-like | SQL:2016 |
| Arrow Native | ✅ | ✅ |
| WASM Support | ✅ | ✅ |

## API Mapping

| DuckDB | Blaze |
|--------|-------|
| `duckdb::Connection` | `blaze::Connection` |
| `conn.execute(sql)` | `conn.execute(sql)?` |
| `conn.query(sql)` | `conn.query(sql)?` |
| `read_csv_auto('file.csv')` | `conn.register_csv("name", "file.csv")?` |
| `read_parquet('file.parquet')` | `conn.register_parquet("name", "file.parquet")?` |

## Key Differences

1. **Error handling**: Blaze uses Rust's `Result<T>` — all operations can fail and must be handled
2. **Table registration**: Files must be registered before querying (no auto-discovery)
3. **DML**: INSERT/UPDATE/DELETE only work on in-memory tables
4. **Persistence**: Not enabled by default — use `PersistentDatabase` for durability
