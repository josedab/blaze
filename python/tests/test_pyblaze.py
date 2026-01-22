"""Tests for PyBlaze Python bindings."""

import pytest
import tempfile
import os


def test_import():
    """Test that pyblaze can be imported."""
    import pyblaze
    assert hasattr(pyblaze, 'Connection')
    assert hasattr(pyblaze, 'connect')
    assert hasattr(pyblaze, '__version__')


def test_connect():
    """Test creating a connection."""
    import pyblaze
    conn = pyblaze.connect()
    assert conn is not None
    assert isinstance(conn.list_tables(), list)


def test_connect_with_options():
    """Test creating a connection with options."""
    import pyblaze
    conn = pyblaze.connect(batch_size=4096, memory_limit=1024 * 1024 * 100)
    assert conn is not None


def test_execute_create_table():
    """Test executing CREATE TABLE."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    tables = conn.list_tables()
    assert "users" in tables


def test_execute_insert():
    """Test executing INSERT."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    rows = conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    assert rows == 2


def test_query_basic():
    """Test basic query execution."""
    import pyblaze
    conn = pyblaze.connect()
    result = conn.query("SELECT 1 + 1 as result")
    assert result.num_rows() == 1
    assert result.num_columns() == 1


def test_query_with_table():
    """Test query with table data."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

    result = conn.query("SELECT * FROM users WHERE id > 1")
    assert result.num_rows() == 2


def test_query_result_to_dicts():
    """Test converting query results to dicts."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    result = conn.query("SELECT * FROM users ORDER BY id")
    dicts = result.to_dicts()

    assert len(dicts) == 2
    assert dicts[0]['id'] == 1
    assert dicts[0]['name'] == 'Alice'
    assert dicts[1]['id'] == 2
    assert dicts[1]['name'] == 'Bob'


def test_query_result_to_dict():
    """Test converting query results to column-oriented dict."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    result = conn.query("SELECT * FROM users ORDER BY id")
    d = result.to_dict()

    assert 'id' in d
    assert 'name' in d
    assert d['id'] == [1, 2]
    assert d['name'] == ['Alice', 'Bob']


def test_table_schema():
    """Test getting table schema."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")

    schema = conn.table_schema("users")
    assert schema is not None
    assert len(schema) == 2
    assert 'id' in schema.names()
    assert 'name' in schema.names()


def test_from_dict():
    """Test creating table from dict."""
    import pyblaze
    conn = pyblaze.connect()
    conn.from_dict("users", {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.3, 92.1]
    })

    assert "users" in conn.list_tables()
    result = conn.query("SELECT * FROM users WHERE score > 90")
    assert result.num_rows() == 2


def test_prepared_statement():
    """Test prepared statements."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

    stmt = conn.prepare("SELECT * FROM users WHERE id = $1")

    result1 = stmt.execute(1)
    assert result1.num_rows() == 1

    result2 = stmt.execute(2)
    assert result2.num_rows() == 1


def test_prepared_statement_cached():
    """Test cached prepared statements."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    # First call creates cached plan
    stmt1 = conn.prepare_cached("SELECT * FROM users WHERE id > $1")
    result1 = stmt1.execute(0)
    assert result1.num_rows() == 2

    # Second call reuses cached plan
    stmt2 = conn.prepare_cached("SELECT * FROM users WHERE id > $1")
    result2 = stmt2.execute(1)
    assert result2.num_rows() == 1


def test_csv_operations():
    """Test CSV file operations."""
    import pyblaze

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name,value\n")
        f.write("1,Alice,100\n")
        f.write("2,Bob,200\n")
        f.write("3,Charlie,300\n")
        csv_path = f.name

    try:
        # Test read_csv
        result = pyblaze.read_csv(csv_path)
        assert result.num_rows() == 3
        assert result.num_columns() == 3

        # Test register_csv
        conn = pyblaze.connect()
        conn.register_csv("data", csv_path)
        assert "data" in conn.list_tables()

        result = conn.query("SELECT * FROM data WHERE value > 150")
        assert result.num_rows() == 2

    finally:
        os.unlink(csv_path)


def test_aggregations():
    """Test SQL aggregations."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE sales (product VARCHAR, amount INT)")
    conn.execute("""
        INSERT INTO sales VALUES
        ('A', 100), ('A', 150), ('B', 200), ('B', 50), ('C', 300)
    """)

    result = conn.query("""
        SELECT product, SUM(amount) as total, COUNT(*) as cnt
        FROM sales
        GROUP BY product
        ORDER BY total DESC
    """)

    dicts = result.to_dicts()
    assert len(dicts) == 3
    assert dicts[0]['product'] == 'C'
    assert dicts[0]['total'] == 300


def test_joins():
    """Test SQL joins."""
    import pyblaze
    conn = pyblaze.connect()

    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    conn.execute("CREATE TABLE orders (user_id INT, product VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 'Widget'), (1, 'Gadget'), (2, 'Widget')")

    result = conn.query("""
        SELECT u.name, COUNT(*) as order_count
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.name
        ORDER BY order_count DESC
    """)

    dicts = result.to_dicts()
    assert len(dicts) == 2
    assert dicts[0]['name'] == 'Alice'
    assert dicts[0]['order_count'] == 2


def test_null_handling():
    """Test NULL value handling."""
    import pyblaze
    conn = pyblaze.connect()
    conn.from_dict("data", {
        "id": [1, 2, 3],
        "value": [100, None, 300]
    })

    result = conn.query("SELECT * FROM data WHERE value IS NULL")
    dicts = result.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]['id'] == 2
    assert dicts[0]['value'] is None


@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="pyarrow not installed"),
    reason="pyarrow not available"
)
def test_to_pyarrow():
    """Test converting to PyArrow Table."""
    import pyblaze
    import pyarrow as pa

    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    result = conn.query("SELECT * FROM users ORDER BY id")
    table = result.to_pyarrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.num_columns == 2


@pytest.mark.skipif(
    not pytest.importorskip("polars", reason="polars not installed"),
    reason="polars not available"
)
def test_to_polars():
    """Test converting to Polars DataFrame."""
    import pyblaze
    import polars as pl

    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    result = conn.query("SELECT * FROM users ORDER BY id")
    df = result.to_polars()

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    assert df.columns == ['id', 'name']


@pytest.mark.skipif(
    not pytest.importorskip("pandas", reason="pandas not installed"),
    reason="pandas not available"
)
def test_to_pandas():
    """Test converting to Pandas DataFrame."""
    import pyblaze
    import pandas as pd

    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    result = conn.query("SELECT * FROM users ORDER BY id")
    df = result.to_pandas()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['id', 'name']


def test_query_result_repr():
    """Test QueryResult string representation."""
    import pyblaze
    conn = pyblaze.connect()
    result = conn.query("SELECT 1, 2, 3")
    repr_str = repr(result)
    assert "QueryResult" in repr_str
    assert "rows=1" in repr_str
    assert "columns=3" in repr_str


def test_connection_repr():
    """Test Connection string representation."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE t1 (id INT)")
    conn.execute("CREATE TABLE t2 (id INT)")
    repr_str = repr(conn)
    assert "Connection" in repr_str
    assert "tables=2" in repr_str


def test_drop_table():
    """Test DROP TABLE."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT)")
    assert "users" in conn.list_tables()

    conn.execute("DROP TABLE users")
    assert "users" not in conn.list_tables()


def test_window_functions():
    """Test window functions."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE sales (id INT, amount INT)")
    conn.execute("INSERT INTO sales VALUES (1, 100), (2, 200), (3, 150)")

    result = conn.query("""
        SELECT id, amount, SUM(amount) OVER () as total
        FROM sales
        ORDER BY id
    """)

    dicts = result.to_dicts()
    assert len(dicts) == 3
    assert all(d['total'] == 450 for d in dicts)


def test_subquery():
    """Test subqueries."""
    import pyblaze
    conn = pyblaze.connect()
    conn.execute("CREATE TABLE users (id INT, score INT)")
    conn.execute("INSERT INTO users VALUES (1, 80), (2, 90), (3, 70)")

    result = conn.query("""
        SELECT * FROM users
        WHERE score > (SELECT AVG(score) FROM users)
        ORDER BY id
    """)

    dicts = result.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]['id'] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
