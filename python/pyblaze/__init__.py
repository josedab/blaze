"""PyBlaze - High-performance embedded OLAP query engine.

PyBlaze is a Python binding for the Blaze query engine, providing
SQL:2016 compliance with native Apache Arrow and Parquet integration.

Example:
    >>> import pyblaze
    >>> conn = pyblaze.connect()
    >>> conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
    >>> conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    >>> result = conn.query("SELECT * FROM users")
    >>> print(result.to_dicts())
    [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
"""

from pyblaze.pyblaze import (
    Connection,
    QueryResult,
    PreparedStatement,
    Schema,
    connect,
    read_csv,
    read_parquet,
    read_delta,
    __version__,
)

__all__ = [
    "Connection",
    "QueryResult",
    "PreparedStatement",
    "Schema",
    "connect",
    "read_csv",
    "read_parquet",
    "read_delta",
    "__version__",
]
