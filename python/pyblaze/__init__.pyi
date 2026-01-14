"""Type stubs for PyBlaze."""

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

__version__: str

class Connection:
    """Database connection for executing SQL queries."""

    def __init__(
        self,
        *,
        batch_size: int = 8192,
        memory_limit: Optional[int] = None,
    ) -> None:
        """Create a new in-memory database connection.

        Args:
            batch_size: The batch size for vectorized execution.
            memory_limit: Optional memory limit in bytes.
        """
        ...

    def query(self, sql: str) -> QueryResult:
        """Execute a SQL query and return the results.

        Args:
            sql: The SQL query to execute.

        Returns:
            A QueryResult containing the query results.
        """
        ...

    def execute(self, sql: str) -> int:
        """Execute a SQL statement that doesn't return results.

        Args:
            sql: The SQL statement to execute.

        Returns:
            The number of rows affected.
        """
        ...

    def register_csv(
        self,
        name: str,
        path: str,
        *,
        delimiter: Optional[str] = None,
        has_header: bool = True,
    ) -> None:
        """Register a CSV file as a table.

        Args:
            name: The name to register the table under.
            path: The path to the CSV file.
            delimiter: Optional delimiter character.
            has_header: Whether the file has a header row.
        """
        ...

    def register_parquet(self, name: str, path: str) -> None:
        """Register a Parquet file as a table.

        Args:
            name: The name to register the table under.
            path: The path to the Parquet file.
        """
        ...

    def list_tables(self) -> List[str]:
        """List all registered tables.

        Returns:
            A list of table names.
        """
        ...

    def table_schema(self, name: str) -> Optional[Schema]:
        """Get the schema of a table.

        Args:
            name: The name of the table.

        Returns:
            A Schema object, or None if the table doesn't exist.
        """
        ...

    def prepare(self, sql: str) -> PreparedStatement:
        """Prepare a SQL statement for repeated execution.

        Args:
            sql: The SQL query with parameter placeholders ($1, $2, etc.).

        Returns:
            A PreparedStatement that can be executed with different parameters.
        """
        ...

    def prepare_cached(self, sql: str) -> PreparedStatement:
        """Prepare a SQL statement using the statement cache.

        Args:
            sql: The SQL query with parameter placeholders.

        Returns:
            A PreparedStatement that can be executed with different parameters.
        """
        ...

    def from_dict(self, name: str, data: Dict[str, List[Any]]) -> None:
        """Create a table from a Python dictionary of lists.

        Args:
            name: The name for the new table.
            data: A dictionary mapping column names to lists of values.
        """
        ...

class QueryResult:
    """Container for query results."""

    def num_rows(self) -> int:
        """Get the number of rows in the result."""
        ...

    def num_columns(self) -> int:
        """Get the number of columns in the result."""
        ...

    def column_names(self) -> List[str]:
        """Get the column names."""
        ...

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to a list of dictionaries (row-oriented).

        Returns:
            A list of dictionaries, where each dictionary represents a row.
        """
        ...

    def to_dict(self) -> Dict[str, List[Any]]:
        """Convert to a dictionary of lists (column-oriented).

        Returns:
            A dictionary mapping column names to lists of values.
        """
        ...

    def to_pyarrow(self) -> Any:
        """Convert to a PyArrow Table.

        Requires the pyarrow package to be installed.

        Returns:
            A pyarrow.Table containing the query results.
        """
        ...

    def to_polars(self) -> Any:
        """Convert to a Polars DataFrame.

        Requires the polars package to be installed.

        Returns:
            A polars.DataFrame containing the query results.
        """
        ...

    def to_pandas(self) -> Any:
        """Convert to a Pandas DataFrame.

        Requires the pandas package to be installed.

        Returns:
            A pandas.DataFrame containing the query results.
        """
        ...

    def __len__(self) -> int:
        """Get the number of rows."""
        ...

class PreparedStatement:
    """A prepared SQL statement for repeated execution."""

    def execute(self, *args: Any) -> QueryResult:
        """Execute the prepared statement with the given parameters.

        Args:
            *args: Positional parameters to substitute for $1, $2, etc.

        Returns:
            A QueryResult containing the query results.
        """
        ...

    def parameters(self) -> List[Dict[str, str]]:
        """Get information about the expected parameters.

        Returns:
            A list of dictionaries with parameter information.
        """
        ...

    def sql(self) -> str:
        """Get the SQL string for this prepared statement."""
        ...

class Schema:
    """Schema information for a table."""

    def names(self) -> List[str]:
        """Get the field names."""
        ...

    def types(self) -> List[str]:
        """Get the field types as strings."""
        ...

    def __len__(self) -> int:
        """Get the number of fields."""
        ...

def connect(
    *,
    batch_size: int = 8192,
    memory_limit: Optional[int] = None,
) -> Connection:
    """Create a new in-memory connection.

    Args:
        batch_size: The batch size for vectorized execution.
        memory_limit: Optional memory limit in bytes.

    Returns:
        A new Connection object.
    """
    ...

def read_csv(
    path: str,
    *,
    delimiter: Optional[str] = None,
    has_header: bool = True,
) -> QueryResult:
    """Read a CSV file and return a QueryResult.

    Args:
        path: The path to the CSV file.
        delimiter: Optional delimiter character.
        has_header: Whether the file has a header row.

    Returns:
        A QueryResult containing the file contents.
    """
    ...

def read_parquet(path: str) -> QueryResult:
    """Read a Parquet file and return a QueryResult.

    Args:
        path: The path to the Parquet file.

    Returns:
        A QueryResult containing the file contents.
    """
    ...

def read_delta(
    path: str,
    *,
    version: Optional[int] = None,
) -> QueryResult:
    """Read a Delta Lake table and return a QueryResult.

    Args:
        path: The path to the Delta table directory.
        version: Optional version number for time travel.

    Returns:
        A QueryResult containing the table contents.
    """
    ...
