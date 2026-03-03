"""Blaze adapter for dbt."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pyblaze


class BlazeConnectionManager:
    """Manages Blaze database connections for dbt."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._conn: Optional[pyblaze.Connection] = None

    @property
    def conn(self) -> pyblaze.Connection:
        if self._conn is None:
            self._conn = pyblaze.Connection()
        return self._conn

    def execute(self, sql: str) -> Tuple[str, List[Dict[str, Any]]]:
        """Execute a SQL statement and return results."""
        try:
            result = self.conn.query(sql)
            return "OK", result.to_dicts() if hasattr(result, "to_dicts") else []
        except Exception as e:
            raise RuntimeError(f"Blaze query failed: {e}") from e

    def close(self) -> None:
        self._conn = None


class BlazeAdapter:
    """dbt adapter implementation for Blaze Query Engine.

    Supports:
    - Table materializations via CREATE TABLE AS SELECT
    - View materializations via CREATE VIEW
    - Incremental models via INSERT INTO ... SELECT
    - Seeds via table creation from CSV data
    - Schema inspection via information_schema
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.connection_manager = BlazeConnectionManager(config)

    @classmethod
    def type(cls) -> str:
        return "blaze"

    def execute(self, sql: str) -> Tuple[str, List[Dict[str, Any]]]:
        return self.connection_manager.execute(sql)

    def get_columns_in_relation(self, relation: str) -> List[Dict[str, str]]:
        """Get column metadata for a table."""
        _, rows = self.execute(
            f"SELECT column_name, data_type, is_nullable "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{relation}'"
        )
        return rows

    def create_schema(self, schema: str) -> None:
        """Blaze uses a single schema; this is a no-op."""
        pass

    def drop_relation(self, relation: str) -> None:
        """Drop a table or view."""
        self.execute(f"DROP TABLE IF EXISTS {relation}")

    def truncate_relation(self, relation: str) -> None:
        """Truncate a table (delete all rows)."""
        self.execute(f"DELETE FROM {relation}")

    def list_relations(self) -> List[str]:
        """List all tables."""
        _, rows = self.execute(
            "SELECT table_name FROM information_schema.tables"
        )
        return [r.get("table_name", "") for r in rows]

    def close(self) -> None:
        self.connection_manager.close()
