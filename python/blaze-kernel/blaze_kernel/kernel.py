"""Blaze SQL Jupyter kernel implementation."""

from __future__ import annotations

from html import escape
from typing import Any, Dict

from ipykernel.kernelbase import Kernel

import pyblaze


class BlazeKernel(Kernel):
    """Jupyter kernel for executing Blaze SQL queries."""

    implementation = "Blaze SQL"
    implementation_version = "0.1.0"
    language = "sql"
    language_version = "SQL:2016"
    language_info = {
        "name": "sql",
        "mimetype": "application/sql",
        "file_extension": ".sql",
    }
    banner = "Blaze SQL Kernel — Embedded OLAP Query Engine"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.conn = pyblaze.Connection()

    def do_execute(
        self,
        code: str,
        silent: bool,
        store_history: bool = True,
        user_expressions: Dict[str, Any] | None = None,
        allow_stdin: bool = False,
    ) -> Dict[str, Any]:
        """Execute SQL code and return results as HTML table."""
        code = code.strip()
        if not code:
            return self._ok_reply()

        try:
            # Try as query first (SELECT, EXPLAIN, etc.)
            if code.upper().startswith(("SELECT", "WITH", "EXPLAIN", "SHOW")):
                result = self.conn.query(code)
                if not silent:
                    html = self._batches_to_html(result)
                    self.send_response(
                        self.iopub_socket,
                        "display_data",
                        {"data": {"text/html": html}, "metadata": {}},
                    )
            else:
                count = self.conn.execute(code)
                if not silent:
                    self.send_response(
                        self.iopub_socket,
                        "stream",
                        {"name": "stdout", "text": f"OK ({count} rows affected)\n"},
                    )
        except Exception as e:
            self.send_response(
                self.iopub_socket,
                "stream",
                {"name": "stderr", "text": f"Error: {e}\n"},
            )
            return {
                "status": "error",
                "ename": type(e).__name__,
                "evalue": str(e),
                "traceback": [],
            }

        return self._ok_reply()

    def do_complete(
        self, code: str, cursor_pos: int
    ) -> Dict[str, Any]:
        """Provide SQL auto-completion using table names."""
        # Extract the word being typed
        text_to_cursor = code[:cursor_pos]
        word_start = max(
            text_to_cursor.rfind(" "),
            text_to_cursor.rfind("\n"),
            text_to_cursor.rfind("("),
            text_to_cursor.rfind(","),
        ) + 1
        prefix = text_to_cursor[word_start:].upper()

        matches = []

        # Complete with table names
        for table in self.conn.list_tables():
            if table.upper().startswith(prefix):
                matches.append(table)

        # Complete with SQL keywords
        keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
            "GROUP", "BY", "ORDER", "LIMIT", "INSERT", "INTO", "VALUES",
            "CREATE", "TABLE", "DROP", "ALTER", "UPDATE", "DELETE",
        ]
        for kw in keywords:
            if kw.startswith(prefix) and prefix:
                matches.append(kw)

        return {
            "status": "ok",
            "matches": matches,
            "cursor_start": word_start,
            "cursor_end": cursor_pos,
            "metadata": {},
        }

    def _batches_to_html(self, result: Any) -> str:
        """Convert query result to HTML table."""
        rows = result.to_dicts() if hasattr(result, "to_dicts") else []
        if not rows:
            return "<em>(empty result)</em>"

        headers = list(rows[0].keys())
        html = '<table border="1" style="border-collapse: collapse;">'
        html += "<tr>" + "".join(f"<th>{escape(h)}</th>" for h in headers) + "</tr>"
        for row in rows[:1000]:  # Limit display to 1000 rows
            html += "<tr>"
            for h in headers:
                val = row.get(h, "")
                html += f"<td>{escape(str(val))}</td>"
            html += "</tr>"
        html += "</table>"
        if len(rows) > 1000:
            html += f"<p><em>... showing 1000 of {len(rows)} rows</em></p>"
        return html

    def _ok_reply(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }
