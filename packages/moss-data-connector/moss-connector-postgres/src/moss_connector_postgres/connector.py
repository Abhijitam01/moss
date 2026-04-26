"""PostgresConnector — reads SQL query results as DocumentInfo objects."""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
from moss import DocumentInfo
from psycopg.rows import dict_row

from .models import RowMapping


class PostgresConnector:
    """Reads rows from a Postgres query and yields DocumentInfo objects.

    Args:
        query: SQL query to execute.
        mapping: Maps column names to DocumentInfo fields.
        conninfo: Postgres connection string. Mutually exclusive with ``connection``.
        connection: Existing psycopg Connection. Mutually exclusive with ``conninfo``.

    Raises:
        ValueError: If both or neither of ``conninfo`` and ``connection`` are provided.
    """

    def __init__(
        self,
        query: str,
        mapping: RowMapping,
        conninfo: str | None = None,
        connection: psycopg.Connection | None = None,
    ) -> None:
        """Initialize the connector."""
        if conninfo is not None and connection is not None:
            raise ValueError("Provide either conninfo or connection, not both")
        if conninfo is None and connection is None:
            raise ValueError("Provide either conninfo or connection")

        self._query = query
        self._mapping = mapping
        self._conninfo = conninfo
        self._connection = connection
        self._owns_connection = conninfo is not None

    def _validate_columns(self, available: set[str]) -> None:
        """Validate that mapped columns exist in the query result.

        Args:
            available: Set of column names from cursor.description.

        Raises:
            ValueError: If a required column is missing.
        """
        required = {self._mapping.id_column, self._mapping.text_column}
        missing = required - available
        if missing:
            raise ValueError(
                f"Required column(s) {missing} not found in query result. "
                f"Available columns: {available}"
            )

        if self._mapping.metadata_columns:
            meta_missing = set(self._mapping.metadata_columns) - available
            if meta_missing:
                raise ValueError(
                    f"Metadata column(s) {meta_missing} not found in query result. "
                    f"Available columns: {available}"
                )

    def _row_to_doc(self, row: dict) -> DocumentInfo:
        """Convert a single row dict to a DocumentInfo.

        Args:
            row: Dict from psycopg dict_row cursor.

        Returns:
            A DocumentInfo with id, text, and optional metadata.
        """
        metadata = None
        if self._mapping.metadata_columns:
            metadata = {
                col: str(row[col]) for col in self._mapping.metadata_columns if row[col] is not None
            }

        return DocumentInfo(
            id=str(row[self._mapping.id_column]),
            text=str(row[self._mapping.text_column]),
            metadata=metadata or None,
        )

    def __iter__(self) -> Iterator[DocumentInfo]:
        """Execute the query and yield DocumentInfo for each row.

        Yields:
            DocumentInfo objects mapped from query results.

        Raises:
            ValueError: If mapped columns are missing from the result set.
        """
        conn = None
        try:
            if self._owns_connection:
                conn = psycopg.connect(self._conninfo)
            else:
                conn = self._connection

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(self._query)

                if cur.description is None:
                    return

                available = {desc[0] for desc in cur.description}
                self._validate_columns(available)

                for row in cur:
                    yield self._row_to_doc(row)
        finally:
            if self._owns_connection and conn is not None:
                conn.close()
