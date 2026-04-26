"""Data models for the Postgres connector."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RowMapping:
    """Maps SQL result columns to DocumentInfo fields.

    Args:
        id_column: Column name to use as the document ID.
        text_column: Column name to use as the document text.
        metadata_columns: Optional list of column names to include as metadata.
    """

    id_column: str
    text_column: str
    metadata_columns: list[str] = field(default_factory=list)
