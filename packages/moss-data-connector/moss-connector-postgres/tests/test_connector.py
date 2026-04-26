"""Unit tests for PostgresConnector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from moss import DocumentInfo

from moss_connector_postgres.connector import PostgresConnector
from moss_connector_postgres.models import RowMapping


def _make_cursor_description(*col_names: str):
    """Build a minimal cursor.description from column names."""
    return [(name, None, None, None, None, None, None) for name in col_names]


def _make_mock_cursor(rows: list[dict], col_names: list[str]):
    """Create a mock cursor that yields rows and has description."""
    cursor = MagicMock()
    cursor.description = _make_cursor_description(*col_names)
    cursor.__iter__ = MagicMock(return_value=iter(rows))
    cursor.execute = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


class TestRowMapping:
    """Tests for the RowMapping dataclass."""

    def test_basic_mapping(self):
        """Test creating a basic mapping."""
        mapping = RowMapping(id_column="id", text_column="content")
        assert mapping.id_column == "id"
        assert mapping.text_column == "content"
        assert mapping.metadata_columns == []

    def test_with_metadata(self):
        """Test mapping with metadata columns."""
        mapping = RowMapping(
            id_column="id",
            text_column="body",
            metadata_columns=["author", "category"],
        )
        assert mapping.metadata_columns == ["author", "category"]

    def test_frozen(self):
        """Test that RowMapping is immutable."""
        mapping = RowMapping(id_column="id", text_column="text")
        with pytest.raises(AttributeError):
            mapping.id_column = "other"  # type: ignore[misc]


class TestPostgresConnectorInit:
    """Tests for PostgresConnector initialization."""

    def test_with_conninfo(self):
        """Test init with conninfo string."""
        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            conninfo="postgresql://localhost/test",
        )
        assert connector._owns_connection is True

    def test_with_connection(self):
        """Test init with existing connection."""
        mapping = RowMapping(id_column="id", text_column="text")
        mock_conn = MagicMock()
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )
        assert connector._owns_connection is False

    def test_both_raises(self):
        """Test that providing both conninfo and connection raises."""
        mapping = RowMapping(id_column="id", text_column="text")
        with pytest.raises(ValueError, match="not both"):
            PostgresConnector(
                query="SELECT * FROM docs",
                mapping=mapping,
                conninfo="postgresql://localhost/test",
                connection=MagicMock(),
            )

    def test_neither_raises(self):
        """Test that providing neither conninfo nor connection raises."""
        mapping = RowMapping(id_column="id", text_column="text")
        with pytest.raises(ValueError, match="Provide either"):
            PostgresConnector(
                query="SELECT * FROM docs",
                mapping=mapping,
            )


class TestPostgresConnectorIter:
    """Tests for PostgresConnector iteration."""

    def test_basic_iteration(self):
        """Test iterating over rows produces DocumentInfo objects."""
        rows = [
            {"id": 1, "content": "Hello world", "author": "alice"},
            {"id": 2, "content": "Goodbye world", "author": "bob"},
        ]
        col_names = ["id", "content", "author"]
        mapping = RowMapping(
            id_column="id",
            text_column="content",
            metadata_columns=["author"],
        )

        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, col_names)
        mock_conn.cursor.return_value = cursor

        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert len(docs) == 2
        assert isinstance(docs[0], DocumentInfo)
        assert docs[0].id == "1"
        assert docs[0].text == "Hello world"
        assert docs[0].metadata == {"author": "alice"}
        assert docs[1].id == "2"
        assert docs[1].metadata == {"author": "bob"}

    def test_no_metadata_columns(self):
        """Test iteration without metadata columns."""
        rows = [{"id": "abc", "body": "Some text"}]
        col_names = ["id", "body"]
        mapping = RowMapping(id_column="id", text_column="body")

        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, col_names)
        mock_conn.cursor.return_value = cursor

        connector = PostgresConnector(
            query="SELECT id, body FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert len(docs) == 1
        assert docs[0].id == "abc"
        assert docs[0].text == "Some text"
        assert docs[0].metadata is None

    def test_empty_result(self):
        """Test iteration over empty result set."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([], ["id", "text"])
        mock_conn.cursor.return_value = cursor
        mapping = RowMapping(id_column="id", text_column="text")

        connector = PostgresConnector(
            query="SELECT * FROM docs WHERE 1=0",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs == []

    def test_no_description(self):
        """Test handling of a query that returns no description (e.g. DDL)."""
        mock_conn = MagicMock()
        cursor = MagicMock()
        cursor.description = None
        cursor.execute = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="CREATE TABLE foo (id int)",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs == []

    @patch("moss_connector_postgres.connector.psycopg.connect")
    def test_owned_connection_closes(self, mock_connect):
        """Test that owned connections are closed after iteration."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([{"id": "1", "text": "doc"}], ["id", "text"])
        mock_conn.cursor.return_value = cursor
        mock_connect.return_value = mock_conn

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            conninfo="postgresql://localhost/test",
        )

        list(connector)
        mock_conn.close.assert_called_once()

    @patch("moss_connector_postgres.connector.psycopg.connect")
    def test_owned_connection_closes_on_error(self, mock_connect):
        """Test that owned connections are closed even when iteration fails."""
        mock_conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("query failed")
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = cursor
        mock_connect.return_value = mock_conn

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            conninfo="postgresql://localhost/test",
        )

        with pytest.raises(RuntimeError, match="query failed"):
            list(connector)

        mock_conn.close.assert_called_once()

    def test_external_connection_not_closed(self):
        """Test that external connections are not closed after iteration."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([{"id": "1", "text": "doc"}], ["id", "text"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        list(connector)
        mock_conn.close.assert_not_called()


class TestColumnValidation:
    """Tests for column validation against cursor.description."""

    def test_missing_id_column(self):
        """Test that missing id_column raises ValueError."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([], ["text", "author"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT text, author FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        with pytest.raises(ValueError, match="not found in query result"):
            list(connector)

    def test_missing_text_column(self):
        """Test that missing text_column raises ValueError."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([], ["id", "author"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(id_column="id", text_column="content")
        connector = PostgresConnector(
            query="SELECT id, author FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        with pytest.raises(ValueError, match="not found in query result"):
            list(connector)

    def test_missing_metadata_column(self):
        """Test that missing metadata column raises ValueError."""
        mock_conn = MagicMock()
        cursor = _make_mock_cursor([], ["id", "text"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(
            id_column="id",
            text_column="text",
            metadata_columns=["author"],
        )
        connector = PostgresConnector(
            query="SELECT id, text FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        with pytest.raises(ValueError, match="Metadata column"):
            list(connector)


class TestMetadataCoercion:
    """Tests for metadata value coercion and None handling."""

    def test_int_metadata_coerced_to_str(self):
        """Test that integer metadata values are coerced to strings."""
        rows = [{"id": "1", "text": "doc", "priority": 5}]
        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, ["id", "text", "priority"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(
            id_column="id",
            text_column="text",
            metadata_columns=["priority"],
        )
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs[0].metadata == {"priority": "5"}

    def test_none_metadata_skipped(self):
        """Test that None metadata values are excluded."""
        rows = [{"id": "1", "text": "doc", "author": None, "category": "tech"}]
        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, ["id", "text", "author", "category"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(
            id_column="id",
            text_column="text",
            metadata_columns=["author", "category"],
        )
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs[0].metadata == {"category": "tech"}

    def test_all_none_metadata_returns_none(self):
        """Test that all-None metadata results in metadata=None."""
        rows = [{"id": "1", "text": "doc", "author": None}]
        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, ["id", "text", "author"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(
            id_column="id",
            text_column="text",
            metadata_columns=["author"],
        )
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs[0].metadata is None

    def test_id_coerced_to_str(self):
        """Test that numeric IDs are coerced to strings."""
        rows = [{"id": 42, "text": "doc"}]
        mock_conn = MagicMock()
        cursor = _make_mock_cursor(rows, ["id", "text"])
        mock_conn.cursor.return_value = cursor

        mapping = RowMapping(id_column="id", text_column="text")
        connector = PostgresConnector(
            query="SELECT * FROM docs",
            mapping=mapping,
            connection=mock_conn,
        )

        docs = list(connector)
        assert docs[0].id == "42"
