"""Unit tests for the Snowflake connector. No live Snowflake needed — we mock
``snowflake.connector`` so the test runs anywhere, and we patch
``moss.MossClient`` inside ingest so no Moss network call is made.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("snowflake.connector")

from moss import DocumentInfo  # noqa: E402

from moss_connector_snowflake import SnowflakeConnector, ingest  # noqa: E402


@dataclass
class FakeMutationResult:
    doc_count: int
    job_id: str = "fake-job-id"
    index_name: str = ""


@dataclass
class FakeMossClient:
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def create_index(self, name, docs, model_id=None):
        docs = list(docs)
        self.calls.append({"name": name, "docs": docs, "model_id": model_id})
        return FakeMutationResult(doc_count=len(docs), index_name=name)


def _snowflake_mock_returning(
    rows: list[dict[str, Any]],
) -> tuple[MagicMock, MagicMock]:
    """Build a mock ``snowflake.connector.connect(...)`` return value.

    Returns (connection, cursor) so the test can assert on either one.
    """
    cursor = MagicMock()
    cursor.__iter__ = lambda self: iter(rows)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


async def test_snowflake_ingest_end_to_end():
    rows_from_snowflake = [
        {"ID": "1", "TITLE": "Refund policy", "BODY": "Refunds take 3–5 days."},
        {"ID": "2", "TITLE": "Shipping", "BODY": "We ship within 24 hours."},
    ]
    fake_conn, fake_cursor = _snowflake_mock_returning(rows_from_snowflake)
    fake_moss = FakeMossClient()

    with (
        patch(
            "moss_connector_snowflake.connector.sf_connect",
            return_value=fake_conn,
        ),
        patch(
            "moss_connector_snowflake.ingest.MossClient",
            return_value=fake_moss,
        ),
    ):
        source = SnowflakeConnector(
            account="xy12345",
            user="ETL_USER",
            password="secret",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            query="SELECT ID, TITLE, BODY FROM ARTICLES",
            mapper=lambda r: DocumentInfo(
                id=r["ID"],
                text=r["BODY"],
                metadata={"title": r["TITLE"]},
            ),
        )
        result = await ingest(source, "fake_id", "fake_key", index_name="articles")

    assert result is not None
    assert result.doc_count == 2

    fake_cursor.execute.assert_called_once_with("SELECT ID, TITLE, BODY FROM ARTICLES")

    moss_docs = fake_moss.calls[0]["docs"]
    assert moss_docs[0].id == "1"
    assert moss_docs[0].text == "Refunds take 3–5 days."
    assert moss_docs[0].metadata == {"title": "Refund policy"}


async def test_empty_result_skips_network():
    fake_conn, _ = _snowflake_mock_returning([])
    fake_moss = FakeMossClient()

    with (
        patch(
            "moss_connector_snowflake.connector.sf_connect",
            return_value=fake_conn,
        ),
        patch(
            "moss_connector_snowflake.ingest.MossClient",
            return_value=fake_moss,
        ),
    ):
        source = SnowflakeConnector(
            account="xy12345",
            user="ETL_USER",
            password="secret",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            query="SELECT 1 WHERE FALSE",
            mapper=lambda r: DocumentInfo(id="x", text="x"),
        )
        result = await ingest(source, "fake_id", "fake_key", index_name="empty")

    assert result is None
    assert len(fake_moss.calls) == 0


async def test_role_is_forwarded():
    fake_conn, _ = _snowflake_mock_returning([])

    with patch(
        "moss_connector_snowflake.connector.sf_connect",
        return_value=fake_conn,
    ) as mock_connect:
        source = SnowflakeConnector(
            account="xy12345",
            user="ETL_USER",
            password="secret",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            query="SELECT 1",
            mapper=lambda r: DocumentInfo(id="x", text="x"),
            role="ANALYST",
        )
        list(source)  # exhaust the iterator

    mock_connect.assert_called_once_with(
        account="xy12345",
        user="ETL_USER",
        password="secret",
        warehouse="WH",
        database="DB",
        schema="PUBLIC",
        role="ANALYST",
    )
