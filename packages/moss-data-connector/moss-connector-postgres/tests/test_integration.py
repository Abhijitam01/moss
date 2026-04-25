"""Integration tests for moss-connector-postgres.

These tests require real Postgres and Moss credentials.
Skip automatically when environment variables are not set.
"""

from __future__ import annotations

import os

import pytest

POSTGRES_CONNINFO = os.environ.get("POSTGRES_CONNINFO")
MOSS_PROJECT_ID = os.environ.get("MOSS_PROJECT_ID")
MOSS_PROJECT_KEY = os.environ.get("MOSS_PROJECT_KEY")

skip_no_postgres = pytest.mark.skipif(
    not POSTGRES_CONNINFO,
    reason="POSTGRES_CONNINFO not set",
)

skip_no_moss = pytest.mark.skipif(
    not MOSS_PROJECT_ID or not MOSS_PROJECT_KEY,
    reason="MOSS_PROJECT_ID and MOSS_PROJECT_KEY not set",
)


@skip_no_postgres
class TestPostgresIntegration:
    """Integration tests against a real Postgres instance."""

    def test_read_rows(self):
        """Test reading rows from a real Postgres database."""
        from moss_connector_postgres import PostgresConnector, RowMapping

        mapping = RowMapping(id_column="id", text_column="content")
        connector = PostgresConnector(
            query="SELECT 1 AS id, 'hello' AS content",
            mapping=mapping,
            conninfo=POSTGRES_CONNINFO,
        )

        docs = list(connector)
        assert len(docs) == 1
        assert docs[0].id == "1"
        assert docs[0].text == "hello"


@skip_no_postgres
@skip_no_moss
class TestIngestIntegration:
    """Integration tests for full ingest pipeline."""

    @pytest.mark.asyncio
    async def test_ingest_to_moss(self):
        """Test ingesting Postgres rows into a Moss index."""
        from moss_connector_postgres import (
            MossClient,
            PostgresConnector,
            RowMapping,
            ingest,
        )

        mapping = RowMapping(id_column="id", text_column="content")
        connector = PostgresConnector(
            query="SELECT 1 AS id, 'integration test doc' AS content",
            mapping=mapping,
            conninfo=POSTGRES_CONNINFO,
        )

        client = MossClient(
            project_id=MOSS_PROJECT_ID,
            project_key=MOSS_PROJECT_KEY,
        )

        results = await ingest(connector, client, "test-postgres-connector")
        assert len(results) >= 1
