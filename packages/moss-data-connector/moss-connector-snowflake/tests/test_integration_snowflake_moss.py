"""End-to-end integration test: Snowflake -> Moss.

Runs a SQL query against a live Snowflake warehouse, ingests rows into a live
Moss project via ``ingest()``, runs a real semantic query, and cleans
everything up on exit.

SKIPPED unless SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
MOSS_PROJECT_ID, and MOSS_PROJECT_KEY are all set.

Run with:
    pytest tests/test_integration_snowflake_moss.py -v -s
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

pytest.importorskip("snowflake.connector")

try:
    from dotenv import load_dotenv

    _here = Path(__file__).resolve()
    for candidate in (
        _here.parents[1] / ".env",
        _here.parents[2] / ".env",
        _here.parents[4] / ".env",
    ):
        if candidate.exists():
            load_dotenv(candidate, override=False)
except ImportError:
    pass

from moss import DocumentInfo, MossClient, QueryOptions  # noqa: E402

from moss_connector_snowflake import SnowflakeConnector, ingest  # noqa: E402

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "MOSS_TEST")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

PROJECT_ID = os.getenv("MOSS_PROJECT_ID")
PROJECT_KEY = os.getenv("MOSS_PROJECT_KEY")

pytestmark = pytest.mark.skipif(
    not (SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER and SNOWFLAKE_PASSWORD and PROJECT_ID and PROJECT_KEY),
    reason=(
        "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, "
        "MOSS_PROJECT_ID, and MOSS_PROJECT_KEY to run this live test."
    ),
)


@pytest.fixture()
def snowflake_table():
    """Create a temporary table with sample data; drop on exit."""
    from snowflake.connector import connect

    table_name = f"E2E_{uuid.uuid4().hex[:8]}"
    conn = connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TEMPORARY TABLE {table_name} "
            f"(ID VARCHAR, TITLE VARCHAR, BODY VARCHAR)"
        )
        cur.execute(
            f"INSERT INTO {table_name} VALUES "
            f"('ART-001', 'Refund policy', 'Refunds are processed within 3 to 5 business days.'), "
            f"('ART-002', 'Shipping time', 'Most orders ship within 24 hours of being placed.'), "
            f"('ART-003', 'Contact support', 'You can reach our support team 24/7 via live chat.')"
        )
        yield table_name
    finally:
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        conn.close()


async def test_snowflake_live_ingest_to_moss(snowflake_table):
    """Full round trip: Snowflake rows -> ingest() -> Moss index -> query -> delete."""
    table_name = snowflake_table
    client = MossClient(PROJECT_ID, PROJECT_KEY)
    index_name = f"moss-connectors-snowflake-e2e-{uuid.uuid4().hex[:8]}"

    try:
        source = SnowflakeConnector(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            query=f"SELECT ID, TITLE, BODY FROM {table_name}",
            mapper=lambda row: DocumentInfo(
                id=row["ID"],
                text=row["BODY"],
                metadata={"title": row["TITLE"]},
            ),
            role=SNOWFLAKE_ROLE,
        )

        result = await ingest(source, PROJECT_ID, PROJECT_KEY, index_name=index_name)
        assert result is not None
        assert result.doc_count == 3

        await client.load_index(index_name)
        result = await client.query(
            index_name, "how long do refunds take", QueryOptions(top_k=3)
        )

        assert result.docs, "expected at least one document in the search result"
        top_ids = [d.id for d in result.docs]
        assert "ART-001" in top_ids, f"refund-policy doc not in top 3: {top_ids}"

    finally:
        try:
            await client.delete_index(index_name)
        except Exception as exc:
            print(f"warning: failed to delete test index {index_name}: {exc}")
