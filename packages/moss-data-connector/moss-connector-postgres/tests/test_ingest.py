"""Unit tests for the ingest function."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from moss import DocumentInfo

from moss_connector_postgres.ingest import ingest


def _make_docs(count: int) -> list[DocumentInfo]:
    """Create a list of DocumentInfo objects for testing."""
    return [DocumentInfo(id=str(i), text=f"Document {i}") for i in range(count)]


def _make_mock_client() -> MagicMock:
    """Create a mock MossClient with async add_docs."""
    client = MagicMock()
    client.add_docs = AsyncMock()

    mock_result = MagicMock()
    mock_result.job_id = "job-123"
    mock_result.index_name = "test-index"
    mock_result.doc_count = 0
    client.add_docs.return_value = mock_result

    return client


class TestIngest:
    """Tests for the ingest function."""

    @pytest.mark.asyncio
    async def test_basic_ingest(self):
        """Test ingesting documents calls add_docs."""
        docs = _make_docs(3)
        client = _make_mock_client()

        with patch.object(
            type(MagicMock()),
            "__iter__",
            return_value=iter(docs),
        ):
            connector = MagicMock()
            connector.__iter__ = MagicMock(return_value=iter(docs))

            results = await ingest(connector, client, "test-index")

        assert len(results) == 1
        client.add_docs.assert_called_once_with("test-index", docs)

    @pytest.mark.asyncio
    async def test_empty_connector(self):
        """Test that empty connector produces no add_docs calls."""
        client = _make_mock_client()
        connector = MagicMock()
        connector.__iter__ = MagicMock(return_value=iter([]))

        results = await ingest(connector, client, "test-index")

        assert results == []
        client.add_docs.assert_not_called()


class TestIngestBatching:
    """Tests for batch behavior in ingest."""

    @pytest.mark.asyncio
    async def test_batching_splits_docs(self):
        """Test that 2500 docs with batch_size=1000 produces 3 calls."""
        docs = _make_docs(2500)
        client = _make_mock_client()
        connector = MagicMock()
        connector.__iter__ = MagicMock(return_value=iter(docs))

        results = await ingest(connector, client, "test-index", batch_size=1000)

        assert len(results) == 3
        assert client.add_docs.call_count == 3

        # Verify batch sizes
        calls = client.add_docs.call_args_list
        assert len(calls[0][0][1]) == 1000
        assert len(calls[1][0][1]) == 1000
        assert len(calls[2][0][1]) == 500

    @pytest.mark.asyncio
    async def test_exact_batch_boundary(self):
        """Test that exactly batch_size docs produces 1 call."""
        docs = _make_docs(1000)
        client = _make_mock_client()
        connector = MagicMock()
        connector.__iter__ = MagicMock(return_value=iter(docs))

        results = await ingest(connector, client, "test-index", batch_size=1000)

        assert len(results) == 1
        client.add_docs.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_batch_size(self):
        """Test with a custom small batch size."""
        docs = _make_docs(5)
        client = _make_mock_client()
        connector = MagicMock()
        connector.__iter__ = MagicMock(return_value=iter(docs))

        results = await ingest(connector, client, "test-index", batch_size=2)

        assert len(results) == 3
        assert client.add_docs.call_count == 3

    @pytest.mark.asyncio
    async def test_returns_all_mutation_results(self):
        """Test that each batch's MutationResult is collected."""
        docs = _make_docs(3)
        client = _make_mock_client()

        result1 = MagicMock()
        result1.job_id = "job-1"
        result2 = MagicMock()
        result2.job_id = "job-2"
        client.add_docs.side_effect = [result1, result2]

        connector = MagicMock()
        connector.__iter__ = MagicMock(return_value=iter(docs))

        results = await ingest(connector, client, "test-index", batch_size=2)

        assert len(results) == 2
        assert results[0].job_id == "job-1"
        assert results[1].job_id == "job-2"
