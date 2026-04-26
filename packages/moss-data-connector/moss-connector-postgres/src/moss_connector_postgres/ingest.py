"""Ingest function — sends DocumentInfo batches from a connector to Moss."""

from __future__ import annotations

from collections.abc import Iterable

from moss import DocumentInfo, MossClient, MutationResult


async def ingest(
    connector: Iterable[DocumentInfo],
    client: MossClient,
    index_name: str,
    batch_size: int = 1000,
) -> list[MutationResult]:
    """Ingest documents from an iterable into a Moss index.

    Streams documents from the iterable in bounded batches to avoid
    materializing the entire dataset in memory.

    Args:
        connector: Any iterable yielding DocumentInfo objects.
        client: An authenticated MossClient instance.
        index_name: Name of the Moss index to add documents to.
        batch_size: Maximum documents per add_docs call. Defaults to 1000.

    Returns:
        A list of MutationResult, one per batch.
    """
    results: list[MutationResult] = []
    batch: list[DocumentInfo] = []

    for doc in connector:
        batch.append(doc)
        if len(batch) == batch_size:
            result = await client.add_docs(index_name, batch)
            results.append(result)
            batch = []

    if batch:
        result = await client.add_docs(index_name, batch)
        results.append(result)

    return results
