"""Load documents from JSON/CSV files or stdin."""

from __future__ import annotations

import csv
import json
import sys
import uuid
from pathlib import Path
from typing import Any, List

import typer
from moss import DocumentInfo


def load_documents(file_path: str, auto_id: bool = False) -> List[DocumentInfo]:
    """Load documents from a JSON/CSV file or stdin ('-').

    Args:
        auto_id: When True, generates UUID4 ids for all documents and the
            'id' field becomes optional in the source data.
    """
    if file_path == "-":
        raw = sys.stdin.read()
        return _parse_json_docs(raw, source="stdin", auto_id=auto_id)

    path = Path(file_path)
    if not path.exists():
        raise typer.BadParameter(f"File not found: {file_path}")

    suffix = path.suffix.lower()
    content = path.read_text()

    if suffix == ".csv":
        return _parse_csv_docs(content, auto_id=auto_id)
    elif suffix == ".jsonl":
        return _parse_jsonl_docs(content, source=file_path, auto_id=auto_id)
    elif suffix == ".json":
        return _parse_json_docs(content, source=file_path, auto_id=auto_id)
    else:
        return _parse_json_docs(content, source=file_path, auto_id=auto_id)


def _parse_json_docs(
    raw: str, source: str = "input", auto_id: bool = False
) -> List[DocumentInfo]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON in {source}: {e}")

    if isinstance(data, dict):
        data = data.get("documents", data.get("docs", []))

    if not isinstance(data, list):
        raise typer.BadParameter(
            f"Expected a JSON array of documents, got {type(data).__name__}"
        )

    return [_dict_to_doc(d, i, auto_id=auto_id) for i, d in enumerate(data)]


def _parse_jsonl_docs(
    raw: str, source: str = "input", auto_id: bool = False
) -> List[DocumentInfo]:
    docs = []
    for line_no, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise typer.BadParameter(f"Invalid JSON on line {line_no} in {source}: {e}")
        docs.append(_dict_to_doc(obj, line_no - 1, auto_id=auto_id))
    return docs


def _parse_csv_docs(content: str, auto_id: bool = False) -> List[DocumentInfo]:
    reader = csv.DictReader(content.splitlines())
    docs = []
    for i, row in enumerate(reader):
        if "text" not in row:
            raise typer.BadParameter(
                f"CSV row {i + 1}: missing required 'text' column"
            )
        if not auto_id and "id" not in row:
            raise typer.BadParameter(
                f"CSV row {i + 1}: missing required 'id' column (use --auto-id to generate IDs)"
            )

        doc_id = str(uuid.uuid4()) if auto_id else row["id"]

        metadata = None
        if "metadata" in row and row["metadata"]:
            try:
                metadata = json.loads(row["metadata"])
            except json.JSONDecodeError:
                raise typer.BadParameter(
                    f"CSV row {i + 1}: invalid JSON in 'metadata' column"
                )

        embedding = None
        if "embedding" in row and row["embedding"]:
            try:
                embedding = json.loads(row["embedding"])
            except json.JSONDecodeError:
                raise typer.BadParameter(
                    f"CSV row {i + 1}: invalid JSON in 'embedding' column"
                )

        docs.append(
            DocumentInfo(
                id=doc_id,
                text=row["text"],
                metadata=metadata,
                embedding=embedding,
            )
        )
    return docs


def _dict_to_doc(d: Any, index: int, auto_id: bool = False) -> DocumentInfo:
    if not isinstance(d, dict):
        raise typer.BadParameter(f"Document at index {index}: expected object, got {type(d).__name__}")
    if "text" not in d:
        raise typer.BadParameter(f"Document at index {index}: missing required 'text' field")
    if not auto_id and "id" not in d:
        raise typer.BadParameter(
            f"Document at index {index}: missing required 'id' field (use --auto-id to generate IDs)"
        )

    doc_id = str(uuid.uuid4()) if auto_id else str(d["id"])
    return DocumentInfo(
        id=doc_id,
        text=str(d["text"]),
        metadata=d.get("metadata"),
        embedding=d.get("embedding"),
    )
