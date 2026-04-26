"""Tests for document loading with auto_id support."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import typer

from moss_cli.documents import load_documents


@pytest.fixture
def tmp_json(tmp_path: Path) -> Path:
    """Create a temp JSON file with documents that have id and text."""
    data = [
        {"id": "1", "text": "hello"},
        {"id": "2", "text": "world"},
    ]
    path = tmp_path / "docs.json"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def tmp_json_no_ids(tmp_path: Path) -> Path:
    """Create a temp JSON file with documents that have only text (no id)."""
    data = [
        {"text": "hello"},
        {"text": "world"},
    ]
    path = tmp_path / "docs_no_id.json"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    """Create a temp CSV file with id and text columns."""
    content = "id,text\n1,hello\n2,world\n"
    path = tmp_path / "docs.csv"
    path.write_text(content)
    return path


@pytest.fixture
def tmp_csv_no_ids(tmp_path: Path) -> Path:
    """Create a temp CSV file with only text column."""
    content = "text\nhello\nworld\n"
    path = tmp_path / "docs_no_id.csv"
    path.write_text(content)
    return path


@pytest.fixture
def tmp_jsonl_no_ids(tmp_path: Path) -> Path:
    """Create a temp JSONL file with documents that have only text."""
    lines = [json.dumps({"text": "hello"}), json.dumps({"text": "world"})]
    path = tmp_path / "docs.jsonl"
    path.write_text("\n".join(lines))
    return path


class TestLoadDocumentsAutoId:
    def test_json_auto_id_generates_uuids(self, tmp_json_no_ids: Path):
        docs = load_documents(str(tmp_json_no_ids), auto_id=True)

        assert len(docs) == 2
        # IDs should be valid UUID4
        uuid.UUID(docs[0].id, version=4)
        uuid.UUID(docs[1].id, version=4)
        # IDs should be unique
        assert docs[0].id != docs[1].id

    def test_json_auto_id_preserves_text(self, tmp_json_no_ids: Path):
        docs = load_documents(str(tmp_json_no_ids), auto_id=True)

        assert docs[0].text == "hello"
        assert docs[1].text == "world"

    def test_json_auto_id_overwrites_existing_ids(self, tmp_json: Path):
        docs = load_documents(str(tmp_json), auto_id=True)

        assert docs[0].id != "1"
        assert docs[1].id != "2"
        uuid.UUID(docs[0].id, version=4)

    def test_json_no_auto_id_requires_id_field(self, tmp_json_no_ids: Path):
        with pytest.raises(typer.BadParameter, match="missing required 'id' field"):
            load_documents(str(tmp_json_no_ids), auto_id=False)

    def test_csv_auto_id_generates_uuids(self, tmp_csv_no_ids: Path):
        docs = load_documents(str(tmp_csv_no_ids), auto_id=True)

        assert len(docs) == 2
        uuid.UUID(docs[0].id, version=4)
        assert docs[0].text == "hello"

    def test_csv_no_auto_id_requires_id_column(self, tmp_csv_no_ids: Path):
        with pytest.raises(typer.BadParameter, match="missing required 'id' column"):
            load_documents(str(tmp_csv_no_ids), auto_id=False)

    def test_jsonl_auto_id_generates_uuids(self, tmp_jsonl_no_ids: Path):
        docs = load_documents(str(tmp_jsonl_no_ids), auto_id=True)

        assert len(docs) == 2
        uuid.UUID(docs[0].id, version=4)
        uuid.UUID(docs[1].id, version=4)

    def test_default_auto_id_is_false(self, tmp_json: Path):
        docs = load_documents(str(tmp_json))

        assert docs[0].id == "1"
        assert docs[1].id == "2"
