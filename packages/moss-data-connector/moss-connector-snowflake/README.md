# moss-connector-snowflake

Snowflake source connector for [Moss](https://github.com/usemoss/moss). Runs a SQL query against a Snowflake warehouse and yields one `DocumentInfo` per row for ingestion into a Moss search index.

## Install

```bash
pip install moss-connector-snowflake
```

## Usage

```python
from moss import DocumentInfo
from moss_connector_snowflake import SnowflakeConnector, ingest

source = SnowflakeConnector(
    account="xy12345.us-east-1",
    user="ETL_USER",
    password="...",
    warehouse="COMPUTE_WH",
    database="PROD",
    schema="PUBLIC",
    query="SELECT ID, TITLE, BODY FROM ARTICLES",
    mapper=lambda row: DocumentInfo(
        id=str(row["ID"]),
        text=row["BODY"],
        metadata={"title": row["TITLE"]},
    ),
)

await ingest(source, project_id="...", project_key="...", index_name="articles")
```

> **Note:** Snowflake returns column names in UPPERCASE by default, so mapper functions should use `row["ID"]` not `row["id"]`.

## Optional parameters

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `role`    | `str \| None` | Snowflake role for RBAC (default: `None`, uses the user's default role) |
