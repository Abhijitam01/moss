"""Snowflake connector.

Runs a SQL query against a Snowflake warehouse via ``snowflake-connector-python``
and yields one ``DocumentInfo`` per result row.

Note: Snowflake returns column names in UPPERCASE by default, so mapper
functions should use ``row["ID"]`` not ``row["id"]``.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator

from moss import DocumentInfo
from snowflake.connector import DictCursor
from snowflake.connector import connect as sf_connect


class SnowflakeConnector:
    """Read rows from a Snowflake SQL query and yield one ``DocumentInfo`` each.

    ``mapper`` turns a row dict into a ``DocumentInfo``; the caller decides
    which columns become id / text / metadata / embedding.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str,
        query: str,
        mapper: Callable[[dict[str, Any]], DocumentInfo],
        role: str | None = None,
    ) -> None:
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.query = query
        self.mapper = mapper
        self.role = role

    def __iter__(self) -> Iterator[DocumentInfo]:
        connect_args: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.role is not None:
            connect_args["role"] = self.role

        conn = sf_connect(**connect_args)
        try:
            cursor = conn.cursor(DictCursor)
            try:
                cursor.execute(self.query)
                for row in cursor:
                    yield self.mapper(row)
            finally:
                cursor.close()
        finally:
            conn.close()
