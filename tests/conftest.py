import os
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pymysql
import pytest

from dbos_mysql import DBOS, DBOSConfig

MYSQL_PORT = int(os.environ.get("DBOS_TEST_DB_PORT", "13307"))
MYSQL_PASSWORD = "dbos-test-root"


def _mysql_connection(database: str | None = None) -> pymysql.Connection:
    return pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password=MYSQL_PASSWORD, database=database, autocommit=True)


class Database:
    """Direct database access for tests that need to arrange a state the public API cannot reach."""

    def __init__(self, backend: str, tmp_path: Path) -> None:
        self.backend = backend
        self._path = tmp_path / "test.sqlite"
        self._mysql_name = f"dbos_t_{uuid.uuid4().hex}"
        if backend == "mysql":
            with _mysql_connection() as connection, connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {self._mysql_name}")

    @property
    def url(self) -> str:
        if self.backend == "sqlite":
            return f"sqlite://{self._path}"
        return f"mysql://root:{MYSQL_PASSWORD}@127.0.0.1:{MYSQL_PORT}/{self._mysql_name}"

    def execute(self, sql: str, *params: Any) -> list[tuple[Any, ...]]:
        if self.backend == "sqlite":
            with sqlite3.connect(self._path, timeout=30) as connection:
                return connection.execute(sql.replace("%s", "?"), params).fetchall()
        with _mysql_connection(self._mysql_name) as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def expire_lease(self, workflow_id: str) -> None:
        """Make a workflow look as if its process crashed, so recovery can take it."""
        column, past = ("claim_after_ms", 0) if self.backend == "sqlite" else ("claim_after", "2000-01-01 00:00:00")
        self.execute(
            f"UPDATE dbos_workflows SET status = 'PENDING', lease_token = 'crashed', {column} = %s WHERE workflow_id = %s",
            past,
            workflow_id,
        )

    def drop(self) -> None:
        if self.backend == "mysql":
            with _mysql_connection() as connection, connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS {self._mysql_name}")


@pytest.fixture(params=["sqlite", "mysql"])
def database(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Database]:
    db = Database(request.param, tmp_path)
    yield db
    db.drop()


@pytest.fixture
def config(database: Database) -> DBOSConfig:
    return {"name": "test-app", "system_database_url": database.url, "recovery_interval_seconds": 3600.0}


@pytest.fixture
def dbos(config: DBOSConfig) -> Iterator[DBOS]:
    DBOS.destroy(destroy_registry=True)
    instance = DBOS(config=config)
    DBOS.launch()
    yield instance
    DBOS.destroy(destroy_registry=True)


def retry_until_success(function: Callable[[], Any], retries: int = 10, interval: float = 1.0) -> Any:
    for attempt in range(retries):
        try:
            return function()
        except AssertionError:
            if attempt == retries - 1:
                raise
            time.sleep(interval)
    return None
