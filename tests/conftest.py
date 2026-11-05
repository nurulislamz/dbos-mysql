import json
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pymysql
import pytest
from _pytest.nodeid import NodeId

from rust_dwos import DBOS, DBOSConfig

MYSQL_PORT = int(os.environ.get("DBOS_TEST_DB_PORT", "13307"))
MYSQL_PASSWORD = "dbos-test-root"
POSTGRES_PORT = int(os.environ.get("DBOS_TEST_PG_PORT", "13309"))


# silence_logs() turns Rust logging off for the rest of that worker. The log
# check has to run first on the same worker. Bridge schemas are per worker
# (dbos_bridge_gwN), so the other bridge tests are not in this group.
_NATIVE_LOG_GROUP = "native_endpoints"
_LOG_CHECK = "test_rust_logs_reach_python_logging"
_LOG_SHUTDOWN = "test_silence_logs"


def _test_name(item: pytest.Item) -> str:
    return getattr(item, "originalname", None) or item.name


@pytest.hookimpl(hookwrapper=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> Iterator[None]:
    for item in items:
        if _test_name(item) in {_LOG_CHECK, _LOG_SHUTDOWN}:
            item.add_marker(pytest.mark.xdist_group(_NATIVE_LOG_GROUP))
    log_at = next((index for index, item in enumerate(items) if _test_name(item) == _LOG_CHECK), None)
    silence_at = next((index for index, item in enumerate(items) if _test_name(item) == _LOG_SHUTDOWN), None)
    if log_at is not None and silence_at is not None and log_at > silence_at:
        items.insert(silence_at, items.pop(log_at))
    yield
    # pytest-xdist 3.8.0 never applies this mark: WorkerInteractor.pytest_collection_modifyitems
    # has no @pytest.hookimpl, and it writes item._nodeid, which pytest 9 does not read.
    # LoadGroupScheduling groups whatever follows the last '@' in the collected node id.
    if getattr(config.option, "dist", "no") != "loadgroup":
        return
    suffix = f"@{_NATIVE_LOG_GROUP}"
    for item in items:
        if _test_name(item) not in {_LOG_CHECK, _LOG_SHUTDOWN}:
            continue
        nodeid = item.nodeid
        if not nodeid.endswith(suffix):
            item._id = NodeId.parse(f"{nodeid}{suffix}")  # noqa: SLF001


def _mysql_connection(database: str | None = None) -> pymysql.Connection:
    return pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password=MYSQL_PASSWORD, database=database, autocommit=True)


def _postgres_connection(database: str = "postgres") -> psycopg.Connection:
    return psycopg.connect(host="127.0.0.1", port=POSTGRES_PORT, user="postgres", password=MYSQL_PASSWORD, dbname=database, autocommit=True)


_SQLITE_SCRIPT = """
import json, sqlite3, sys
connection = sqlite3.connect(sys.argv[1], timeout=30)
with connection:
    rows = connection.execute(sys.argv[2], json.loads(sys.argv[3])).fetchall()
connection.close()
print(json.dumps(rows))
"""


def _sqlite_in_subprocess(path: Path, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
    """The extension bundles its own SQLite, and two SQLite copies in one process do not coordinate file locks."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-I", "-c", _SQLITE_SCRIPT, str(path), sql, json.dumps(params)],
        capture_output=True,
        check=True,
        text=True,
    )
    return [tuple(row) for row in json.loads(result.stdout)]


class Database:
    """Direct database access for tests that need to arrange a state the public API cannot reach."""

    def __init__(self, backend: str, tmp_path: Path) -> None:
        self.backend = backend
        self._path = tmp_path / "test.sqlite"
        self._name = f"dbos_t_{uuid.uuid4().hex}"
        if backend == "mysql":
            with _mysql_connection() as connection, connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {self._name}")
        elif backend == "postgres":
            with _postgres_connection() as connection:
                connection.execute(f"CREATE DATABASE {self._name}")

    @property
    def url(self) -> str:
        if self.backend == "sqlite":
            return f"sqlite://{self._path}"
        if self.backend == "postgres":
            return f"postgres://postgres:{MYSQL_PASSWORD}@127.0.0.1:{POSTGRES_PORT}/{self._name}"
        return f"mysql://root:{MYSQL_PASSWORD}@127.0.0.1:{MYSQL_PORT}/{self._name}"

    def execute(self, sql: str, *params: Any) -> list[tuple[Any, ...]]:
        if self.backend == "sqlite":
            return _sqlite_in_subprocess(self._path, sql.replace("%s", "?"), params)
        if self.backend == "postgres":
            with _postgres_connection(self._name) as connection, connection.cursor() as cursor:
                cursor.execute(sql, params or None)
                return list(cursor.fetchall()) if cursor.description else []
        with _mysql_connection(self._name) as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def expire_lease(self, workflow_id: str) -> None:
        """Make a workflow look as if its process crashed, so recovery can take it."""
        self.execute(
            "UPDATE workflow_status SET status = 'PENDING', output = NULL, error = NULL, lease_token = 'crashed', claim_after = 0 WHERE workflow_uuid = %s",
            workflow_id,
        )

    def plant_step(self, workflow_id: str, step_id: int, step_name: str) -> None:
        """Record a step behind the running workflow's back, as another writer would."""
        self.execute(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, serialization, completed_at_epoch_ms) VALUES (%s, %s, %s, 'portable_json', 0)",
            workflow_id,
            step_id,
            step_name,
        )

    def take_over(self, workflow_id: str) -> None:
        """Hand the workflow to another owner without touching when its lease expires."""
        self.execute("UPDATE workflow_status SET lease_token = 'another-execution' WHERE workflow_uuid = %s", workflow_id)

    def drop(self) -> None:
        if self.backend == "mysql":
            with _mysql_connection() as connection, connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS {self._name}")
        elif self.backend == "postgres":
            with _postgres_connection() as connection:
                connection.execute(f"DROP DATABASE IF EXISTS {self._name} WITH (FORCE)")


@pytest.fixture(params=["sqlite", "mysql", "postgres"])
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


@pytest.fixture
def gate(dbos: DBOS) -> Iterator[Callable[[], threading.Event]]:
    """Events released at teardown, so a failing test cannot leave a sync workflow thread blocking process exit."""
    events: list[threading.Event] = []

    def create() -> threading.Event:
        event = threading.Event()
        events.append(event)
        return event

    yield create
    for event in events:
        event.set()


def retry_until_success(function: Callable[[], Any], interval: float = 1.0, max_attempts: int = 10) -> Any:
    error: Exception | None = None
    for _ in range(max_attempts):
        try:
            return function()
        except Exception as caught:  # noqa: BLE001
            error = caught
            time.sleep(interval)
    assert error is not None
    raise error
