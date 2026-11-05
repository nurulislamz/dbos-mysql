"""The boundary between Python and Rust: errors, logs, cancellation and the event loop."""

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pymysql
import pytest

from rust_dwos import StoreError, _native

MYSQL_PORT = int(os.environ.get("DBOS_TEST_DB_PORT", "13307"))
MYSQL_PASSWORD = "dbos-test-root"
BRIDGE_DATABASE = f"dbos_bridge_{os.environ.get('PYTEST_XDIST_WORKER', 'master')}"
MYSQL_URL = os.environ.get(
    "DBOS_TEST_MYSQL_URL",
    f"mysql://root:{MYSQL_PASSWORD}@127.0.0.1:{MYSQL_PORT}/{BRIDGE_DATABASE}",
)


@pytest.fixture
async def engine() -> AsyncIterator[_native.Engine]:
    with pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password=MYSQL_PASSWORD, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {BRIDGE_DATABASE}")
        cursor.execute(f"CREATE DATABASE {BRIDGE_DATABASE}")
    connected = await _native.Engine.connect(MYSQL_URL)
    try:
        yield connected
    finally:
        connected.close()


@pytest.fixture
def row_lock() -> Iterator[pymysql.Connection]:
    connection = pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password=MYSQL_PASSWORD, database=BRIDGE_DATABASE)
    try:
        yield connection
    finally:
        connection.close()


async def _hold_workflow_row(engine: _native.Engine, connection: pymysql.Connection) -> _native.Run:
    """Lock a workflow's row, so that saving a step waits inside Rust."""
    begun = await engine.begin("wf", "{}", 100, "bridge")
    assert begun.run is not None
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM workflow_status WHERE workflow_uuid = 'bridge' FOR UPDATE")
    return begun.run


async def test_cancelling_a_python_task_rolls_back_the_rust_transaction(engine: _native.Engine, row_lock: pymysql.Connection) -> None:
    run = await _hold_workflow_row(engine, row_lock)
    task = asyncio.ensure_future(engine.save_step_output(run, 1, "step", "1"))
    await asyncio.sleep(0.3)
    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    row_lock.rollback()

    assert await engine.list_steps("bridge") == []
    for _ in range(100):
        with row_lock.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.innodb_trx t "
                "JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id WHERE p.db = %s",
                (BRIDGE_DATABASE,),
            )
            open_transactions = cursor.fetchone()[0]
        row_lock.rollback()
        if open_transactions == 0:
            break
        await asyncio.sleep(0.02)
    assert open_transactions == 0


async def test_event_loop_stays_free_during_a_rust_await(engine: _native.Engine, row_lock: pymysql.Connection) -> None:
    run = await _hold_workflow_row(engine, row_lock)
    task = asyncio.ensure_future(engine.save_step_output(run, 1, "step", "1"))
    asyncio.get_running_loop().call_later(0.3, row_lock.rollback)
    ticks = 0
    while not task.done():
        await asyncio.sleep(0.01)
        ticks += 1
    await task
    assert ticks > 10


async def test_rust_logs_reach_python_logging(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="dwos_core"):
        await _native.Engine.connect(f"sqlite://{tmp_path / 'logs.sqlite'}")
    assert "connected to sqlite" in caplog.text


async def test_unsupported_url_is_a_python_exception() -> None:
    with pytest.raises(StoreError, match="unsupported database URL"):
        await _native.Engine.connect("mongodb://localhost/app")


async def test_password_is_not_in_connection_errors() -> None:
    bad_url = MYSQL_URL.replace(MYSQL_PASSWORD, "not-the-real-secret")
    with pytest.raises(StoreError) as error:
        await _native.Engine.connect(bad_url)
    assert "not-the-real-secret" not in str(error.value)
