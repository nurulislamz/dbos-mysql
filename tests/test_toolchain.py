import asyncio
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

import dbos_mysql

MYSQL_URL = os.environ.get(
    "DBOS_TEST_MYSQL_URL",
    f"mysql://root:dbos-test-root@127.0.0.1:{os.environ.get('DBOS_TEST_DB_PORT', '13307')}/dbos_test",
)


@pytest.fixture(params=["mysql", "sqlite"])
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[dbos_mysql.Store]:
    url = MYSQL_URL if request.param == "mysql" else f"sqlite://{tmp_path / 'test.sqlite'}"
    connected = await dbos_mysql.connect(url)
    await connected.spike_reset()
    yield connected


async def wait_until_idle(store: dbos_mysql.Store) -> None:
    for _ in range(200):
        if store.connections_in_use() == 0:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{store.connections_in_use()} connections still in use")


async def test_ping(store: dbos_mysql.Store) -> None:
    assert await store.ping() == 1


async def test_session_settings(store: dbos_mysql.Store) -> None:
    settings = await store.session_settings()
    if store.backend == "mysql":
        assert settings == ["READ-COMMITTED", "+00:00"]
    else:
        assert settings == ["wal", "2", "1"]


async def test_finished_transaction_commits(store: dbos_mysql.Store) -> None:
    await store.spike_hold_transaction(0)
    assert await store.spike_rows() == 1


async def test_cancelling_python_task_rolls_back_rust_transaction(store: dbos_mysql.Store) -> None:
    task = asyncio.ensure_future(store.spike_hold_transaction(30))
    await asyncio.sleep(0.5)
    assert store.connections_in_use() == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await wait_until_idle(store)
    assert await store.spike_rows() == 0
    if store.backend == "mysql":
        assert await store.spike_open_transactions() == 0


async def test_event_loop_stays_free_during_rust_await(store: dbos_mysql.Store) -> None:
    task = asyncio.ensure_future(store.spike_hold_transaction(0.3))
    ticks = 0
    while not task.done():
        await asyncio.sleep(0.01)
        ticks += 1
    await task
    assert ticks > 10


async def test_rust_logs_reach_python_logging(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="dbos_mysql_core"):
        await dbos_mysql.connect(f"sqlite://{tmp_path / 'logs.sqlite'}")
    assert "connected to sqlite" in caplog.text


async def test_unsupported_url_is_a_python_exception() -> None:
    with pytest.raises(dbos_mysql.StoreError, match="unsupported database URL"):
        await dbos_mysql.connect("postgresql://localhost/app")


async def test_password_is_not_in_connection_errors() -> None:
    bad_url = MYSQL_URL.replace("dbos-test-root", "not-the-real-secret")
    with pytest.raises(dbos_mysql.StoreError) as error:
        await dbos_mysql.connect(bad_url)
    assert "not-the-real-secret" not in str(error.value)
