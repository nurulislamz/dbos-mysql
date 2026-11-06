import asyncio
import logging

import pytest

from yado import DBOS, DBOSException


def test_run_step(dbos: DBOS) -> None:
    def add(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    def workflow() -> int:
        return DBOS.run_step({"name": "add"}, add, 2)

    assert workflow() == 3


async def test_run_step_async(dbos: DBOS) -> None:
    async def add(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    async def workflow() -> int:
        return await DBOS.run_step_async({"name": "add_async"}, add, 2)

    assert await workflow() == 3


async def test_asyncio_wait(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow() -> int:
        task = asyncio.create_task(asyncio.sleep(0, result=3))
        done, pending = await DBOS.asyncio_wait([task])
        assert not pending
        return done.pop().result()

    assert await workflow() == 3


def test_repeated_run_step_does_not_reregister(dbos: DBOS, caplog: pytest.LogCaptureFixture) -> None:
    def add(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    def workflow() -> list[int]:
        return [DBOS.run_step({"name": "add_repeated"}, add, i) for i in range(3)]

    with caplog.at_level(logging.WARNING, logger="dbos"):
        assert workflow() == [1, 2, 3]
    assert "Duplicate registration" not in caplog.text


async def test_sync_calls_refuse_a_running_loop(dbos: DBOS) -> None:
    with pytest.raises(RuntimeError, match="run_step_async"):
        DBOS.run_step(None, lambda: 1)
    with pytest.raises(RuntimeError, match="wait_first_async"):
        DBOS.wait_first([])


async def test_run_step_async_accepts_preemptible(dbos: DBOS) -> None:
    async def add(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    async def workflow() -> int:
        return await DBOS.run_step_async({"name": "add_preemptible", "preemptible": True}, add, 2)

    assert await workflow() == 3


def test_run_step_refuses_preemptible_on_sync_steps(dbos: DBOS) -> None:
    def add(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    def workflow() -> int:
        return DBOS.run_step({"preemptible": True}, add, 2)

    with pytest.raises(DBOSException, match="only supported for async"):
        workflow()
