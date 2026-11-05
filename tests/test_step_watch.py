"""The SDK calls for step attempt limits reach Rust and succeed. The rules are tested in dwos-core."""

import asyncio
import threading

import pytest

from rust_dwos import DBOS, DBOSStepTimeoutError


def test_sync_step_with_limits(dbos: DBOS) -> None:
    @DBOS.step(timeout_seconds=60, heartbeat_timeout_seconds=60)
    def step() -> int:
        DBOS.heartbeat()
        return threading.get_ident()

    @DBOS.workflow()
    def workflow() -> int:
        return step()

    assert workflow() != threading.get_ident()


def test_sync_step_expiry_reaches_the_body(dbos: DBOS) -> None:
    release = threading.Event()

    @DBOS.step(heartbeat_timeout_seconds=0.05)
    def silent() -> None:
        release.wait(60)

    @DBOS.workflow()
    def workflow() -> str:
        try:
            silent()
        except DBOSStepTimeoutError as error:
            return error.reason
        return "finished"

    try:
        assert workflow() == "missed_heartbeat"
    finally:
        release.set()


async def test_async_step_with_limits(dbos: DBOS) -> None:
    @DBOS.step(timeout_seconds=60, heartbeat_timeout_seconds=60)
    async def step() -> int:
        DBOS.heartbeat()
        return 1

    @DBOS.workflow()
    async def workflow() -> int:
        return await step()

    assert await workflow() == 1


async def test_async_step_expiry_reaches_the_body(dbos: DBOS) -> None:
    @DBOS.step(timeout_seconds=0.05)
    async def silent() -> None:
        await asyncio.sleep(60)

    @DBOS.workflow()
    async def workflow() -> str:
        try:
            await silent()
        except DBOSStepTimeoutError as error:
            return error.reason
        return "finished"

    assert await workflow() == "deadline"


def test_a_step_without_limits_stays_on_the_callers_thread(dbos: DBOS) -> None:
    @DBOS.step()
    def step() -> int:
        DBOS.heartbeat()
        return threading.get_ident()

    @DBOS.workflow()
    def workflow() -> int:
        return step()

    assert workflow() == threading.get_ident()


def test_heartbeat_outside_a_step_raises(dbos: DBOS) -> None:
    with pytest.raises(Exception, match="inside a step"):
        DBOS.heartbeat()


async def test_async_sleep_reaches_the_core(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow() -> int:
        await DBOS.sleep_async(0.001)
        return 1

    assert await workflow() == 1
