import asyncio

from rust_dwos import DBOS


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
