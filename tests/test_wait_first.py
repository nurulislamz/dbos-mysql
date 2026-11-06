import pytest

from yado import DBOS, SetWorkflowID


def test_wait_first(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> int:
        return 3

    @DBOS.workflow()
    def parent() -> str:
        handle = DBOS.start_workflow(child)
        assert DBOS.wait_first([handle]) is handle
        return handle.get_workflow_id()

    with SetWorkflowID("parent"):
        assert parent()
    with pytest.raises(ValueError):
        DBOS.wait_first([])


async def test_wait_first_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child() -> int:
        return 3

    @DBOS.workflow()
    async def parent() -> int:
        handle = await DBOS.start_workflow_async(child)
        assert await DBOS.wait_first_async([handle]) is handle
        with pytest.raises(ValueError):
            await DBOS.wait_first_async([handle, handle])
        return await handle.get_result()

    assert await parent() == 3
