import pytest

import yado
from yado import DBOS, DBOSException, SetWorkflowID


def test_continue_as_new(dbos: DBOS) -> None:
    @DBOS.step()
    def checkpoint(value: int) -> int:
        return value

    @DBOS.workflow()
    def count(value: int) -> int:
        checkpoint(value)
        if value < 3:
            yado.continue_as_new(value + 1)
        return value

    with SetWorkflowID("continue"):
        assert count(0) == 3
    assert DBOS.retrieve_workflow("continue").get_result() == 3
    steps = DBOS.list_workflow_steps("continue")
    assert len(steps) == 1
    assert steps[0]["function_id"] == 4
    with pytest.raises(DBOSException, match="workflow"):
        yado.continue_as_new(0)


@pytest.mark.asyncio
async def test_continue_as_new_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def count(value: int) -> int:
        if value < 3:
            DBOS.continue_as_new(value + 1)
        return value

    with SetWorkflowID("async-continue"):
        assert await count(0) == 3
    assert await (await DBOS.retrieve_workflow_async("async-continue")).get_result() == 3


def test_queued_continue_as_new(dbos: DBOS) -> None:
    @DBOS.workflow()
    def work(value: int) -> int:
        if value < 2:
            DBOS.continue_as_new(value + 1)
        return value

    queue = DBOS.register_queue("continue-queue")
    handle = queue.enqueue(work, 0)
    assert handle.get_result() == 2
    assert handle.get_status().status == "SUCCESS"


def test_invalid_continuation_records_error(dbos: DBOS) -> None:
    @DBOS.workflow()
    def work(value: int) -> int:
        DBOS.continue_as_new(value, "extra")
        return value

    with SetWorkflowID("bad-continue"), pytest.raises(TypeError):
        work(1)
    assert DBOS.get_workflow_status("bad-continue").status == "ERROR"
