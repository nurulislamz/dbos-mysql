import time

import pytest

from yado import DBOS, DBOSConfig
from yado.errors import DBOSException


def test_delete_calls_reach_the_core(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> str:
        return "done"

    first = DBOS.start_workflow(wf)
    second = DBOS.start_workflow(wf)
    assert first.get_result() == second.get_result() == "done"
    assert DBOS.delete_workflow(first.workflow_id) == [first.workflow_id]
    assert DBOS.delete_finished_workflows(int(time.time() * 1000) + 60_000) == 1
    assert DBOS.list_workflows() == []


async def test_async_delete_calls_reach_the_core(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def wf() -> str:
        return "done"

    first = await DBOS.start_workflow_async(wf)
    second = await DBOS.start_workflow_async(wf)
    assert await first.get_result() == await second.get_result() == "done"
    assert await DBOS.delete_workflow_async(first.workflow_id) == [first.workflow_id]
    assert await DBOS.delete_finished_workflows_async(int(time.time() * 1000) + 60_000) == 1


def test_the_retention_policy_deletes_finished_workflows_in_the_background(config: DBOSConfig) -> None:
    with pytest.raises(DBOSException):
        DBOS(config={**config, "retention_rows": 0})
    DBOS.destroy(destroy_registry=True)
    DBOS(config={**config, "retention_hours": 0.000001, "retention_interval_seconds": 0.05})
    DBOS.launch()
    try:

        @DBOS.workflow()
        def wf() -> str:
            return "done"

        handle = DBOS.start_workflow(wf)
        assert handle.get_result() == "done"
        deadline = time.monotonic() + 20
        while DBOS.get_workflow_status(handle.workflow_id) is not None:
            assert time.monotonic() < deadline, "the retention policy never deleted the workflow"
            time.sleep(0.05)
    finally:
        DBOS.destroy(destroy_registry=True)
