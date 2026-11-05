import time

from rust_dwos import DBOS


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
