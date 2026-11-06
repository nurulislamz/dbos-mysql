import asyncio

from rust_dwos import DBOS, DBOSClient, SetWorkflowID

from tests.conftest import Database

calls: list[int] = []


def test_fork_rewind_and_send_to_forks_reach_the_core(dbos: DBOS) -> None:
    calls.clear()

    @DBOS.step()
    def step(n: int) -> int:
        calls.append(n)
        return n

    @DBOS.workflow()
    def wf() -> int:
        return step(1) + step(2) + len(DBOS.list_workflows(forked_from=DBOS.workflow_id))

    with SetWorkflowID("orig"):
        assert wf() == 3
    with SetWorkflowID("fork"):
        fork = DBOS.fork_workflow("orig", 2)
    assert fork.workflow_id == "fork"
    assert fork.get_result() == 3
    assert calls == [1, 2, 2]
    status = DBOS.get_workflow_status("fork")
    assert status is not None and status.forked_from == "orig"
    original = DBOS.get_workflow_status("orig")
    assert original is not None and original.was_forked_from

    assert DBOS.rewind_workflow("orig").get_result() == 4
    assert calls == [1, 2, 2, 1, 2]

    DBOS.send("orig", "hi", "t", send_to_forks=True)
    assert len(DBOS.list_workflows(forked_from="orig")) == 1


async def test_async_fork_and_rewind_reach_the_core(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def wf() -> str:
        return "done"

    handle = await DBOS.start_workflow_async(wf)
    assert await handle.get_result() == "done"
    fork = await DBOS.fork_workflow_async(handle.workflow_id, 1)
    assert await fork.get_result() == "done"
    rewound = await DBOS.rewind_workflow_async(handle.workflow_id)
    assert await rewound.get_result() == "done"
    await DBOS.send_async(handle.workflow_id, "hi", send_to_forks=True)


def test_list_fork_filters(dbos: DBOS, database: Database) -> None:
    @DBOS.workflow()
    def wf() -> str:
        return "done"

    with SetWorkflowID("original"):
        assert wf() == "done"
    with SetWorkflowID("fork"):
        assert DBOS.fork_workflow("original", 1).get_result() == "done"

    client = DBOSClient(system_database_url=database.url)
    try:
        for list_workflows in (DBOS.list_workflows, client.list_workflows):
            assert [row.workflow_id for row in list_workflows(is_fork=True)] == ["fork"]
            assert [row.workflow_id for row in list_workflows(was_forked_from=True)] == ["original"]
            assert [row.workflow_id for row in list_workflows(is_fork=False, was_forked_from=True)] == ["original"]
        async def check_async() -> None:
            for list_workflows in (DBOS.list_workflows_async, client.list_workflows_async):
                assert [row.workflow_id for row in await list_workflows(is_fork=True, was_forked_from=False)] == ["fork"]
                assert await list_workflows(is_fork=True, was_forked_from=True) == []

        asyncio.run(check_async())
    finally:
        client.destroy()
