import asyncio
from collections.abc import Iterator

import pytest

from tests.conftest import Database
from rust_dwos import DBOS, DBOSClient, DBOSException, DBOSNonExistentWorkflowError, EnqueueOptions


@pytest.fixture
def client(dbos: DBOS, database: Database) -> Iterator[DBOSClient]:
    instance = DBOSClient(system_database_url=database.url)
    yield instance
    instance.destroy()


def test_client_enqueues_messages_and_manages_workflows(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow(name="client_target")
    def target(prefix: str) -> str:
        DBOS.set_event("ready", prefix)
        return prefix + str(DBOS.recv("go", timeout_seconds=30))

    DBOS.register_queue("client_queue")
    options: EnqueueOptions = {"queue_name": "client_queue", "workflow_name": "client_target", "workflow_id": "client-wf", "attributes": {"team": "a"}}
    handle = client.enqueue(options, "n=")
    assert client.get_event("client-wf", "ready", timeout_seconds=30) == "n="
    client.send("client-wf", 7, "go", "once")
    assert handle.get_result() == "n=7"
    assert handle.get_status().status == "SUCCESS"
    assert [s.workflow_id for s in client.list_workflows(queue_name="client_queue")] == ["client-wf"]
    assert client.list_workflows(workflow_ids=["client-wf"])[0].attributes == {"team": "a"}
    assert len(client.list_workflow_steps("client-wf")) >= 2
    assert client.retrieve_workflow("client-wf").get_result() == "n=7"
    with pytest.raises(DBOSNonExistentWorkflowError):
        client.retrieve_workflow("missing")
    assert [q.name for q in client.list_queues()] == ["client_queue"]


def test_client_cancel_and_resume_hand_work_to_the_app(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow(name="client_waiter")
    def waiter() -> str:
        return str(DBOS.recv("go", timeout_seconds=30))

    client.register_queue("client_queue")
    DBOS.register_queue("client_queue")
    handle = client.enqueue({"queue_name": "client_queue", "workflow_name": "client_waiter", "workflow_id": "wait-wf"})
    client.cancel_workflow("wait-wf")
    assert handle.get_status().status == "CANCELLED"
    client.send("wait-wf", "done", "go")
    assert client.resume_workflow("wait-wf", queue_name="client_queue").get_result() == "done"


def test_client_async_calls(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow(name="client_double")
    def double(value: int) -> int:
        return value * 2

    DBOS.register_queue("client_queue")

    async def run() -> int:
        handle = await client.enqueue_async({"queue_name": "client_queue", "workflow_name": "client_double"}, 21)
        return await handle.get_result()

    assert asyncio.run(run()) == 42


def test_client_does_not_migrate(database: Database) -> None:
    client = DBOSClient(system_database_url=database.url)
    try:
        with pytest.raises(Exception):  # noqa: B017, PT011 - no tables exist, so any read fails
            client.list_workflows()
    finally:
        client.destroy()


def test_client_works_without_a_dbos_process(config: dict, database: Database) -> None:
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)  # type: ignore[arg-type]
    DBOS.launch()  # Only to migrate the schema.
    DBOS.destroy(destroy_registry=True)
    client = DBOSClient(system_database_url=database.url)
    try:
        client.register_queue("alone")
        handle = client.enqueue({"queue_name": "alone", "workflow_name": "nobody_runs_this", "workflow_id": "alone-wf"}, {"a": 1})
        assert handle.get_status().status == "ENQUEUED"
        assert client.list_queued_workflows()[0].workflow_id == "alone-wf"
        assert client.get_event("alone-wf", "never", timeout_seconds=0) is None
    finally:
        client.destroy()


def test_client_refuses_options_dbos_rejects(client: DBOSClient) -> None:
    with pytest.raises(DBOSException):
        client.register_queue("q", on_conflict="update_if_latest_version")
    with pytest.raises(DBOSException):
        client.enqueue({"queue_name": "q", "workflow_name": "w", "authenticated_user": "u"})  # type: ignore[typeddict-unknown-key]
