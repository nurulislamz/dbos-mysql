import pytest

from rust_dwos import DBOS, DBOSQueueDeduplicatedError, SetEnqueueOptions, SetWorkflowAttributes, StoreError


def test_deduplication_connects_to_queue(dbos: DBOS) -> None:
    @DBOS.workflow()
    def waiting() -> str:
        return DBOS.recv("release", 30)

    queue = DBOS.register_queue("dedup")
    with SetEnqueueOptions(deduplication_id="key"):
        first = queue.enqueue(waiting)
        with pytest.raises(DBOSQueueDeduplicatedError):
            queue.enqueue(waiting)
    with SetEnqueueOptions(deduplication_id="key", duplication_policy="return-existing"):
        assert queue.enqueue(waiting).get_workflow_id() == first.get_workflow_id()
    DBOS.send(first.get_workflow_id(), "done", "release")
    assert first.get_result() == "done"


def test_priority_option_reaches_store(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow() -> None:
        pass

    queue = DBOS.register_queue("priority")
    with SetEnqueueOptions(priority=0), pytest.raises(StoreError, match="priority"):
        queue.enqueue(workflow)


def test_delay_option_and_setter(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow() -> str:
        return "done"

    queue = DBOS.register_queue("delayed")
    with SetEnqueueOptions(delay_seconds=60):
        handle = queue.enqueue(workflow)
    assert handle.get_status().status == "DELAYED"
    DBOS.set_workflow_delay(handle.get_workflow_id(), delay_seconds=0)
    assert handle.get_result() == "done"


def test_worker_concurrency_setting(dbos: DBOS) -> None:
    queue = DBOS.register_queue("worker-limit", global_concurrency=3, worker_concurrency=1)
    assert queue.worker_concurrency == 1
    queue.set_worker_concurrency(2)
    assert queue.worker_concurrency == 2


def test_enqueue_by_name_with_options(dbos: DBOS) -> None:
    @DBOS.workflow(name="by-name")
    def workflow(value: str) -> str:
        return value

    DBOS.register_queue("named")
    handle = DBOS.enqueue_workflow_with_options({"workflow_name": "by-name", "queue_name": "named", "workflow_id": "named-id", "priority": 1}, "ok")
    assert handle.get_workflow_id() == "named-id"
    assert handle.get_result() == "ok"


def test_queue_management(dbos: DBOS) -> None:
    queue = DBOS.register_queue("management", global_concurrency=3)
    DBOS.register_queue("management", global_concurrency=4, on_conflict="never_update")
    assert queue.global_concurrency == 3
    queue.set_global_concurrency(2)
    assert queue.global_concurrency == 2
    assert "management" in [item.name for item in DBOS.list_queues()]
    DBOS.delete_queue("management")
    assert DBOS.retrieve_queue("management") is None


def test_limiter_setting(dbos: DBOS) -> None:
    queue = DBOS.register_queue("limiter", limiter={"limit": 2, "period": 30})
    assert queue.limiter == {"limit": 2, "period": 30}
    DBOS.register_queue("limiter", on_conflict="never_update")
    assert queue.limiter == {"limit": 2, "period": 30}
    queue.set_limiter(None)
    assert queue.limiter is None


def test_enqueue_partition(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow() -> str:
        return "done"

    queue = DBOS.register_queue("partitioned", partition_concurrency=1)
    with SetEnqueueOptions(queue_partition_key="customer"):
        handle = queue.enqueue(workflow)
    assert handle.get_result() == "done"
    queue.set_partition_limiter({"limit": 2, "period": 10})
    assert queue.partition_limiter == {"limit": 2, "period": 10}
    queue.set_partition_worker_concurrency(1)
    assert queue.partition_worker_concurrency == 1


def test_queued_initial_attributes(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow() -> str:
        return "done"

    queue = DBOS.register_queue("attributes")
    with SetWorkflowAttributes({"team": "queues"}), SetEnqueueOptions(delay_seconds=60):
        handle = queue.enqueue(workflow)
    assert handle.get_status().attributes == {"team": "queues"}
    DBOS.set_workflow_delay(handle.get_workflow_id(), delay_seconds=0)
    assert handle.get_result() == "done"
