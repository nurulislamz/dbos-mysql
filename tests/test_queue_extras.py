import pytest

from rust_dwos import DBOS, DBOSQueueDeduplicatedError, SetEnqueueOptions, StoreError


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
