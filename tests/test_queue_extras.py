import pytest

from rust_dwos import DBOS, DBOSQueueDeduplicatedError, SetEnqueueOptions


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
