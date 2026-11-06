# Adapted from dbos-transact-py tests/test_workflow_management.py at commit 236142b. See MANIFEST.md.
import time
import uuid

import pytest

from yado import DBOS, DBOSException, SetWorkflowID, WorkflowHandle


def test_polling_interval_must_be_positive() -> None:
    with pytest.raises(DBOSException, match="polling_interval_sec must be positive"):
        WorkflowHandle("unused").get_result(polling_interval_sec=0)


def test_cancel_children_bulk_resume_and_poll_interval(dbos: DBOS, gate) -> None:
    hold = gate()

    @DBOS.workflow()
    def child() -> str:
        hold.wait(30)
        return "child"

    @DBOS.workflow()
    def parent() -> str:
        return child()

    @DBOS.workflow()
    def blocked() -> str:
        hold.wait(30)
        return "ran"

    parent_id = f"parent-{uuid.uuid4()}"
    with SetWorkflowID(parent_id):
        DBOS.start_workflow(parent)
    deadline = time.monotonic() + 10
    kids = []
    while time.monotonic() < deadline:
        kids = DBOS.list_workflows(parent_workflow_id=parent_id)
        if kids:
            break
        time.sleep(0.05)
    assert len(kids) == 1
    DBOS.cancel_workflow(parent_id, cancel_children=True)
    assert DBOS.get_workflow_status(parent_id).status == "CANCELLED"
    assert DBOS.get_workflow_status(kids[0].workflow_id).status == "CANCELLED"

    first = DBOS.start_workflow(blocked)
    second = DBOS.start_workflow(blocked)
    ids = [first.workflow_id, second.workflow_id]
    DBOS.cancel_workflows(ids)
    assert DBOS.get_workflow_status(first.workflow_id).status == "CANCELLED"
    resumed = DBOS.resume_workflows(ids)
    assert [handle.workflow_id for handle in resumed] == ids
    hold.set()
    assert resumed[0].get_result(polling_interval_sec=0.2) == "ran"
    assert resumed[1].get_result(polling_interval_sec=0.2) == "ran"
    assert dbos.retrieve_workflow(parent_id).get_status().status == "CANCELLED"
