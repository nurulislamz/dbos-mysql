# Adapted from dbos-transact-py tests/test_workflow_management.py at commit 236142b. See MANIFEST.md.
import threading
import uuid
from datetime import datetime, timedelta

import pytest

from rust_dwos import (
    DBOS,
    DBOSAwaitedWorkflowCancelledError,
    DBOSNonExistentWorkflowError,
    SetWorkflowID,
    WorkflowStatusString,
)
from tests.conftest import retry_until_success


def test_cancel_resume(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        step_one()
        main_thread_event.set()
        workflow_event.wait()
        # A handler like this should not catch DBOSWorkflowCancelledError
        try:
            step_two()
        except Exception:
            raise
        return x

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        cancelled_handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        cancelled_handle.get_result()

    # get_result returns once the status is CANCELLED, before the cancelled run
    # reaches step_two. Wait for it to exit, or a resume could let it run step_two.
    def execution_left() -> None:
        with dbos._active_lock:
            assert all(future.done() for future in dbos._active)

    retry_until_success(execution_left, interval=0.1, max_attempts=100)
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    # The original handle should also retrieve the correct result
    assert cancelled_handle.get_result() == input

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2


def test_cancel_after_final_step(dbos: DBOS) -> None:
    # A workflow cancelled after its final step completes (but before it
    # finishes) must not be able to complete successfully. CANCELLED is terminal.
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        # The only step runs and records its output...
        step_one()
        # ...then the workflow is cancelled before it returns.
        main_thread_event.set()
        workflow_event.wait()
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        cancelled_handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()

    # The workflow must not complete successfully.
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        cancelled_handle.get_result()
    assert steps_completed == 1
    assert DBOS.get_workflow_status(wfid).status == "CANCELLED"  # type: ignore[union-attr]

    # Resuming it should let it complete successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert DBOS.get_workflow_status(wfid).status == "SUCCESS"  # type: ignore[union-attr]
    assert steps_completed == 1  # step_one was already recorded, not re-run


def test_resume_nonexistent_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    missing_id = str(uuid.uuid4())

    # Resuming a missing ID must fail, not return a handle whose get_result() polls forever
    with pytest.raises(DBOSNonExistentWorkflowError):
        DBOS.resume_workflow(missing_id)

    # Resuming a workflow that exists but already completed is still a legal no-op
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(5) == 5
    assert DBOS.resume_workflow(wfid).get_result() == 5


def test_list_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x + 1

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(1) == 2

    outputs = DBOS.list_workflows()
    assert len(outputs) == 1
    output = outputs[0]
    assert output.workflow_id == wfid
    assert output.status == "SUCCESS"
    assert output.name == simple_workflow.__qualname__
    assert output.created_at is not None and output.created_at > 0
    assert output.updated_at is not None and output.updated_at > 0
    assert output.recovery_attempts == 1
    assert output.input is not None
    assert output.output is not None

    outputs = DBOS.list_workflows(load_input=False, load_output=False)
    assert len(outputs) == 1
    output = outputs[0]
    assert output.input is None
    assert output.output is None

    outputs = DBOS.list_workflows(status="PENDING")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(status="SUCCESS")
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(status=["SUCCESS", "PENDING"])
    assert len(outputs) == 1

    outputs = DBOS.list_workflows(name="no")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(name=simple_workflow.__qualname__)
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(name=[simple_workflow.__qualname__, "no"])
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(name=["no", "also_no"])
    assert len(outputs) == 0

    outputs = DBOS.list_workflows(workflow_ids=["no"])
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(workflow_ids=[wfid, "no"])
    assert len(outputs) == 1


def test_list_workflow_limit(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    num_workflows = 5
    for i in range(num_workflows):
        with SetWorkflowID(str(i)):
            simple_workflow()

    outputs = DBOS.list_workflows()
    assert len(outputs) == num_workflows
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    outputs = DBOS.list_workflows(sort_desc=True)
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(num_workflows - i - 1)

    outputs = DBOS.list_workflows(limit=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    outputs = DBOS.list_workflows(limit=2, offset=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 2)

    outputs = DBOS.list_workflows(offset=num_workflows - 1)
    assert len(outputs) == 1
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 4)


def test_list_workflow_start_end_times(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    now = datetime.now()
    starttime = (now - timedelta(seconds=20)).isoformat()
    simple_workflow()
    endtime = datetime.now().isoformat()

    output = DBOS.list_workflows(start_time=starttime, end_time=endtime)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    newstarttime = (now - timedelta(seconds=30)).isoformat()
    newendtime = starttime

    output = DBOS.list_workflows(
        start_time=newstarttime,
        end_time=newendtime,
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"


def test_list_workflow_prefix(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    with SetWorkflowID("test1"):
        simple_workflow()
    with SetWorkflowID("test_"):
        simple_workflow()

    output = DBOS.list_workflows(workflow_id_prefix="invalid")
    assert len(output) == 0
    output = DBOS.list_workflows(workflow_id_prefix="test")
    assert len(output) == 2
    output = DBOS.list_workflows(workflow_id_prefix="test_")
    assert len(output) == 1
    output = DBOS.list_workflows(workflow_id_prefix="test1")
    assert len(output) == 1
    output = DBOS.list_workflows(workflow_id_prefix=["test1", "test_"])
    assert len(output) == 2
    output = DBOS.list_workflows(workflow_id_prefix=["test1", "invalid"])
    assert len(output) == 1


def test_list_workflows_by_parent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow(name: str) -> str:
        return f"child_{name}"

    @DBOS.workflow()
    def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        child_workflow("sync")
        handle1 = dbos.start_workflow(child_workflow, "async1")
        handle2 = dbos.start_workflow(child_workflow, "async2")
        handle1.get_result()
        handle2.get_result()
        return parent_id, handle1.workflow_id, handle2.workflow_id

    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        _, async_child1_id, async_child2_id = parent_workflow()

    sync_child_id = f"{parent_id}-1"

    sync_child_status = DBOS.get_workflow_status(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    async_child1_status = DBOS.get_workflow_status(async_child1_id)
    assert async_child1_status is not None
    assert async_child1_status.parent_workflow_id == parent_id

    async_child2_status = DBOS.get_workflow_status(async_child2_id)
    assert async_child2_status is not None
    assert async_child2_status.parent_workflow_id == parent_id

    all_workflows = DBOS.list_workflows()
    assert len(all_workflows) == 4

    children = DBOS.list_workflows(parent_workflow_id=parent_id)
    assert len(children) == 3
    for child in children:
        assert child.parent_workflow_id == parent_id
        assert child.name == child_workflow.__qualname__

    parent_status = DBOS.get_workflow_status(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    no_children = DBOS.list_workflows(parent_workflow_id="nonexistent")
    assert len(no_children) == 0

    children = DBOS.list_workflows(parent_workflow_id=[parent_id, "nonexistent"])
    assert len(children) == 3
    no_children = DBOS.list_workflows(parent_workflow_id=["nonexistent", "also_nonexistent"])
    assert len(no_children) == 0


def test_list_workflows_has_parent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow() -> None:
        return

    @DBOS.workflow()
    def parent_workflow() -> None:
        child_workflow()

    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        parent_workflow()

    standalone_id = str(uuid.uuid4())
    with SetWorkflowID(standalone_id):
        child_workflow()

    all_wfs = DBOS.list_workflows()
    assert len(all_wfs) == 3

    with_parent = DBOS.list_workflows(has_parent=True)
    assert len(with_parent) == 1
    assert with_parent[0].parent_workflow_id == parent_id

    without_parent = DBOS.list_workflows(has_parent=False)
    assert len(without_parent) == 2
    ids = {wf.workflow_id for wf in without_parent}
    assert parent_id in ids
    assert standalone_id in ids


def test_list_workflow_completed_at(dbos: DBOS) -> None:
    release = threading.Event()

    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    @DBOS.workflow()
    def failing_workflow() -> None:
        raise RuntimeError("boom")

    @DBOS.workflow()
    def pending_workflow() -> None:
        release.wait()

    before_success = datetime.now().isoformat()
    simple_workflow()
    after_success = datetime.now().isoformat()

    [success_status] = DBOS.list_workflows(name=simple_workflow.__qualname__)
    assert success_status.status == "SUCCESS"
    assert success_status.completed_at is not None
    assert success_status.completed_at >= success_status.created_at  # type: ignore[operator]

    with pytest.raises(RuntimeError):
        failing_workflow()
    [error_status] = DBOS.list_workflows(name=failing_workflow.__qualname__)
    assert error_status.status == "ERROR"
    assert error_status.completed_at is not None

    cancel_id = str(uuid.uuid4())
    with SetWorkflowID(cancel_id):
        DBOS.start_workflow(pending_workflow)
    DBOS.cancel_workflow(cancel_id)
    cancelled = DBOS.get_workflow_status(cancel_id)
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"
    assert cancelled.completed_at is not None

    resumed_handle = DBOS.resume_workflow(cancel_id)
    resumed = DBOS.get_workflow_status(cancel_id)
    assert resumed is not None
    assert resumed.completed_at is None

    in_range = DBOS.list_workflows(completed_after=before_success, completed_before=after_success)
    ids_in_range = {w.workflow_id for w in in_range}
    assert success_status.workflow_id in ids_in_range
    assert error_status.workflow_id not in ids_in_range
    assert cancel_id not in ids_in_range

    only_completed = DBOS.list_workflows(completed_after=before_success)
    completed_ids = {w.workflow_id for w in only_completed}
    assert success_status.workflow_id in completed_ids
    assert error_status.workflow_id in completed_ids
    assert cancel_id not in completed_ids

    far_past = (datetime.now() - timedelta(days=1)).isoformat()
    none_yet = DBOS.list_workflows(completed_before=far_past)
    assert len(none_yet) == 0

    release.set()
    resumed_handle.get_result()
