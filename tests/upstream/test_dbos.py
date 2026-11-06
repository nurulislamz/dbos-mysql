# Adapted from dbos-transact-py tests/test_dbos.py at commit 236142b. See MANIFEST.md.
import asyncio
import logging
import time
import uuid

import pytest

from yado import DBOS, DBOSConfig, SetWorkflowID, WorkflowHandle, WorkflowStatusString
from tests.conftest import Database



def test_simple_workflow(dbos: DBOS) -> None:
    other_step_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_other_step(var2)
        res2 = test_step(var)
        DBOS.logger.info("I'm test_workflow " + var + var2)
        return res + res2

    @DBOS.step()
    def test_other_step(var2: str) -> str:
        assert DBOS.step_id == 1
        nonlocal other_step_counter
        other_step_counter += 1
        DBOS.logger.info("I'm test_other_step " + var2)
        return var2 + "1"

    @DBOS.step()
    def test_step(var: str) -> str:
        assert DBOS.step_id == 2
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == 2
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step " + var)
        return var

    assert test_workflow("bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert wf_counter == 2
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert other_step_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once
    assert wf_counter == 2  # Only increment once

    # Starting a completed workflow by ID returns its recorded result
    with SetWorkflowID(wfuuid):
        handle = DBOS.start_workflow(test_workflow, "alice", "alice")
    assert handle.get_result() == "alice1alice"
    assert wf_counter == 2


def test_simple_workflow_attempts_counter(dbos: DBOS) -> None:
    @DBOS.workflow()
    def noop() -> None:
        time.sleep(2)

    wfuuid = str(uuid.uuid4())
    for i in range(10):
        with SetWorkflowID(wfuuid):
            noop()
        result = DBOS.get_workflow_status(wfuuid)
        assert result is not None
        assert (
            result.recovery_attempts == 1
        )  # This is just runs, and does not imply recovery retries
        assert result.updated_at >= result.created_at


def test_child_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow(var: str) -> str:
        return var + "_child"

    @DBOS.workflow()
    def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        result = child_workflow("test")
        child_handle = dbos.start_workflow(child_workflow, "async")
        child_result = child_handle.get_result()
        return parent_id, child_handle.workflow_id, result + child_result

    # Run the parent workflow and get its ID
    handle = dbos.start_workflow(parent_workflow)
    parent_id, async_child_id, result = handle.get_result()

    assert result == "test_childasync_child"

    # Verify parent workflow has no parent
    parent_status = DBOS.get_workflow_status(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    # The synchronous child workflow ID follows the pattern: parent_id-function_id
    sync_child_id = f"{parent_id}-1"
    sync_child_status = DBOS.get_workflow_status(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    # The async child workflow should also have the parent ID set
    async_child_status = DBOS.get_workflow_status(async_child_id)
    assert async_child_status is not None
    assert async_child_status.parent_workflow_id == parent_id


def test_child_workflow_assigned_id(dbos: DBOS) -> None:
    other_step_counter: int = 0

    @DBOS.step()
    def test_other_step(var: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        return var + "1"

    @DBOS.workflow()
    def child_with_assigned_id(var: str) -> str:
        assert DBOS.workflow_id == "assigned_child_id"
        return test_other_step(var)

    @DBOS.workflow()
    def parent_with_assigned_child() -> str:
        with SetWorkflowID("assigned_child_id"):
            res1 = child_with_assigned_id("first")
        # Call again with same ID - should return cached result
        with SetWorkflowID("assigned_child_id"):
            handle = dbos.start_workflow(child_with_assigned_id, "second")
            res2 = handle.get_result()
        return res1 + res2

    # Run twice - the step should only execute once due to idempotency
    original_parent_id = str(uuid.uuid4())
    with SetWorkflowID(original_parent_id):
        assert parent_with_assigned_child() == "first1first1"
    assert parent_with_assigned_child() == "first1first1"
    assert other_step_counter == 1  # The step only ran once

    # Verify the assigned child has parent_workflow_id set
    child_status = DBOS.get_workflow_status("assigned_child_id")
    assert child_status is not None
    assert child_status.parent_workflow_id == original_parent_id


def test_exception_workflow(dbos: DBOS) -> None:
    other_step_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.step()
    def exception_other_step(var: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        raise Exception(var)

    @DBOS.step()
    def exception_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        raise Exception(var)

    @DBOS.workflow()
    def exception_workflow() -> None:
        nonlocal wf_counter
        wf_counter += 1
        err1 = None
        err2 = None
        try:
            exception_other_step("test error")
        except Exception as e:
            err1 = e

        try:
            exception_step("test error")
        except Exception as e:
            err2 = e
        assert err1 is not None and err2 is not None
        assert str(err1) == str(err2)

        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow()

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)
    assert other_step_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once

    # Starting a failed workflow by ID surfaces its recorded error, not a new run
    with SetWorkflowID(wfuuid):
        handle = DBOS.start_workflow(exception_workflow)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "test error" == str(exc_info.value)
    assert wf_counter == 2  # The workflow error is directly returned without running


def test_recovery_workflow(dbos: DBOS, database: Database) -> None:
    other_step_counter: int = 0
    other_step_return_none_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_other_step(var2)
        should_be_none = test_other_step_return_none()
        assert should_be_none is None
        return res + var

    @DBOS.step()
    def test_other_step(var2: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        return var2 + "1"

    @DBOS.step()
    def test_other_step_return_none() -> None:
        nonlocal other_step_return_none_counter
        other_step_return_none_counter += 1
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"

    # Change the workflow status to pending
    database.expire_lease(wfuuid)

    # Recovery should execute the workflow again but skip the step
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1bob"
    assert wf_counter == 2
    assert other_step_counter == 1
    assert other_step_return_none_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2  # original attempt + recovery attempt


def test_recovery_workflow_step(dbos: DBOS, database: Database) -> None:
    step_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        should_be_none = test_step(var2)
        assert should_be_none is None
        return var

    @DBOS.step()
    def test_step(var2: str) -> None:
        nonlocal step_counter
        step_counter += 1
        print(f"I'm a test_step {var2}!")
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob"

    # Change the workflow status to pending
    database.expire_lease(wfuuid)

    # Recovery should execute the workflow again but skip the step
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob"
    assert wf_counter == 2
    assert step_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2


def test_workflow_returns_none(dbos: DBOS, database: Database) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> None:
        nonlocal wf_counter
        wf_counter += 1
        assert var == var2 == "bob"
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 1

    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 1

    handle: WorkflowHandle[None] = DBOS.retrieve_workflow(wfuuid)
    assert handle.get_result() == None
    assert wf_counter == 1

    # Change the workflow status to pending
    database.expire_lease(wfuuid)

    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() is None
    assert wf_counter == 2

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert (
        stat.recovery_attempts == 2
    )  # 1 actual call to test_workflow + 1 recovery attempt


def test_start_workflow(dbos: DBOS) -> None:
    other_step_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_other_step(var2)
        return res + var

    @DBOS.step()
    def test_other_step(var2: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        return var2 + "1"

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        assert DBOS.workflow_id is None
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        assert DBOS.workflow_id is None
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"
        assert DBOS.workflow_id is None
    assert other_step_counter == 1
    assert wf_counter == 1


def test_retrieve_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        time.sleep(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.workflow()
    def test_sleep_workthrow(secs: float) -> str:
        time.sleep(secs)
        raise Exception("Wake Up!")

    @DBOS.workflow()
    def test_workflow(x: int) -> int:
        return x

    dest_uuid = "aaaa"
    with pytest.raises(Exception) as exc_info:
        dbos.retrieve_workflow(dest_uuid)
    pattern = f"Non-existent target workflow ID: {dest_uuid}"
    assert pattern in str(exc_info.value)

    # These return
    sleep_wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)

    sleep_pwfh: WorkflowHandle[str] = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id
    dbos.logger.info(f"UUID: {sleep_pwfh.get_workflow_id()}")
    hres = sleep_pwfh.get_result()
    assert hres == sleep_pwfh.get_workflow_id()
    dbos.logger.info(f"RES: {hres}")
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    assert sleep_wfh.get_result() == sleep_wfh.get_workflow_id()
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)
    assert istat.output == sleep_wfh.get_workflow_id()

    # These throw
    sleep_wfh = dbos.start_workflow(test_sleep_workthrow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)
    sleep_pwfh = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id

    with pytest.raises(Exception) as exc_info:
        sleep_pwfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)
    assert istat.output is None
    assert istat.error is not None
    assert isinstance(istat.error, Exception)
    assert str(istat.error) == "Wake Up!"

    with pytest.raises(Exception) as exc_info:
        sleep_wfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)

    # Validate the status properly stores the output of the workflow
    x = 5
    handle = DBOS.start_workflow(test_workflow, x)
    assert handle.get_result() == x
    retrieved_handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        handle.get_workflow_id()
    )
    assert retrieved_handle.get_result() == x
    assert retrieved_handle.get_status().output == x
    assert retrieved_handle.get_status().error is None


def test_duplicate_registration(
    dbos: DBOS,
    caplog: pytest.LogCaptureFixture,
    config: DBOSConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caplog.set_level(logging.WARNING, "dbos")
    monkeypatch.setattr(logging.getLogger("dbos"), "propagate", True)

    @DBOS.step()
    def my_first_step() -> None:
        pass

    @DBOS.step()
    def my_first_step() -> None:
        pass

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_first_step'"
        in caplog.text
    )

    @DBOS.step()
    def my_step() -> None:
        pass

    @DBOS.step()
    def my_step() -> None:
        pass

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_step'"
        in caplog.text
    )

    @DBOS.workflow()
    def my_workflow() -> None:
        my_step()
        my_first_step()

    @DBOS.workflow()
    def my_workflow() -> None:
        my_step()
        my_first_step()

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_workflow'"
        in caplog.text
    )

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()


def test_custom_names(dbos: DBOS) -> None:
    workflow_name = "workflow_name"
    step_name = "step_name"

    @DBOS.workflow(name=workflow_name)
    def workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    handle = DBOS.start_workflow(workflow)
    assert handle.get_status().name == workflow_name
    assert handle.get_result() == handle.workflow_id
    assert DBOS.get_result(handle.workflow_id) == handle.workflow_id

    @DBOS.step(name=step_name)
    def step() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    handle = DBOS.start_workflow(step)
    assert handle.get_status().name == f"<temp>.{step_name}"
    assert handle.get_result() == handle.workflow_id

    # Verify we can declare another workflow with the same function name
    # but a different custom name

    another_workflow = "another_workflow"

    @DBOS.workflow(name=another_workflow)
    def workflow(x: int) -> int:
        return x

    value = 5
    handle = DBOS.start_workflow(workflow, value)  # type: ignore
    assert handle.get_status().name == another_workflow
    assert handle.get_result() == value  # type: ignore


@pytest.mark.asyncio
async def test_step_without_dbos(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)

    @DBOS.step()
    def step(x: int) -> int:
        assert DBOS.workflow_id is None
        return x

    @DBOS.step()
    async def async_step(x: int) -> int:
        assert DBOS.workflow_id is None
        return x

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    DBOS(config=config)

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    DBOS.launch()

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    assert len(await DBOS.list_workflows_async()) == 0


def test_nested_steps(dbos: DBOS) -> None:

    @DBOS.step()
    def outer_step() -> str:
        return inner_step()

    @DBOS.step()
    def inner_step() -> str:
        id = DBOS.workflow_id
        assert id is not None
        return id

    @DBOS.workflow()
    def workflow() -> str:
        return outer_step()

    id = workflow()
    steps = DBOS.list_workflow_steps(id)
    assert len(steps) == 1
    assert steps[0]["function_name"] == outer_step.__qualname__
