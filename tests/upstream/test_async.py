# Adapted from dbos-transact-py tests/test_async.py at commit 236142b. See MANIFEST.md.
import asyncio
import functools
import logging
import threading
import time
import uuid
from collections.abc import Generator
from typing import Any

import pytest

from dbos_mysql import (
    DBOS,
    DBOSConfig,
    DBOSMaxStepRetriesExceeded,
    DBOSUnexpectedStepError,
    MaxRecoveryAttemptsExceededError,
    SetWorkflowID,
    StepStatus,
    WorkflowHandle,
    WorkflowHandleAsync,
    WorkflowStatusString,
)
from tests.conftest import Database, retry_until_success



@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    other_step_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = await asyncio.to_thread(test_other_step, var1)
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.step()
    def test_other_step(var: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        DBOS.logger.info("I'm test_other_step")
        return var + f"other{other_step_counter}1"

    wfuuid = f"test_async_workflow-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "aliceother11bobstep1"

    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "aliceother11bobstep1"

    assert wf_counter == 1  # Completed replay does not re-run the body (#762)
    assert step_counter == 1
    assert other_step_counter == 1

    # Test DBOS.start_workflow_async
    handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")
    assert (await handle.get_result()) == "aliceother21bobstep2"

    # Test DBOS.start_workflow. Not recommended for async workflows,
    # but needed for backwards compatibility.
    def fn() -> None:
        sync_handle = DBOS.start_workflow(test_workflow, "alice", "bob")
        assert sync_handle.get_result() == "aliceother31bobstep3"  # type: ignore

    await asyncio.to_thread(fn)

    # Test DBOS.start_workflow_async on steps
    handle = await DBOS.start_workflow_async(test_step, "alice")
    assert (await handle.get_result()) == "alicestep4"


@pytest.mark.asyncio
async def test_async_step(dbos: DBOS) -> None:
    other_step_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = await asyncio.to_thread(test_other_step, var1)
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        await asyncio.sleep(0.1)
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.step()
    def test_other_step(var: str) -> str:
        nonlocal other_step_counter
        other_step_counter += 1
        DBOS.logger.info("I'm test_other_step")
        return var + f"other{other_step_counter}1"

    wfuuid = f"test_async_step-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "aliceother11bobstep1"

    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "aliceother11bobstep1"

    assert wf_counter == 1  # Completed replay does not re-run the body (#762)
    assert step_counter == 1
    assert other_step_counter == 1


@pytest.mark.asyncio
async def test_async_completed_replay_no_hook_rerun(dbos: DBOS) -> None:
    # A completed async workflow returns its recorded result without re-running its body or wrapping decorators (#762).
    hook_counter: int = 0
    body_counter: int = 0
    step_counter: int = 0

    def app_hook(func: Any) -> Any:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal hook_counter
            hook_counter += 1
            return await func(*args, **kwargs)

        return wrapper

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + ":step"

    @DBOS.workflow()
    @app_hook
    async def test_workflow(var: str) -> str:
        nonlocal body_counter
        body_counter += 1
        return await test_step(var)

    wfuuid = f"test_async_completed_replay-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        assert await test_workflow("direct") == "direct:step"
    assert (hook_counter, body_counter, step_counter) == (1, 1, 1)

    # Completed replay: the stored result is returned, nothing is re-executed.
    with SetWorkflowID(wfuuid):
        assert await test_workflow("mutated") == "direct:step"
    assert (hook_counter, body_counter, step_counter) == (1, 1, 1)


@pytest.mark.asyncio
async def test_start_workflow_async(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0
    wfuuid = f"test_start_workflow_async-{time.time_ns()}"
    wf_el_id: int = 0
    step_el_id: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_el_id
        wf_el_id = id(asyncio.get_running_loop())
        nonlocal wf_counter
        wf_counter += 1
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return var1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_el_id
        step_el_id = id(asyncio.get_running_loop())
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    with SetWorkflowID(wfuuid):
        handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    assert handle.get_workflow_id() == wfuuid
    result = await handle.get_result()
    assert result == "alicebobstep1"

    with SetWorkflowID(wfuuid):
        handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    assert handle.get_workflow_id() == wfuuid
    result = cast(str, await DBOS.get_result_async(wfuuid))
    assert result == "alicebobstep1"

    assert wf_counter == 1
    assert step_counter == 1
    assert wf_el_id == id(asyncio.get_running_loop())
    assert step_el_id == id(asyncio.get_running_loop())


@pytest.mark.asyncio
async def test_retrieve_workflow_async(dbos: DBOS) -> None:
    wfuuid = f"test_retrieve_workflow_async-{time.time_ns()}"

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        var1 = await test_step(var1)
        await DBOS.sleep_async(5)
        return var1 + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        return var + "d"

    with SetWorkflowID(wfuuid):
        await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    handle: WorkflowHandleAsync[str] = await DBOS.retrieve_workflow_async(wfuuid)
    assert handle.get_workflow_id() == wfuuid
    result = await handle.get_result()
    assert result == "alicedbob"
    wfstatus = await handle.get_status()
    assert wfstatus.status == "SUCCESS"
    assert wfstatus.workflow_id == wfuuid


def test_unawaited_workflow(dbos: DBOS) -> None:
    input = 5
    child_id = str(uuid.uuid4())
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    async def child_workflow(x: int) -> int:
        await asyncio.sleep(0.1)
        return x

    @DBOS.workflow()
    async def parent_workflow(x: int) -> None:
        with SetWorkflowID(child_id):
            await DBOS.start_workflow_async(child_workflow, x)

    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    assert handle.get_result() == 5


def test_unawaited_workflow_exception(dbos: DBOS) -> None:
    child_id = str(uuid.uuid4())
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    async def child_workflow(s: str) -> int:
        await asyncio.sleep(0.1)
        raise Exception(s)

    @DBOS.workflow()
    async def parent_workflow(s: str) -> None:
        with SetWorkflowID(child_id):
            await DBOS.start_workflow_async(child_workflow, s)

    # Verify the unawaited child properly throws an exception
    input = "alice"
    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert input in str(exc_info.value)

    # Verify it works if run again
    input = "bob"
    child_id = str(uuid.uuid4())
    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
    handle = DBOS.retrieve_workflow(child_id, existing_workflow=False)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert input in str(exc_info.value)


@pytest.mark.asyncio
async def test_main_loop(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    await DBOS.register_queue_async("queue")

    @DBOS.workflow()
    async def test_workflow() -> int:
        await DBOS.sleep_async(0.1)
        return id(asyncio.get_running_loop())

    # Verify the enqueued task is submitted into the main event loop
    handle = await DBOS.enqueue_workflow_async("queue", test_workflow)
    assert await handle.get_result() == id(asyncio.get_running_loop())


@pytest.mark.asyncio
async def test_workflow_with_task_cancellation(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_workflow(duration: float) -> str:
        await DBOS.sleep_async(duration)
        return "completed"

    # Run the workflow in an asyncio task
    wfid = str(uuid.uuid4())
    event = asyncio.Event()

    async def run_workflow_task() -> str:
        with SetWorkflowID(wfid):
            handle = await DBOS.start_workflow_async(test_workflow, 1.0)
        event.set()
        return await handle.get_result()

    task = asyncio.create_task(run_workflow_task())

    # Wait for the workflow to start
    await event.wait()

    # Cancel the task
    task.cancel()

    # Verify the task was cancelled
    with pytest.raises(asyncio.CancelledError):
        await task

    # Verify the workflow completes despite the task cancellation
    handle: WorkflowHandleAsync[str] = await DBOS.retrieve_workflow_async(wfid)
    assert await handle.get_result() == "completed"


@pytest.mark.asyncio
async def test_child_workflow_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child_workflow(var: str) -> str:
        return var + "_child"

    @DBOS.workflow()
    async def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        result = await child_workflow("test")
        child_handle = await dbos.start_workflow_async(child_workflow, "async")
        child_result = await child_handle.get_result()
        return parent_id, child_handle.workflow_id, result + child_result

    # Run the parent workflow and get its ID
    handle = await dbos.start_workflow_async(parent_workflow)
    parent_id, async_child_id, result = await handle.get_result()

    assert result == "test_childasync_child"

    # Verify parent workflow has no parent
    parent_status = await DBOS.get_workflow_status_async(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    # The synchronous child workflow ID follows the pattern: parent_id-function_id
    sync_child_id = f"{parent_id}-1"
    sync_child_status = await DBOS.get_workflow_status_async(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    # The async child workflow should also have the parent ID set
    async_child_status = await DBOS.get_workflow_status_async(async_child_id)
    assert async_child_status is not None
    assert async_child_status.parent_workflow_id == parent_id


@pytest.mark.asyncio
async def test_asyncio_wait(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def fast_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return val + "_done"

    @DBOS.step()
    async def slow_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val + "_done"

    @DBOS.workflow()
    async def wait_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [fast_step("fast"), slow_step("slow")],
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert len(done) == 1
        assert len(pending) == 1
        assert [t.result() for t in done] == ["fast_done"]

        # Let the slow step finish and wait for it
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 1
        assert len(pending2) == 0
        assert [t.result() for t in done2] == ["slow_done"]

    handle = await DBOS.start_workflow_async(wait_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Verify recorded steps
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert len(steps) == 4
    # Step 1: first asyncio_wait snapshots its context before the tasks run.
    # Recorded done indices [0] means the first future (fast_step) completed.
    assert steps[0]["function_id"] == 1
    assert steps[0]["function_name"] == "DBOS.asyncio_wait"
    assert steps[0]["output"] == [0]
    # Steps 2 & 3: the step coroutines execute inside the asyncio tasks
    assert steps[1]["function_id"] == 2
    assert steps[1]["function_name"] == fast_step.__qualname__
    assert steps[1]["output"] == "fast_done"
    assert steps[2]["function_id"] == 3
    assert steps[2]["function_name"] == slow_step.__qualname__
    assert steps[2]["output"] == "slow_done"
    # Step 4: second asyncio_wait on the pending set
    assert steps[3]["function_id"] == 4
    assert steps[3]["function_name"] == "DBOS.asyncio_wait"
    assert steps[3]["output"] == [0]

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_all_completed(dbos: DBOS) -> None:
    step_counter: int = 0

    @DBOS.step()
    async def my_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return val

    @DBOS.workflow()
    async def wait_all_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [my_step("a"), my_step("b")], return_when=asyncio.ALL_COMPLETED
        )

        assert len(done) == 2
        assert len(pending) == 0
        assert sorted([t.result() for t in done]) == ["a", "b"]

    handle = await DBOS.start_workflow_async(wait_all_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_first_exception(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def error_step() -> str:
        nonlocal step_counter
        step_counter += 1
        raise ValueError("boom")

    @DBOS.step()
    async def slow_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val

    @DBOS.workflow()
    async def wait_exception_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [error_step(), slow_step("ok")],
            return_when=asyncio.FIRST_EXCEPTION,
        )

        assert len(done) == 1
        assert len(pending) == 1
        task = next(iter(done))
        assert isinstance(task.exception(), ValueError)
        assert "boom" in str(task.exception())

        # Let the slow step finish and wait for it
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 1
        assert len(pending2) == 0
        assert next(iter(done2)).result() == "ok"

    handle = await DBOS.start_workflow_async(wait_exception_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_timeout(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def blocked_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val

    @DBOS.workflow()
    async def wait_timeout_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [blocked_step("a"), blocked_step("b")],
            timeout=0.1,
        )

        # Both should still be pending after timeout
        assert len(done) == 0
        assert len(pending) == 2

        # Unblock and wait for all
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 2
        assert len(pending2) == 0
        assert sorted([t.result() for t in done2]) == ["a", "b"]

    handle = await DBOS.start_workflow_async(wait_timeout_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_workflow_recovery_async(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    step_counter: int = 0
    wf_counter: int = 0
    value = "value"

    wf_event_loop_id: int = 0

    @DBOS.workflow()
    async def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter, wf_event_loop_id
        wf_counter += 1
        wf_event_loop_id = id(asyncio.get_running_loop())
        output = await test_step(var2)
        assert output == var2
        return var

    @DBOS.step()
    async def test_step(var2: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var2

    test_event_loop_id = id(asyncio.get_running_loop())

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert (await test_workflow(value, value)) == value
    # Verify the workflow runs in the test event loop
    assert wf_event_loop_id == test_event_loop_id
    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        )

    # Run recovery (which is fundamentally sync) in a thread
    def recover_in_thread() -> Any:
        handles = DBOS._recover_pending_workflows()
        assert len(handles) == 1
        return handles[0].get_result()

    recovered_result = await asyncio.to_thread(recover_in_thread)
    assert recovered_result
    assert wf_counter == 2
    assert step_counter == 1
    # Verify the recovered workflow runs in the test event loop
    assert wf_event_loop_id == test_event_loop_id

    # Test that there was a recovery attempt of this
    stat = await DBOS.get_workflow_status_async(workflow_id)
    assert stat
    assert stat.recovery_attempts == 2
