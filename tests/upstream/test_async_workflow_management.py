# Adapted from dbos-transact-py tests/test_async_workflow_management.py at commit 236142b. See MANIFEST.md.
import asyncio
import uuid
from typing import List

import pytest

from yado import DBOS, DBOSAwaitedWorkflowCancelledError, DBOSNonExistentWorkflowError, SetWorkflowID
from yado.dbos import WorkflowStatus


@pytest.mark.asyncio
async def test_cancel_workflow_async(dbos: DBOS) -> None:
    """Test async cancel_workflow method."""
    steps_completed = 0
    workflow_event = asyncio.Event()
    main_event = asyncio.Event()
    input_val = 5

    @DBOS.step()
    async def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    async def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one()
        main_event.set()
        await workflow_event.wait()
        await step_two()
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(simple_workflow, input_val)
    await main_event.wait()
    await DBOS.cancel_workflow_async(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    assert steps_completed == 1


@pytest.mark.asyncio
async def test_resume_workflow_async(dbos: DBOS) -> None:
    """Test async resume_workflow method."""
    steps_completed = 0
    workflow_event = asyncio.Event()
    main_event = asyncio.Event()
    input_val = 5

    @DBOS.step()
    async def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    async def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one()
        main_event.set()
        await workflow_event.wait()
        await step_two()
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        cancelled_handle = await DBOS.start_workflow_async(simple_workflow, input_val)
    await main_event.wait()
    await DBOS.cancel_workflow_async(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await cancelled_handle.get_result()
    assert steps_completed == 1

    async_handle = await DBOS.resume_workflow_async(wfid)
    assert (await async_handle.get_result()) == input_val
    assert steps_completed == 2

    assert (await cancelled_handle.get_result()) == input_val


@pytest.mark.asyncio
async def test_resume_nonexistent_workflow_async(dbos: DBOS) -> None:
    missing_id = str(uuid.uuid4())

    with pytest.raises(DBOSNonExistentWorkflowError):
        await DBOS.resume_workflow_async(missing_id)


@pytest.mark.asyncio
async def test_list_workflows_async(dbos: DBOS) -> None:
    """Test async list_workflows method."""
    workflow_ids = []

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        return x

    for i in range(3):
        wfid = str(uuid.uuid4())
        workflow_ids.append(wfid)
        with SetWorkflowID(wfid):
            handle = await DBOS.start_workflow_async(simple_workflow, i)
            await handle.get_result()

    workflows: List[WorkflowStatus] = await DBOS.list_workflows_async()

    assert len(workflows) >= 3

    found_ids = {wf.workflow_id for wf in workflows}
    for wfid in workflow_ids:
        assert wfid in found_ids

    filtered_workflows = await DBOS.list_workflows_async(workflow_ids=workflow_ids[:2])
    assert len(filtered_workflows) == 2
    assert all(wf.workflow_id in workflow_ids[:2] for wf in filtered_workflows)


@pytest.mark.asyncio
async def test_list_workflow_steps_async(dbos: DBOS) -> None:
    """Test async list_workflow_steps method."""

    @DBOS.step()
    async def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    async def step_two(x: int) -> int:
        return x + 2

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one(x)
        await step_two(x)
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(simple_workflow, 1)
        await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(wfid)

    assert len(steps) >= 2
    step_names = {step["function_name"] for step in steps}
    assert any("step_one" in name for name in step_names)
    assert any("step_two" in name for name in step_names)
