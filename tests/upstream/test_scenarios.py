# Rewritten from dbos-transact-ts (306b732) and dbos-transact-py (236142b) tests. See MANIFEST.md.
import asyncio
import threading
import uuid
from collections.abc import Callable

import pytest

import rust_dwos
from rust_dwos import (
    DBOS,
    DBOSConflictingWorkflowError,
    DBOSStepNondeterminismError,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowStatusString,
)
from tests.conftest import Database, retry_until_success


async def test_step_oaoo(dbos: DBOS) -> None:
    counter = 0

    @DBOS.step()
    async def test_step() -> int:
        nonlocal counter
        value = counter
        counter += 1
        return value

    @DBOS.workflow()
    async def test_comm_workflow() -> int:
        return await test_step()

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert await test_comm_workflow() == 0
    assert counter == 1

    with SetWorkflowID(workflow_id):
        assert await test_comm_workflow() == 0
    assert counter == 1

    assert await test_comm_workflow() == 1
    assert counter == 2


async def test_workflow_oaoo(dbos: DBOS) -> None:
    username = "postgres"

    @DBOS.step()
    async def step_one(name: str) -> str:
        return name

    @DBOS.step()
    async def step_two(name: str) -> str:
        return name

    @DBOS.workflow()
    async def test_tx_workflow(name: str) -> str:
        await step_one(name)
        await step_two(name)
        return name

    workflow_ids: list[str] = []
    for _ in range(10):
        handle = await DBOS.start_workflow_async(test_tx_workflow, username)
        workflow_ids.append(handle.workflow_id)
        assert await handle.get_result() == username

    for workflow_id in workflow_ids:
        with SetWorkflowID(workflow_id):
            assert await test_tx_workflow(username) == username


async def test_reject_name_mismatch_throws_in_use(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def echo(value: str) -> str:
        return value

    @DBOS.workflow()
    async def other_workflow(value: str) -> str:
        return value

    workflow_id = f"reuse-mismatch-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(echo, "original")
    assert await handle.get_result() == "original"

    with SetWorkflowID(workflow_id), pytest.raises(DBOSConflictingWorkflowError) as error:
        await DBOS.start_workflow_async(other_workflow, "new")
    assert "echo" in str(error.value)


async def test_stale_owner_cannot_write_the_outcome(
    dbos: DBOS, database: Database, gate: Callable[[], threading.Event]
) -> None:
    started = gate()
    release = gate()
    finishing = gate()
    runs = 0

    @DBOS.workflow()
    async def outcome_workflow() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            started.set()
            await asyncio.to_thread(release.wait)
            finishing.set()
            return "stale"
        return "owner"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(outcome_workflow)
    await asyncio.to_thread(started.wait)

    database.take_over(workflow_id)
    release.set()
    await asyncio.to_thread(finishing.wait)
    await asyncio.sleep(0.5)

    status = await handle.get_status()
    assert status.status == WorkflowStatusString.PENDING.value
    assert status.output is None

    database.expire_lease(workflow_id)
    recovered = await asyncio.to_thread(DBOS._recover_pending_workflows)
    assert len(recovered) == 1
    assert await asyncio.to_thread(recovered[0].get_result) == "owner"
    assert await handle.get_result() == "owner"
    assert (await handle.get_status()).output == "owner"


async def test_step_conc(dbos: DBOS, database: Database) -> None:
    step_runs = 0
    step_active = 0
    max_step_active = 0
    workflow_active = 0
    max_workflow_active = 0

    @DBOS.step()
    async def test_conc_step() -> None:
        nonlocal step_runs, step_active, max_step_active
        step_runs += 1
        step_active += 1
        max_step_active = max(step_active, max_step_active)
        await asyncio.sleep(1)
        step_active -= 1

    @DBOS.workflow()
    async def test_conc_workflow() -> None:
        nonlocal workflow_active, max_workflow_active
        workflow_active += 1
        max_workflow_active = max(workflow_active, max_workflow_active)
        await asyncio.sleep(0.5)
        await test_conc_step()
        await asyncio.sleep(0.5)
        workflow_active -= 1

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        first = await DBOS.start_workflow_async(test_conc_workflow)
    with SetWorkflowID(workflow_id):
        second = await DBOS.start_workflow_async(test_conc_workflow)

    await first.get_result()
    await second.get_result()
    assert max_step_active == 1
    assert max_workflow_active == 1

    database.expire_lease(workflow_id)
    recovered = await asyncio.to_thread(DBOS._recover_pending_workflows)
    await asyncio.to_thread(recovered[0].get_result)
    assert max_step_active == 1
    assert max_workflow_active == 1

    runs_before = step_runs
    database.expire_lease(workflow_id)
    first_dispatch = await asyncio.to_thread(DBOS._recover_pending_workflows)
    database.expire_lease(workflow_id)
    second_dispatch = await asyncio.to_thread(DBOS._recover_pending_workflows)
    assert await asyncio.to_thread(first_dispatch[0].get_result) is None
    assert await asyncio.to_thread(second_dispatch[0].get_result) is None
    assert step_runs == runs_before
    assert (await first.get_status()).status == WorkflowStatusString.SUCCESS.value


async def test_step_recorded_twice_by_the_owner_is_nondeterminism(
    dbos: DBOS, database: Database, gate: Callable[[], threading.Event]
) -> None:
    started = gate()
    release = gate()
    step_id: int | None = None

    @DBOS.step()
    async def blocked_step() -> str:
        nonlocal step_id
        step_id = DBOS.step_id
        started.set()
        await asyncio.to_thread(release.wait)
        return "blocked"

    @DBOS.workflow()
    async def cancelled_workflow() -> str:
        return await blocked_step()

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(cancelled_workflow)
    await asyncio.to_thread(started.wait)

    assert step_id is not None
    database.plant_step(workflow_id, step_id, blocked_step.__qualname__)
    release.set()

    with pytest.raises(DBOSStepNondeterminismError):
        await handle.get_result()
    assert (await handle.get_status()).status == WorkflowStatusString.ERROR.value


def test_local_recovery(dbos: DBOS, database: Database) -> None:
    total = 0

    @DBOS.workflow()
    def test_recovery_workflow(value: int) -> int:
        nonlocal total
        total += value
        return value

    handle: WorkflowHandle[int] = DBOS.start_workflow(test_recovery_workflow, 5)
    assert handle.get_result() == 5

    database.expire_lease(handle.workflow_id)
    recovered = DBOS._recover_pending_workflows()

    assert len(recovered) == 1
    assert recovered[0].get_result() == 5
    assert handle.get_result() == 5
    assert total == 10


def test_dead_letter_queue_scenario(dbos: DBOS, database: Database) -> None:
    max_recovery_attempts = 5
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def dead_letter_workflow() -> None:
        nonlocal recovery_count
        recovery_count += 1

    handle: WorkflowHandle[None] = DBOS.start_workflow(dead_letter_workflow)
    handle.get_result()

    for attempt in range(max_recovery_attempts):
        database.expire_lease(handle.workflow_id)
        recovered = [h for h in DBOS._recover_pending_workflows() if h.workflow_id == handle.workflow_id]
        assert len(recovered) == 1
        recovered[0].get_result()
        assert recovery_count == attempt + 2

    completed_before = handle.get_status().completed_at
    assert completed_before is not None
    database.expire_lease(handle.workflow_id)
    DBOS._recover_pending_workflows()

    def check_dead_lettered() -> None:
        assert handle.get_status().status == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value

    retry_until_success(check_dead_lettered)
    status = handle.get_status()
    assert status.recovery_attempts == max_recovery_attempts + 2
    assert status.completed_at >= completed_before

    runs_before = recovery_count
    with SetWorkflowID(handle.workflow_id):
        direct: WorkflowHandle[None] = DBOS.start_workflow(dead_letter_workflow)
    with pytest.raises(rust_dwos.DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        direct.get_result()
    assert recovery_count == runs_before

    retrieved: WorkflowHandle[None] = DBOS.retrieve_workflow(handle.workflow_id)
    with pytest.raises(rust_dwos.DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        retrieved.get_result()


def test_child_workflow_recovery(dbos: DBOS, gate: Callable[[], threading.Event]) -> None:
    event = gate()
    counter = 0

    @DBOS.step()
    def step_one() -> int:
        nonlocal counter
        value = counter
        counter += 1
        return value

    @DBOS.step()
    def step_two() -> int:
        nonlocal counter
        value = counter
        counter += 1
        return value

    @DBOS.workflow()
    def child_workflow() -> str | None:
        for index in range(10):
            assert step_one() == index * 2
            assert step_two() == index * 2 + 1
            event.wait()
        return DBOS.workflow_id

    @DBOS.workflow()
    def parent_workflow() -> str:
        return DBOS.start_workflow(child_workflow).workflow_id

    child_id = parent_workflow()
    original: WorkflowHandle[str] = DBOS.retrieve_workflow(child_id)
    with SetWorkflowID(child_id):
        recovered: WorkflowHandle[str] = DBOS.start_workflow(child_workflow)
    event.set()

    assert original.get_result() == child_id
    assert recovered.get_result() == child_id


async def test_recorded_step_output(dbos: DBOS) -> None:
    @DBOS.step()
    async def fast_step(value: str) -> str:
        return value + "_done"

    @DBOS.workflow()
    async def workflow() -> None:
        await fast_step("fast")

    handle = await DBOS.start_workflow_async(workflow)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert len(steps) == 1
    assert steps[0]["function_id"] == 1
    assert steps[0]["function_name"] == fast_step.__qualname__
    assert steps[0]["output"] == "fast_done"


async def test_parallel_steps_replay_from_recorded_results(dbos: DBOS, database: Database) -> None:
    step_counter = 0

    @DBOS.step()
    async def my_step(value: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return value

    @DBOS.workflow()
    async def wait_all_workflow() -> list[str]:
        return list(await asyncio.gather(my_step("a"), my_step("b")))

    handle = await DBOS.start_workflow_async(wait_all_workflow)
    assert await handle.get_result() == ["a", "b"]
    assert step_counter == 2

    database.expire_lease(handle.workflow_id)
    recovered = await asyncio.to_thread(DBOS._recover_pending_workflows)
    assert await asyncio.to_thread(recovered[0].get_result) == ["a", "b"]
    assert step_counter == 2
