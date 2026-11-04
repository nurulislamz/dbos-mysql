# Adapted from dbos-transact-py tests/test_failures.py at commit 236142b. See MANIFEST.md.
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



class BadException(Exception):
    def __init__(self, one: int, two: int) -> None:
        super().__init__(f"Message: {one}, {two}")


def test_dead_letter_queue(dbos: DBOS) -> None:
    event = threading.Event()
    max_recovery_attempts = 3
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def dead_letter_workflow() -> None:
        nonlocal recovery_count
        recovery_count += 1

    # Start a workflow that we will restart
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(dead_letter_workflow)
    handle.get_result()

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        set_workflow_status(dbos._sys_db, wfid, "PENDING")
        handles = DBOS._recover_pending_workflows()
        # Select our workflow rather than trusting ordering: recovery returns a
        # polling handle for every pending row, and get_result() on the wrong one
        # polls forever, turning a failure into a suite-level timeout.
        recovered = [h for h in handles if h.workflow_id == wfid]
        assert len(recovered) == 1
        recovered[0].get_result()
        assert recovery_count == i + 2

    # Verify an additional attempt puts the workflow in the DLQ status.
    completed_before_dlq = handle.get_status().completed_at
    assert completed_before_dlq is not None
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    DBOS._recover_pending_workflows()

    # Recovery re-enqueues, so the DLQ transition happens when the queue dequeues the workflow.
    def check_dlq() -> None:
        assert (
            handle.get_status().status
            == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
        )

    retry_until_success(check_dlq)
    # Terminal like ERROR/CANCELLED, so the DLQ write stamps its own completion time.
    dlq_completed_at = handle.get_status().completed_at
    assert dlq_completed_at is not None and dlq_completed_at >= completed_before_dlq
    # A direct call does not re-run the body; it awaits the row and surfaces its terminal status.
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfid):
            dead_letter_workflow()
    assert exc_info.errisinstance(DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded)

    # Resume the workflow. Verify it can recover again without error.
    resumed_handle = dbos.resume_workflow(wfid)
    DBOS._recover_pending_workflows()

    # Complete the resumed workflow
    event.set()
    assert handle.get_result() == resumed_handle.get_result() == None
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify that retries of a completed workflow do not raise the DLQ exception
    for _ in range(max_recovery_attempts * 2):
        with SetWorkflowID(wfid):
            dead_letter_workflow()

    @DBOS.workflow(max_recovery_attempts=None)
    def infinite_dead_letter_workflow() -> None:
        return

    # Verify that a workflow with max_recovery_attempts=None is retried infinitely.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(infinite_dead_letter_workflow)
        handle.get_result

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(DEFAULT_MAX_RECOVERY_ATTEMPTS * 2):
        handles = DBOS._recover_pending_workflows()
        for handle in handles:
            assert handle.get_result() == None


def test_nondeterministic_workflow(dbos: DBOS) -> None:
    flag = True

    @DBOS.step()
    def step_one() -> None:
        return

    @DBOS.step()
    def step_two() -> None:
        return

    @DBOS.workflow()
    def non_deterministic_workflow() -> None:
        if flag:
            step_one()
        else:
            step_two()

    # Start the workflow. It will complete step_one then wait.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        non_deterministic_workflow()

    # To simulate nondeterminism, set the flag then restart the workflow w/ fork;
    flag = False
    handle_two = DBOS.fork_workflow(wfid, 2)

    # Due to the nondeterminism, the workflow should encounter an unexpected step.
    with pytest.raises(DBOSUnexpectedStepError) as exc_info:
        handle_two.get_result()


def test_step_retries(dbos: DBOS) -> None:
    step_counter = 0

    DBOS.register_queue("test-queue")
    max_attempts = 2

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    @DBOS.workflow()
    def enqueue_failing_step() -> None:
        DBOS.enqueue_workflow("test-queue", failing_step).get_result()

    error_message = f"Step {failing_step.__qualname__} has exceeded its maximum of {max_attempts} retries"

    # Test calling the step directly
    with pytest.raises(Exception) as excinfo:
        failing_step()

    # Test calling the workflow
    step_counter = 0
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        failing_workflow()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts
    assert len(excinfo.value.errors) == max_attempts
    for error in excinfo.value.errors:
        assert isinstance(error, Exception)
        assert error
        assert "fail" in str(error)

    # Test enqueueing the step
    step_counter = 0
    handle = DBOS.enqueue_workflow("test-queue", failing_step)
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        handle.get_result()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    # Test enqueuing the workflow
    step_counter = 0
    handle = DBOS.enqueue_workflow("test-queue", failing_workflow)
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        handle.get_result()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    # Test enqueuing the step from a workflow
    step_counter = 0
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        enqueue_failing_step()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    assert queue_entries_are_cleaned_up(dbos)


def test_step_retries_no_final_sleep(dbos: DBOS) -> None:
    # Regression test for #667: the retry loop must not sleep after the
    # final failed attempt — otherwise DBOSMaxStepRetriesExceeded is delayed
    # by a full backoff interval for nothing.
    max_attempts = 3
    interval_seconds = 1.0
    backoff_rate = 2.0
    # Useful backoffs total interval * (1 + backoff_rate) = 3.0s; a wasted
    # final sleep would add another backoff_rate**(max_attempts-1) = 4.0s.
    expected_max_seconds = 3.5
    step_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=interval_seconds,
        backoff_rate=backoff_rate,
        max_attempts=max_attempts,
    )
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    start = time.monotonic()
    with pytest.raises(DBOSMaxStepRetriesExceeded):
        failing_workflow()
    elapsed = time.monotonic() - start
    assert step_counter == max_attempts
    assert elapsed < expected_max_seconds, (
        f"Retry loop took {elapsed:.2f}s; expected < {expected_max_seconds}s "
        "(a wasted final sleep would push this past 7s)"
    )


@pytest.mark.asyncio
async def test_step_retries_no_final_sleep_async(dbos: DBOS) -> None:
    # Async variant of the #667 regression test (covers Pending._retry).
    max_attempts = 3
    interval_seconds = 1.0
    backoff_rate = 2.0
    expected_max_seconds = 3.5
    step_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=interval_seconds,
        backoff_rate=backoff_rate,
        max_attempts=max_attempts,
    )
    async def failing_step_async() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    async def failing_workflow_async() -> None:
        await failing_step_async()

    start = time.monotonic()
    with pytest.raises(DBOSMaxStepRetriesExceeded):
        await failing_workflow_async()
    elapsed = time.monotonic() - start
    assert step_counter == max_attempts
    assert elapsed < expected_max_seconds, (
        f"Retry loop took {elapsed:.2f}s; expected < {expected_max_seconds}s "
        "(a wasted final sleep would push this past 7s)"
    )


def test_step_status(dbos: DBOS) -> None:
    step_counter = 0

    max_attempts = 5

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == 1
        assert step_status.current_attempt == step_counter
        assert step_status.max_attempts == max_attempts
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    assert failing_workflow() == None
    step_counter = 0


def test_recovery_during_retries(dbos: DBOS) -> None:
    step_counter = 0
    start_event = threading.Event()
    blocking_event = threading.Event()

    max_attempts = 3

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("fail")
        else:
            start_event.set()
            blocking_event.wait()

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    handle = DBOS.start_workflow(failing_workflow)
    start_event.wait()
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    blocking_event.set()
    assert handle.get_result() is None
    assert recovery_handles[0].get_result() is None


def test_workflow_error_serialization(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.step()
    def step() -> None:
        raise BadException(1, 2)

    @DBOS.workflow()
    def workflow() -> None:
        step()

    handle = DBOS.start_workflow(workflow)

    with pytest.raises(BadException):
        handle.get_result()

    workflows = DBOS.list_workflows()
    assert len(workflows) == 1
    assert workflows[0].error is not None

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 1
    assert steps[0]["error"] is not None

    status = handle.get_status()
    assert status.error is not None

    status = client.retrieve_workflow(handle.workflow_id).get_status()
    assert status.error is not None


def test_nonserializable_return(dbos: DBOS) -> None:
    @DBOS.step()
    def step() -> Generator[str, Any, None]:
        yield "val"

    @DBOS.workflow()
    def workflow() -> None:
        step()

    with pytest.raises(TypeError):
        workflow()


def test_recovery_attempts(dbos: DBOS, config: DBOSConfig) -> None:

    config["application_version"] = "0.0.1"
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    event = threading.Event()

    child_id = str(uuid.uuid4())

    @DBOS.workflow(max_recovery_attempts=1)
    def child_workflow() -> None:
        event.wait()

    @DBOS.workflow(max_recovery_attempts=2)
    def parent_workflow() -> None:
        with SetWorkflowID(child_id):
            child_workflow()
        event.wait()

    DBOS.register_queue("test_queue", polling_interval_sec=0.1)

    parent_handle = DBOS.enqueue_workflow("test_queue", parent_workflow)
    child_handle: WorkflowHandle[None] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )

    def check_attempt_1() -> None:
        assert parent_handle.get_status().recovery_attempts == 1
        assert child_handle.get_status().recovery_attempts == 1

    retry_until_success(check_attempt_1)

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    def check_attempt_2() -> None:
        assert parent_handle.get_status().recovery_attempts == 2
        assert child_handle.get_status().recovery_attempts == 2

    retry_until_success(check_attempt_2)

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    def check_attempt_3() -> None:
        assert parent_handle.get_status().recovery_attempts == 3
        assert child_handle.get_status().recovery_attempts == 3
        assert (
            child_handle.get_status().status
            == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
        )
        assert parent_handle.get_status().status == WorkflowStatusString.ERROR.value
        assert isinstance(
            parent_handle.get_status().error,
            DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
        )

    retry_until_success(check_attempt_3)
    event.set()
    DBOS.destroy(destroy_registry=True)
