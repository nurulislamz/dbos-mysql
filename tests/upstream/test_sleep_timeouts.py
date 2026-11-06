# Adapted from dbos-transact-py tests/test_dbos.py and tests/test_async.py at 236142b.
import time
import uuid

import pytest

from yado import (
    DBOS,
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    SetWorkflowID,
    SetWorkflowTimeout,
    assert_current_dbos_context,
    get_workflow_start_options,
)


def test_sleep(dbos: DBOS) -> None:
    sleep_counter = 0

    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        nonlocal sleep_counter
        sleep_counter += 1
        dbos.sleep(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    start_time = time.time()
    sleep_uuid = test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    with SetWorkflowID(sleep_uuid):
        assert test_sleep_workflow(1.5) == sleep_uuid
    # Completed replay returns the recorded result without re-running the sleep.
    assert sleep_counter == 1


@pytest.mark.asyncio
async def test_sleep_async(dbos: DBOS) -> None:
    sleep_counter = 0

    @DBOS.workflow()
    async def test_sleep_workflow(secs: float) -> str:
        nonlocal sleep_counter
        sleep_counter += 1
        await dbos.sleep_async(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    start_time = time.time()
    sleep_uuid = await test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    with SetWorkflowID(sleep_uuid):
        assert (await test_sleep_workflow(1.5)) == sleep_uuid
    # Completed replay returns the recorded result without re-running the sleep.
    assert sleep_counter == 1


def test_workflow_timeout(dbos: DBOS) -> None:

    @DBOS.workflow()
    def blocked_workflow() -> None:
        assert get_workflow_start_options().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        while True:
            DBOS.sleep(0.1)

    # Verify a blocked workflow called with a timeout is cancelled
    wfid = str(uuid.uuid4())
    with SetWorkflowTimeout(0.1):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            with SetWorkflowID(wfid):
                blocked_workflow()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        start_time = time.time() * 1000
        handle = DBOS.start_workflow(blocked_workflow)
        status = handle.get_status()
        assert status.workflow_timeout_ms == 100
        assert (
            status.workflow_deadline_epoch_ms is not None
            and status.workflow_deadline_epoch_ms > start_time
        )
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()

    @DBOS.workflow()
    def parent_workflow_with_timeout() -> None:
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        with SetWorkflowTimeout(0.1):
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                blocked_workflow()
            handle = DBOS.start_workflow(blocked_workflow)
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                handle.get_result()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None

    # Verify if a parent calls a blocked workflow with a timeout, the child is cancelled
    assert parent_workflow_with_timeout() == None

    start_child, direct_child = str(uuid.uuid4()), str(uuid.uuid4())

    @DBOS.workflow()
    def parent_workflow() -> None:
        assert get_workflow_start_options().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        with SetWorkflowID(start_child):
            DBOS.start_workflow(blocked_workflow)
        with SetWorkflowID(direct_child):
            blocked_workflow()

    # Verify if a parent called with a timeout calls a blocked child
    # the deadline propagates and the children are also cancelled.
    with SetWorkflowTimeout(1.0):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            parent_workflow()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(start_child).get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(direct_child).get_result()

    # Verify the context variables are set correctly
    with SetWorkflowTimeout(1.0):
        assert get_workflow_start_options().workflow_timeout_ms == 1000
        with SetWorkflowTimeout(2.0):
            assert get_workflow_start_options().workflow_timeout_ms == 2000
        with SetWorkflowTimeout(None):
            assert get_workflow_start_options().workflow_timeout_ms is None
        assert get_workflow_start_options().workflow_timeout_ms == 1000
    assert get_workflow_start_options().workflow_timeout_ms is None


@pytest.mark.asyncio
async def test_workflow_timeout_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def blocked_workflow() -> None:
        assert get_workflow_start_options().workflow_timeout_ms is None
        while True:
            await DBOS.sleep_async(0.1)

    with SetWorkflowTimeout(0.1):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await blocked_workflow()
        handle = await DBOS.start_workflow_async(blocked_workflow)
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await handle.get_result()

    @DBOS.workflow()
    async def parent_workflow_with_timeout() -> None:
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        with SetWorkflowTimeout(0.1):
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                await blocked_workflow()
            handle = await DBOS.start_workflow_async(blocked_workflow)
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                await handle.get_result()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None

    # Verify if a parent calls a blocked workflow with a timeout, the child is cancelled
    await parent_workflow_with_timeout()

    start_child, direct_child = str(uuid.uuid4()), str(uuid.uuid4())

    @DBOS.workflow()
    async def parent_workflow() -> None:
        assert get_workflow_start_options().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        with SetWorkflowID(start_child):
            await DBOS.start_workflow_async(blocked_workflow)
        with SetWorkflowID(direct_child):
            await blocked_workflow()

    # Verify if a parent called with a timeout calls a blocked child
    # the deadline propagates and the children are also cancelled.
    with SetWorkflowTimeout(1.0):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await parent_workflow()

    with pytest.raises(Exception) as exc_info:
        await (await DBOS.retrieve_workflow_async(start_child)).get_result()
    assert "was cancelled" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        await (await DBOS.retrieve_workflow_async(direct_child)).get_result()
    assert "was cancelled" in str(exc_info.value)


def test_timeout_setter_rejects_overlapping_use() -> None:
    timeout = SetWorkflowTimeout(5)
    with timeout:
        with pytest.raises(DBOSException, match="already in use"):
            with timeout:
                pass
        assert get_workflow_start_options().workflow_timeout_ms == 5000
    assert get_workflow_start_options().workflow_timeout_ms is None
    # Sequential reuse is fine.
    with timeout:
        assert get_workflow_start_options().workflow_timeout_ms == 5000
    assert get_workflow_start_options().workflow_timeout_ms is None
