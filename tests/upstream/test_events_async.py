# Adapted from dbos-transact-py tests/test_async.py at commit 236142b. See MANIFEST.md.
import time
import uuid
from typing import Optional

import pytest

from yado import DBOS, SetWorkflowID


async def test_send_recv_async(dbos: DBOS) -> None:
    send_counter: int = 0
    recv_counter: int = 0

    @DBOS.workflow()
    async def test_send_workflow(dest_uuid: str, topic: str) -> str:
        await dbos.send_async(dest_uuid, "test1")
        await dbos.send_async(dest_uuid, "test2", topic=topic)
        await dbos.send_async(dest_uuid, "test3")
        nonlocal send_counter
        send_counter += 1
        return dest_uuid

    @DBOS.workflow()
    async def test_recv_workflow(topic: str) -> str:
        msg1 = await dbos.recv_async(topic, timeout_seconds=10)
        msg2 = await dbos.recv_async(timeout_seconds=10)
        msg3 = await dbos.recv_async(timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        return "-".join([str(msg1), str(msg2), str(msg3)])

    @DBOS.workflow()
    async def test_recv_timeout(timeout_seconds: float) -> None:
        msg = await dbos.recv_async(timeout_seconds=timeout_seconds)
        assert msg is None

    @DBOS.workflow()
    async def test_send_none(dest_uuid: str) -> None:
        await dbos.send_async(dest_uuid, None)

    dest_uuid = str(uuid.uuid4())

    # Send to non-existent uuid should fail
    with pytest.raises(Exception) as exc_info:
        await test_send_workflow(dest_uuid, "testtopic")
    assert (
        f"Non-existent `send` destination workflow ID: {dest_uuid}"
        in str(exc_info.value)
    )

    with SetWorkflowID(dest_uuid):
        handle = await dbos.start_workflow_async(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str(uuid.uuid4())
    with SetWorkflowID(send_uuid):
        res = await test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
    begin_time = time.time()
    assert (await handle.get_result()) == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 9.0

    # Test send 'None'
    none_uuid = str(uuid.uuid4())
    none_handle = None
    with SetWorkflowID(none_uuid):
        none_handle = await dbos.start_workflow_async(test_recv_timeout, 10.0)
    await test_send_none(none_uuid)
    begin_time = time.time()
    result = await none_handle.get_result()  # type: ignore
    assert result is None
    duration = time.time() - begin_time
    assert duration < 9.0

    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        await test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration > 0.7

    # Test OAOO: re-invoking a completed workflow returns the recorded result without re-running the body (#762)
    with SetWorkflowID(send_uuid):
        res = await test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
        assert send_counter == 1

    with SetWorkflowID(dest_uuid):
        begin_time = time.time()
        res = await test_recv_workflow("testtopic")
        duration = time.time() - begin_time
        assert duration < 3.0
        assert res == "test2-test1-test3"
        assert recv_counter == 1  # Completed replay does not re-run the body (#762)

    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        await test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration < 0.9

    # Test recv outside of a workflow
    with pytest.raises(Exception) as exc_info:
        await dbos.recv_async("test1")
    assert "recv() must be called from within a workflow" in str(exc_info.value)


async def test_set_get_events(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_setevent_workflow() -> None:
        await dbos.set_event_async("key1", "value1")
        await dbos.set_event_async("key2", "value2")
        await dbos.set_event_async("key3", None)

    getevent_counter = 0

    @DBOS.workflow()
    async def test_getevent_workflow(
        target_uuid: str, key: str, timeout_seconds: float = 10
    ) -> Optional[str]:
        nonlocal getevent_counter
        getevent_counter += 1
        msg = await dbos.get_event_async(target_uuid, key, timeout_seconds)
        return str(msg) if msg is not None else None

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        await test_setevent_workflow()
    with SetWorkflowID(wfuuid):
        await test_setevent_workflow()

    value1 = await test_getevent_workflow(wfuuid, "key1")
    assert value1 == "value1"

    value2 = await test_getevent_workflow(wfuuid, "key2")
    assert value2 == "value2"

    # Run getEvent outside of a workflow
    value1 = await dbos.get_event_async(wfuuid, "key1")
    assert value1 == "value1"

    value2 = await dbos.get_event_async(wfuuid, "key2")
    assert value2 == "value2"

    begin_time = time.time()
    value3 = await test_getevent_workflow(wfuuid, "key3")
    assert value3 is None
    duration = time.time() - begin_time
    assert duration < 1  # None is from the event not from the timeout

    # Test OAOO
    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        res = await test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

    calls_before_replay = getevent_counter
    with SetWorkflowID(timeout_uuid):
        res = await test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        assert res is None
    # Completed replay returns the recorded result without re-running the wait.
    assert getevent_counter == calls_before_replay

    # No OAOO for getEvent outside of a workflow
    begin_time = time.time()
    res = await dbos.get_event_async("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    begin_time = time.time()
    res = await dbos.get_event_async("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    # Test setEvent outside of a workflow
    with pytest.raises(Exception) as exc_info:
        await dbos.set_event_async("key1", "value1")
    assert "set_event() must be called from within a workflow" in str(exc_info.value)


async def test_get_events_async(dbos: DBOS) -> None:
    """Test the async version of get_events function that retrieves all events for a workflow."""

    @DBOS.workflow()
    async def async_events_workflow() -> str:
        # Set multiple events using async methods
        await DBOS.set_event_async("event1", "value1")
        await DBOS.set_event_async("event2", {"nested": "data", "count": 42})
        await DBOS.set_event_async("event3", [1, 2, 3, 4, 5])
        return "completed"

    # Execute the workflow
    handle = await DBOS.start_workflow_async(async_events_workflow)
    result = await handle.get_result()
    assert result == "completed"

    # Get all events for the workflow using async method
    events = await DBOS.get_all_events_async(handle.workflow_id)

    # Verify all events are present with correct values
    assert len(events) == 3
    assert events["event1"] == "value1"
    assert events["event2"] == {"nested": "data", "count": 42}
    assert events["event3"] == [1, 2, 3, 4, 5]

    # Test with a workflow that has no events
    @DBOS.workflow()
    async def no_events_workflow() -> str:
        await DBOS.sleep_async(0.01)
        return "no events"

    handle2 = await DBOS.start_workflow_async(no_events_workflow)
    await handle2.get_result()

    # Should return empty dict for workflow with no events
    events2 = await DBOS.get_all_events_async(handle2.workflow_id)
    assert events2 == {}

