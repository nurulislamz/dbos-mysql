# Adapted from dbos-transact-py tests/test_debouncer.py at commit 236142b. See MANIFEST.md.
import threading
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from yado import (
    DBOS,
    DBOSException,
    DBOSQueueDeduplicatedError,
    Debouncer,
    DebouncerClient,
    EnqueueOptions,
    SetEnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
)


def test_debouncer(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    @DBOS.step()
    def generate_uuid() -> str:
        return str(uuid.uuid4())

    def debouncer_test() -> None:
        debounce_period = 2

        debouncer = Debouncer.create(workflow)
        first_handle = debouncer.debounce("key", debounce_period, first_value)
        debouncer = Debouncer.create(workflow)
        second_handle = debouncer.debounce("key", debounce_period, second_value)
        assert first_handle.workflow_id == second_handle.workflow_id
        assert first_handle.get_result() == second_value
        assert second_handle.get_result() == second_value

        debouncer = Debouncer.create(workflow)
        third_handle = debouncer.debounce("key", debounce_period, third_value)
        debouncer = Debouncer.create(workflow)
        fourth_handle = debouncer.debounce("key", debounce_period, fourth_value)
        assert third_handle.workflow_id != first_handle.workflow_id
        assert third_handle.workflow_id == fourth_handle.workflow_id
        assert third_handle.get_result() == fourth_value
        assert fourth_handle.get_result() == fourth_value

        wfid = generate_uuid()
        with SetWorkflowID(wfid):
            handle = debouncer.debounce("key", debounce_period, first_value)
        assert handle.workflow_id == wfid
        assert handle.get_result() == first_value

    debouncer_test()

    # The same operations inside a workflow. Dropped: the upstream recovery rerun, which needs set_workflow_status.
    debouncer_test_workflow = DBOS.workflow()(debouncer_test)
    with SetWorkflowID(str(uuid.uuid4())):
        debouncer_test_workflow()


def test_debouncer_timeout(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer.create(workflow, debounce_timeout_sec=2)
    long_debounce_period = 10000000

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", long_debounce_period, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce("key", long_debounce_period, third_value)
    fourth_handle = debouncer.debounce("key", long_debounce_period, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    debouncer = Debouncer.create(workflow)
    short_debounce_period = 1

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", short_debounce_period, second_value)
    assert fourth_handle.workflow_id != first_handle.workflow_id
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value


def test_multiple_debouncers(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer_one = Debouncer.create(workflow)
    debouncer_two = Debouncer.create(workflow)
    debounce_period = 2

    first_handle = debouncer_one.debounce("key_one", debounce_period, first_value)
    second_handle = debouncer_one.debounce("key_one", debounce_period, second_value)
    third_handle = debouncer_two.debounce("key_two", debounce_period, third_value)
    fourth_handle = debouncer_two.debounce("key_two", debounce_period, fourth_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.workflow_id != third_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value


def test_debouncer_queue(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    queue = DBOS.register_queue("test-queue")

    debouncer = Debouncer.create(workflow, queue=queue)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, first_value)
    second_handle = debouncer.debounce("key", debounce_period_sec, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert second_handle.get_status().queue_name == queue.name

    with SetWorkflowTimeout(5.0):
        third_handle = debouncer.debounce("key", debounce_period_sec, third_value)
        fourth_handle = debouncer.debounce("key", debounce_period_sec, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value
    assert fourth_handle.get_status().queue_name == queue.name
    assert fourth_handle.get_status().workflow_timeout_ms == 5000.0
    assert fourth_handle.get_status().workflow_deadline_epoch_ms

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name

    # Dropped: the upstream app_version pass-through, which needs GlobalParams.
    debouncer_by_name = Debouncer.create(workflow, queue=queue.name)
    name_handle = debouncer_by_name.debounce("string-key", debounce_period_sec, first_value)
    assert name_handle.get_result() == first_value
    assert name_handle.get_status().queue_name == queue.name


async def test_debouncer_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow_async(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer.create_async(workflow_async)
    debounce_period_sec = 2

    first_handle = await debouncer.debounce_async("key", debounce_period_sec, first_value)
    second_handle = await debouncer.debounce_async("key", debounce_period_sec, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle = await debouncer.debounce_async("key", debounce_period_sec, third_value)
    fourth_handle = await debouncer.debounce_async("key", debounce_period_sec, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value


def test_debouncer_client(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    DBOS.register_queue("test-queue")

    # shortcut: no DBOSClient exists yet, so a stand-in exposes the two members DebouncerClient uses; switch to DBOSClient once it lands.
    client: Any = SimpleNamespace(_engine=dbos._engine, retrieve_workflow=DBOS.retrieve_workflow)
    options: EnqueueOptions = {"workflow_name": workflow.__qualname__, "queue_name": "test-queue"}
    debouncer = DebouncerClient(client, options)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, first_value)
    second_handle = debouncer.debounce("key", debounce_period_sec, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce("key", debounce_period_sec, third_value)
    fourth_handle = debouncer.debounce("key", debounce_period_sec, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    wfid = str(uuid.uuid4())
    options["workflow_id"] = wfid
    handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value


def test_debounce_flip_clears_dedup(dbos: DBOS) -> None:
    started = threading.Event()
    blocker = threading.Event()

    @DBOS.workflow()
    def workflow(x: int) -> int:
        started.set()
        blocker.wait()
        return x

    debouncer = Debouncer.create(workflow)
    handle = debouncer.debounce("key", 1000000, 1)

    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status.status == "DELAYED"
    assert status.deduplication_id == f"{workflow.__qualname__}-key"

    handle2 = debouncer.debounce("key", 1, 2)
    assert handle2.workflow_id == handle.workflow_id
    assert started.wait(timeout=60)

    # Released and running: a same-key debounce starts fresh before the first one finishes.
    handle3 = debouncer.debounce("key", 1, 3)
    assert handle3.workflow_id != handle.workflow_id
    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status.status == "PENDING"
    assert status.deduplication_id is None

    blocker.set()
    assert handle2.get_result() == 2
    assert handle3.get_result() == 3


def test_debounce_cancel_then_redebounce(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)
    handle = debouncer.debounce("key", 1000000, 1)
    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None and status.status == "DELAYED"

    DBOS.cancel_workflow(handle.workflow_id)

    handle2 = debouncer.debounce("key", 1, 2)
    assert handle2.workflow_id != handle.workflow_id
    assert handle2.get_result() == 2


def test_debounce_user_dedup_conflict_raises(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        DBOS.sleep(1000000)
        return x

    # Upstream enqueues on the internal queue, which users cannot reach here, so this uses a named queue.
    queue = DBOS.register_queue("dedup-queue")
    with SetEnqueueOptions(deduplication_id=f"{workflow.__qualname__}-key"):
        blocker = queue.enqueue(workflow, 1)

    debouncer = Debouncer.create(workflow, queue=queue)
    with pytest.raises(DBOSQueueDeduplicatedError):
        debouncer.debounce("key", 1, 2)

    DBOS.cancel_workflow(blocker.workflow_id)


def test_debounce_fixed_workflow_id_reuse(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        first_handle = debouncer.debounce("key", 2, 1)
    with SetWorkflowID(wfid):
        second_handle = debouncer.debounce("key", 2, 2)
    assert first_handle.workflow_id == wfid
    assert second_handle.workflow_id == wfid
    assert first_handle.get_result() == 2
    assert second_handle.get_result() == 2

    wfid2 = str(uuid.uuid4())
    with SetWorkflowID(wfid2):
        third_handle = debouncer.debounce("key2", 1000000, 3)
    with SetWorkflowID(wfid2):
        fourth_handle = debouncer.debounce("key2", 1, 4)
    assert third_handle.workflow_id == wfid2
    assert fourth_handle.workflow_id == wfid2
    assert fourth_handle.get_result() == 4


@pytest.mark.parametrize(
    "options",
    [{"deduplication_id": "x"}, {"delay_seconds": 1.0}, {"priority": 1}, {"queue_partition_key": "p"}, {"deduplication_id": "x", "duplication_policy": "return-existing"}],
)
def test_debounce_rejects_caller_dedup_and_delay(dbos: DBOS, options: dict[str, Any]) -> None:
    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)
    with SetEnqueueOptions(**options), pytest.raises(DBOSException, match="Cannot debounce"):
        debouncer.debounce("key", 1, 1)
    with SetWorkflowID(str(uuid.uuid4()), workflow_id_reuse_policy="reject"), pytest.raises(DBOSException, match="Cannot debounce"):
        debouncer.debounce("key", 1, 1)
