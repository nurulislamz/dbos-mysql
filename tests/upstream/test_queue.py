# Adapted from dbos-transact-py tests/test_queue.py at commit 236142b. See MANIFEST.md.
import asyncio
import gc
import threading
import time
import uuid
import weakref
from typing import Any, List

import pytest
from pydantic import BaseModel

from rust_dwos import DBOS, DBOSConfig, DBOSException, Queue, SetWorkflowID



def test_simple_queue(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = test_step(var1)
        return var1 + var2

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    DBOS.register_queue("test_queue")

    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_workflow, "abc", "123")
    assert handle.get_result() == "abcd123"
    with SetWorkflowID(wfid):
        assert test_workflow("abc", "123") == "abcd123"
    assert wf_counter == 1
    assert step_counter == 1


def test_queue_cannot_be_constructed(dbos: DBOS) -> None:
    """Queue is exported as a type, but only DBOS can build one: a queue comes
    from DBOS.register_queue or DBOS.retrieve_queue."""
    with pytest.raises(DBOSException, match="cannot be constructed directly"):
        Queue("constructed_queue")
    # Nor by guessing at the construction token.
    with pytest.raises(DBOSException, match="cannot be constructed directly"):
        Queue("constructed_queue", token=True)


def test_internal_queue_names_are_reserved(dbos: DBOS) -> None:
    """A user queue cannot claim a name DBOS uses for one of its own queues."""
    with pytest.raises(ValueError, match="is reserved"):
        DBOS.register_queue("_dbos_internal_queue")


def test_queue_childwf(dbos: DBOS) -> None:
    DBOS.register_queue("child_queue", concurrency=3)

    @DBOS.workflow()
    def test_child_wf(val: str) -> str:
        DBOS.recv("release", 30)
        return val + "d"

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        wfh1 = DBOS.enqueue_workflow("child_queue", test_child_wf, var1)
        wfh2 = DBOS.enqueue_workflow("child_queue", test_child_wf, var2)
        wfh3 = DBOS.enqueue_workflow("child_queue", test_child_wf, var1)
        wfh4 = DBOS.enqueue_workflow("child_queue", test_child_wf, var2)

        DBOS.sleep(1)

        DBOS.send(wfh1.get_workflow_id(), "go", "release")
        DBOS.send(wfh2.get_workflow_id(), "go", "release")
        DBOS.send(wfh3.get_workflow_id(), "go", "release")
        DBOS.send(wfh4.get_workflow_id(), "go", "release")

        return (
            wfh1.get_result()
            + wfh2.get_result()
            + wfh3.get_result()
            + wfh4.get_result()
        )

    assert test_workflow("a", "b") == "adbdadbd"


def test_queue_step(dbos: DBOS) -> None:
    step_counter: int = 0
    wfid = str(uuid.uuid4())

    @DBOS.step()
    def test_step(var: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal step_counter
        step_counter += 1
        return var + "1"

    DBOS.register_queue("test_queue")

    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_step, "abc")
    assert handle.get_result() == "abc1"
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_step, "abc")
    assert handle.get_result() == "abc1"
    assert step_counter == 1


@pytest.mark.asyncio
async def test_simple_queue_async(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = await test_step(var1)
        return var1 + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    await DBOS.register_queue_async("test_queue")

    with SetWorkflowID(wfid):
        handle = await DBOS.enqueue_workflow_async(
            "test_queue", test_workflow, "abc", "123"
        )
    assert (await handle.get_result()) == "abcd123"
    with SetWorkflowID(wfid):
        assert (await test_workflow("abc", "123")) == "abcd123"
    assert wf_counter == 1  # Direct re-invoke does not re-run the body (#762)
    assert step_counter == 1


class InnerType(BaseModel):
    one: str
    two: int


class OuterType(BaseModel):
    inner: InnerType


def test_complex_type(dbos: DBOS) -> None:
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    def workflow(input: OuterType) -> OuterType:
        return input

    # Verify a workflow with non-basic inputs and outputs can be enqueued
    inner = InnerType(one="one", two=2)
    outer = OuterType(inner=inner)

    handle = DBOS.enqueue_workflow("test_queue", workflow, outer)
    result = handle.get_result()

    def check(result: Any) -> None:
        assert isinstance(result, OuterType)
        assert isinstance(result.inner, InnerType)
        assert result.inner.one == outer.inner.one
        assert result.inner.two == outer.inner.two

    check(result)


def test_enqueued_async_workflow_survives_gc(dbos: DBOS) -> None:
    """Regression test for https://github.com/dbos-inc/dbos-transact-py/issues/710"""

    entered = threading.Event()
    interrupted: List[str] = []
    loops: List[asyncio.AbstractEventLoop] = []
    fut_refs: List["weakref.ref[asyncio.Future[str]]"] = []

    @DBOS.workflow()
    async def hanging_workflow() -> str:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        loops.append(loop)
        # Expose the future only weakly so the test does not itself keep the
        # workflow task reachable.
        fut_refs.append(weakref.ref(fut))
        entered.set()
        try:
            return await fut
        except BaseException as exc:
            interrupted.append(type(exc).__name__)
            raise

    DBOS.register_queue("gc_pin_queue")
    handle = DBOS.enqueue_workflow("gc_pin_queue", hanging_workflow)

    assert entered.wait(timeout=30)
    time.sleep(0.2)  # let the workflow suspend on its future

    gc.collect()
    time.sleep(0.2)

    # Unpinned, gc.collect() destroys the task: interrupted == ["GeneratorExit"]
    assert interrupted == []

    # The workflow is still alive: unblock it and verify it completes.
    fut = fut_refs[0]()
    assert fut is not None, "workflow's pending future was garbage-collected"
    loops[0].call_soon_threadsafe(fut.set_result, "done")
    assert handle.get_result() == "done"  # type: ignore


def test_queue_crud(dbos: DBOS) -> None:
    queue_name = f"test_crud_queue_{uuid.uuid4()}"
    assert DBOS.retrieve_queue(queue_name) is None
    registered = DBOS.register_queue(
        queue_name,
        global_concurrency=10,
        polling_interval_sec=2.5,
    )
    assert registered.name == queue_name
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.global_concurrency == 10
    assert retrieved.polling_interval_sec == 2.5


@pytest.mark.asyncio
async def test_queue_crud_async(dbos: DBOS) -> None:
    queue_name = f"test_crud_async_queue_{uuid.uuid4()}"
    assert await DBOS.retrieve_queue_async(queue_name) is None
    registered = await DBOS.register_queue_async(
        queue_name,
        global_concurrency=10,
        polling_interval_sec=2.5,
    )
    assert registered.name == queue_name
    retrieved = await DBOS.retrieve_queue_async(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.global_concurrency == 10
    assert retrieved.polling_interval_sec == 2.5


@pytest.mark.parametrize("concurrency", [None, 3])
def test_queue_lookup_after_restart(dbos: DBOS, config: DBOSConfig, concurrency: int | None) -> None:
    queue_name = f"test_crud_queue_{uuid.uuid4()}"
    DBOS.register_queue(queue_name, global_concurrency=concurrency)
    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.global_concurrency == concurrency


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [None, 3])
async def test_queue_lookup_after_restart_async(dbos: DBOS, config: DBOSConfig, concurrency: int | None) -> None:
    queue_name = f"test_crud_async_queue_{uuid.uuid4()}"
    await DBOS.register_queue_async(queue_name, global_concurrency=concurrency)
    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()
    retrieved = await DBOS.retrieve_queue_async(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.global_concurrency == concurrency


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
