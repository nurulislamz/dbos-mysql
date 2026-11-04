import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from rust_dwos import DBOS, DBOSAwaitedWorkflowCancelledError, DBOSException, SetWorkflowID
from tests.conftest import Database

_PROGRAM = """
import asyncio, os, signal, sys, threading, time
from pathlib import Path
from rust_dwos import DBOS, SetWorkflowID

url, workflow_id, kind, mode, ready = sys.argv[1:]

@DBOS.step()
def sync_step() -> None:
    pass

@DBOS.step()
async def async_step() -> None:
    pass

@DBOS.workflow()
def sync_workflow() -> str:
    Path(ready).write_text("ready")
    if mode == "step_after_destroy":
        while DBOS.workflow_id is not None:
            sync_step()
            time.sleep(0.01)
    while mode == "sigterm_in_body":
        pass
    time.sleep(60)
    return "finished"

@DBOS.workflow()
async def async_workflow() -> str:
    Path(ready).write_text("ready")
    if mode == "step_after_destroy":
        while True:
            await async_step()
            await asyncio.sleep(0.01)
    while mode == "sigterm_in_body":
        pass
    await asyncio.sleep(60)
    return "finished"

DBOS(config={"name": "t", "system_database_url": url, "recovery_interval_seconds": 3600.0, "shutdown_timeout_seconds": 2.0})
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
DBOS.launch()

async def inline_async() -> None:
    with SetWorkflowID(workflow_id):
        await async_workflow()

def inline_sync() -> None:
    with SetWorkflowID(workflow_id):
        sync_workflow()

if mode == "step_after_destroy":
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(async_workflow if kind == "async" else sync_workflow)
    while not Path(ready).exists():
        time.sleep(0.01)
    time.sleep(0.1)
    DBOS.destroy()
elif kind == "async":
    asyncio.run(inline_async())
else:
    inline_sync()
"""


def _run_until_stopped(database: Database, tmp_path: Path, kind: str, mode: str) -> str:
    workflow_id = str(uuid.uuid4())
    ready = tmp_path / "ready"
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", _PROGRAM, database.url, workflow_id, kind, mode, str(ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        if mode.startswith("sigterm"):
            deadline = time.monotonic() + 20
            while not ready.exists():
                assert time.monotonic() < deadline, "the workflow did not start"
                assert process.poll() is None, process.stdout.read() if process.stdout else ""
                time.sleep(0.01)
            time.sleep(0.1)
            process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)
    finally:
        process.kill()
        process.wait()
    return workflow_id


@pytest.mark.parametrize("kind", ["async", "sync"])
@pytest.mark.parametrize("mode", ["sigterm", "sigterm_in_body", "step_after_destroy"])
def test_a_shutdown_is_not_stored_as_an_error(database: Database, tmp_path: Path, kind: str, mode: str) -> None:
    workflow_id = _run_until_stopped(database, tmp_path, kind, mode)
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert rows == [("PENDING", None)]
    steps = database.execute("SELECT function_id FROM operation_outputs WHERE workflow_uuid = %s AND error IS NOT NULL", workflow_id)
    assert steps == []


def test_a_user_error_is_still_stored_as_an_error(database: Database) -> None:
    class Declined(Exception):
        pass

    @DBOS.workflow()
    def declines() -> None:
        raise Declined("no")

    DBOS.destroy(destroy_registry=False)
    DBOS(config={"name": "t", "system_database_url": database.url, "recovery_interval_seconds": 3600.0})
    DBOS.launch()
    try:
        handle = DBOS.start_workflow(declines)
        with pytest.raises(Declined):
            handle.get_result()
        rows = database.execute("SELECT status FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
        assert rows == [("ERROR",)]
    finally:
        DBOS.destroy(destroy_registry=True)


def test_a_failure_during_destroy_leaves_the_workflow_to_recover(database: Database) -> None:
    DBOS.destroy(destroy_registry=True)
    started = threading.Event()
    release = threading.Event()

    @DBOS.workflow()
    def fails_on_shutdown() -> None:
        started.set()
        release.wait()
        raise DBOSException("a dependency closed during shutdown")

    DBOS(config={"name": "t", "system_database_url": database.url, "recovery_interval_seconds": 3600.0, "shutdown_timeout_seconds": 5.0})
    DBOS.launch()
    handle = DBOS.start_workflow(fails_on_shutdown)
    assert started.wait(10)
    destroyer = threading.Thread(target=DBOS.destroy)
    destroyer.start()
    time.sleep(0.2)
    release.set()
    destroyer.join(30)
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
    assert rows == [("PENDING", None)]


async def test_an_async_body_stopped_by_a_base_exception_is_not_stored(dbos: DBOS, database: Database) -> None:
    class Stop(BaseException):
        pass

    @DBOS.workflow()
    async def stopped() -> None:
        raise Stop

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id), pytest.raises(Stop):
        await stopped()
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert rows == [("PENDING", None)]


_DYING_LOOP_APP = """
import asyncio, sys, time
from rust_dwos import DBOS

@DBOS.workflow(name="uses_a_thread")
async def uses_a_thread() -> int:
    await asyncio.to_thread(time.sleep, 0.01)
    return 1

async def main() -> None:
    DBOS(config={"name": "app", "system_database_url": sys.argv[1], "lease_seconds": 1.0, "recovery_interval_seconds": 0.05})
    DBOS.launch()
    asyncio.create_task(asyncio.to_thread(time.sleep, 2))  # keeps the closing loop running
    await asyncio.sleep(0)
    sys.exit(0)

asyncio.run(main())
"""


def test_a_workflow_recovered_while_the_app_loop_closes_is_not_stored_as_an_error(config: DBOSConfig, database: Database) -> None:
    DBOS.destroy(destroy_registry=True)

    @DBOS.workflow(name="uses_a_thread")
    async def uses_a_thread() -> int:
        return 1

    DBOS(config=config)
    DBOS.launch()
    handle = DBOS.start_workflow(uses_a_thread)
    assert handle.get_result() == 1
    DBOS.destroy(destroy_registry=True)
    database.expire_lease(handle.workflow_id)

    subprocess.run([sys.executable, "-c", _DYING_LOOP_APP, database.url], check=True, timeout=30)
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
    assert rows[0][0] != "ERROR", rows[0][1]


_EXITING_APP = """
import asyncio, sys, time
from rust_dwos import DBOS, SetWorkflowID

@DBOS.workflow()
async def keeps_using_threads() -> None:
    while True:
        await asyncio.to_thread(time.sleep, 0.01)

DBOS(config={"name": "app", "system_database_url": sys.argv[1]})
DBOS.launch()
with SetWorkflowID(sys.argv[2]):
    DBOS.start_workflow(keeps_using_threads)
time.sleep(0.3)
"""


def test_thread_executors_stopping_at_exit_is_not_stored_as_an_error(database: Database) -> None:
    workflow_id = str(uuid.uuid4())
    subprocess.run([sys.executable, "-c", _EXITING_APP, database.url, workflow_id], check=True, timeout=30)
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert rows[0][0] == "PENDING", rows[0][1]


def test_a_cancelled_workflow_raises_when_awaited(dbos: DBOS, database: Database) -> None:
    @DBOS.workflow()
    def workflow() -> int:
        return 1

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    database.execute("UPDATE workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = %s", handle.workflow_id)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(handle.workflow_id).get_result()
