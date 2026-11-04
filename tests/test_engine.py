import time
import asyncio
import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from dbos_mysql import (
    DBOSException,
    DBOS,
    DBOSConfig,
    DBOSMaxStepRetriesExceeded,
    DBOSUnexpectedStepError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    SetWorkflowID,
    StoreError,
)


class PaymentDeclined(Exception):
    pass
from tests.conftest import Database
from tests.crash_app import register


@pytest.fixture
def fast_recovery(config: DBOSConfig) -> Iterator[None]:
    DBOS.destroy(destroy_registry=True)
    DBOS(config={**config, "lease_seconds": 1.0, "recovery_interval_seconds": 0.1})
    yield
    DBOS.destroy(destroy_registry=True)


@pytest.mark.parametrize("kind", ["async", "sync"])
def test_a_crashed_process_is_recovered_without_repeating_finished_steps(
    fast_recovery: None, database: Database, tmp_path: Path, kind: str
) -> None:
    ledger = tmp_path / "ledger.txt"
    workflow_id = str(uuid.uuid4())
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).parent.parent)}
    child = subprocess.run(
        [sys.executable, "-m", "tests.crash_app", database.url, workflow_id, kind, str(ledger)],
        env=environment,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert child.returncode == 9, child.stderr.decode()
    assert ledger.read_text().split() == ["one"]

    register(ledger, crash=False)
    DBOS.launch()

    handle = DBOS.retrieve_workflow(workflow_id)
    assert handle.get_result() == "onetwo"
    assert ledger.read_text().split() == ["one", "two"]
    assert handle.get_status().recovery_attempts == 2


def test_a_stale_owner_gets_the_new_owners_result(dbos: DBOS, database: Database) -> None:
    release = threading.Event()
    runs = 0

    @DBOS.workflow()
    def workflow() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            assert release.wait(10)
            return "stale"
        return "owner"

    stale = DBOS.start_workflow(workflow)
    database.expire_lease(stale.workflow_id)
    recovered = DBOS._recover_pending_workflows()
    assert len(recovered) == 1
    assert recovered[0].get_result() == "owner"

    release.set()
    assert stale.get_result() == "owner"
    assert DBOS.retrieve_workflow(stale.workflow_id).get_result() == "owner"


async def test_losing_the_lease_cancels_an_async_body(fast_recovery: None, database: Database) -> None:
    started = asyncio.Event()
    runs = 0
    cancelled = threading.Event()

    @DBOS.workflow()
    async def workflow() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return "owner"

    DBOS.launch()
    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        running = asyncio.ensure_future(workflow())
    await asyncio.wait_for(started.wait(), 10)
    database.expire_lease(workflow_id)

    assert await asyncio.wait_for(running, 15) == "owner"
    assert cancelled.is_set()
    assert runs == 2


def test_a_caught_custom_exception_is_still_that_type_after_recovery(dbos: DBOS, database: Database) -> None:
    started = threading.Event()
    release = threading.Event()

    @DBOS.step()
    def charge() -> None:
        raise PaymentDeclined("card declined")

    @DBOS.workflow()
    def pay() -> str:
        try:
            charge()
        except PaymentDeclined:
            path = "caught"
        else:
            path = "paid"
        started.set()
        assert release.wait(10)
        return path

    handle = DBOS.start_workflow(pay)
    assert started.wait(10)
    database.expire_lease(handle.workflow_id)
    recovered = DBOS._recover_pending_workflows()
    release.set()
    assert [item.get_result() for item in recovered] == ["caught"]
    assert handle.get_result() == "caught"


def test_too_many_recoveries_are_reported(dbos: DBOS, database: Database) -> None:
    release = threading.Event()

    @DBOS.workflow(max_recovery_attempts=0)
    def workflow() -> str:
        assert release.wait(10)
        return "done"

    handle = DBOS.start_workflow(workflow)
    database.expire_lease(handle.workflow_id)
    assert DBOS._recover_pending_workflows() == []

    with pytest.raises(DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        DBOS.retrieve_workflow(handle.workflow_id).get_result()
    release.set()
    with pytest.raises(DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        handle.get_result()


async def test_cancelling_an_inline_workflow_hands_it_over_without_using_an_attempt(dbos: DBOS) -> None:
    started = asyncio.Event()

    @DBOS.workflow()
    async def workflow() -> str:
        started.set()
        await asyncio.sleep(60)
        return "done"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        running = asyncio.ensure_future(workflow())
    await asyncio.wait_for(started.wait(), 10)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    status = await (await DBOS.retrieve_workflow_async(workflow_id)).get_status()
    assert status.status == "PENDING"
    assert status.recovery_attempts == 0


def test_step_retries_follow_the_policy_and_failures_replay(dbos: DBOS) -> None:
    attempts: list[int | None] = []

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3)
    def always_fails() -> None:
        status = DBOS.step_status
        assert status is not None
        assert status.max_attempts == 3
        attempts.append(status.current_attempt)
        raise ValueError("no")

    @DBOS.workflow()
    def workflow() -> None:
        always_fails()

    workflow_id = str(uuid.uuid4())
    for _ in range(2):
        with SetWorkflowID(workflow_id), pytest.raises(DBOSMaxStepRetriesExceeded):
            workflow()
    assert attempts == [0, 1, 2]


async def test_async_step_retries_stop_on_success(dbos: DBOS) -> None:
    calls = 0

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3)
    async def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise ValueError("no")
        return calls

    @DBOS.workflow()
    async def workflow() -> int:
        return await flaky()

    assert await workflow() == 2
    assert calls == 2


def test_invalid_step_policies_fail_at_decoration(dbos: DBOS) -> None:
    with pytest.raises(StoreError, match="max_attempts"):
        DBOS.step(retries_allowed=True, max_attempts=0)(lambda: None)


def test_child_workflows_replay_from_the_recorded_id(dbos: DBOS) -> None:
    child_runs = 0

    @DBOS.workflow()
    def child(value: int) -> int:
        nonlocal child_runs
        child_runs += 1
        return value + 1

    @DBOS.workflow()
    def parent() -> int:
        return child(1)

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert parent() == 2
    with SetWorkflowID(workflow_id):
        assert parent() == 2
    assert child_runs == 1

    steps = DBOS.list_workflow_steps(workflow_id)
    assert [step["child_workflow_id"] for step in steps] == [f"{workflow_id}-1"]
    assert DBOS.retrieve_workflow(f"{workflow_id}-1").get_result() == 2


async def test_gathered_steps_keep_stable_ids(dbos: DBOS) -> None:
    @DBOS.step()
    async def first() -> str:
        await asyncio.sleep(0.05)
        return "first"

    @DBOS.step()
    async def second() -> str:
        return "second"

    @DBOS.workflow()
    async def workflow() -> list[str]:
        return list(await asyncio.gather(first(), second()))

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert await workflow() == ["first", "second"]
    steps = await DBOS.list_workflow_steps_async(workflow_id)
    assert [step["function_id"] for step in steps] == [1, 2]
    assert steps[0]["function_name"].endswith("first")


def test_a_changed_step_is_rejected_on_recovery(dbos: DBOS, database: Database) -> None:
    release = threading.Event()
    runs = 0

    @DBOS.step(name="original")
    def original() -> None:
        return None

    @DBOS.step(name="replacement")
    def replacement() -> None:
        return None

    @DBOS.workflow()
    def workflow() -> None:
        nonlocal runs
        runs += 1
        if runs == 1:
            original()
            assert release.wait(10)
            return
        replacement()

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        stale = DBOS.start_workflow(workflow)
    for _ in range(200):
        if DBOS.list_workflow_steps(workflow_id):
            break
    database.expire_lease(workflow_id)
    recovered = DBOS._recover_pending_workflows()
    with pytest.raises(DBOSUnexpectedStepError):
        recovered[0].get_result()
    release.set()
    with pytest.raises(DBOSUnexpectedStepError):
        stale.get_result()


def test_a_blocked_sync_workflow_does_not_stop_the_process_exiting(database: Database) -> None:
    program = f"""
import threading
from dbos_mysql import DBOS

blocker = threading.Event()

@DBOS.workflow()
def blocked() -> None:
    blocker.wait()

DBOS(config={{"name": "t", "system_database_url": {database.url!r}}})
DBOS.launch()
DBOS.start_workflow(blocked)
print("started", flush=True)
"""
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=30, check=True)
    assert "started" in result.stdout


def test_destroy_stops_renewing_a_running_sync_workflows_lease(database: Database) -> None:
    DBOS.destroy(destroy_registry=True)
    release = threading.Event()
    started = threading.Event()

    @DBOS.workflow()
    def blocked() -> None:
        started.set()
        release.wait()

    DBOS(config={"name": "t", "system_database_url": database.url, "lease_seconds": 0.3, "shutdown_timeout_seconds": 0.1})
    DBOS.launch()
    handle = DBOS.start_workflow(blocked)
    assert started.wait(10)
    query = "SELECT claim_after FROM workflow_status WHERE workflow_uuid = %s"
    DBOS.destroy(destroy_registry=True)
    time.sleep(0.2)
    before = database.execute(query, handle.workflow_id)
    time.sleep(0.5)
    assert database.execute(query, handle.workflow_id) == before
    release.set()


def test_destroy_never_records_a_false_failure(database: Database) -> None:
    DBOS.destroy(destroy_registry=True)
    release = threading.Event()
    started = threading.Event()

    @DBOS.step()
    def after_release() -> str:
        return "done"

    @DBOS.workflow()
    def blocked() -> str:
        started.set()
        release.wait()
        return after_release()

    DBOS(config={"name": "t", "system_database_url": database.url, "shutdown_timeout_seconds": 0.1})
    DBOS.launch()
    handle = DBOS.start_workflow(blocked)
    assert started.wait(10)
    DBOS.destroy(destroy_registry=True)
    release.set()
    with pytest.raises(DBOSException, match="destroyed"):
        handle.get_result()
    rows = database.execute("SELECT status, error FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
    assert rows == [("PENDING", None)]


@pytest.mark.timeout(120)
def test_exiting_with_running_workflows_never_crashes(database: Database) -> None:
    program = f"""
import asyncio
import logging
import sys
import threading
import time
from dbos_mysql import DBOS

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
started = threading.Event()

@DBOS.workflow()
def blocked_sync() -> None:
    started.set()
    threading.Event().wait()

@DBOS.workflow()
async def blocked_async() -> None:
    await asyncio.sleep(60)

DBOS(config={{"name": "t", "system_database_url": {database.url!r}, "lease_seconds": 0.05, "shutdown_timeout_seconds": 0.05}})
DBOS.launch()
for _ in range(5):
    DBOS.start_workflow(blocked_sync)
    DBOS.start_workflow(blocked_async)
started.wait(10)
time.sleep(float(sys.argv[1]))
"""
    for delay in ("0", "0.02", "0.1") * 5:
        result = subprocess.run([sys.executable, "-c", program, delay], capture_output=True, text=True, timeout=30, check=False)
        assert result.returncode == 0, result.stderr[-2000:]


def test_rows_use_millisecond_times_and_record_their_format(dbos: DBOS, database: Database) -> None:
    @DBOS.step()
    def step() -> int:
        return 1

    @DBOS.workflow()
    def workflow() -> int:
        return step()

    before = int(time.time() * 1000) - 5000
    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    after = int(time.time() * 1000) + 5000

    rows = database.execute(
        "SELECT serialization, created_at, updated_at, completed_at, claim_after FROM workflow_status WHERE workflow_uuid = %s",
        handle.workflow_id,
    )
    steps = database.execute(
        "SELECT serialization, completed_at_epoch_ms FROM operation_outputs WHERE workflow_uuid = %s",
        handle.workflow_id,
    )
    serialization, *times, claim_after = rows[0]
    assert serialization == "py_pickle"
    assert all(before < moment < after for moment in times)
    assert claim_after is None
    assert steps[0][0] == "py_pickle"
    assert before < steps[0][1] < after
