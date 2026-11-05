import asyncio
import concurrent.futures
import gc
import logging
import sys
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from rust_dwos import DBOS, DBOSAwaitedWorkflowCancelledError, SetWorkflowID
from tests.conftest import retry_until_success

_BOUND = 15.0  # Seconds. A blocked body would hold get_result until the 60s test timeout.
_PAST_THE_POLL = 0.5  # The stored-row poll backs off to 250ms at most.


class _RecordingEngine:
    """Keeps every await_result future, to show the wait stops when the body wins."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self.waits: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._engine, name)

    def await_result(self, workflow_id: str) -> Any:
        wait = self._engine.await_result(workflow_id)
        self.waits.append(wait)
        return wait


def _blocked_workflow(release: threading.Event, entered: threading.Event) -> Callable[[], str]:
    @DBOS.workflow()
    def blocked() -> str:
        entered.set()
        release.wait()
        return "body finished"

    return blocked


def _delay_after_error_row(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    write_row = dbos._finish_error_sync

    def delayed(*args: Any) -> Any:
        try:
            return write_row(*args)
        finally:
            time.sleep(_PAST_THE_POLL)

    monkeypatch.setattr(dbos, "_finish_error_sync", delayed)


def test_cancel_raises_while_the_sync_body_is_blocked(dbos: DBOS, gate: Callable[[], threading.Event]) -> None:
    release, entered = gate(), threading.Event()
    blocked = _blocked_workflow(release, entered)
    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(blocked)
    assert entered.wait(_BOUND)
    DBOS.cancel_workflow(workflow_id)
    started = time.monotonic()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert time.monotonic() - started < _BOUND
    assert not handle._local.done()

    release.set()
    concurrent.futures.wait([handle._local], timeout=_BOUND)
    assert handle._local.done()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert DBOS.get_workflow_status(workflow_id).status == "CANCELLED"  # type: ignore[union-attr]


async def test_cancel_raises_while_a_sync_body_started_async_is_blocked(dbos: DBOS, gate: Callable[[], threading.Event], caplog: pytest.LogCaptureFixture) -> None:
    release, entered = gate(), threading.Event()
    blocked = _blocked_workflow(release, entered)
    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(blocked)
    assert isinstance(handle._local, concurrent.futures.Future)
    assert await asyncio.to_thread(entered.wait, _BOUND)
    await DBOS.cancel_workflow_async(workflow_id)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await asyncio.wait_for(handle.get_result(), _BOUND)
    assert not handle._local.done()

    # The body's late error lands on the future get_result left behind. It must not log as unretrieved.
    with caplog.at_level(logging.ERROR, logger="asyncio"):
        release.set()
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await asyncio.wait_for(asyncio.wrap_future(handle._local), _BOUND)
        await asyncio.sleep(0)
        gc.collect()
    assert "never retrieved" not in caplog.text


async def test_cancel_raises_while_an_async_body_ignores_cancellation(dbos: DBOS) -> None:
    release, entered = asyncio.Event(), asyncio.Event()

    @DBOS.workflow()
    async def stubborn() -> str:
        entered.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                pass
        return "body finished"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(stubborn)
    assert isinstance(handle._local, asyncio.Task)
    try:
        await asyncio.wait_for(entered.wait(), _BOUND)
        await DBOS.cancel_workflow_async(workflow_id)
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await asyncio.wait_for(handle.get_result(), _BOUND)
        assert not handle._local.done()
    finally:
        release.set()
    await asyncio.gather(handle._local, return_exceptions=True)


def test_resume_while_the_old_body_is_blocked_follows_the_stored_row(dbos: DBOS, gate: Callable[[], threading.Event]) -> None:
    release, entered = gate(), threading.Event()
    runs = 0

    @DBOS.workflow()
    def resumable() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            entered.set()
            release.wait()
        return "second run"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(resumable)
    assert entered.wait(_BOUND)
    DBOS.cancel_workflow(workflow_id)
    assert DBOS.resume_workflow(workflow_id).get_result() == "second run"
    assert handle.get_result() == "second run"
    assert not handle._local.done()


def test_a_finished_body_keeps_its_own_exception(dbos: DBOS) -> None:
    error = ValueError("kept")

    @DBOS.workflow()
    def fails() -> None:
        raise error

    handle = DBOS.start_workflow(fails)
    with pytest.raises(ValueError) as raised:
        handle.get_result()
    assert raised.value is error


def test_an_error_row_written_just_before_the_body_finishes_keeps_the_exception(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    _delay_after_error_row(dbos, monkeypatch)
    error = ValueError("kept")

    @DBOS.workflow()
    def fails() -> None:
        raise error

    with pytest.raises(ValueError) as raised:
        DBOS.start_workflow(fails).get_result()
    assert raised.value is error


async def test_an_error_row_written_just_before_the_body_finishes_keeps_the_exception_async(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    _delay_after_error_row(dbos, monkeypatch)
    error = ValueError("kept")

    @DBOS.workflow()
    def fails() -> None:
        raise error

    handle = await DBOS.start_workflow_async(fails)
    with pytest.raises(ValueError) as raised:
        await handle.get_result()
    assert raised.value is error


# sys.exit releases the row without a final state, so only a cancel can end the stored wait.
def test_the_stored_wait_stops_when_the_sync_body_wins(dbos: DBOS, monkeypatch: pytest.MonkeyPatch, gate: Callable[[], threading.Event]) -> None:
    engine = _RecordingEngine(dbos._engine)
    monkeypatch.setattr(dbos, "_engine", engine)
    release = gate()

    @DBOS.workflow()
    def exits() -> None:
        release.wait()
        sys.exit(0)

    handle = DBOS.start_workflow(exits)
    timer = threading.Timer(_PAST_THE_POLL, release.set)
    timer.start()
    with pytest.raises(SystemExit):
        handle.get_result()
    timer.join()

    def stopped() -> None:
        assert len(engine.waits) == 1
        assert engine.waits[0].cancelled()
        assert not asyncio.all_tasks(dbos._state._loop)

    retry_until_success(stopped, interval=0.05, max_attempts=100)


async def test_the_stored_wait_stops_when_the_async_body_wins(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _RecordingEngine(dbos._engine)
    monkeypatch.setattr(dbos, "_engine", engine)
    release = asyncio.Event()

    @DBOS.workflow()
    async def exits() -> None:
        await release.wait()
        sys.exit(0)

    handle = await DBOS.start_workflow_async(exits)
    asyncio.get_running_loop().call_later(_PAST_THE_POLL, release.set)
    assert await handle.get_result() is None
    assert len(engine.waits) == 1
    assert engine.waits[0].cancelled()


async def test_cancelling_get_result_stops_the_stored_wait_and_keeps_the_body(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _RecordingEngine(dbos._engine)
    monkeypatch.setattr(dbos, "_engine", engine)
    release = asyncio.Event()

    @DBOS.workflow()
    async def waits() -> str:
        await release.wait()
        return "body finished"

    handle = await DBOS.start_workflow_async(waits)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(handle.get_result(), _PAST_THE_POLL)
    assert engine.waits[0].cancelled()
    assert not handle._local.done()
    release.set()
    assert await handle.get_result() == "body finished"


def test_destroy_during_get_result_does_not_strand_the_caller(dbos: DBOS, gate: Callable[[], threading.Event]) -> None:
    release, entered = gate(), threading.Event()
    handle = DBOS.start_workflow(_blocked_workflow(release, entered))
    assert entered.wait(_BOUND)
    ended: list[BaseException | str] = []

    def wait_for_result() -> None:
        try:
            ended.append(handle.get_result())
        except BaseException as error:  # noqa: BLE001
            ended.append(error)

    waiter = threading.Thread(target=wait_for_result, daemon=True)
    waiter.start()
    time.sleep(_PAST_THE_POLL)
    timer = threading.Timer(_PAST_THE_POLL, release.set)
    timer.start()
    DBOS.destroy()
    timer.join()
    waiter.join(_BOUND)
    assert ended
