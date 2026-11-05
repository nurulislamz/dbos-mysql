from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from rust_dwos import _native
from rust_dwos.errors import DBOSException, DBOSNonExistentWorkflowError
from rust_dwos.serialization import Codec, decode_exception

_PLAIN = Codec(lambda: None)
_POLL_MIN = 0.01
_POLL_MAX = 1.0
_SEND_STEP = "DBOS.send"
_GET_EVENTS_STEP = "DBOS.get_events"


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def _timeout_ms(timeout_seconds: float) -> int:
    return 0 if timeout_seconds <= 0 else int(timeout_seconds * 1000)


def _decode(poll: _native.Poll) -> Any:
    return _PLAIN.decode_output(poll.value, poll.serialization)


# shortcut: the core only offers polling for these waits, so a value can arrive up to 1s before we see it. Upgrade when the core offers a wakeup.
def _sync_poll(once: Callable[[], _native.Poll]) -> Any:
    delay = _POLL_MIN
    while True:
        poll = once()
        if poll.kind == "ready":
            return _decode(poll)
        time.sleep(max(min(delay, poll.remaining_ms / 1000), 0))
        delay = min(delay * 2, _POLL_MAX)


async def _async_poll(once: Callable[[], Awaitable[_native.Poll]]) -> Any:
    delay = _POLL_MIN
    while True:
        poll = await once()
        if poll.kind == "ready":
            return _decode(poll)
        await asyncio.sleep(max(min(delay, poll.remaining_ms / 1000), 0))
        delay = min(delay * 2, _POLL_MAX)


def _check_outcome(result: _native.SendResult, serialization: str) -> None:
    if result.kind == "replayed_error":
        raise decode_exception(result.error, serialization)


def _recv_ids(run: _native.Run | None, in_step: bool) -> tuple[int, int]:
    if run is None or in_step:
        raise DBOSException("recv() must be called from within a workflow")
    return run.step_id_unless_in_step(False), run.step_id_unless_in_step(False)


def _event_ids(run: _native.Run | None, in_step: bool) -> tuple[int | None, int | None]:
    if run is None or in_step:
        return None, None
    return run.step_id_unless_in_step(False), run.step_id_unless_in_step(False)


class Events:
    @classmethod
    def send(cls, destination_id: str, message: Any, topic: str | None = None, *, idempotency_key: str | None = None) -> None:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = run.serialization if run is not None else instance._engine.serialization
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        text = _PLAIN.encode_output(message, serialization)
        try:
            result = instance._blocking(instance._engine.send, destination_id, text, serialization, topic, idempotency_key, run, step_id)
        except DBOSNonExistentWorkflowError as error:
            if step_id is not None:
                instance._save_failure_sync(run, step_id, _SEND_STEP, error)
            raise
        _check_outcome(result, serialization)

    @classmethod
    async def send_async(cls, destination_id: str, message: Any, topic: str | None = None, *, idempotency_key: str | None = None) -> None:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = run.serialization if run is not None else instance._engine.serialization
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        text = _PLAIN.encode_output(message, serialization)
        try:
            result = await instance._engine.send(destination_id, text, serialization, topic, idempotency_key, run, step_id)
        except DBOSNonExistentWorkflowError as error:
            if step_id is not None:
                await instance._save_failure(run, step_id, _SEND_STEP, error)
            raise
        _check_outcome(result, serialization)

    @classmethod
    def recv(cls, topic: str | None = None, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        run = dbos._current_run.get()
        step_id, timeout_id = _recv_ids(run, dbos._current_step.get() is not None)
        instance = dbos._running()
        timeout_ms = _timeout_ms(timeout_seconds)
        return _sync_poll(lambda: instance._blocking(instance._engine.poll_recv, run, step_id, timeout_id, timeout_ms, topic))

    @classmethod
    async def recv_async(cls, topic: str | None = None, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        run = dbos._current_run.get()
        step_id, timeout_id = _recv_ids(run, dbos._current_step.get() is not None)
        engine = dbos._running()._engine
        timeout_ms = _timeout_ms(timeout_seconds)
        return await _async_poll(lambda: engine.poll_recv(run, step_id, timeout_id, timeout_ms, topic))

    @classmethod
    def set_event(cls, key: str, value: Any) -> None:
        dbos = _dbos()
        run = dbos._current_run.get()
        if run is None:
            raise DBOSException("set_event() must be called from within a workflow")
        instance = dbos._running()
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None)
        text = _PLAIN.encode_output(value, run.serialization)
        _check_outcome(instance._blocking(instance._engine.set_event, run, key, text, run.serialization, step_id), run.serialization)

    @classmethod
    async def set_event_async(cls, key: str, value: Any) -> None:
        dbos = _dbos()
        run = dbos._current_run.get()
        if run is None:
            raise DBOSException("set_event() must be called from within a workflow")
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None)
        text = _PLAIN.encode_output(value, run.serialization)
        _check_outcome(await dbos._running()._engine.set_event(run, key, text, run.serialization, step_id), run.serialization)

    @classmethod
    def get_event(cls, workflow_id: str, key: str, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        step_id, timeout_id = _event_ids(run, dbos._current_step.get() is not None)
        timeout_ms = _timeout_ms(timeout_seconds)
        deadline_ms = None

        def once() -> _native.Poll:
            nonlocal deadline_ms
            poll = instance._blocking(instance._engine.poll_event, workflow_id, key, timeout_ms, deadline_ms, run, step_id, timeout_id)
            deadline_ms = poll.deadline_ms if poll.kind == "waiting" else deadline_ms
            return poll

        return _sync_poll(once)

    @classmethod
    async def get_event_async(cls, workflow_id: str, key: str, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        engine = dbos._running()._engine
        run = dbos._current_run.get()
        step_id, timeout_id = _event_ids(run, dbos._current_step.get() is not None)
        timeout_ms = _timeout_ms(timeout_seconds)
        deadline_ms = None

        async def once() -> _native.Poll:
            nonlocal deadline_ms
            poll = await engine.poll_event(workflow_id, key, timeout_ms, deadline_ms, run, step_id, timeout_id)
            deadline_ms = poll.deadline_ms if poll.kind == "waiting" else deadline_ms
            return poll

        return await _async_poll(once)

    @classmethod
    def get_all_events(cls, workflow_id: str) -> dict[str, Any]:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        step_id, replay = (None, None)
        if run is not None:
            step_id, replay = instance._blocking(instance._engine.begin_step, run, _GET_EVENTS_STEP, dbos._current_step.get() is not None)
        if replay is not None:
            return dbos._replay_step(replay, _PLAIN, run.serialization)
        events = _events_of(instance._blocking(instance._engine.list_events, workflow_id))
        return events if step_id is None else instance._save_success_sync(run, step_id, _GET_EVENTS_STEP, _PLAIN, events)

    @classmethod
    async def get_all_events_async(cls, workflow_id: str) -> dict[str, Any]:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        step_id, replay = (None, None)
        if run is not None:
            step_id, replay = await instance._engine.begin_step(run, _GET_EVENTS_STEP, dbos._current_step.get() is not None)
        if replay is not None:
            return dbos._replay_step(replay, _PLAIN, run.serialization)
        events = _events_of(await instance._engine.list_events(workflow_id))
        return events if step_id is None else await instance._save_success(run, step_id, _GET_EVENTS_STEP, _PLAIN, events)


def _events_of(rows: list[_native.EventRow]) -> dict[str, Any]:
    return {row.key: _PLAIN.decode_output(row.value, row.serialization) for row in rows}
