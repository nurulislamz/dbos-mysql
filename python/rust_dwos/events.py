from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from enum import Enum

from rust_dwos import _native
from rust_dwos.errors import DBOSException, DBOSNonExistentWorkflowError
from rust_dwos.serialization import Codec, decode_exception

_PLAIN = Codec(lambda: None)
_SEND_STEP = "DBOS.send"
_GET_EVENTS_STEP = "DBOS.get_events"


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def _timeout_ms(timeout_seconds: float) -> int:
    return 0 if timeout_seconds <= 0 else int(timeout_seconds * 1000)


def _decode(poll: _native.Poll) -> Any:
    return _PLAIN.decode_output(poll.value, poll.serialization)


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


class WorkflowSerializationFormat(str, Enum):
    PORTABLE = "portable"
    NATIVE = "native"
    DEFAULT = None


@dataclass
class SendMessage:
    destination_id: str
    message: Any
    topic: str | None = None
    idempotency_key: str | None = None
    shard_key: str | None = None


def _serialization(instance: Any, run: Any, kind: WorkflowSerializationFormat | None) -> str:
    if kind == WorkflowSerializationFormat.PORTABLE:
        return _native.PORTABLE_JSON
    if kind not in (None, WorkflowSerializationFormat.DEFAULT, WorkflowSerializationFormat.NATIVE):
        raise DBOSException(f"Invalid serialization_type: {kind}")
    if kind == WorkflowSerializationFormat.NATIVE:
        return instance._engine.serialization
    return run.serialization if run is not None else instance._engine.serialization


def _bulk_args(messages: list[SendMessage], serialization: str) -> list[tuple[str, str | None, str, str | None]]:
    if any(message.shard_key is not None for message in messages):
        raise DBOSException("shard_key is not supported")
    return [(m.destination_id, m.topic, _PLAIN.encode_output(m.message, serialization), m.idempotency_key) for m in messages]


class Events:
    @classmethod
    def send_bulk(cls, messages: list[SendMessage], *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False) -> None:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = _serialization(instance, run, serialization_type)
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        encoded = _bulk_args(messages, serialization)
        try:
            send = instance._engine.send_bulk_to_forks if send_to_forks else instance._engine.send_bulk
            result = instance._blocking(send, encoded, serialization, run, step_id)
        except DBOSNonExistentWorkflowError as error:
            if step_id is not None:
                instance._save_failure_sync(run, step_id, "DBOS.send_bulk", error)
            raise
        _check_outcome(result, serialization)

    @classmethod
    async def send_bulk_async(cls, messages: list[SendMessage], *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False) -> None:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = _serialization(instance, run, serialization_type)
        step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        encoded = _bulk_args(messages, serialization)
        try:
            send = instance._engine.send_bulk_to_forks if send_to_forks else instance._engine.send_bulk
            result = await send(encoded, serialization, run, step_id)
        except DBOSNonExistentWorkflowError as error:
            if step_id is not None:
                await instance._save_failure(run, step_id, "DBOS.send_bulk", error)
            raise
        _check_outcome(result, serialization)

    @classmethod
    def send(cls, destination_id: str, message: Any, topic: str | None = None, *, idempotency_key: str | None = None, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False, shard_key: str | None = None) -> None:
        if send_to_forks:
            return cls.send_bulk([SendMessage(destination_id, message, topic, idempotency_key, shard_key)], serialization_type=serialization_type, send_to_forks=True)
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        if shard_key is not None:
            raise DBOSException("shard_key is not supported")
        serialization = _serialization(instance, run, serialization_type)
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
    async def send_async(cls, destination_id: str, message: Any, topic: str | None = None, *, idempotency_key: str | None = None, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False, shard_key: str | None = None) -> None:
        if send_to_forks:
            return await cls.send_bulk_async([SendMessage(destination_id, message, topic, idempotency_key, shard_key)], serialization_type=serialization_type, send_to_forks=True)
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        if shard_key is not None:
            raise DBOSException("shard_key is not supported")
        serialization = _serialization(instance, run, serialization_type)
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
        return _decode(instance._waiting(instance._engine.wait_recv, run, step_id, timeout_id, _timeout_ms(timeout_seconds), topic))

    @classmethod
    async def recv_async(cls, topic: str | None = None, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        run = dbos._current_run.get()
        step_id, timeout_id = _recv_ids(run, dbos._current_step.get() is not None)
        return _decode(await dbos._running()._engine.wait_recv(run, step_id, timeout_id, _timeout_ms(timeout_seconds), topic))

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
        return _decode(instance._waiting(instance._engine.wait_event, workflow_id, key, _timeout_ms(timeout_seconds), run, step_id, timeout_id))

    @classmethod
    async def get_event_async(cls, workflow_id: str, key: str, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        engine = dbos._running()._engine
        run = dbos._current_run.get()
        step_id, timeout_id = _event_ids(run, dbos._current_step.get() is not None)
        return _decode(await engine.wait_event(workflow_id, key, _timeout_ms(timeout_seconds), run, step_id, timeout_id))

    @classmethod
    def get_all_events(cls, workflow_id: str) -> dict[str, Any]:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        step_id = _native.step_id_for_call(run, dbos._current_step.get() is not None)
        replay = instance._blocking(instance._engine.begin_step, run, _GET_EVENTS_STEP, step_id) if step_id is not None else None
        if replay is not None:
            return dbos._replay_step(replay, _PLAIN, run.serialization)
        events = _events_of(instance._blocking(instance._engine.list_events, workflow_id))
        return events if step_id is None else instance._save_success_sync(run, step_id, _GET_EVENTS_STEP, _PLAIN, events)

    @classmethod
    async def get_all_events_async(cls, workflow_id: str) -> dict[str, Any]:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        step_id = _native.step_id_for_call(run, dbos._current_step.get() is not None)
        replay = await instance._engine.begin_step(run, _GET_EVENTS_STEP, step_id) if step_id is not None else None
        if replay is not None:
            return dbos._replay_step(replay, _PLAIN, run.serialization)
        events = _events_of(await instance._engine.list_events(workflow_id))
        return events if step_id is None else await instance._save_success(run, step_id, _GET_EVENTS_STEP, _PLAIN, events)


def _events_of(rows: list[_native.EventRow]) -> dict[str, Any]:
    return {row.key: _PLAIN.decode_output(row.value, row.serialization) for row in rows}
