from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from typing import Any

from rust_dwos import _native
from rust_dwos.errors import DBOSException, DBOSStreamTimeoutError
from rust_dwos.events import WorkflowSerializationFormat, _check_outcome, _serialization
from rust_dwos.serialization import Codec

_PLAIN = Codec(lambda: None)


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def _writer(name: str) -> tuple[Any, _native.Run, int | None]:
    """The running instance, the workflow run, and the step ID that records the write (None inside a step)."""
    dbos = _dbos()
    run = dbos._current_run.get()
    if run is None:
        raise DBOSException(f"{name}() must be called from within a workflow or step")
    return dbos._running(), run, run.step_id_unless_in_step(dbos._current_step.get() is not None)


def _no_shard(shard_key: str | None) -> None:
    if shard_key is not None:
        raise DBOSException("shard_key is not supported")


def _value(item: _native.StreamItem, workflow_id: str, key: str, offset: int) -> Any:
    if item.kind != "value":
        raise DBOSStreamTimeoutError(workflow_id, key, offset)
    return _PLAIN.decode_output(item.value, item.serialization)


class Streams:
    @classmethod
    def write_stream(cls, key: str, value: Any, *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT) -> None:
        instance, run, step_id = _writer("write_stream")
        serialization = _serialization(instance, run, serialization_type)
        text = _PLAIN.encode_output(value, serialization)
        _check_outcome(instance._blocking(instance._engine.write_stream, run, key, text, serialization, step_id), serialization)

    @classmethod
    async def write_stream_async(cls, key: str, value: Any, *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT) -> None:
        instance, run, step_id = _writer("write_stream")
        serialization = _serialization(instance, run, serialization_type)
        text = _PLAIN.encode_output(value, serialization)
        _check_outcome(await instance._engine.write_stream(run, key, text, serialization, step_id), serialization)

    @classmethod
    def close_stream(cls, key: str) -> None:
        instance, run, step_id = _writer("close_stream")
        _check_outcome(instance._blocking(instance._engine.close_stream, run, key, step_id), run.serialization)

    @classmethod
    async def close_stream_async(cls, key: str) -> None:
        instance, run, step_id = _writer("close_stream")
        _check_outcome(await instance._engine.close_stream(run, key, step_id), run.serialization)

    @classmethod
    def read_stream(cls, workflow_id: str, key: str, *, offset: int = 0, polling_interval_sec: float | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> Generator[Any, Any, None]:
        _no_shard(shard_key)
        instance = _dbos()._running()
        while True:
            item = instance._waiting(instance._engine.read_stream, workflow_id, key, offset, timeout_seconds, polling_interval_sec)
            if item.kind == "closed":
                return
            yield _value(item, workflow_id, key, offset)
            offset += 1

    @classmethod
    async def read_stream_async(cls, workflow_id: str, key: str, *, offset: int = 0, polling_interval_sec: float | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> AsyncGenerator[Any, None]:
        _no_shard(shard_key)
        engine = _dbos()._running()._engine
        while True:
            item = await engine.read_stream(workflow_id, key, offset, timeout_seconds, polling_interval_sec)
            if item.kind == "closed":
                return
            yield _value(item, workflow_id, key, offset)
            offset += 1

    @classmethod
    def read_stream_offset(cls, workflow_id: str, key: str, offset: int, *, polling_interval_sec: float | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> Any:
        _no_shard(shard_key)
        instance = _dbos()._running()
        return _value(instance._waiting(instance._engine.read_stream, workflow_id, key, offset, timeout_seconds, polling_interval_sec), workflow_id, key, offset)

    @classmethod
    async def read_stream_offset_async(cls, workflow_id: str, key: str, offset: int, *, polling_interval_sec: float | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> Any:
        _no_shard(shard_key)
        item = await _dbos()._running()._engine.read_stream(workflow_id, key, offset, timeout_seconds, polling_interval_sec)
        return _value(item, workflow_id, key, offset)
