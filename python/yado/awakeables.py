from __future__ import annotations

from typing import Any

from yado.errors import DBOSException
from yado.events import _PLAIN, _dbos, _decode, _recv_ids, _timeout_ms


class Awakeables:
    @classmethod
    def create_awakeable(cls) -> str:
        dbos = _dbos()
        run = dbos._current_run.get()
        if run is None or dbos._current_step.get() is not None:
            raise DBOSException("create_awakeable() must be called from within a workflow")
        instance = dbos._running()
        return instance._blocking(instance._engine.create_awakeable, run, run.step_id_unless_in_step(False))

    @classmethod
    async def create_awakeable_async(cls) -> str:
        dbos = _dbos()
        run = dbos._current_run.get()
        if run is None or dbos._current_step.get() is not None:
            raise DBOSException("create_awakeable() must be called from within a workflow")
        return await dbos._running()._engine.create_awakeable(run, run.step_id_unless_in_step(False))

    @classmethod
    def wait_awakeable(cls, id: str, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        run = dbos._current_run.get()
        step, timeout = _recv_ids(run, dbos._current_step.get() is not None)
        instance = dbos._running()
        return _decode(instance._waiting(instance._engine.wait_awakeable, run, step, timeout, id, _timeout_ms(timeout_seconds)))

    @classmethod
    async def wait_awakeable_async(cls, id: str, timeout_seconds: float = 60) -> Any:
        dbos = _dbos()
        run = dbos._current_run.get()
        step, timeout = _recv_ids(run, dbos._current_step.get() is not None)
        return _decode(await dbos._running()._engine.wait_awakeable(run, step, timeout, id, _timeout_ms(timeout_seconds)))

    @classmethod
    def complete_awakeable(cls, id: str, value: Any) -> bool:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = run.serialization if run is not None else instance._engine.serialization
        step = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        return instance._blocking(instance._engine.complete_awakeable, id, _PLAIN.encode_output(value, serialization), serialization, run, step)

    @classmethod
    async def complete_awakeable_async(cls, id: str, value: Any) -> bool:
        dbos = _dbos()
        instance = dbos._running()
        run = dbos._current_run.get()
        serialization = run.serialization if run is not None else instance._engine.serialization
        step = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
        return await instance._engine.complete_awakeable(id, _PLAIN.encode_output(value, serialization), serialization, run, step)


class ClientAwakeables:
    def complete_awakeable(self, id: str, value: Any) -> bool:
        serialization = self._engine.serialization
        return self._engine.complete_awakeable(id, _PLAIN.encode_output(value, serialization), serialization, blocking=True)

    async def complete_awakeable_async(self, id: str, value: Any) -> bool:
        serialization = self._engine.serialization
        return await self._engine.complete_awakeable(id, _PLAIN.encode_output(value, serialization), serialization)
