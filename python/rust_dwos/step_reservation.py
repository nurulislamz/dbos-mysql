from __future__ import annotations

import contextvars
import threading
from typing import Any, Callable

from rust_dwos.errors import DBOSException

_reserved_step: contextvars.ContextVar[int | None] = contextvars.ContextVar("dbos_reserved_step", default=None)


class StepReservation:
    def __init__(self, run: Any, step_id: int) -> None:
        self._run = run
        self._step_id = step_id
        self._used = False
        self._lock = threading.Lock()

    @property
    def step_id(self) -> int:
        return self._step_id

    def _claim(self) -> None:
        from rust_dwos.dbos import _current_run, _current_step

        with self._lock:
            if self._used:
                raise DBOSException("A step reservation can only be run once")
            if _current_run.get() is not self._run or _current_step.get() is not None:
                raise DBOSException("A step reservation must be run from the reserving workflow, outside a step")
            self._used = True

    def run(self, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        from rust_dwos.dbos import DBOS

        self._claim()
        token = _reserved_step.set(self.step_id)
        try:
            return DBOS.run_step(dbos_step_options, func, *args, **kwargs)
        finally:
            _reserved_step.reset(token)

    async def run_async(self, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        from rust_dwos.dbos import DBOS

        self._claim()
        token = _reserved_step.set(self.step_id)
        try:
            return await DBOS.run_step_async(dbos_step_options, func, *args, **kwargs)
        finally:
            _reserved_step.reset(token)
