from __future__ import annotations

import contextvars
import logging
import os
from typing import Any

from rust_dwos.errors import DBOSException


class DBOSContext:
    logger = logging.getLogger("dbos")

    @property
    def app_id(self) -> str:
        return os.environ.get("DBOS__APPID", "")

    @property
    def workflow_id(self) -> str:
        from rust_dwos.dbos import DBOS
        return DBOS.workflow_id or ""

    @property
    def function_id(self) -> int:
        from rust_dwos.dbos import DBOS, _current_run
        run = _current_run.get()
        return DBOS.step_id if DBOS.step_id is not None else (run.last_step_id if run else -1)

    @property
    def curr_step_function_id(self) -> int:
        from rust_dwos.dbos import DBOS
        return DBOS.step_id if DBOS.step_id is not None else -1

    @property
    def workflow_deadline_epoch_ms(self) -> int | None:
        from rust_dwos.dbos import _current_run
        run = _current_run.get()
        return run.workflow_deadline_epoch_ms if run else None

    @property
    def executor_id(self) -> str:
        from rust_dwos.dbos import DBOS
        return DBOS.executor_id

    @property
    def step_status(self) -> Any:
        from rust_dwos.dbos import DBOS
        return DBOS.step_status

    def is_workflow(self) -> bool:
        return bool(self.workflow_id) and self.step_status is None

    def is_within_workflow(self) -> bool:
        return bool(self.workflow_id)

    def is_step(self) -> bool:
        return self.step_status is not None


_context: contextvars.ContextVar[DBOSContext | None] = contextvars.ContextVar("dbos_context", default=None)


class DBOSContextEnsure:
    def __init__(self) -> None:
        self._entered = False
        self._token: contextvars.Token[DBOSContext | None] | None = None
        self.created_ctx = False

    def __enter__(self) -> DBOSContext:
        if self._entered:
            raise DBOSException("This DBOSContextEnsure is already in use")
        self._entered = True
        self.created_ctx = False
        context = _context.get()
        if context is None:
            context = DBOSContext()
            self._token = _context.set(context)
            self.created_ctx = True
        return context

    def __exit__(self, *_: object) -> bool:
        if self._token is not None:
            _context.reset(self._token)
            self._token = None
        self._entered = False
        return False
