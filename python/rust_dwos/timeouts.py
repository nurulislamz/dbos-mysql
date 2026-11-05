from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any

from rust_dwos.errors import DBOSException

_timeout_override: contextvars.ContextVar[tuple[int | None, bool] | None] = contextvars.ContextVar("dbos_timeout", default=None)


@dataclass(frozen=True)
class WorkflowStartOptions:
    workflow_timeout_ms: int | None = None
    workflow_deadline_epoch_ms: int | None = None


class SetWorkflowTimeout:
    """
    Set the workflow timeout (in seconds) to be used for the enclosed workflow invocations.

    Typical Usage
        ```
        with SetWorkflowTimeout(<timeout in seconds>):
            result = workflow_function(...)
        ```
    """

    def __init__(self, workflow_timeout_sec: float | None) -> None:
        if workflow_timeout_sec and not workflow_timeout_sec > 0:
            raise Exception(f"Invalid workflow timeout {workflow_timeout_sec}. Timeouts must be positive.")
        self.workflow_timeout_ms = int(workflow_timeout_sec * 1000) if workflow_timeout_sec is not None else None
        self._token: contextvars.Token[tuple[int | None, bool] | None] | None = None

    def __enter__(self) -> SetWorkflowTimeout:
        if self._token is not None:
            raise DBOSException(f"This {type(self).__name__} is already in use; create a new one for each with block.")
        self._token = _timeout_override.set((self.workflow_timeout_ms, True))
        return self

    def __exit__(self, *_: object) -> None:
        if self._token is not None:
            _timeout_override.reset(self._token)
            self._token = None


def timeout_start_args() -> tuple[int | None, bool]:
    return _timeout_override.get() or (None, False)


def get_workflow_start_options() -> WorkflowStartOptions:
    value = _timeout_override.get()
    return WorkflowStartOptions() if value is None else WorkflowStartOptions(workflow_timeout_ms=value[0])


def assert_current_dbos_context() -> Any:
    from rust_dwos.dbos import _current_run

    run = _current_run.get()
    if run is not None:
        return run
    assert _timeout_override.get() is not None
    return get_workflow_start_options()
