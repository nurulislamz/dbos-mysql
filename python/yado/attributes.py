from __future__ import annotations

import contextvars
import json
from typing import Any

from yado.errors import DBOSException
from yado.events import _check_outcome

_attributes: contextvars.ContextVar[str | None] = contextvars.ContextVar("workflow_attributes", default=None)


def _encode(attributes: dict[str, Any] | None) -> str | None:
    if attributes is None:
        return None
    if not isinstance(attributes, dict):
        raise DBOSException(f"Invalid workflow attributes {attributes}. Attributes must be a dict.")
    try:
        return json.dumps(attributes, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise DBOSException(f"Invalid workflow attributes {attributes}. Attributes must be JSON-serializable.") from error


class SetWorkflowAttributes:
    def __init__(self, attributes: dict[str, Any] | None) -> None:
        self._attributes = _encode(attributes)
        self._token: contextvars.Token[str | None] | None = None

    def __enter__(self) -> SetWorkflowAttributes:
        self._token = _attributes.set(self._attributes)
        return self

    def __exit__(self, *_: object) -> None:
        if self._token is not None:
            _attributes.reset(self._token)


def start_attributes() -> dict[str, str]:
    value = _attributes.get()
    return {"attributes": value} if value is not None else {}


def _dbos() -> Any:
    from yado import dbos
    return dbos


def _args(workflow_id: str, attributes: dict[str, Any] | None, merge: bool, shard_key: str | None) -> tuple[Any, ...]:
    if shard_key is not None:
        raise DBOSException("shard_key is not supported")
    if merge and attributes is None:
        raise DBOSException("Cannot merge None into workflow attributes")
    text = _encode(attributes)
    dbos = _dbos()
    run = dbos._current_run.get()
    step_id = run.step_id_unless_in_step(dbos._current_step.get() is not None) if run is not None else None
    return workflow_id, text, merge, run, step_id


class WorkflowAttributes:
    @classmethod
    def update_workflow_attributes(cls, workflow_id: str, attributes: dict[str, Any] | None, *, merge: bool = False, shard_key: str | None = None) -> None:
        args = _args(workflow_id, attributes, merge, shard_key)
        instance = _dbos()._running()
        result = instance._blocking(instance._engine.update_workflow_attributes, *args)
        run = args[3]
        _check_outcome(result, run.serialization if run is not None else instance._engine.serialization)

    @classmethod
    async def update_workflow_attributes_async(cls, workflow_id: str, attributes: dict[str, Any] | None, *, merge: bool = False, shard_key: str | None = None) -> None:
        args = _args(workflow_id, attributes, merge, shard_key)
        instance = _dbos()._running()
        result = await instance._engine.update_workflow_attributes(*args)
        run = args[3]
        _check_outcome(result, run.serialization if run is not None else instance._engine.serialization)
