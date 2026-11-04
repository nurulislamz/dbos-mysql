from __future__ import annotations

import builtins
import inspect
import json
import typing
from collections.abc import Callable
from typing import Any

from pydantic import TypeAdapter
from pydantic.errors import PydanticUserError
from pydantic_core import PydanticSerializationError, ValidationError

from dbos_mysql.errors import (
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSRecordedError,
    DBOSStepNondeterminismError,
    DBOSUnexpectedStepError,
    StoreError,
)

_ANY: TypeAdapter[Any] = TypeAdapter(Any)
_MISSING = object()
_OWN_ERRORS: tuple[type[DBOSException], ...] = (
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSNonExistentWorkflowError,
    DBOSStepNondeterminismError,
    DBOSUnexpectedStepError,
    StoreError,
)


def _adapter(hint: Any) -> TypeAdapter[Any]:
    if hint is _MISSING:
        return _ANY
    try:
        return TypeAdapter(hint)
    except (PydanticUserError, TypeError, NameError, ValueError):
        return _ANY


def _dumps(adapter: TypeAdapter[Any], value: Any) -> str:
    if inspect.isgenerator(value) or inspect.isasyncgen(value) or inspect.iscoroutine(value):
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored")
    try:
        return adapter.dump_json(value, warnings="error").decode()
    except (PydanticSerializationError, ValueError, TypeError):
        pass
    try:
        return _ANY.dump_json(value).decode()
    except (PydanticSerializationError, ValueError, TypeError) as error:
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored as JSON: {error}") from error


def _loads(adapter: TypeAdapter[Any], text: str) -> Any:
    try:
        return adapter.validate_json(text)
    except ValidationError:
        return json.loads(text)


class Codec:
    """Stores a function's inputs and output as JSON, typed from its annotations."""

    def __init__(self, function: Callable[..., Any]) -> None:
        self._function = inspect.unwrap(function)
        self._signature = inspect.signature(self._function)
        self._adapters: dict[str, TypeAdapter[Any]] | None = None

    def _resolved(self) -> dict[str, TypeAdapter[Any]]:
        if self._adapters is None:
            try:
                hints = typing.get_type_hints(self._function)
            except Exception:  # noqa: BLE001
                hints = {}
            names = [*self._signature.parameters, "return"]
            self._adapters = {name: _adapter(hints.get(name, _MISSING)) for name in names}
        return self._adapters

    def encode_input(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        adapters = self._resolved()
        payload: dict[str, Any] = {}
        for name, value in self._signature.bind(*args, **kwargs).arguments.items():
            kind = self._signature.parameters[name].kind
            if kind is inspect.Parameter.VAR_POSITIONAL:
                payload[name] = [json.loads(_dumps(adapters[name], item)) for item in value]
            elif kind is inspect.Parameter.VAR_KEYWORD:
                payload[name] = {key: json.loads(_dumps(adapters[name], item)) for key, item in value.items()}
            else:
                payload[name] = json.loads(_dumps(adapters[name], value))
        return json.dumps(payload)

    def decode_input(self, text: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
        adapters = self._resolved()
        bound = self._signature.bind_partial()
        for name, value in json.loads(text).items():
            kind = self._signature.parameters[name].kind
            if kind is inspect.Parameter.VAR_POSITIONAL:
                bound.arguments[name] = tuple(_loads(adapters[name], json.dumps(item)) for item in value)
            elif kind is inspect.Parameter.VAR_KEYWORD:
                bound.arguments[name] = {key: _loads(adapters[name], json.dumps(item)) for key, item in value.items()}
            else:
                bound.arguments[name] = _loads(adapters[name], json.dumps(value))
        return bound.args, bound.kwargs

    def encode_output(self, value: Any) -> str:
        return _dumps(self._resolved()["return"], value)

    def decode_output(self, text: str) -> Any:
        return _loads(self._resolved()["return"], text)


def encode_exception(error: BaseException) -> str:
    kind = type(error)
    try:
        args = json.loads(_ANY.dump_json(list(error.args)))
    except (PydanticSerializationError, ValueError, TypeError):
        args = None
    payload: dict[str, Any] = {"module": kind.__module__, "type": kind.__qualname__, "message": str(error), "args": args}
    if isinstance(error, DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        payload["workflow_id"] = error.workflow_id
    if isinstance(error, DBOSMaxStepRetriesExceeded):
        payload["step_name"] = error.step_name
        payload["max_retries"] = error.max_retries
    return json.dumps(payload)


def decode_exception(text: str) -> Exception:
    payload = json.loads(text)
    type_name = payload["type"]
    if payload["module"] == DBOSMaxStepRetriesExceeded.__module__ and type_name == DBOSMaxStepRetriesExceeded.__qualname__:
        return DBOSMaxStepRetriesExceeded(payload["step_name"], payload["max_retries"])
    if payload["module"] == DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded.__module__ and type_name == DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded.__qualname__:
        return DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded(payload["workflow_id"])
    for own in _OWN_ERRORS:
        if payload["module"] == own.__module__ and type_name == own.__qualname__:
            return own(payload["message"])
    kind = getattr(builtins, type_name, None) if payload["module"] == "builtins" else None
    if isinstance(kind, type) and issubclass(kind, Exception):
        try:
            return kind(*payload["args"]) if payload["args"] is not None else kind(payload["message"])
        except Exception:  # noqa: BLE001
            pass
    return DBOSRecordedError(type_name, payload["message"])
