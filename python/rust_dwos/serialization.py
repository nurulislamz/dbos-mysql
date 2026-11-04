from __future__ import annotations

import base64
import inspect
import json
import pickle
import sys
import typing
from collections.abc import Callable, Collection
from typing import Any

from pydantic import TypeAdapter
from pydantic.errors import PydanticUserError
from pydantic_core import PydanticSerializationError, ValidationError

from rust_dwos import errors
from rust_dwos._native import PORTABLE_JSON, PY_PICKLE
from rust_dwos.errors import DBOSException, DBOSMaxStepRetriesExceeded, DBOSRecordedError

_ANY: TypeAdapter[Any] = TypeAdapter(Any)
_MISSING = object()
_pickle_allowed = False


def allow_pickle(allowed: bool) -> None:
    global _pickle_allowed
    _pickle_allowed = allowed


def _uses_pickle(serialization: str) -> bool:
    # The only way to reach pickle. A row's tag alone never turns it on, because loading a pickle can run code.
    if serialization == PORTABLE_JSON:
        return False
    if serialization != PY_PICKLE:
        raise DBOSException(f"A stored value has the unknown format {serialization!r}, so it was not loaded")
    if not _pickle_allowed:
        raise DBOSException('A stored value uses pickle, so it was not loaded. Set "serializer": "pickle" in the DBOS config only if you trust this database')
    return True


def _pickle_dumps(value: Any) -> str:
    if inspect.isgenerator(value) or inspect.isasyncgen(value) or inspect.iscoroutine(value):
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored")
    try:
        return base64.b64encode(pickle.dumps(value)).decode("ascii")
    except (TypeError, AttributeError, pickle.PicklingError) as error:
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored with pickle: {error}") from error


def _pickle_loads(text: str) -> Any:
    try:
        return pickle.loads(base64.b64decode(text))  # noqa: S301
    except Exception as error:  # noqa: BLE001
        raise DBOSException(f"A stored pickle could not be loaded: {error}") from error


def _adapter(hint: Any) -> TypeAdapter[Any]:
    if hint is _MISSING:
        return _ANY
    try:
        return TypeAdapter(hint)
    except (PydanticUserError, TypeError, NameError, ValueError):
        return _ANY


def _collection_items(value: Any) -> Any:
    return list(value) if isinstance(value, Collection) and not isinstance(value, bytes | bytearray | memoryview) else None


def _dumps(adapter: TypeAdapter[Any], value: Any) -> str:
    if inspect.isgenerator(value) or inspect.isasyncgen(value) or inspect.iscoroutine(value):
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored")
    try:
        text = adapter.dump_json(value, warnings="error").decode()
    except (PydanticSerializationError, ValueError, TypeError):
        try:
            text = _ANY.dump_json(value).decode()
        except (PydanticSerializationError, ValueError, TypeError) as error:
            raise TypeError(f"A value of type {type(value).__name__} cannot be stored as JSON: {error}") from error
    try:
        # Pydantic writes NaN and infinities as null, which would silently change the value.
        json.dumps(_ANY.dump_python(value, warnings="none"), allow_nan=False, skipkeys=True, default=_collection_items)
    except ValueError as error:
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored as JSON: {error}") from error
    return text


def _parse(text: str) -> Any:
    try:
        return json.loads(text)
    except ValueError as error:
        raise DBOSException("A stored value is not valid JSON, so it was not loaded") from error


def _loads(adapter: TypeAdapter[Any], text: str) -> Any:
    try:
        return adapter.validate_json(text)
    except ValidationError:
        return _parse(text)


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

    def _convert(self, bound: inspect.BoundArguments, convert: Callable[[TypeAdapter[Any], Any], Any]) -> None:
        adapters = self._resolved()
        for name, value in bound.arguments.items():
            kind = self._signature.parameters[name].kind
            if kind is inspect.Parameter.VAR_POSITIONAL:
                bound.arguments[name] = tuple(convert(adapters[name], item) for item in value)
            elif kind is inspect.Parameter.VAR_KEYWORD:
                bound.arguments[name] = {key: convert(adapters[name], item) for key, item in value.items()}
            else:
                bound.arguments[name] = convert(adapters[name], value)

    def encode_input(self, args: tuple[Any, ...], kwargs: dict[str, Any], serialization: str = PORTABLE_JSON) -> str:
        bound = self._signature.bind(*args, **kwargs)
        if _uses_pickle(serialization):
            return _pickle_dumps((bound.args, bound.kwargs))
        self._convert(bound, lambda adapter, value: json.loads(_dumps(adapter, value)))
        return json.dumps({"positionalArgs": list(bound.args), "namedArgs": bound.kwargs})

    def decode_input(self, text: str, serialization: str = PORTABLE_JSON) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if _uses_pickle(serialization):
            args, kwargs = _pickle_loads(text)
            return tuple(args), dict(kwargs)
        payload = _parse(text)
        if not isinstance(payload, dict):
            raise DBOSException("A stored workflow input has the wrong shape, so it was not loaded")
        bound = self._signature.bind(*(payload.get("positionalArgs") or ()), **(payload.get("namedArgs") or {}))
        self._convert(bound, lambda adapter, value: _loads(adapter, json.dumps(value)))
        return bound.args, bound.kwargs

    def encode_output(self, value: Any, serialization: str = PORTABLE_JSON) -> str:
        if _uses_pickle(serialization):
            return _pickle_dumps(value)
        return _dumps(self._resolved()["return"], value)

    def decode_output(self, text: str, serialization: str = PORTABLE_JSON) -> Any:
        if _uses_pickle(serialization):
            return _pickle_loads(text)
        return _loads(self._resolved()["return"], text)


def _plain(value: Any) -> Any:
    return json.loads(_dumps(_ANY, value))


def _exception_payload(error: BaseException) -> dict[str, Any]:
    try:
        args = _plain(list(error.args))
    except TypeError:
        args = None
    attributes = {}
    for name, value in vars(error).items():
        try:
            attributes[name] = _plain(value)
        except TypeError:
            continue
    kind = type(error)
    data: dict[str, Any] = {"module": kind.__module__, "args": args, "attributes": attributes}
    if isinstance(error, DBOSMaxStepRetriesExceeded):
        attributes.pop("errors", None)
        data["errors"] = [_exception_payload(item) for item in error.errors]
    return {"name": kind.__qualname__, "message": str(error), "code": None, "data": data}


def _reloads(error: BaseException) -> bool:
    # Loading calls the exception's constructor, so a class that cannot be rebuilt fails here, not at replay.
    try:
        _pickle_loads(_pickle_dumps(error))
    except (TypeError, DBOSException):
        return False
    return True


def encode_exception(error: BaseException, serialization: str = PORTABLE_JSON) -> str:
    if _uses_pickle(serialization):
        return _pickle_dumps(error if _reloads(error) else DBOSRecordedError(type(error).__qualname__, str(error)))
    return json.dumps(_exception_payload(error))


def _loaded_exception_class(module: Any, qualname: str) -> type[Exception] | None:
    # Reads only what is already loaded: stored text must never import a module or run a module __getattr__.
    owner: Any = errors if module == DBOSException.__module__ else sys.modules.get(module)
    for part in qualname.split("."):
        try:
            owner = vars(owner).get(part)
        except TypeError:
            return None
    return owner if isinstance(owner, type) and issubclass(owner, Exception) else None


def _exception_from(payload: dict[str, Any]) -> Exception:
    name, message = payload["name"], payload["message"]
    data = payload.get("data") or {}
    kind = _loaded_exception_class(data.get("module"), name)
    if kind is None:
        return DBOSRecordedError(name, message)
    args = data.get("args")
    args = (message,) if args is None else tuple(args)
    if kind.__module__ == "builtins":
        error = kind(*args)
    else:
        # Skips the class's own __new__ and __init__, so stored text cannot run them.
        base = next(item for item in kind.__mro__ if item.__module__ == "builtins")
        error = base.__new__(kind)
        error.args = args
    vars(error).update(data.get("attributes") or {})
    if isinstance(error, DBOSMaxStepRetriesExceeded):
        error.errors = [_exception_from(item) for item in data.get("errors") or []]
    return error


def decode_exception(text: str, serialization: str = PORTABLE_JSON) -> Exception:
    if _uses_pickle(serialization):
        try:
            value = _pickle_loads(text)
        except DBOSException:
            return DBOSRecordedError("Exception", "stored error could not be loaded")
        return value if isinstance(value, Exception) else DBOSRecordedError(type(value).__qualname__, str(value))
    try:
        return _exception_from(json.loads(text))
    except Exception:  # noqa: BLE001
        try:
            payload = json.loads(text)
            return DBOSRecordedError(str(payload["name"]), str(payload["message"]))
        except Exception:  # noqa: BLE001
            return DBOSRecordedError("Exception", "stored error could not be loaded")
