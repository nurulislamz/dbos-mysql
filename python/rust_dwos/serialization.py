from __future__ import annotations

import base64
import inspect
import pickle
from collections.abc import Callable
from typing import Any

from rust_dwos.errors import DBOSRecordedError


def _reject_live(value: Any) -> None:
    if inspect.isgenerator(value) or inspect.isasyncgen(value) or inspect.iscoroutine(value):
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored")


def dumps(value: Any) -> str:
    _reject_live(value)
    try:
        blob = pickle.dumps(value)
    except (TypeError, pickle.PicklingError) as error:
        raise TypeError(f"A value of type {type(value).__name__} cannot be stored: {error}") from error
    return base64.b64encode(blob).decode("ascii")


def loads(text: str) -> Any:
    return pickle.loads(base64.b64decode(text))  # noqa: S301


def _exception_blob(error: BaseException) -> bytes | None:
    """Return pickle bytes only when loading them rebuilds the exception.

    Loading calls ``__init__`` with the saved arguments, so a constructor that
    does not accept those arguments fails here and the caller stores a fallback.
    """
    try:
        blob = pickle.dumps(error)
        pickle.loads(blob)  # noqa: S301
    except Exception:  # noqa: BLE001
        return None
    return blob


class Codec:
    """Stores a function's inputs and output as base64-encoded pickle."""

    def __init__(self, function: Callable[..., Any]) -> None:
        self._signature = inspect.signature(inspect.unwrap(function))

    def encode_input(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        bound = self._signature.bind(*args, **kwargs)
        return dumps((bound.args, bound.kwargs))

    def decode_input(self, text: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args, kwargs = loads(text)
        return tuple(args), dict(kwargs)

    def encode_output(self, value: Any) -> str:
        return dumps(value)

    def decode_output(self, text: str) -> Any:
        return loads(text)


def encode_exception(error: BaseException) -> str:
    blob = _exception_blob(error)
    if blob is None:
        blob = _exception_blob(DBOSRecordedError(type(error).__qualname__, str(error)))
    if blob is None:
        raise TypeError(f"A value of type {type(error).__name__} cannot be stored")
    return base64.b64encode(blob).decode("ascii")


def decode_exception(text: str) -> Exception:
    try:
        value = loads(text)
    except Exception:  # noqa: BLE001
        return DBOSRecordedError("Exception", "stored error could not be loaded")
    if isinstance(value, Exception):
        return value
    return DBOSRecordedError(type(value).__qualname__, str(value))
