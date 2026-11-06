from __future__ import annotations

from collections.abc import Callable
from typing import Any

from yado.errors import DBOSException

_PENDING: list[tuple[str, Any, str, str, tuple[Any, ...], dict[str, Any]]] = []


def _dbos() -> Any:
    from yado import dbos

    return dbos


def flush(instance: Any) -> None:
    pending = list(_PENDING)
    _PENDING.clear()
    for name, info, cron, queue, args, kwargs in pending:
        install(instance, name, info, cron, queue, args, kwargs)


def clear() -> None:
    _PENDING.clear()


def install(instance: Any, name: str, info: Any, cron: str, queue: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    try:
        encoded = info.codec.encode_input(args, kwargs, instance._engine.serialization)
    except TypeError as error:
        raise DBOSException(f"Schedule arguments do not fit workflow {info.name}: {error}") from error
    instance._blocking(
        instance._engine.ensure_schedule,
        name,
        info.name,
        cron,
        queue,
        encoded,
        instance._engine.serialization,
        info.max_recovery_attempts,
    )


def remember(name: str, info: Any, cron: str, queue: str, args: tuple[Any, ...] = (), kwargs: dict[str, Any] | None = None) -> None:
    kwargs = kwargs or {}
    module = _dbos()
    instance = module._instance
    if instance is not None and instance._engine is not None:
        install(instance, name, info, cron, queue, args, kwargs)
        return
    _PENDING.append((name, info, cron, queue, args, kwargs))


class Schedules:
    @classmethod
    def register_schedule(
        cls,
        name: str,
        workflow: Callable[..., Any],
        cron: str,
        *,
        queue: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Register a UTC cron that enqueues `workflow` on `queue`.

        Every run gets the same `args` and `kwargs`. A new schedule does not run a tick that has already
        passed. Later, only the latest missed tick is enqueued, and that tick is skipped while
        a previous run of this schedule is still pending. The same definition replaces the old one.
        """
        remember(name, _dbos()._info_of(workflow), cron, queue, args, kwargs)

    @classmethod
    def delete_schedule(cls, name: str) -> None:
        """Stop a schedule. A missing name is not an error, and runs already started keep going."""
        instance = _dbos()._running()
        instance._blocking(instance._engine.delete_schedule, name)

    @classmethod
    def scheduled(cls, cron: str, *, queue: str, name: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Put this above `@DBOS.workflow()`. `name` is the schedule id, and defaults to the workflow name."""

        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            info = getattr(function, "__dbos_workflow__", None)
            if info is None:
                raise DBOSException("DBOS.scheduled goes above @DBOS.workflow(), and the workflow takes no arguments")
            remember(name or info.name, info, cron, queue)
            return function

        return decorate
