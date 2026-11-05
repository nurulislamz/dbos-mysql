from __future__ import annotations

from collections.abc import Callable
from typing import Any

from rust_dwos.errors import DBOSException

_PENDING: list[tuple[str, Any, str, str]] = []


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def flush(instance: Any) -> None:
    pending = list(_PENDING)
    _PENDING.clear()
    for name, info, cron, queue in pending:
        install(instance, name, info, cron, queue)


def clear() -> None:
    _PENDING.clear()


def install(instance: Any, name: str, info: Any, cron: str, queue: str) -> None:
    try:
        encoded = info.codec.encode_input((), {}, instance._engine.serialization)
    except TypeError as error:
        raise DBOSException(f"Scheduled workflow {info.name} must take no arguments") from error
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


def remember(name: str, info: Any, cron: str, queue: str) -> None:
    module = _dbos()
    instance = module._instance
    if instance is not None and instance._engine is not None:
        install(instance, name, info, cron, queue)
        return
    _PENDING.append((name, info, cron, queue))


class Schedules:
    @classmethod
    def register_schedule(cls, name: str, workflow: Callable[..., Any], cron: str, *, queue: str) -> None:
        """Register a UTC cron that enqueues `workflow` on `queue`.

        The workflow takes no arguments. A new schedule does not run a tick that has already
        passed. Later, only the latest missed tick is enqueued, and that tick is skipped while
        a previous run of this schedule is still pending. The same definition replaces the old one.
        """
        remember(name, _dbos()._info_of(workflow), cron, queue)

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
