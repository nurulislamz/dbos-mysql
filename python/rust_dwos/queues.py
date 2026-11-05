from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rust_dwos.errors import DBOSException

if TYPE_CHECKING:
    from collections.abc import Callable

    from rust_dwos.dbos import WorkflowHandle, WorkflowHandleAsync

DEFAULT_QUEUE_POLLING_INTERVAL_SEC = 0.25

# DBOS's own queues are named under this prefix, so users may not claim it.
RESERVED_QUEUE_NAME_PREFIX = "_dbos_"

# Presented by DBOS when it builds a Queue. Module-private and unguessable, so
# the exported Queue is usable as a type but not as a constructor: a queue can
# only come from DBOS.register_queue or DBOS.retrieve_queue.
_INTERNAL_QUEUE_CONSTRUCTION = object()


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


class Queue:
    """
    Workflow queue.

    Workflow queues allow workflows to be started at a later time, based on
    concurrency limits. A queue is declared with ``DBOS.register_queue`` and
    looked up with ``DBOS.retrieve_queue``.
    """

    def __init__(
        self,
        name: str,
        concurrency: int | None = None,
        *,  # Disable positional arguments from here on
        polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC,
        # Proof the caller is DBOS itself; see _INTERNAL_QUEUE_CONSTRUCTION.
        token: object = None,
    ) -> None:
        if token is not _INTERNAL_QUEUE_CONSTRUCTION:
            raise DBOSException("Queue objects cannot be constructed directly. Declare a queue with DBOS.register_queue(name, ...) and look one up with DBOS.retrieve_queue(name).")
        self.name = name
        self._concurrency = concurrency
        self._polling_interval_sec = polling_interval_sec

    @staticmethod
    def _validate_queue(
        *,
        name: str,
        concurrency: int | None,
        global_concurrency: int | None,
        polling_interval_sec: float,
        unsupported: dict[str, Any],
    ) -> None:
        """Validate queue configuration parameters, raising ValueError on bad input."""
        if unsupported:
            raise DBOSException(f"These queue options are not supported: {', '.join(sorted(unsupported))}")
        if name.startswith(RESERVED_QUEUE_NAME_PREFIX):
            raise ValueError(f"Queue name {name} is reserved: names starting with '{RESERVED_QUEUE_NAME_PREFIX}' belong to DBOS's internal queues.")
        if concurrency is not None and global_concurrency is not None:
            raise ValueError("concurrency is deprecated in favor of global_concurrency; set only one of them")
        if polling_interval_sec <= 0:
            raise ValueError("polling_interval_sec must be positive")

    @property
    def concurrency(self) -> int | None:
        return self._concurrency

    @property
    def global_concurrency(self) -> int | None:
        return self._concurrency

    @property
    def polling_interval_sec(self) -> float:
        return self._polling_interval_sec

    def enqueue(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> WorkflowHandle[Any]:
        dbos = _dbos()
        instance = dbos._running()
        workflow_id = instance._blocking(instance._engine.enqueue, self.name, *dbos._begin_args(dbos._info_of(func), args, kwargs))
        return dbos.WorkflowHandle(workflow_id)

    async def enqueue_async(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> WorkflowHandleAsync[Any]:
        dbos = _dbos()
        instance = dbos._running()
        workflow_id = await instance._engine.enqueue(self.name, *dbos._begin_args(dbos._info_of(func), args, kwargs))
        return dbos.WorkflowHandleAsync(workflow_id)


class Queues:
    @classmethod
    def register_queue(
        cls,
        name: str,
        *,
        global_concurrency: int | None = None,
        polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC,
        concurrency: int | None = None,  # Deprecated, retained for backwards compatibility
        **unsupported: Any,
    ) -> Queue:
        """Register a queue, persist its concurrency, and start pulling work from it in this process."""
        Queue._validate_queue(name=name, concurrency=concurrency, global_concurrency=global_concurrency, polling_interval_sec=polling_interval_sec, unsupported=unsupported)
        limit = concurrency if global_concurrency is None else global_concurrency
        instance = _dbos()._running()
        instance._blocking(instance._engine.ensure_queue, name, limit)
        return instance._add_queue(Queue(name, limit, polling_interval_sec=polling_interval_sec, token=_INTERNAL_QUEUE_CONSTRUCTION))

    @classmethod
    async def register_queue_async(
        cls,
        name: str,
        *,
        global_concurrency: int | None = None,
        polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC,
        concurrency: int | None = None,  # Deprecated, retained for backwards compatibility
        **unsupported: Any,
    ) -> Queue:
        """Async version of :meth:`register_queue`."""
        Queue._validate_queue(name=name, concurrency=concurrency, global_concurrency=global_concurrency, polling_interval_sec=polling_interval_sec, unsupported=unsupported)
        limit = concurrency if global_concurrency is None else global_concurrency
        instance = _dbos()._running()
        await instance._engine.ensure_queue(name, limit)
        return instance._add_queue(Queue(name, limit, polling_interval_sec=polling_interval_sec, token=_INTERNAL_QUEUE_CONSTRUCTION))

    @classmethod
    def retrieve_queue(cls, name: str) -> Queue | None:
        instance = _dbos()._running()
        known = instance._queues.get(name)
        if known is not None:
            return known
        concurrency = instance._blocking(instance._engine.get_queue, name)
        if concurrency is None:
            return None
        return Queue(name, None if concurrency == 2**32 - 1 else concurrency, token=_INTERNAL_QUEUE_CONSTRUCTION)

    @classmethod
    async def retrieve_queue_async(cls, name: str) -> Queue | None:
        instance = _dbos()._running()
        known = instance._queues.get(name)
        if known is not None:
            return known
        concurrency = await instance._engine.get_queue(name)
        if concurrency is None:
            return None
        return Queue(name, None if concurrency == 2**32 - 1 else concurrency, token=_INTERNAL_QUEUE_CONSTRUCTION)

    @classmethod
    def enqueue_workflow(cls, queue_name: str, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> WorkflowHandle[Any]:
        """Enqueue a workflow on a queue, returning a handle to the ongoing execution."""
        return Queue(queue_name, token=_INTERNAL_QUEUE_CONSTRUCTION).enqueue(func, *args, **kwargs)

    @classmethod
    async def enqueue_workflow_async(cls, queue_name: str, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> WorkflowHandleAsync[Any]:
        """Async version of :meth:`enqueue_workflow`."""
        return await Queue(queue_name, token=_INTERNAL_QUEUE_CONSTRUCTION).enqueue_async(func, *args, **kwargs)
