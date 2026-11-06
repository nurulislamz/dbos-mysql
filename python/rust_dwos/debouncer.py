"""Debouncing, copied in shape from dbos-transact-py ``_debouncer.py`` at 236142b (MIT).

Rust decides everything: whether a call coalesces into the waiting workflow or enqueues a new
one, the delay and its cap, and the saved step that a replay returns. Python only encodes the
inputs and builds the handle.
"""

from __future__ import annotations

import asyncio
import types
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

from rust_dwos.attributes import _encode, start_attributes
from rust_dwos.errors import DBOSException
from rust_dwos.queues import _INTERNAL_QUEUE_CONSTRUCTION, EnqueueOptions, Queue, _enqueue_options

if TYPE_CHECKING:
    from rust_dwos.dbos import DBOS, WorkflowHandle, WorkflowHandleAsync

P = ParamSpec("P")
R = TypeVar("R", covariant=True)

# Where a debounced workflow waits when the debouncer names no queue, as in DBOS.
INTERNAL_QUEUE_NAME = "_dbos_internal_queue"


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def _resolve_queue_name(queue: Queue | str | None) -> str | None:
    return queue.name if isinstance(queue, Queue) else queue


def _reject_conflicting_options(*, has_deduplication_id: bool, has_delay: bool, has_priority: bool, has_partition_key: bool, has_return_existing: bool = False, has_reject_reuse_policy: bool = False) -> None:
    if has_deduplication_id:
        raise DBOSException("Cannot debounce a workflow with a deduplication_id set: the debounce key is used as the workflow's deduplication ID.")
    if has_delay:
        raise DBOSException("Cannot debounce a workflow with a delay set: the debounce period controls the workflow's delay.")
    if has_priority:
        raise DBOSException("Cannot debounce a workflow with a priority set: priority is not supported for debounced workflows.")
    if has_partition_key:
        raise DBOSException("Cannot debounce a workflow with a queue partition key set: partitioned queues do not support deduplication, which debouncing requires.")
    if has_return_existing:
        raise DBOSException("Cannot debounce a workflow with duplication_policy 'return-existing': a debounce owns the workflow's deduplication behavior.")
    if has_reject_reuse_policy:
        raise DBOSException("Cannot debounce a workflow with workflow_id_reuse_policy 'reject': a debounce owns how its workflow is started.")


def _listen_internal_queue(instance: DBOS, *, create: bool) -> str:
    if INTERNAL_QUEUE_NAME not in instance._queues:
        if create:
            # No limit, and update=False, so a peer's stored settings stay.
            instance._blocking(instance._engine.register_queue, INTERNAL_QUEUE_NAME, None, None, False, None, None, None, None, None, None, 1.0)
        elif instance._blocking(instance._engine.get_queue, INTERNAL_QUEUE_NAME) is None:
            return INTERNAL_QUEUE_NAME
        instance._add_queue(Queue(INTERNAL_QUEUE_NAME, token=_INTERNAL_QUEUE_CONSTRUCTION))
    return INTERNAL_QUEUE_NAME


def resume_internal_queue(instance: DBOS) -> None:
    """At launch, pull debounced workflows another run left waiting, without creating the queue."""
    _listen_internal_queue(instance, create=False)


class Debouncer(Generic[P, R]):
    def __init__(self, workflow_name: str, *, debounce_timeout_sec: float | None = None, queue: Queue | str | None = None, application_name: str | None = None) -> None:
        self.func_name = workflow_name
        self.options = {"debounce_timeout_sec": debounce_timeout_sec, "queue_name": _resolve_queue_name(queue), "workflow_name": workflow_name, "application_name": application_name}

    @staticmethod
    def create(workflow: Callable[P, R], *, debounce_timeout_sec: float | None = None, queue: Queue | str | None = None, application_name: str | None = None) -> Debouncer[P, R]:
        if isinstance(workflow, types.MethodType):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](_dbos()._info_of(workflow).name, debounce_timeout_sec=debounce_timeout_sec, queue=queue, application_name=application_name)

    @staticmethod
    def create_async(workflow: Callable[P, Coroutine[Any, Any, R]], *, debounce_timeout_sec: float | None = None, queue: Queue | str | None = None, application_name: str | None = None) -> Debouncer[P, R]:
        if isinstance(workflow, types.MethodType):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](_dbos()._info_of(workflow).name, debounce_timeout_sec=debounce_timeout_sec, queue=queue, application_name=application_name)

    def _args(self, debounce_key: str, debounce_period_sec: float, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[DBOS, tuple[Any, ...], dict[str, Any]]:
        dbos = _dbos()
        instance = dbos._running()
        start = _enqueue_options.get()
        _reject_conflicting_options(
            has_deduplication_id=start.get("deduplication_id") is not None,
            has_delay=start.get("delay_seconds") is not None,
            has_priority=start.get("priority") is not None,
            has_partition_key=start.get("queue_partition_key") is not None,
            has_return_existing=start.get("duplication_policy") == "return-existing",
            has_reject_reuse_policy=dbos._next_reuse_policy.get() == "reject",
        )
        info = dbos._registry.get(self.options["workflow_name"])
        if info is None:
            raise DBOSException(f"Invalid workflow name provided to debouncer: {self.options['workflow_name']}")
        queue = self.options["queue_name"] or _listen_internal_queue(instance, create=True)
        name, text, max_attempts, workflow_id, run, in_step, timeout_ms, _, reuse, serialization = dbos._begin_args(info, args, kwargs)
        positional = (queue, name, text, max_attempts, debounce_key, debounce_period_sec, self.options["debounce_timeout_sec"], workflow_id, run, in_step, timeout_ms, reuse, serialization)
        return instance, positional, {"app_version": start.get("app_version"), "application_name": self.options["application_name"], **start_attributes()}

    def debounce(self, debounce_key: str, debounce_period_sec: float, *args: P.args, **kwargs: P.kwargs) -> WorkflowHandle[R]:
        instance, positional, named = self._args(debounce_key, debounce_period_sec, args, kwargs)
        return _dbos().WorkflowHandle(instance._blocking(instance._engine.debounce, *positional, **named))

    async def debounce_async(self, debounce_key: str, debounce_period_sec: float, *args: P.args, **kwargs: P.kwargs) -> WorkflowHandleAsync[R]:
        instance, positional, named = self._args(debounce_key, debounce_period_sec, args, kwargs)
        return _dbos().WorkflowHandleAsync(await instance._engine.debounce(*positional, **named))


class DebouncerClient:
    """Debounces by workflow name from a client. ``client`` needs ``_engine`` and ``retrieve_workflow``."""

    def __init__(self, client: Any, workflow_options: EnqueueOptions, *, debounce_timeout_sec: float | None = None, queue: Queue | str | None = None, application_name: str | None = None) -> None:
        self.workflow_options = workflow_options
        self.debouncer_options = {"debounce_timeout_sec": debounce_timeout_sec, "queue_name": _resolve_queue_name(queue), "workflow_name": workflow_options["workflow_name"], "application_name": application_name}
        self.client = client

    def debounce(self, debounce_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any) -> WorkflowHandle[R]:
        from rust_dwos.serialization import Codec

        options: dict[str, Any] = dict(self.workflow_options)
        _reject_conflicting_options(
            has_deduplication_id=options.get("deduplication_id") is not None,
            has_delay=options.get("delay_seconds") is not None,
            has_priority=options.get("priority") is not None,
            has_partition_key=options.get("queue_partition_key") is not None,
            has_return_existing=options.get("duplication_policy") == "return-existing",
            has_reject_reuse_policy=options.get("workflow_id_reuse_policy") == "reject",
        )
        engine = self.client._engine
        queue = self.debouncer_options["queue_name"] or options["queue_name"]
        text = Codec(lambda *a, **k: None).encode_input(args, kwargs, engine.serialization)
        timeout = options.get("workflow_timeout")
        attributes = {"attributes": _encode(options["attributes"])} if "attributes" in options else {}
        application = self.debouncer_options["application_name"] or options.get("application_name")
        workflow_id = engine.debounce(
            queue, options["workflow_name"], text, None, debounce_key, debounce_period_sec, self.debouncer_options["debounce_timeout_sec"],
            options.get("workflow_id"), None, False, int(timeout * 1000) if timeout is not None else None, None, None,
            app_version=options.get("app_version"), application_name=application, blocking=True, **attributes,
        )
        return self.client.retrieve_workflow(workflow_id)

    async def debounce_async(self, debounce_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any) -> WorkflowHandleAsync[R]:
        handle = await asyncio.to_thread(self.debounce, debounce_key, debounce_period_sec, *args, **kwargs)
        return _dbos().WorkflowHandleAsync(handle.workflow_id)
