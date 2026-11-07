from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import concurrent.futures.thread  # registers its exit hook first, so ours runs before it
import contextvars
import functools
import inspect
import json
import logging
import os
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, NotRequired, TypedDict

from yado import _native, tracing
from yado.errors import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSException,
    DBOSLeaseLostError,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSStepTimeoutError,
    DBOSWorkflowCancelledError,
)
from yado.attributes import WorkflowAttributes, _attributes, start_attributes
from yado.events import Events, WorkflowSerializationFormat
from yado.awakeables import Awakeables
from yado.forks import Forks
from yado.patching import Patching
from yado.versions import Versions
from yado.queues import DEFAULT_QUEUE_POLLING_INTERVAL_SEC, Queue, Queues
from yado.retention import Retention, retention_policy_of
from yado.schedules import Schedules, clear as clear_schedules, flush as flush_schedules
from yado.debouncer import resume_internal_queue
from yado.serialization import Codec, Serializer, allow_pickle, decode_exception, encode_exception, set_custom_serializer
from yado._sleep import Sleep
from yado.streams import Streams
from yado.sdk_calls import SDKCalls
from yado.step_reservation import _reserved_step
from yado.timeouts import _timeout_override, timeout_start_args
from yado.workflow_management import WorkflowManagement
from yado._wait_first import WaitFirst
from yado.validation import make_pydantic_args_validator, pydantic_args_validator, ValidateArgsCallable

logger = logging.getLogger("dbos")

_ENGINE_OPTIONS = ("application_version", "lease_seconds", "max_run_seconds", "recovery_interval_seconds", "max_concurrent_recoveries", "run_migrations", "serializer")
_PLAIN_CODEC = Codec(lambda: None)
_SYSTEM_EXIT = object()
# This process writes the row just before its body finishes, and a poll can land in between.
# shortcut: a body hung past a row another run finished delays get_result by this, upgrade if conclude can signal handles.
_BODY_SETTLE_SECONDS = 1.0


class DBOSConfig(TypedDict):
    name: str
    system_database_url: str
    run_migrations: NotRequired[bool]
    lease_seconds: NotRequired[float]
    max_run_seconds: NotRequired[float]
    recovery_interval_seconds: NotRequired[float]
    max_concurrent_recoveries: NotRequired[int]
    shutdown_timeout_seconds: NotRequired[float]
    serializer: NotRequired[Literal["json", "pickle"] | Serializer]
    log_level: NotRequired[str]
    console_log_level: NotRequired[str]
    # Finished workflows older than retention_hours, or beyond the newest retention_rows, are deleted every interval.
    retention_hours: NotRequired[float]
    retention_rows: NotRequired[int]
    retention_interval_seconds: NotRequired[float]
    executor_id: NotRequired[str | None]
    # Stamped on new workflows. Recovery and queues take only unversioned rows and this version.
    application_version: NotRequired[str]
    # OpenTelemetry spans for workflows and steps. Needs yado[otel]. Other otlp_* keys are not supported yet.
    enable_otlp: NotRequired[bool]
    otlp_traces_endpoints: NotRequired[list[str]]
    otlp_attributes: NotRequired[dict[str, str]]


class WorkflowStatusString(Enum):
    PENDING = "PENDING"
    ENQUEUED = "ENQUEUED"
    DELAYED = "DELAYED"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    MAX_RECOVERY_ATTEMPTS_EXCEEDED = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class WorkflowStatus:
    workflow_id: str
    status: str
    name: str
    input: Any
    output: Any
    error: Exception | None
    recovery_attempts: int
    max_recovery_attempts: int | None
    parent_workflow_id: str | None
    created_at: int
    updated_at: int
    completed_at: int | None
    workflow_timeout_ms: int | None = None
    workflow_deadline_epoch_ms: int | None = None
    application_version: str | None = None
    application_name: str | None = None
    executor_id: str | None = None
    forked_from: str | None = None
    was_forked_from: bool = False
    schedule_name: str | None = None
    authenticated_user: str | None = None
    assumed_role: str | None = None
    authenticated_roles: str | None = None
    queue_name: str | None = None
    priority: int | None = None
    deduplication_id: str | None = None
    queue_partition_key: str | None = None
    dequeued_at_ms: int | None = None
    delay_until_ms: int | None = None
    attributes: dict[str, Any] | None = None


@dataclass(frozen=True)
class StepStatus:
    step_id: int
    current_attempt: int | None
    max_attempts: int | None
    _name: str = field(default="", repr=False, compare=False)
    _watch: _native.StepWatch | None = field(default=None, repr=False, compare=False)


@dataclass
class _Workflow:
    name: str
    function: Callable[..., Any]
    codec: Codec
    is_async: bool
    max_recovery_attempts: int | None
    validate_args: ValidateArgsCallable | None = None
    serialization_type: WorkflowSerializationFormat | None = None


_current_run: contextvars.ContextVar[_native.Run | None] = contextvars.ContextVar("dbos_run", default=None)
_current_step: contextvars.ContextVar[StepStatus | None] = contextvars.ContextVar("dbos_step", default=None)
_next_workflow_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("dbos_next_id", default=None)
_next_reuse_policy: contextvars.ContextVar[str | None] = contextvars.ContextVar("dbos_reuse_policy", default=None)
_current_active: contextvars.ContextVar[_native.ActiveRun | None] = contextvars.ContextVar("dbos_active", default=None)
# Set while run_step, asyncio_wait or wait_first build their step per call. Those are not user registrations.
_unregistered_step: contextvars.ContextVar[bool] = contextvars.ContextVar("dbos_unregistered_step", default=False)
# Datasources this run called. Their checkpoints are dropped once the body ends, because the step rows then cover them.
_used_datasources: contextvars.ContextVar[list[Any] | None] = contextvars.ContextVar("dbos_used_datasources", default=None)


class SetWorkflowID:
    def __init__(
        self,
        workflow_id: str,
        *,
        workflow_id_reuse_policy: Literal["return-existing", "reject"] | None = None,
    ) -> None:
        if workflow_id_reuse_policy not in (None, "return-existing", "reject"):
            raise DBOSException(
                f"Invalid workflow_id_reuse_policy {workflow_id_reuse_policy}. "
                "Must be either 'return-existing' or 'reject'."
            )
        self._workflow_id = workflow_id
        self._reuse_policy = workflow_id_reuse_policy
        self._token: contextvars.Token[str | None] | None = None
        self._reuse_token: contextvars.Token[str | None] | None = None

    def __enter__(self) -> SetWorkflowID:
        self._token = _next_workflow_id.set(self._workflow_id)
        self._reuse_token = _next_reuse_policy.set(self._reuse_policy)
        return self

    def __exit__(self, *_: object) -> None:
        if self._reuse_token is not None:
            _next_reuse_policy.reset(self._reuse_token)
            self._reuse_token = None
        if self._token is not None:
            _next_workflow_id.reset(self._token)
            self._token = None


class _LoopThread:
    def __init__(self, name: str) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name=name, daemon=True)
        self._closing = False
        self._thread.start()

    def submit[T](self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        if self._closing:
            coroutine.close()
            raise DBOSException("DBOS was destroyed while this work was running")
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop)

    def run[T](self, coroutine: Coroutine[Any, Any, T]) -> T:
        return self.submit(coroutine).result()

    def stop(self) -> None:
        self._closing = True
        asyncio.run_coroutine_threadsafe(_drain(), self._loop).result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


_registry: dict[str, _Workflow] = {}
_instance: DBOS | None = None


def _register(workflow: _Workflow) -> None:
    if workflow.name in _registry:
        logger.warning(f"Duplicate registration of function '{workflow.name}'")
    _registry[workflow.name] = workflow


def _running() -> DBOS:
    if _instance is None or _instance._engine is None:
        raise DBOSException("DBOS is not launched. Call DBOS(config=...) and DBOS.launch() first")
    if _instance._pid != os.getpid():
        raise DBOSException("DBOS cannot be used after a fork. Call DBOS.launch() in the child process")
    return _instance


def _bind_run(run: _native.Run, active: _native.ActiveRun) -> None:
    from yado.context import DBOSContext, _context
    from yado.queues import _enqueue_options

    _current_run.set(run)
    _context.set(DBOSContext())
    _current_active.set(active)
    _current_step.set(None)
    _used_datasources.set([])
    _next_workflow_id.set(None)
    _next_reuse_policy.set(None)
    _attributes.set(None)
    _enqueue_options.set({})
    _timeout_override.set(None)


async def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await method(*args, **kwargs)


def _retrieve_exception(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


class WorkflowHandle[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self, *, polling_interval_sec: float | None = None) -> T:
        _check_poll(polling_interval_sec)
        if self._local is not None:
            result = self._row_before_body(self._local, polling_interval_sec)
            if result is not None:
                return _decode_result(result)
            try:
                return self._local.result()
            except DBOSAwaitedWorkflowCancelledError:
                pass  # It may have been resumed since, so the stored row decides.
        instance = _running()
        return _decode_result(instance._waiting(instance._engine.await_result, *_result_args(self.workflow_id, polling_interval_sec)))

    def _row_before_body(self, local: concurrent.futures.Future[T], polling_interval_sec: float | None) -> _native.WorkflowResult | None:
        # None means the body answers: it finished, or no row can be read.
        if local.done():
            return None
        try:
            instance = _running()
            stored = instance._state.submit(_call(instance._engine.await_result, *_result_args(self.workflow_id, polling_interval_sec)))
        except DBOSException:
            return None
        try:
            concurrent.futures.wait([local, stored], return_when=concurrent.futures.FIRST_COMPLETED)
            if local.done() or stored.cancelled() or stored.exception() is not None:
                return None
            result = stored.result()
            if result.kind != "cancelled":
                concurrent.futures.wait([local], timeout=_BODY_SETTLE_SECONDS)
            return None if local.done() else result
        finally:
            stored.cancel()  # An abandoned await_result would poll in Rust until the row is final.

    def get_status(self) -> WorkflowStatus:
        instance = _running()
        return _status_of(instance._blocking(instance._engine.get_status, self.workflow_id))


class WorkflowHandleAsync[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | asyncio.Task[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self, *, polling_interval_sec: float | None = None) -> T:
        _check_poll(polling_interval_sec)
        local = self._local
        if isinstance(local, asyncio.Task) and local.get_loop() is not asyncio.get_running_loop():
            local = None
        if local is not None:
            watched = local if isinstance(local, asyncio.Task) else asyncio.wrap_future(local)
            watched.add_done_callback(_retrieve_exception)
            result = None if local.done() else await self._row_before_body(watched, polling_interval_sec)
            if result is not None:
                return _decode_result(result)
            try:
                return await asyncio.shield(watched)
            except DBOSAwaitedWorkflowCancelledError:
                pass  # It may have been resumed since, so the stored row decides.
        return _decode_result(await _running()._engine.await_result(*_result_args(self.workflow_id, polling_interval_sec)))

    async def _row_before_body(self, watched: asyncio.Future[T], polling_interval_sec: float | None) -> _native.WorkflowResult | None:
        # None means the body answers: it finished, or no row can be read.
        try:
            stored = asyncio.ensure_future(_running()._engine.await_result(*_result_args(self.workflow_id, polling_interval_sec)))
        except DBOSException:
            return None
        stored.add_done_callback(_retrieve_exception)
        try:
            await asyncio.wait({watched, stored}, return_when=asyncio.FIRST_COMPLETED)
            if watched.done() or stored.cancelled() or stored.exception() is not None:
                return None
            result = stored.result()
            if result.kind != "cancelled":
                await asyncio.wait({watched}, timeout=_BODY_SETTLE_SECONDS)
            return None if watched.done() else result
        finally:
            stored.cancel()  # An abandoned await_result would poll in Rust until the row is final.

    async def get_status(self) -> WorkflowStatus:
        return _status_of(await _running()._engine.get_status(self.workflow_id))


class _ContinueAsNew(BaseException):
    def __init__(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self.args_to_pass = args
        self.kwargs_to_pass = kwargs


class _ContinuedRun:
    def __init__(self, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self.run = run
        self.args = args
        self.kwargs = kwargs


class _DBOSMeta(type):
    @property
    def executor_id(cls) -> str:
        """Process label; leases, not this label, decide workflow ownership."""
        return _instance._executor_id if _instance is not None else os.environ.get("DBOS__VMID") or "local"

    @property
    def application_version(cls) -> str | None:
        return _instance._engine.application_version if _instance is not None and _instance._engine is not None else None

    @property
    def workflow_id(cls) -> str | None:
        run = _current_run.get()
        return run.workflow_id if run is not None else None

    @property
    def step_id(cls) -> int | None:
        step = _current_step.get()
        return step.step_id if step is not None else None

    @property
    def step_status(cls) -> StepStatus | None:
        return _current_step.get()

    @property
    def span(cls) -> Any:
        """The OpenTelemetry span of the current workflow or step, or None."""
        return tracing.current_span()


class DBOS(Versions, Awakeables, WaitFirst, SDKCalls, Sleep, Patching, WorkflowManagement, Forks, WorkflowAttributes, Events, Streams, Queues, Schedules, Retention, metaclass=_DBOSMeta):
    logger = logger

    def __new__(cls, *, config: DBOSConfig) -> DBOS:
        global _instance
        _check_config_keys(config)
        retention_policy_of(config)
        if _instance is not None:
            raise DBOSException("DBOS is already configured. Call DBOS.destroy() first")
        _instance = super().__new__(cls)
        return _instance

    def __init__(self, *, config: DBOSConfig) -> None:
        self._config = config
        self._executor_id = config.get("executor_id")
        if self._executor_id is None:
            self._executor_id = os.environ.get("DBOS__VMID") or "local"
        if "log_level" in config or "console_log_level" in config:
            logger.setLevel(config.get("log_level", "INFO"))
            handler = next((h for h in logger.handlers if h.name == "__dbos_console_log_handler__"), None)
            if handler is None:
                handler = logging.StreamHandler()
                handler.name = "__dbos_console_log_handler__"
                logger.addHandler(handler)
            handler.setLevel(config.get("console_log_level", config.get("log_level", "INFO")))
        tracing.setup(bool(config.get("enable_otlp")), config.get("otlp_traces_endpoints") or [], config.get("otlp_attributes") or {})
        self._shutdown_timeout = float(config.get("shutdown_timeout_seconds", 5.0))
        self._engine: Any = None
        self._pid = os.getpid()
        self._active: set[concurrent.futures.Future[Any]] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._keepers: set[_native.ActiveRun] = set()
        self._active_lock = threading.Lock()
        self._app_loop: asyncio.AbstractEventLoop | None = None
        self._app_loop_watch: asyncio.Task[Any] | None = None
        self._queues: dict[str, Queue] = {}
        self._listening_queues: set[str] | None = None
        self._queue_loops: list[concurrent.futures.Future[Any]] = []
        self._retention_policy = retention_policy_of(config)

    @classmethod
    def launch(cls) -> None:
        if _instance is None:
            raise DBOSException("Call DBOS(config=...) before DBOS.launch()")
        _instance._launch()

    def _launch(self) -> None:
        if self._engine is not None:
            return
        self._pid = os.getpid()
        self._state = _LoopThread("dbos-state")
        self._workflows = _LoopThread("dbos-workflows")
        try:
            self._app_loop = asyncio.get_running_loop()
            self._app_loop_watch = self._app_loop.create_task(asyncio.Event().wait())
        except RuntimeError:
            self._app_loop = None
        options = {name: self._config[name] for name in _ENGINE_OPTIONS if name in self._config}
        custom = self._config.get("serializer")
        if isinstance(custom, Serializer):
            options.pop("serializer")
            options["custom_serializer"] = custom.name()
        options["executor_id"] = self._executor_id
        try:
            self._engine = self._state.run(_call(_native.Engine.connect, self._config["system_database_url"], **options))
            allow_pickle(self._engine.serialization == _native.PY_PICKLE)
            custom_serializer = custom if isinstance(custom, Serializer) else None
            set_custom_serializer(custom_serializer, self._engine.serialization if custom_serializer is not None else None)
            flush_schedules(self)
            # A debounced workflow left waiting by an earlier run still needs a puller after a restart.
            resume_internal_queue(self)
        except BaseException:
            if self._engine is not None:
                self._engine.close()
                self._engine = None
            self._workflows.stop()
            self._state.stop()
            raise
        self._recovery = self._workflows.submit(self._recovery_loop())
        self._schedules = self._workflows.submit(self._schedule_loop())
        self._retention_task = self._workflows.submit(self._retention_loop())

    def _blocking(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        # Waits on this thread with the GIL released. No event loop is involved, so a step pays no thread hops.
        if self._state._closing:
            raise DBOSException("DBOS was destroyed while this work was running")
        return method(*args, blocking=True, **kwargs)

    def _waiting(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        # For a wait with no end of its own. Destroy cancels what runs on this loop, which wakes the caller.
        return self._state.run(_call(method, *args, **kwargs))

    @classmethod
    def destroy(cls, *, destroy_registry: bool = False) -> None:
        global _instance
        instance = _instance
        if instance is not None and instance._engine is not None:
            instance._engine.close()
        _instance = None
        if destroy_registry:
            _registry.clear()
            clear_schedules()
        if instance is not None and instance._engine is not None:
            instance._shutdown()

    def _shutdown(self) -> None:
        with self._active_lock:
            active = list(self._active)
            tasks = list(self._tasks)
        for future in active:
            future.cancel()
        for task in tasks:
            _cancel_task(task)
        if self._app_loop_watch is not None:
            _cancel_task(self._app_loop_watch)
        self._recovery.cancel()
        self._schedules.cancel()
        self._retention_task.cancel()
        for loop in self._queue_loops:
            loop.cancel()
        concurrent.futures.wait([*active, self._recovery, self._schedules, self._retention_task, *self._queue_loops], timeout=self._shutdown_timeout)
        self._workflows.stop()
        self._state.stop()
        with self._active_lock:
            keepers = list(self._keepers)
        for keeper in keepers:
            keeper.stop()

    @classmethod
    def workflow(cls, *, name: str | None = None, max_recovery_attempts: int | None = _native.DEFAULT_MAX_RECOVERY_ATTEMPTS, validate_args: ValidateArgsCallable | None = None, serialization_type: WorkflowSerializationFormat | None = None) -> Callable[..., Any]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            is_async = inspect.iscoroutinefunction(function)
            validator = make_pydantic_args_validator(function) if validate_args is pydantic_args_validator else validate_args
            info = _Workflow(name or function.__qualname__, function, Codec(function), is_async, max_recovery_attempts, validator, serialization_type)
            _register(info)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                begun = instance._blocking(instance._engine.begin, *_begin_args(info, args, kwargs), **start_attributes())
                if begun.run is not None:
                    return instance._run_sync(info, begun.run, args, kwargs)
                result = begun.result or instance._waiting(instance._engine.await_result, begun.workflow_id)
                return _decode_result(result, info)

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                begun = await instance._engine.begin(*_begin_args(info, args, kwargs), **start_attributes())
                if begun.run is not None:
                    return await instance._run_async(info, begun.run, args, kwargs)
                result = begun.result or await instance._engine.await_result(begun.workflow_id)
                return _decode_result(result, info)

            wrapper: Any = async_wrapper if is_async else sync_wrapper
            wrapper.__dbos_workflow__ = info
            return wrapper

        return decorate

    @classmethod
    def step(
        cls,
        *,
        name: str | None = None,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
        should_retry: Callable[[BaseException], bool | Awaitable[bool]] | None = None,
        timeout_seconds: float | None = None,
        heartbeat_timeout_seconds: float | None = None,
    ) -> Callable[..., Any]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            step_name = name or function.__qualname__
            codec = Codec(function)
            policy = _native.StepPolicy(retries_allowed, interval_seconds, max_attempts, backoff_rate, timeout_seconds, heartbeat_timeout_seconds)
            is_async = inspect.iscoroutinefunction(function)

            def failure(errors: list[Exception], attempts: _native.StepAttempts) -> Exception:
                if not attempts.wraps_failures:
                    return errors[-1]
                return DBOSMaxStepRetriesExceeded(step_name, policy.max_attempts, errors)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                if run is not None and should_retry is not None and inspect.iscoroutinefunction(should_retry):
                    raise DBOSException(f"Step {step_name} is sync but should_retry is async. Use an async step to pair with an async validator.")
                step_id = _native.step_id_for_call(run, _current_step.get() is not None, _reserved_step.get())
                if step_id is None:
                    return function(*args, **kwargs)
                instance = _running()
                replay = instance._blocking(instance._engine.begin_step, run, step_name, step_id)
                if replay is not None:
                    return _replay_step(replay, codec, run.serialization)
                errors: list[Exception] = []
                attempts = policy.attempts()
                while True:
                    watch = policy.watch()
                    reset = _current_step.set(StepStatus(step_id, *attempts.status(), step_name, watch))
                    try:
                        with tracing.span(step_name, _step_attributes(run, step_id)):
                            value = function(*args, **kwargs) if watch is None else _call_watched(instance, watch, step_name, function, args, kwargs)
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        retryable = should_retry(error) if should_retry is not None and policy.wraps_failures else True
                        if inspect.isawaitable(retryable):
                            if inspect.iscoroutine(retryable):
                                retryable.close()
                            raise DBOSException(f"Step {step_name} is sync but should_retry is async. Use an async step to pair with an async validator.")
                        delay = attempts.failed(bool(retryable))
                        if delay is None:
                            break
                        time.sleep(delay)
                        continue
                    finally:
                        _current_step.reset(reset)
                    return instance._save_success_sync(run, step_id, step_name, codec, value)
                raise instance._save_failure_sync(run, step_id, step_name, failure(errors, attempts))

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                step_id = _native.step_id_for_call(run, _current_step.get() is not None, _reserved_step.get())
                if step_id is None:
                    return await function(*args, **kwargs)
                instance = _running()
                replay = await instance._engine.begin_step(run, step_name, step_id)
                if replay is not None:
                    return _replay_step(replay, codec, run.serialization)
                errors: list[Exception] = []
                attempts = policy.attempts()
                while True:
                    watch = policy.watch()
                    reset = _current_step.set(StepStatus(step_id, *attempts.status(), step_name, watch))
                    try:
                        with tracing.span(step_name, _step_attributes(run, step_id)):
                            value = await function(*args, **kwargs) if watch is None else await _await_watched(watch, step_name, function(*args, **kwargs))
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        retryable = should_retry(error) if should_retry is not None and policy.wraps_failures else True
                        if inspect.isawaitable(retryable):
                            retryable = await retryable
                        delay = attempts.failed(bool(retryable))
                        if delay is None:
                            break
                        await asyncio.sleep(delay)
                        continue
                    finally:
                        _current_step.reset(reset)
                    return await instance._save_success(run, step_id, step_name, codec, value)
                raise await instance._save_failure(run, step_id, step_name, failure(errors, attempts))

            wrapper: Any = async_wrapper if is_async else sync_wrapper
            if _unregistered_step.get():
                return wrapper
            temp = _Workflow(f"<temp>.{step_name}", wrapper, codec, is_async, _native.DEFAULT_MAX_RECOVERY_ATTEMPTS)
            _registry[temp.name] = temp
            wrapper.__dbos_workflow__ = temp
            if step_name in _registry and temp.name != step_name:
                logger.warning(f"Duplicate registration of function '{step_name}'")
            _registry.setdefault(step_name, temp)
            wrapper.__dbos_step__ = step_name
            return wrapper

        return decorate

    @classmethod
    def heartbeat(cls) -> None:
        step = _current_step.get()
        if step is None:
            raise DBOSException("DBOS.heartbeat() must be called from inside a step")
        if step._watch is not None and (reason := step._watch.beat()) is not None:
            raise DBOSStepTimeoutError(step._name, reason)

    @classmethod
    def start_workflow(cls, function: Callable[..., Any], *args: Any, **kwargs: Any) -> WorkflowHandle[Any]:
        instance = _running()
        info = _info_of(function)
        begun = instance._blocking(instance._engine.begin, *_begin_args(info, args, kwargs), **start_attributes())
        return WorkflowHandle(begun.workflow_id, instance._background(info, begun, args, kwargs))

    @classmethod
    async def start_workflow_async(cls, function: Callable[..., Any], *args: Any, **kwargs: Any) -> WorkflowHandleAsync[Any]:
        instance = _running()
        info = _info_of(function)
        begun = await instance._engine.begin(*_begin_args(info, args, kwargs), **start_attributes())
        if begun.run is not None and info.is_async:
            return WorkflowHandleAsync(begun.workflow_id, instance._task_on_this_loop(info, begun.run, args, kwargs))
        return WorkflowHandleAsync(begun.workflow_id, instance._background(info, begun, args, kwargs))

    @classmethod
    def retrieve_workflow(cls, workflow_id: str, existing_workflow: bool = True) -> WorkflowHandle[Any]:
        instance = _running()
        if existing_workflow:
            instance._blocking(instance._engine.get_status, workflow_id)
        return WorkflowHandle(workflow_id)

    @classmethod
    async def retrieve_workflow_async(cls, workflow_id: str, existing_workflow: bool = True) -> WorkflowHandleAsync[Any]:
        if existing_workflow:
            await _running()._engine.get_status(workflow_id)
        return WorkflowHandleAsync(workflow_id)

    @classmethod
    def get_result(cls, workflow_id: str, *, polling_interval_sec: float | None = None) -> Any:
        _check_poll(polling_interval_sec)
        instance = _running()
        return _decode_result(instance._waiting(instance._engine.await_result, *_result_args(workflow_id, polling_interval_sec)))

    @classmethod
    async def get_result_async(cls, workflow_id: str, *, polling_interval_sec: float | None = None) -> Any:
        _check_poll(polling_interval_sec)
        return _decode_result(await _running()._engine.await_result(*_result_args(workflow_id, polling_interval_sec)))

    @classmethod
    def get_workflow_status(cls, workflow_id: str) -> WorkflowStatus | None:
        instance = _running()
        try:
            return _status_of(instance._blocking(instance._engine.get_status, workflow_id))
        except DBOSNonExistentWorkflowError:
            return None

    @classmethod
    async def get_workflow_status_async(cls, workflow_id: str) -> WorkflowStatus | None:
        try:
            return _status_of(await _running()._engine.get_status(workflow_id))
        except DBOSNonExistentWorkflowError:
            return None

    @classmethod
    def list_workflow_steps(cls, workflow_id: str, *, load_output: bool = True, limit: int | None = None, offset: int | None = None, shard_key: str | None = None) -> list[dict[str, Any]]:
        instance = _running()
        if shard_key is not None:
            raise ValueError("shard_key is not supported")
        return [_step_of(step, load_output=load_output) for step in instance._blocking(instance._engine.list_steps, workflow_id, limit=limit, offset=offset)]

    @classmethod
    async def list_workflow_steps_async(cls, workflow_id: str, *, load_output: bool = True, limit: int | None = None, offset: int | None = None, shard_key: str | None = None) -> list[dict[str, Any]]:
        if shard_key is not None:
            raise ValueError("shard_key is not supported")
        return [_step_of(step, load_output=load_output) for step in await _running()._engine.list_steps(workflow_id, limit=limit, offset=offset)]

    @classmethod
    def continue_as_new(cls, *args: Any, **kwargs: Any) -> None:
        if _current_run.get() is None or _current_step.get() is not None:
            raise DBOSException("DBOS.continue_as_new must be called from a workflow")
        raise _ContinueAsNew(args, kwargs)

    @classmethod
    def _recover_pending_workflows(cls) -> list[WorkflowHandle[Any]]:
        instance = _running()
        return [instance._resume(claim) for claim in instance._blocking(instance._engine.recover_now, list(_registry))]

    def _background(self, info: _Workflow, begun: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> concurrent.futures.Future[Any] | None:
        if begun.run is None:
            return None
        return self._launch_run(info, begun.run, args, kwargs)

    def _launch_run(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> concurrent.futures.Future[Any]:
        if not info.is_async:
            future = _in_daemon_thread(self._run_sync, info, run, args, kwargs)
        elif self._use_app_loop:
            future = asyncio.run_coroutine_threadsafe(self._run_async(info, run, args, kwargs), self._app_loop)
        else:
            future = self._workflows.submit(self._run_async(info, run, args, kwargs))
        with self._active_lock:
            self._active.add(future)
        future.add_done_callback(self._forget)
        return future

    def _forget(self, future: concurrent.futures.Future[Any]) -> None:
        with self._active_lock:
            self._active.discard(future)

    @property
    def _use_app_loop(self) -> bool:
        # A closing loop still reports running but cannot use threads. Closing starts by cancelling every task.
        return self._app_loop is not None and self._app_loop.is_running() and not self._app_loop_watch.done()

    def _task_on_this_loop(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> asyncio.Task[Any]:
        task = asyncio.get_running_loop().create_task(self._run_async(info, run, args, kwargs))
        with self._active_lock:
            self._tasks.add(task)
        task.add_done_callback(self._forget_task)
        return task

    def _forget_task(self, task: asyncio.Task[Any]) -> None:
        with self._active_lock:
            self._tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _keep_lease(self, run: _native.Run) -> _native.ActiveRun:
        keeper = self._engine.keep_lease(run)
        with self._active_lock:
            self._keepers.add(keeper)
        return keeper

    def _stop_keeper(self, keeper: _native.ActiveRun) -> None:
        keeper.stop()
        with self._active_lock:
            self._keepers.discard(keeper)

    async def _run_async(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        while True:
            result = await self._run_async_once(info, run, args, kwargs)
            if not isinstance(result, _ContinuedRun):
                return result
            run, args, kwargs = result.run, result.args, result.kwargs

    async def _run_async_once(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        active = self._keep_lease(run)
        context = contextvars.copy_context()
        context.run(_bind_run, run, active)

        async def guarded() -> Any:
            try:
                validated_args, validated_kwargs = info.validate_args(args, kwargs) if info.validate_args else (args, kwargs)
                with tracing.span(info.name, _workflow_attributes(run), await self._parent_async(run)):
                    return await info.function(*validated_args, **validated_kwargs)
            except SystemExit:
                return _SYSTEM_EXIT

        # SystemExit is caught inside the task, because asyncio would re-raise it into the workflows loop and end that thread.
        body = asyncio.get_running_loop().create_task(guarded(), context=context)
        lost = asyncio.ensure_future(active.lost())
        try:
            await asyncio.wait({body, lost}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            body.cancel()
            await asyncio.gather(body, return_exceptions=True)
            await self._engine.conclude(run)
            raise
        finally:
            self._stop_keeper(active)
            lost.cancel()

        if not body.done():
            logger.warning(f"Workflow {run.workflow_id} lost its lease")
            body.cancel()
            await asyncio.gather(body, return_exceptions=True)
            return _decode_result(await self._engine.conclude(run, lease_lost=True), info)
        if body.cancelled():
            await self._engine.conclude(run)
            raise asyncio.CancelledError
        error = body.exception()
        if used := context.get(_used_datasources):
            from yado.datasources import drop_checkpoints_async

            await drop_checkpoints_async(run, used)
        if error is None:
            value = body.result()
            if value is _SYSTEM_EXIT:
                logger.warning(f"Workflow {run.workflow_id} called sys.exit(), so it was released for recovery")
                await self._engine.conclude(run)
                return None
            return await self._finish_success(info, run, value)
        # A lost lease may still look owned in the row, so its error must not be offered as the result.
        if isinstance(error, DBOSWorkflowCancelledError | DBOSLeaseLostError):
            return _decode_result(await self._engine.conclude(run, lease_lost=True), info)
        if isinstance(error, _ContinueAsNew):
            try:
                text = info.codec.encode_input(error.args_to_pass, error.kwargs_to_pass, run.serialization)
                next_run = await self._engine.continue_as_new(run, text)
            except DBOSLeaseLostError:
                return _decode_result(await self._engine.conclude(run, lease_lost=True), info)
            except Exception as failure:  # noqa: BLE001
                return await self._finish_error(info, run, failure)
            return _ContinuedRun(next_run, error.args_to_pass, error.kwargs_to_pass)
        if isinstance(error, Exception):
            return await self._finish_error(info, run, error)
        await self._engine.conclude(run)
        raise error

    def _run_sync(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        while True:
            result = self._run_sync_once(info, run, args, kwargs)
            if not isinstance(result, _ContinuedRun):
                return result
            run, args, kwargs = result.run, result.args, result.kwargs

    def _run_sync_once(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        active = self._keep_lease(run)
        context = contextvars.copy_context()
        context.run(_bind_run, run, active)
        try:
            validated_args, validated_kwargs = info.validate_args(args, kwargs) if info.validate_args else (args, kwargs)
            parent = self._parent_sync(run)
            value = context.run(_spanned, info.name, _workflow_attributes(run), parent, info.function, *validated_args, **validated_kwargs)
        # A lost lease may still look owned in the row, so its error must not be offered as the result.
        except _ContinueAsNew as continued:
            _drop_datasource_checkpoints(run, context)
            self._stop_keeper(active)
            try:
                text = info.codec.encode_input(continued.args_to_pass, continued.kwargs_to_pass, run.serialization)
                next_run = self._blocking(self._engine.continue_as_new, run, text)
            except DBOSLeaseLostError:
                return _decode_result(self._waiting(self._engine.conclude, run, lease_lost=True), info)
            except Exception as failure:  # noqa: BLE001
                return self._finish_error_sync(info, run, failure)
            return _ContinuedRun(next_run, continued.args_to_pass, continued.kwargs_to_pass)
        except (DBOSWorkflowCancelledError, DBOSLeaseLostError):
            self._stop_keeper(active)
            return _decode_result(self._waiting(self._engine.conclude, run, lease_lost=True), info)
        except Exception as error:  # noqa: BLE001
            _drop_datasource_checkpoints(run, context)
            self._stop_keeper(active)
            return self._finish_error_sync(info, run, error)
        except BaseException:
            self._stop_keeper(active)
            self._blocking(self._engine.conclude, run)
            raise
        _drop_datasource_checkpoints(run, context)
        self._stop_keeper(active)
        return self._finish_success_sync(info, run, value)

    def _parent_sync(self, run: _native.Run) -> Any:
        # A workflow started elsewhere, or recovered, has no active span. Its stored carrier gives the parent.
        if not tracing.wants_parent():
            return None
        try:
            return tracing.parent_from(self._blocking(self._engine.get_status, run.workflow_id).attributes)
        except Exception:  # noqa: BLE001  # a trace lookup must never fail the workflow
            return None

    async def _parent_async(self, run: _native.Run) -> Any:
        if not tracing.wants_parent():
            return None
        try:
            return tracing.parent_from((await self._engine.get_status(run.workflow_id)).attributes)
        except Exception:  # noqa: BLE001  # a trace lookup must never fail the workflow
            return None

    def _finish_success_sync(self, info: _Workflow, run: _native.Run, value: Any) -> Any:
        try:
            text = info.codec.encode_output(value, run.serialization)
        except TypeError as error:
            return self._finish_error_sync(info, run, error)
        superseded = self._blocking(self._engine.conclude, run, output=text)
        if superseded is not None:
            return _decode_result(superseded, info)
        return info.codec.decode_output(text, run.serialization)

    def _finish_error_sync(self, info: _Workflow, run: _native.Run, error: BaseException) -> Any:
        superseded = self._blocking(self._engine.conclude, run, error=encode_exception(error, run.serialization))
        if superseded is not None:
            return _decode_result(superseded, info)
        raise error

    def _save_success_sync(self, run: _native.Run, step_id: int, step_name: str, codec: Codec, value: Any) -> Any:
        try:
            text = codec.encode_output(value, run.serialization)
        except TypeError as error:
            raise self._save_failure_sync(run, step_id, step_name, error) from error
        self._blocking(self._engine.save_step_output, run, step_id, step_name, text)
        return codec.decode_output(text, run.serialization)

    def _save_failure_sync(self, run: _native.Run, step_id: int, step_name: str, error: Exception) -> Exception:
        self._blocking(self._engine.save_step_error, run, step_id, step_name, encode_exception(error, run.serialization))
        return error

    async def _finish_success(self, info: _Workflow, run: _native.Run, value: Any) -> Any:
        try:
            text = info.codec.encode_output(value, run.serialization)
        except TypeError as error:
            return await self._finish_error(info, run, error)
        superseded = await self._engine.conclude(run, output=text)
        if superseded is not None:
            return _decode_result(superseded, info)
        return info.codec.decode_output(text, run.serialization)

    async def _finish_error(self, info: _Workflow, run: _native.Run, error: BaseException) -> Any:
        superseded = await self._engine.conclude(run, error=encode_exception(error, run.serialization))
        if superseded is not None:
            return _decode_result(superseded, info)
        raise error

    async def _save_success(self, run: _native.Run, step_id: int, step_name: str, codec: Codec, value: Any) -> Any:
        try:
            text = codec.encode_output(value, run.serialization)
        except TypeError as error:
            raise await self._save_failure(run, step_id, step_name, error) from error
        await self._engine.save_step_output(run, step_id, step_name, text)
        return codec.decode_output(text, run.serialization)

    async def _save_failure(self, run: _native.Run, step_id: int, step_name: str, error: Exception) -> Exception:
        await self._engine.save_step_error(run, step_id, step_name, encode_exception(error, run.serialization))
        return error

    async def _recovery_loop(self) -> None:
        while True:
            for claim in await self._engine.recovery_tick(list(_registry)):
                try:
                    self._resume(claim)
                except Exception:  # noqa: BLE001
                    logger.exception(f"Could not resume workflow {claim.workflow_id}. It will be retried when its lease expires")

    async def _schedule_loop(self) -> None:
        while True:
            try:
                await self._engine.fire_schedules()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("firing schedules failed, and will be tried again")
            await asyncio.sleep(1)

    def _add_queue(self, queue: Queue) -> Queue:
        with self._active_lock:
            known = self._queues.setdefault(queue.name, queue)
            if known is queue and (self._listening_queues is None or queue.name in self._listening_queues):
                self._queue_loops.append(self._workflows.submit(self._queue_loop(queue)))
        return known

    def _wake_queue(self, name: str) -> None:
        self._engine.wake_queue(name)

    def _wake_queues(self) -> None:
        for name in list(self._queues):
            self._wake_queue(name)

    async def _queue_loop(self, queue: Queue) -> None:
        while True:
            try:
                interval = await queue.get_polling_interval_sec_async()
            except Exception:  # A deleted queue may be registered again later.
                await asyncio.sleep(DEFAULT_QUEUE_POLLING_INTERVAL_SEC)
                continue
            for claim in await self._engine.wait_queue_claims(queue.name, list(_registry), interval):
                try:
                    self._launch_claim(queue, claim)
                except Exception:  # noqa: BLE001
                    logger.exception(f"Could not start workflow {claim.workflow_id}. It will be retried when its lease expires")

    def _launch_claim(self, queue: Queue, claim: _native.Claim) -> None:
        info = _registry[claim.name]
        args, kwargs = info.codec.decode_input(claim.input, claim.run.serialization)
        future = self._launch_run(info, claim.run, args, kwargs)
        future.add_done_callback(lambda _: self._wake_queue(queue.name))

    def _resume(self, claim: _native.Claim) -> WorkflowHandle[Any]:
        info = _registry[claim.name]
        logger.info(f"Recovering workflow {claim.workflow_id}, attempt {claim.recovery_attempts}")
        args, kwargs = info.codec.decode_input(claim.input, claim.run.serialization)
        return WorkflowHandle(claim.workflow_id, self._launch_run(info, claim.run, args, kwargs))


def _close_before_executors_stop() -> None:
    if _instance is not None and _instance._engine is not None:
        _instance._engine.close()


# Runs before concurrent.futures stops its threads, which is before atexit, so the errors that causes are not final.
threading._register_atexit(_close_before_executors_stop)


def _at_exit() -> None:
    DBOS.destroy()
    # Rust threads outlive the interpreter, and a log call into a finalizing interpreter segfaults.
    _native.silence_logs()


atexit.register(_at_exit)


def _drop_datasource_checkpoints(run: _native.Run, context: contextvars.Context) -> None:
    if used := context.get(_used_datasources):
        from yado.datasources import drop_checkpoints

        drop_checkpoints(run, used)


def _workflow_attributes(run: _native.Run) -> dict[str, Any]:
    return {"dbos.operation.workflow_id": run.workflow_id}


def _step_attributes(run: _native.Run, step_id: int) -> dict[str, Any]:
    return {"dbos.operation.workflow_id": run.workflow_id, "dbos.operation.step_id": step_id}


def _spanned(name: str, attributes: dict[str, Any], parent: Any, function: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    with tracing.span(name, attributes, parent):
        return function(*args, **kwargs)


def _check_poll(polling_interval_sec: float | None) -> None:
    if polling_interval_sec is not None and not polling_interval_sec > 0:
        raise DBOSException("polling_interval_sec must be positive")


def _result_args(workflow_id: str, polling_interval_sec: float | None) -> tuple[Any, ...]:
    # Leave the interval off unless asked, so a stand-in engine can keep the one-argument method.
    if polling_interval_sec is None:
        return (workflow_id,)
    return (workflow_id, polling_interval_sec)


def _begin_args(info: _Workflow, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    # PORTABLE is the workflow's own choice. DEFAULT and NATIVE use the engine's format.
    serialization = _native.PORTABLE_JSON if info.serialization_type == WorkflowSerializationFormat.PORTABLE else _running()._engine.serialization
    text = info.codec.encode_input(args, kwargs, serialization)
    return (info.name, text, info.max_recovery_attempts, _next_workflow_id.get(), _current_run.get(), _current_step.get() is not None, *timeout_start_args(), _next_reuse_policy.get(), serialization)


def _check_config_keys(config: DBOSConfig) -> None:
    known = DBOSConfig.__required_keys__ | DBOSConfig.__optional_keys__
    unknown = sorted(set(config) - known)
    if unknown:
        raise DBOSException(f"Unknown DBOSConfig keys: {', '.join(unknown)}. Supported keys: {', '.join(sorted(known))}")


def _info_of(function: Callable[..., Any]) -> _Workflow:
    info = getattr(function, "__dbos_workflow__", None)
    if info is None:
        raise DBOSException(f"{getattr(function, '__qualname__', function)} is not a registered workflow or step")
    return info


def _replay_step(replay: _native.StepReplay, codec: Codec, serialization: str) -> Any:
    if replay.error is not None:
        raise decode_exception(replay.error, serialization)
    return codec.decode_output(replay.output, serialization)


def _decode_result(result: _native.WorkflowResult, info: _Workflow | None = None) -> Any:
    if result.kind == "max_recovery_attempts":
        raise DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded(result.workflow_id)
    if result.kind == "cancelled":
        raise DBOSAwaitedWorkflowCancelledError(result.workflow_id)
    if result.kind == "error":
        raise decode_exception(result.value, result.serialization)
    return _codec_for(result.name, info).decode_output(result.value, result.serialization)


def _codec_for(name: str, info: _Workflow | None = None) -> Codec:
    known = info or _registry.get(name)
    return known.codec if known is not None else _PLAIN_CODEC


def _status_of(row: _native.WorkflowRow, *, load_input: bool = True, load_output: bool = True) -> WorkflowStatus:
    return WorkflowStatus(
        workflow_id=row.workflow_id,
        status=row.status,
        name=row.name,
        input=row.input if load_input else None,
        output=_codec_for(row.name).decode_output(row.output, row.serialization) if load_output and row.output is not None else None,
        error=decode_exception(row.error, row.serialization) if load_output and row.error is not None else None,
        recovery_attempts=row.recovery_attempts,
        max_recovery_attempts=row.max_recovery_attempts,
        parent_workflow_id=row.parent_workflow_id,
        created_at=row.created_at_ms,
        updated_at=row.updated_at_ms,
        completed_at=row.completed_at_ms,
        workflow_timeout_ms=row.workflow_timeout_ms,
        workflow_deadline_epoch_ms=row.workflow_deadline_epoch_ms,
        application_version=row.application_version,
        application_name=row.application_name,
        executor_id=row.executor_id,
        forked_from=row.forked_from,
        was_forked_from=row.was_forked_from,
        schedule_name=row.schedule_name,
        authenticated_user=row.authenticated_user,
        assumed_role=row.assumed_role,
        authenticated_roles=row.authenticated_roles,
        queue_name=row.queue_name,
        priority=row.priority,
        deduplication_id=row.deduplication_id,
        queue_partition_key=row.queue_partition_key,
        dequeued_at_ms=row.dequeued_at_ms,
        delay_until_ms=row.delay_until_ms,
        attributes=json.loads(row.attributes) if row.attributes is not None else None,
    )


def _step_of(step: _native.StepRow, *, load_output: bool = True) -> dict[str, Any]:
    return {
        "started_at_epoch_ms": step.started_at_epoch_ms,
        "completed_at_epoch_ms": step.completed_at_epoch_ms,
        "function_id": step.step_id,
        "function_name": step.step_name,
        "output": _codec_for(step.step_name).decode_output(step.output, step.serialization) if load_output and step.output is not None else None,
        "error": decode_exception(step.error, step.serialization) if load_output and step.error is not None else None,
        "child_workflow_id": step.child_workflow_id,
        "child_shard_key": None,
    }


def _in_daemon_thread[T](function: Callable[..., T], *args: Any) -> concurrent.futures.Future[T]:
    future: concurrent.futures.Future[T] = concurrent.futures.Future()

    def target() -> None:
        if not future.set_running_or_notify_cancel():
            return
        try:
            future.set_result(function(*args))
        except BaseException as error:  # noqa: BLE001
            future.set_exception(error)

    threading.Thread(target=target, name="dbos-sync", daemon=True).start()
    return future


def _call_watched(instance: DBOS, watch: _native.StepWatch, step_name: str, function: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    # shortcut: Python cannot kill a thread, so an expired body runs on until it calls DBOS.heartbeat() or returns,
    # and it can overlap the next attempt. Upgrade by running such steps in a subprocess if that overlap matters.
    body = _in_daemon_thread(functools.partial(contextvars.copy_context().run, function, *args, **kwargs))
    body.add_done_callback(lambda _: watch.finish())
    reason = instance._blocking(watch.expired)
    if reason is not None:
        raise DBOSStepTimeoutError(step_name, reason)
    return body.result()


async def _await_watched(watch: _native.StepWatch, step_name: str, coroutine: Coroutine[Any, Any, Any]) -> Any:
    body = asyncio.ensure_future(coroutine)
    expired = asyncio.ensure_future(watch.expired())
    try:
        await asyncio.wait({body, expired}, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        body.cancel()
        await asyncio.gather(body, return_exceptions=True)
        raise
    finally:
        expired.cancel()
    reason = watch.finish()
    if reason is None:
        return body.result()
    body.cancel()
    await asyncio.gather(body, return_exceptions=True)
    raise DBOSStepTimeoutError(step_name, reason)


def _cancel_task(task: asyncio.Task[Any]) -> None:
    try:
        task.get_loop().call_soon_threadsafe(task.cancel)
    except RuntimeError:
        pass


async def _drain() -> None:
    current = asyncio.current_task()
    tasks = [task for task in asyncio.all_tasks() if task is not current]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
