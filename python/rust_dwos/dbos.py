from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import concurrent.futures.thread  # registers its exit hook first, so ours runs before it
import contextvars
import functools
import inspect
import logging
import os
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, NotRequired, TypedDict

from rust_dwos import _native
from rust_dwos.errors import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSWorkflowCancelledError,
)
from rust_dwos.events import Events
from rust_dwos.queues import Queue, Queues
from rust_dwos.serialization import Codec, allow_pickle, decode_exception, encode_exception
from rust_dwos.sleep import Sleep
from rust_dwos.timeouts import _timeout_override, timeout_start_args
from rust_dwos.workflow_management import WorkflowManagement

logger = logging.getLogger("dbos")

_ENGINE_OPTIONS = ("lease_seconds", "max_run_seconds", "recovery_interval_seconds", "max_concurrent_recoveries", "run_migrations", "serializer")
_PLAIN_CODEC = Codec(lambda: None)
_SYSTEM_EXIT = object()


class DBOSConfig(TypedDict):
    name: str
    system_database_url: str
    run_migrations: NotRequired[bool]
    lease_seconds: NotRequired[float]
    max_run_seconds: NotRequired[float]
    recovery_interval_seconds: NotRequired[float]
    max_concurrent_recoveries: NotRequired[int]
    shutdown_timeout_seconds: NotRequired[float]
    serializer: NotRequired[Literal["json", "pickle"]]


class WorkflowStatusString(Enum):
    PENDING = "PENDING"
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


@dataclass(frozen=True)
class StepStatus:
    step_id: int
    current_attempt: int | None
    max_attempts: int | None


@dataclass
class _Workflow:
    name: str
    function: Callable[..., Any]
    codec: Codec
    is_async: bool
    max_recovery_attempts: int | None


_current_run: contextvars.ContextVar[_native.Run | None] = contextvars.ContextVar("dbos_run", default=None)
_current_step: contextvars.ContextVar[StepStatus | None] = contextvars.ContextVar("dbos_step", default=None)
_next_workflow_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("dbos_next_id", default=None)
_current_active: contextvars.ContextVar[_native.ActiveRun | None] = contextvars.ContextVar("dbos_active", default=None)


class SetWorkflowID:
    def __init__(self, workflow_id: str) -> None:
        self._workflow_id = workflow_id
        self._token: contextvars.Token[str | None] | None = None

    def __enter__(self) -> SetWorkflowID:
        self._token = _next_workflow_id.set(self._workflow_id)
        return self

    def __exit__(self, *_: object) -> None:
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
    _current_run.set(run)
    _current_active.set(active)
    _current_step.set(None)
    _next_workflow_id.set(None)
    _timeout_override.set(None)


async def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await method(*args, **kwargs)


class WorkflowHandle[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> T:
        if self._local is not None:
            try:
                return self._local.result()
            except DBOSAwaitedWorkflowCancelledError:
                pass  # It may have been resumed since, so the stored row decides.
        instance = _running()
        return _decode_result(instance._waiting(instance._engine.await_result, self.workflow_id))

    def get_status(self) -> WorkflowStatus:
        instance = _running()
        return _status_of(instance._blocking(instance._engine.get_status, self.workflow_id))


class WorkflowHandleAsync[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | asyncio.Task[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self) -> T:
        local = self._local
        try:
            if isinstance(local, asyncio.Task):
                if local.get_loop() is asyncio.get_running_loop():
                    return await asyncio.shield(local)
            elif local is not None:
                return await asyncio.shield(asyncio.wrap_future(local))
        except DBOSAwaitedWorkflowCancelledError:
            pass  # It may have been resumed since, so the stored row decides.
        return _decode_result(await _running()._engine.await_result(self.workflow_id))

    async def get_status(self) -> WorkflowStatus:
        return _status_of(await _running()._engine.get_status(self.workflow_id))


class _DBOSMeta(type):
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


class DBOS(Sleep, WorkflowManagement, Events, Queues, metaclass=_DBOSMeta):
    logger = logger

    def __new__(cls, *, config: DBOSConfig) -> DBOS:
        global _instance
        if _instance is not None:
            raise DBOSException("DBOS is already configured. Call DBOS.destroy() first")
        _instance = super().__new__(cls)
        return _instance

    def __init__(self, *, config: DBOSConfig) -> None:
        self._config = config
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
        self._queue_wakes: dict[str, asyncio.Event] = {}
        self._queue_loops: list[concurrent.futures.Future[Any]] = []

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
        try:
            self._engine = self._state.run(_call(_native.Engine.connect, self._config["system_database_url"], **options))
        except BaseException:
            self._workflows.stop()
            self._state.stop()
            raise
        allow_pickle(self._engine.serialization == _native.PY_PICKLE)
        self._recovery = self._workflows.submit(self._recovery_loop())

    def _blocking(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        # Waits on this thread with the GIL released. No event loop is involved, so a step pays no thread hops.
        if self._state._closing:
            raise DBOSException("DBOS was destroyed while this work was running")
        return method(*args, blocking=True, **kwargs)

    def _waiting(self, method: Callable[..., Any], *args: Any) -> Any:
        # For a wait with no end of its own. Destroy cancels what runs on this loop, which wakes the caller.
        return self._state.run(_call(method, *args))

    @classmethod
    def destroy(cls, *, destroy_registry: bool = False) -> None:
        global _instance
        instance = _instance
        if instance is not None and instance._engine is not None:
            instance._engine.close()
        _instance = None
        if destroy_registry:
            _registry.clear()
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
        for loop in self._queue_loops:
            loop.cancel()
        concurrent.futures.wait([*active, self._recovery, *self._queue_loops], timeout=self._shutdown_timeout)
        self._workflows.stop()
        self._state.stop()
        with self._active_lock:
            keepers = list(self._keepers)
        for keeper in keepers:
            keeper.stop()

    @classmethod
    def workflow(cls, *, name: str | None = None, max_recovery_attempts: int | None = _native.DEFAULT_MAX_RECOVERY_ATTEMPTS) -> Callable[..., Any]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            is_async = inspect.iscoroutinefunction(function)
            info = _Workflow(name or function.__qualname__, function, Codec(function), is_async, max_recovery_attempts)
            _register(info)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                begun = instance._blocking(instance._engine.begin, *_begin_args(info, args, kwargs))
                if begun.run is not None:
                    return instance._run_sync(info, begun.run, args, kwargs)
                result = begun.result or instance._waiting(instance._engine.await_result, begun.workflow_id)
                return _decode_result(result, info)

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                begun = await instance._engine.begin(*_begin_args(info, args, kwargs))
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
    ) -> Callable[..., Any]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            step_name = name or function.__qualname__
            codec = Codec(function)
            policy = _native.StepPolicy(retries_allowed, interval_seconds, max_attempts, backoff_rate)
            is_async = inspect.iscoroutinefunction(function)

            def failure(errors: list[Exception]) -> Exception:
                if not policy.wraps_failures:
                    return errors[-1]
                return DBOSMaxStepRetriesExceeded(step_name, policy.max_attempts, errors)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                if run is None:
                    return function(*args, **kwargs)
                instance = _running()
                step_id, replay = instance._blocking(instance._engine.begin_step, run, step_name, _current_step.get() is not None)
                if step_id is None:
                    return function(*args, **kwargs)
                if replay is not None:
                    return _replay_step(replay, codec, run.serialization)
                errors: list[Exception] = []
                attempts = policy.attempts()
                while True:
                    reset = _current_step.set(StepStatus(step_id, *attempts.status()))
                    try:
                        value = function(*args, **kwargs)
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        delay = attempts.failed()
                        if delay is None:
                            break
                        time.sleep(delay)
                        continue
                    finally:
                        _current_step.reset(reset)
                    return instance._save_success_sync(run, step_id, step_name, codec, value)
                raise instance._save_failure_sync(run, step_id, step_name, failure(errors))

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                if run is None:
                    return await function(*args, **kwargs)
                instance = _running()
                step_id, replay = await instance._engine.begin_step(run, step_name, _current_step.get() is not None)
                if step_id is None:
                    return await function(*args, **kwargs)
                if replay is not None:
                    return _replay_step(replay, codec, run.serialization)
                errors: list[Exception] = []
                attempts = policy.attempts()
                while True:
                    reset = _current_step.set(StepStatus(step_id, *attempts.status()))
                    try:
                        value = await function(*args, **kwargs)
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        delay = attempts.failed()
                        if delay is None:
                            break
                        await asyncio.sleep(delay)
                        continue
                    finally:
                        _current_step.reset(reset)
                    return await instance._save_success(run, step_id, step_name, codec, value)
                raise await instance._save_failure(run, step_id, step_name, failure(errors))

            wrapper: Any = async_wrapper if is_async else sync_wrapper
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
    def start_workflow(cls, function: Callable[..., Any], *args: Any, **kwargs: Any) -> WorkflowHandle[Any]:
        instance = _running()
        info = _info_of(function)
        begun = instance._blocking(instance._engine.begin, *_begin_args(info, args, kwargs))
        return WorkflowHandle(begun.workflow_id, instance._background(info, begun, args, kwargs))

    @classmethod
    async def start_workflow_async(cls, function: Callable[..., Any], *args: Any, **kwargs: Any) -> WorkflowHandleAsync[Any]:
        instance = _running()
        info = _info_of(function)
        begun = await instance._engine.begin(*_begin_args(info, args, kwargs))
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
    def get_result(cls, workflow_id: str) -> Any:
        instance = _running()
        return _decode_result(instance._waiting(instance._engine.await_result, workflow_id))

    @classmethod
    async def get_result_async(cls, workflow_id: str) -> Any:
        return _decode_result(await _running()._engine.await_result(workflow_id))

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
    def list_workflow_steps(cls, workflow_id: str) -> list[dict[str, Any]]:
        instance = _running()
        return [_step_of(step) for step in instance._blocking(instance._engine.list_steps, workflow_id)]

    @classmethod
    async def list_workflow_steps_async(cls, workflow_id: str) -> list[dict[str, Any]]:
        return [_step_of(step) for step in await _running()._engine.list_steps(workflow_id)]

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
        active = self._keep_lease(run)
        context = contextvars.copy_context()
        context.run(_bind_run, run, active)

        async def guarded() -> Any:
            try:
                return await info.function(*args, **kwargs)
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
            return _decode_result(await self._engine.await_result(run.workflow_id), info)
        if body.cancelled():
            await self._engine.conclude(run)
            raise asyncio.CancelledError
        error = body.exception()
        if error is None:
            value = body.result()
            if value is _SYSTEM_EXIT:
                logger.warning(f"Workflow {run.workflow_id} called sys.exit(), so it was released for recovery")
                await self._engine.conclude(run)
                return None
            return await self._finish_success(info, run, value)
        if isinstance(error, DBOSWorkflowCancelledError):
            await self._engine.conclude(run)
            return _decode_result(await self._engine.await_result(run.workflow_id), info)
        if isinstance(error, Exception):
            return await self._finish_error(info, run, error)
        await self._engine.conclude(run)
        raise error

    def _run_sync(self, info: _Workflow, run: _native.Run, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        active = self._keep_lease(run)
        context = contextvars.copy_context()
        context.run(_bind_run, run, active)
        try:
            value = context.run(info.function, *args, **kwargs)
        except DBOSWorkflowCancelledError:
            self._stop_keeper(active)
            self._blocking(self._engine.conclude, run)
            return _decode_result(self._waiting(self._engine.await_result, run.workflow_id), info)
        except Exception as error:  # noqa: BLE001
            self._stop_keeper(active)
            return self._finish_error_sync(info, run, error)
        except BaseException:
            self._stop_keeper(active)
            self._blocking(self._engine.conclude, run)
            raise
        self._stop_keeper(active)
        return self._finish_success_sync(info, run, value)

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

    def _add_queue(self, queue: Queue) -> Queue:
        with self._active_lock:
            known = self._queues.setdefault(queue.name, queue)
            if known is queue:
                self._queue_loops.append(self._workflows.submit(self._queue_loop(queue)))
        return known

    def _wake_queue(self, name: str) -> None:
        wake = self._queue_wakes.get(name)
        if wake is not None and not self._workflows._closing:
            self._workflows._loop.call_soon_threadsafe(wake.set)

    def _wake_queues(self) -> None:
        for name in self._queue_wakes:
            self._wake_queue(name)

    async def _queue_loop(self, queue: Queue) -> None:
        # Local enqueues and finishes wake this at once. The interval only matters for work from other processes.
        wake = self._queue_wakes[queue.name] = asyncio.Event()
        while True:
            wake.clear()
            try:
                claims = await self._engine.claim_queue(queue.name, list(_registry))
            except Exception:  # noqa: BLE001
                logger.exception(f"Could not read queue {queue.name}. It will be tried again")
                claims = []
            for claim in claims:
                try:
                    self._launch_claim(queue, claim)
                except Exception:  # noqa: BLE001
                    logger.exception(f"Could not start workflow {claim.workflow_id}. It will be retried when its lease expires")
            if claims:
                continue
            try:
                await asyncio.wait_for(wake.wait(), queue.polling_interval_sec)
            except TimeoutError:
                pass

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


def _begin_args(info: _Workflow, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    text = info.codec.encode_input(args, kwargs, _running()._engine.serialization)
    return (info.name, text, info.max_recovery_attempts, _next_workflow_id.get(), _current_run.get(), _current_step.get() is not None, *timeout_start_args())


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
    )


def _step_of(step: _native.StepRow) -> dict[str, Any]:
    return {
        "function_id": step.step_id,
        "function_name": step.step_name,
        "output": _codec_for(step.step_name).decode_output(step.output, step.serialization) if step.output is not None else None,
        "error": decode_exception(step.error, step.serialization) if step.error is not None else None,
        "child_workflow_id": step.child_workflow_id,
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
