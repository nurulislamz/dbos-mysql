from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import inspect
import logging
import os
import random
import threading
import time
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import Enum
from typing import Any, NotRequired, TypedDict

from dbos_mysql import _native
from dbos_mysql.errors import (
    DBOSException,
    DBOSLeaseLostError,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSUnexpectedStepError,
    MaxRecoveryAttemptsExceededError,
)
from dbos_mysql.serialization import Codec, decode_exception, encode_exception

logger = logging.getLogger("dbos")

DEFAULT_MAX_RECOVERY_ATTEMPTS = 100
POLL_MIN_SECONDS = 0.01
POLL_MAX_SECONDS = 1.0


class DBOSConfig(TypedDict):
    name: str
    system_database_url: str
    run_migrations: NotRequired[bool]
    lease_seconds: NotRequired[float]
    max_run_seconds: NotRequired[float]
    recovery_interval_seconds: NotRequired[float]
    max_concurrent_recoveries: NotRequired[int]
    shutdown_timeout_seconds: NotRequired[float]


class WorkflowStatusString(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    MAX_RECOVERY_ATTEMPTS_EXCEEDED = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"


@dataclass(frozen=True)
class WorkflowStatus:
    workflow_id: str
    status: str
    name: str
    input: Any
    output: Any
    error: Exception | None
    recovery_attempts: int
    max_recovery_attempts: int


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
    max_recovery_attempts: int


class _Run:
    def __init__(self, workflow_id: str, token: str) -> None:
        self.workflow_id = workflow_id
        self.token = token
        self._lock = threading.Lock()
        self._last_step_id = 0

    def next_step_id(self) -> int:
        with self._lock:
            self._last_step_id += 1
            return self._last_step_id


_current_run: contextvars.ContextVar[_Run | None] = contextvars.ContextVar("dbos_run", default=None)
_current_step: contextvars.ContextVar[StepStatus | None] = contextvars.ContextVar("dbos_step", default=None)
_next_workflow_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("dbos_next_id", default=None)


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
        self._thread.start()

    def submit[T](self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop)

    def run[T](self, coroutine: Coroutine[Any, Any, T]) -> T:
        return self.submit(coroutine).result()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


_registry: dict[str, _Workflow] = {}
_instance: DBOS | None = None


def _register(workflow: _Workflow) -> None:
    if workflow.name in _registry:
        logger.warning(f"Duplicate registration of function '{workflow.name}'")
    _registry[workflow.name] = workflow


def _running() -> DBOS:
    if _instance is None or _instance._store is None:
        raise DBOSException("DBOS is not launched. Call DBOS(config=...) and DBOS.launch() first")
    if _instance._pid != os.getpid():
        raise DBOSException("DBOS cannot be used after a fork. Call DBOS.launch() in the child process")
    return _instance


def _bind_run(run: _Run) -> None:
    _current_run.set(run)
    _current_step.set(None)
    _next_workflow_id.set(None)


@dataclass
class _Begun:
    workflow_id: str
    token: str | None
    existing: Any


def _is_final(status: str) -> bool:
    return status != WorkflowStatusString.PENDING.value


class WorkflowHandle[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> T:
        if self._local is not None:
            return self._local.result()
        instance = _running()
        return instance._state.run(instance._await_result(self.workflow_id))

    def get_status(self) -> WorkflowStatus:
        instance = _running()
        return instance._state.run(instance._status(self.workflow_id))


class WorkflowHandleAsync[T]:
    def __init__(self, workflow_id: str, local: concurrent.futures.Future[T] | None = None) -> None:
        self.workflow_id = workflow_id
        self._local = local

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self) -> T:
        if self._local is not None:
            return await asyncio.shield(asyncio.wrap_future(self._local))
        return await _running()._await_result(self.workflow_id)

    async def get_status(self) -> WorkflowStatus:
        return await _running()._status(self.workflow_id)


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


class DBOS(metaclass=_DBOSMeta):
    logger = logger

    def __new__(cls, *, config: DBOSConfig) -> DBOS:
        global _instance
        if _instance is not None:
            raise DBOSException("DBOS is already configured. Call DBOS.destroy() first")
        _instance = super().__new__(cls)
        return _instance

    def __init__(self, *, config: DBOSConfig) -> None:
        self._config = config
        self._lease_seconds = float(config.get("lease_seconds", 30.0))
        self._max_run_seconds = float(config.get("max_run_seconds", 3600.0))
        self._recovery_interval = float(config.get("recovery_interval_seconds", 1.0))
        self._max_recoveries = int(config.get("max_concurrent_recoveries", 50))
        self._shutdown_timeout = float(config.get("shutdown_timeout_seconds", 5.0))
        self._store: _native.Store | None = None
        self._pid = os.getpid()
        self._active: set[concurrent.futures.Future[Any]] = set()
        self._active_lock = threading.Lock()
        self._stopping = threading.Event()

    @classmethod
    def launch(cls) -> None:
        if _instance is None:
            raise DBOSException("Call DBOS(config=...) before DBOS.launch()")
        _instance._launch()

    def _launch(self) -> None:
        if self._store is not None:
            return
        self._pid = os.getpid()
        self._state = _LoopThread("dbos-state")
        self._workflows = _LoopThread("dbos-workflows")
        self._threads = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="dbos-sync")
        try:
            self._store = self._state.run(self._connect())
        except BaseException:
            self._threads.shutdown(wait=False)
            self._workflows.stop()
            self._state.stop()
            raise
        self._recovery = self._workflows.submit(self._recovery_loop())

    async def _connect(self) -> _native.Store:
        store = await _native.connect(self._config["system_database_url"])
        if self._config.get("run_migrations", True):
            await store.migrate()
        return store

    @classmethod
    def destroy(cls, *, destroy_registry: bool = False) -> None:
        global _instance
        instance = _instance
        _instance = None
        if destroy_registry:
            _registry.clear()
        if instance is not None and instance._store is not None:
            instance._shutdown()

    def _shutdown(self) -> None:
        self._stopping.set()
        with self._active_lock:
            active = list(self._active)
        for future in active:
            future.cancel()
        self._recovery.cancel()
        concurrent.futures.wait([*active, self._recovery], timeout=self._shutdown_timeout)
        self._workflows.run(_drain())
        self._threads.shutdown(wait=False)
        self._workflows.stop()
        self._state.stop()
        self._store = None

    @classmethod
    def workflow(cls, *, name: str | None = None, max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS) -> Callable[..., Any]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            is_async = inspect.iscoroutinefunction(function)
            info = _Workflow(name or function.__qualname__, function, Codec(function), is_async, max_recovery_attempts)
            _register(info)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                parent, explicit, step_id = _prepare_call()
                begun = instance._state.run(instance._begin(info, args, kwargs, parent, step_id, explicit))
                if begun.token is None:
                    return instance._state.run(instance._await_result(begun.workflow_id, info))
                return instance._run_sync(info, begun.workflow_id, begun.token, args, kwargs)

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                instance = _running()
                parent, explicit, step_id = _prepare_call()
                begun = await instance._begin(info, args, kwargs, parent, step_id, explicit)
                if begun.token is None:
                    return await instance._await_result(begun.workflow_id, info)
                return await instance._run_async(info, begun.workflow_id, begun.token, args, kwargs)

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
            attempts = max_attempts if retries_allowed else 1
            is_async = inspect.iscoroutinefunction(function)

            def retry_delay(attempt: int) -> float:
                return interval_seconds * backoff_rate ** (attempt - 1)

            def status(step_id: int, attempt: int) -> StepStatus:
                if not retries_allowed:
                    return StepStatus(step_id, None, None)
                return StepStatus(step_id, attempt - 1, max_attempts)

            def exhausted(errors: list[Exception]) -> Exception:
                if not retries_allowed:
                    return errors[-1]
                return DBOSMaxStepRetriesExceeded(step_name, max_attempts, errors)

            @functools.wraps(function)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                if run is None or _current_step.get() is not None:
                    return function(*args, **kwargs)
                instance = _running()
                step_id = run.next_step_id()
                saved = instance._state.run(instance._store_call("load_step", run.workflow_id, step_id, step_name))
                if saved is not None:
                    return _replay_step(saved, codec)
                errors: list[Exception] = []
                for attempt in range(1, attempts + 1):
                    reset = _current_step.set(status(step_id, attempt))
                    try:
                        value = function(*args, **kwargs)
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        if attempt < attempts:
                            time.sleep(retry_delay(attempt))
                        continue
                    finally:
                        _current_step.reset(reset)
                    return instance._state.run(instance._save_success(run, step_id, step_name, codec, value))
                raise instance._state.run(instance._save_failure(run, step_id, step_name, exhausted(errors)))

            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                run = _current_run.get()
                if run is None or _current_step.get() is not None:
                    return await function(*args, **kwargs)
                instance = _running()
                step_id = run.next_step_id()
                saved = await instance._store_call("load_step", run.workflow_id, step_id, step_name)
                if saved is not None:
                    return _replay_step(saved, codec)
                errors: list[Exception] = []
                for attempt in range(1, attempts + 1):
                    reset = _current_step.set(status(step_id, attempt))
                    try:
                        value = await function(*args, **kwargs)
                    except Exception as error:  # noqa: BLE001
                        errors.append(error)
                        if attempt < attempts:
                            await asyncio.sleep(retry_delay(attempt))
                        continue
                    finally:
                        _current_step.reset(reset)
                    return await instance._save_success(run, step_id, step_name, codec, value)
                raise await instance._save_failure(run, step_id, step_name, exhausted(errors))

            wrapper: Any = async_wrapper if is_async else sync_wrapper
            temp = _Workflow(f"<temp>.{step_name}", wrapper, codec, is_async, DEFAULT_MAX_RECOVERY_ATTEMPTS)
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
        parent, explicit, step_id = _prepare_call()
        begun = instance._state.run(instance._begin(info, args, kwargs, parent, step_id, explicit))
        return WorkflowHandle(begun.workflow_id, instance._background(info, begun, args, kwargs))

    @classmethod
    async def start_workflow_async(cls, function: Callable[..., Any], *args: Any, **kwargs: Any) -> WorkflowHandleAsync[Any]:
        instance = _running()
        info = _info_of(function)
        parent, explicit, step_id = _prepare_call()
        begun = await instance._begin(info, args, kwargs, parent, step_id, explicit)
        return WorkflowHandleAsync(begun.workflow_id, instance._background(info, begun, args, kwargs))

    @classmethod
    def retrieve_workflow(cls, workflow_id: str) -> WorkflowHandle[Any]:
        instance = _running()
        instance._state.run(instance._status(workflow_id))
        return WorkflowHandle(workflow_id)

    @classmethod
    async def retrieve_workflow_async(cls, workflow_id: str) -> WorkflowHandleAsync[Any]:
        await _running()._status(workflow_id)
        return WorkflowHandleAsync(workflow_id)

    @classmethod
    def get_result(cls, workflow_id: str) -> Any:
        instance = _running()
        return instance._state.run(instance._await_result(workflow_id))

    @classmethod
    def list_workflows(cls) -> list[WorkflowStatus]:
        instance = _running()
        return instance._state.run(instance._list_workflows())

    @classmethod
    async def list_workflows_async(cls) -> list[WorkflowStatus]:
        return await _running()._list_workflows()

    @classmethod
    def list_workflow_steps(cls, workflow_id: str) -> list[dict[str, Any]]:
        instance = _running()
        return instance._state.run(instance._list_steps(workflow_id))

    @classmethod
    async def list_workflow_steps_async(cls, workflow_id: str) -> list[dict[str, Any]]:
        return await _running()._list_steps(workflow_id)

    @classmethod
    def _recover_pending_workflows(cls) -> list[WorkflowHandle[Any]]:
        instance = _running()
        return instance._workflows.run(instance._recover_once(instance._max_recoveries))

    async def _store_call(self, method: str, *args: Any) -> Any:
        assert self._store is not None
        return await getattr(self._store, method)(*args)

    async def _begin(
        self,
        info: _Workflow,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        parent: _Run | None,
        step_id: int | None,
        explicit: str | None,
    ) -> _Begun:
        workflow_id = explicit or str(uuid.uuid4())
        if parent is not None and step_id is not None:
            workflow_id = await self._record_child(info, parent, step_id, explicit)
        text = info.codec.encode_input(args, kwargs)
        started = await self._store_call(
            "start_workflow", workflow_id, info.name, text, info.max_recovery_attempts, self._lease_seconds
        )
        return _Begun(workflow_id, started.token, started.existing)

    async def _record_child(self, info: _Workflow, parent: _Run, step_id: int, explicit: str | None) -> str:
        saved = await self._store_call("load_step", parent.workflow_id, step_id, info.name)
        if saved is not None:
            if saved.child_workflow_id is None:
                raise DBOSUnexpectedStepError(f"Step {step_id} of workflow {parent.workflow_id} is not a child workflow")
            return saved.child_workflow_id
        child_id = explicit or f"{parent.workflow_id}-{step_id}"
        await self._store_call("save_step", parent.workflow_id, parent.token, step_id, info.name, None, None, child_id)
        return child_id

    def _background(self, info: _Workflow, begun: _Begun, args: tuple[Any, ...], kwargs: dict[str, Any]) -> concurrent.futures.Future[Any] | None:
        if begun.token is None:
            return None
        return self._launch_run(info, begun.workflow_id, begun.token, args, kwargs)

    def _launch_run(self, info: _Workflow, workflow_id: str, token: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> concurrent.futures.Future[Any]:
        if info.is_async:
            future = self._workflows.submit(self._run_async(info, workflow_id, token, args, kwargs))
        else:
            future = self._threads.submit(self._run_sync, info, workflow_id, token, args, kwargs)
        with self._active_lock:
            self._active.add(future)
        future.add_done_callback(self._forget)
        return future

    def _forget(self, future: concurrent.futures.Future[Any]) -> None:
        with self._active_lock:
            self._active.discard(future)

    def _keeper(self, workflow_id: str, token: str) -> _native.LeaseKeeper:
        assert self._store is not None
        return self._store.keep_lease(workflow_id, token, self._lease_seconds, self._max_run_seconds)

    async def _run_async(self, info: _Workflow, workflow_id: str, token: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        keeper = self._keeper(workflow_id, token)
        context = contextvars.copy_context()
        context.run(_bind_run, _Run(workflow_id, token))
        body = asyncio.get_running_loop().create_task(info.function(*args, **kwargs), context=context)
        lost = asyncio.ensure_future(keeper.lost())
        try:
            await asyncio.wait({body, lost}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            body.cancel()
            await asyncio.gather(body, return_exceptions=True)
            await self._release(workflow_id, token)
            raise
        finally:
            keeper.stop()
            lost.cancel()

        if not body.done():
            logger.warning(f"Workflow {workflow_id} lost its lease")
            body.cancel()
            await asyncio.gather(body, return_exceptions=True)
            return await self._await_result(workflow_id, info)
        if body.cancelled():
            await self._release(workflow_id, token)
            raise asyncio.CancelledError
        error = body.exception()
        if error is not None:
            return await self._finish_error(info, workflow_id, token, error)
        return await self._finish_success(info, workflow_id, token, body.result())

    def _run_sync(self, info: _Workflow, workflow_id: str, token: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        keeper = self._keeper(workflow_id, token)
        context = contextvars.copy_context()
        context.run(_bind_run, _Run(workflow_id, token))
        try:
            value = context.run(info.function, *args, **kwargs)
        except Exception as error:  # noqa: BLE001
            keeper.stop()
            return self._state.run(self._finish_error(info, workflow_id, token, error))
        except BaseException:
            keeper.stop()
            self._state.run(self._release(workflow_id, token))
            raise
        keeper.stop()
        return self._state.run(self._finish_success(info, workflow_id, token, value))

    async def _finish_success(self, info: _Workflow, workflow_id: str, token: str, value: Any) -> Any:
        try:
            text = info.codec.encode_output(value)
        except TypeError as error:
            return await self._finish_error(info, workflow_id, token, error)
        try:
            await self._store_call("finish_success", workflow_id, token, text)
        except DBOSLeaseLostError:
            logger.warning(f"Workflow {workflow_id} finished after losing its lease, so its result was discarded")
            return await self._await_result(workflow_id, info)
        return info.codec.decode_output(text)

    async def _finish_error(self, info: _Workflow, workflow_id: str, token: str, error: BaseException) -> Any:
        if isinstance(error, DBOSLeaseLostError):
            return await self._await_result(workflow_id, info)
        try:
            await self._store_call("finish_error", workflow_id, token, encode_exception(error))
        except DBOSLeaseLostError:
            logger.warning(f"Workflow {workflow_id} failed after losing its lease, so its error was discarded")
            return await self._await_result(workflow_id, info)
        raise error

    async def _release(self, workflow_id: str, token: str) -> None:
        try:
            await self._store_call("release_lease", workflow_id, token)
        except DBOSLeaseLostError:
            pass

    async def _save_success(self, run: _Run, step_id: int, step_name: str, codec: Codec, value: Any) -> Any:
        try:
            text = codec.encode_output(value)
        except TypeError as error:
            raise await self._save_failure(run, step_id, step_name, error) from error
        await self._store_call("save_step", run.workflow_id, run.token, step_id, step_name, text, None, None)
        return codec.decode_output(text)

    async def _save_failure(self, run: _Run, step_id: int, step_name: str, error: Exception) -> Exception:
        await self._store_call("save_step", run.workflow_id, run.token, step_id, step_name, None, encode_exception(error), None)
        return error

    async def _await_result(self, workflow_id: str, info: _Workflow | None = None) -> Any:
        delay = POLL_MIN_SECONDS
        while True:
            row = await self._store_call("get_workflow", workflow_id)
            if row is None:
                raise DBOSNonExistentWorkflowError(f"Workflow {workflow_id} does not exist")
            if _is_final(row.status):
                return _result_of(row, info or _registry.get(row.name))
            await asyncio.sleep(delay)
            delay = min(delay * 2, POLL_MAX_SECONDS)

    async def _status(self, workflow_id: str) -> WorkflowStatus:
        row = await self._store_call("get_workflow", workflow_id)
        if row is None:
            raise DBOSNonExistentWorkflowError(f"Workflow {workflow_id} does not exist")
        return _status_of(row)

    async def _list_workflows(self) -> list[WorkflowStatus]:
        return [_status_of(row) for row in await self._store_call("list_workflows")]

    async def _list_steps(self, workflow_id: str) -> list[dict[str, Any]]:
        steps = await self._store_call("list_steps", workflow_id)
        return [
            {
                "function_id": step.step_id,
                "function_name": step.step_name,
                "output": step.output,
                "error": step.error,
                "child_workflow_id": step.child_workflow_id,
            }
            for step in steps
        ]

    async def _recovery_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._recover_once(max(self._max_recoveries - len(self._active), 0))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Workflow recovery failed, and will be tried again")
            await asyncio.sleep(self._recovery_interval * random.uniform(0.5, 1.5))  # noqa: S311

    async def _recover_once(self, limit: int) -> list[WorkflowHandle[Any]]:
        names = list(_registry)
        if not names or limit == 0:
            return []
        claims = await self._store_call("reclaim", names, limit, self._lease_seconds)
        return [self._resume(claim) for claim in claims]

    def _resume(self, claim: Any) -> WorkflowHandle[Any]:
        info = _registry[claim.name]
        logger.info(f"Recovering workflow {claim.workflow_id}, attempt {claim.recovery_attempts}")
        args, kwargs = info.codec.decode_input(claim.input)
        return WorkflowHandle(claim.workflow_id, self._launch_run(info, claim.workflow_id, claim.token, args, kwargs))


def _prepare_call() -> tuple[_Run | None, str | None, int | None]:
    parent = _current_run.get()
    explicit = _next_workflow_id.get()
    step_id = parent.next_step_id() if parent is not None and _current_step.get() is None else None
    return parent, explicit, step_id


def _info_of(function: Callable[..., Any]) -> _Workflow:
    info = getattr(function, "__dbos_workflow__", None)
    if info is None:
        raise DBOSException(f"{getattr(function, '__qualname__', function)} is not a registered workflow or step")
    return info


def _replay_step(saved: Any, codec: Codec) -> Any:
    if saved.error is not None:
        raise decode_exception(saved.error)
    if saved.output is None:
        raise DBOSUnexpectedStepError(f"Step {saved.step_id} ({saved.step_name}) is a child workflow, not a step")
    return codec.decode_output(saved.output)


def _result_of(row: Any, info: _Workflow | None) -> Any:
    if row.status == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value:
        raise MaxRecoveryAttemptsExceededError(row.workflow_id, row.max_recovery_attempts)
    if row.error is not None:
        raise decode_exception(row.error)
    if info is not None:
        return info.codec.decode_output(row.output)
    return Codec(lambda: None).decode_output(row.output) if row.output is not None else None


def _status_of(row: Any) -> WorkflowStatus:
    info = _registry.get(row.name)
    output = None
    if row.output is not None:
        output = _result_of(row, info)
    return WorkflowStatus(
        workflow_id=row.workflow_id,
        status=row.status,
        name=row.name,
        input=row.input,
        output=output,
        error=decode_exception(row.error) if row.error is not None else None,
        recovery_attempts=row.recovery_attempts,
        max_recovery_attempts=row.max_recovery_attempts,
    )


async def _drain() -> None:
    current = asyncio.current_task()
    tasks = [task for task in asyncio.all_tasks() if task is not current]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
