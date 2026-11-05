from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Callable


def _no_running_loop(fn: str) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(f"{fn}() was called while an event loop is running. Use await {fn}_async(...) instead.")


def _internal_step(options: dict[str, Any], function: Callable[..., Any]) -> Callable[..., Any]:
    # Built per call and never started as a workflow, so it stays out of DBOS's registry.
    from rust_dwos.dbos import DBOS, _unregistered_step

    token = _unregistered_step.set(True)
    try:
        return DBOS.step(**options)(function)
    finally:
        _unregistered_step.reset(token)


class SDKCalls:
    @classmethod
    def reserve_step(cls) -> Any:
        from rust_dwos.dbos import _current_run, _current_step
        from rust_dwos.errors import DBOSException
        from rust_dwos.step_reservation import StepReservation

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            raise DBOSException("DBOS.reserve_step must be called from a workflow, outside a step")
        return StepReservation(run, run.reserve_step_id())

    @classmethod
    async def asyncio_wait(cls, fs: list[Any], *, timeout: float | None = None, return_when: str = asyncio.ALL_COMPLETED) -> tuple[set[Any], set[Any]]:
        tasks = [asyncio.ensure_future(f) for f in fs]

        async def wait() -> list[int]:
            done, _ = await asyncio.wait(tasks, timeout=timeout, return_when=return_when)
            return [i for i, task in enumerate(tasks) if task in done]

        indices = set(await _internal_step({"name": "DBOS.asyncio_wait"}, wait)())
        done = {task for i, task in enumerate(tasks) if i in indices}
        pending = {task for i, task in enumerate(tasks) if i not in indices}
        if done:
            await asyncio.wait(done)
        return done, pending

    @classmethod
    def run_step(cls, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        _no_running_loop("run_step")
        options = {"name": func.__qualname__, **(dbos_step_options or {})}
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)  # keeps func's annotations, which the step codec reads
            async def invoke(*call_args: Any, **call_kwargs: Any) -> Any:
                return await func(*call_args, **call_kwargs)
            # shortcut: each async call gets its own event loop, so loop-bound objects are not shared across steps, upgrade to DBOS's background loop if async steps need them.
            return asyncio.run(_internal_step(options, invoke)(*args, **kwargs))
        return _internal_step(options, func)(*args, **kwargs)

    @classmethod
    async def run_step_async(cls, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        options = {"name": func.__qualname__, **(dbos_step_options or {})}
        if inspect.iscoroutinefunction(func):
            return await _internal_step(options, func)(*args, **kwargs)

        @functools.wraps(func)
        async def invoke(*call_args: Any, **call_kwargs: Any) -> Any:
            return await asyncio.to_thread(func, *call_args, **call_kwargs)

        return await _internal_step(options, invoke)(*args, **kwargs)
