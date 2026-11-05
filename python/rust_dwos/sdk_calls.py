from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable


class SDKCalls:
    @classmethod
    async def asyncio_wait(cls, fs: list[Any], *, timeout: float | None = None, return_when: str = asyncio.ALL_COMPLETED) -> tuple[set[Any], set[Any]]:
        tasks = [asyncio.ensure_future(f) for f in fs]

        @cls.step(name="DBOS.asyncio_wait")
        async def wait() -> list[int]:
            done, _ = await asyncio.wait(tasks, timeout=timeout, return_when=return_when)
            return [i for i, task in enumerate(tasks) if task in done]

        indices = set(await wait())
        done = {task for i, task in enumerate(tasks) if i in indices}
        pending = {task for i, task in enumerate(tasks) if i not in indices}
        if done:
            await asyncio.wait(done)
        return done, pending

    @classmethod
    def run_step(cls, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        options = {"name": func.__qualname__, **(dbos_step_options or {})}
        if inspect.iscoroutinefunction(func):
            async def invoke() -> Any:
                return await func(*args, **kwargs)
            return asyncio.run(cls.step(**options)(invoke)())
        return cls.step(**options)(func)(*args, **kwargs)

    @classmethod
    async def run_step_async(cls, dbos_step_options: dict[str, Any] | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        options = {"name": func.__qualname__, **(dbos_step_options or {})}
        if inspect.iscoroutinefunction(func):
            return await cls.step(**options)(func)(*args, **kwargs)
        async def invoke() -> Any:
            return await asyncio.to_thread(func, *args, **kwargs)
        return await cls.step(**options)(invoke)()
