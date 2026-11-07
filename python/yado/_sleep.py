from __future__ import annotations

import asyncio
import time

from yado.errors import DBOSLeaseLostError


class Sleep:
    @classmethod
    def sleep(cls, seconds: float) -> None:
        if seconds <= 0:
            return
        from yado.dbos import _current_active, _current_run, _current_step, _running

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            time.sleep(seconds)
            return
        instance = _running()
        remaining = instance._blocking(instance._engine.sleep, run, seconds, False)
        if instance._blocking(instance._engine.wait_sleep, run, _current_active.get(), remaining) is not None:
            raise DBOSLeaseLostError(f"this process no longer owns workflow {run.workflow_id}")

    @classmethod
    async def sleep_async(cls, seconds: float) -> None:
        if seconds <= 0:
            return
        from yado.dbos import _current_active, _current_run, _current_step, _running

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            await asyncio.sleep(seconds)
            return
        instance = _running()
        remaining = await instance._engine.sleep(run, seconds, False)
        if await instance._engine.wait_sleep(run, _current_active.get(), remaining) is not None:
            raise DBOSLeaseLostError(f"this process no longer owns workflow {run.workflow_id}")
