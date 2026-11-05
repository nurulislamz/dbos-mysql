from __future__ import annotations

import asyncio
import time

from rust_dwos.errors import DBOSLeaseLostError


class Sleep:
    @classmethod
    def sleep(cls, seconds: float) -> None:
        if seconds <= 0:
            return
        from rust_dwos.dbos import _current_active, _current_run, _current_step, _running

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            time.sleep(seconds)
            return
        instance = _running()
        remaining = instance._blocking(instance._engine.sleep, run, seconds, False)
        # Waits on the lease keeper, so a cancelled workflow wakes up without polling.
        if _current_active.get().wait_lost(remaining) is not None:
            raise DBOSLeaseLostError(f"this process no longer owns workflow {run.workflow_id}")

    @classmethod
    async def sleep_async(cls, seconds: float) -> None:
        if seconds <= 0:
            return
        from rust_dwos.dbos import _current_run, _current_step, _running

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            await asyncio.sleep(seconds)
            return
        await asyncio.sleep(await _running()._engine.sleep(run, seconds, False))
