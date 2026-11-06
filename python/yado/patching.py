from __future__ import annotations

from typing import Any

from yado.errors import DBOSException
from yado.sdk_calls import _no_running_loop


def _workflow_run(method: str) -> tuple[Any, Any]:
    from yado.dbos import _current_run, _current_step, _running

    run = _current_run.get()
    if run is None or _current_step.get() is not None:
        raise DBOSException(f"DBOS.{method} must be called from a workflow")
    return _running(), run


class Patching:
    @classmethod
    def patch(cls, patch_name: str) -> bool:
        _no_running_loop("patch")
        instance, run = _workflow_run("patch")
        return bool(instance._blocking(instance._engine.patch, run, patch_name))

    @classmethod
    async def patch_async(cls, patch_name: str) -> bool:
        instance, run = _workflow_run("patch_async")
        return bool(await instance._engine.patch(run, patch_name))

    @classmethod
    def deprecate_patch(cls, patch_name: str) -> bool:
        _no_running_loop("deprecate_patch")
        instance, run = _workflow_run("deprecate_patch")
        instance._blocking(instance._engine.deprecate_patch, run, patch_name)
        return True

    @classmethod
    async def deprecate_patch_async(cls, patch_name: str) -> bool:
        instance, run = _workflow_run("deprecate_patch_async")
        await instance._engine.deprecate_patch(run, patch_name)
        return True
