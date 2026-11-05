from __future__ import annotations

from typing import Any


def _handles(handles: list[Any]) -> dict[str, Any]:
    if not handles:
        raise ValueError("handles must not be empty")
    result = {handle.workflow_id: handle for handle in handles}
    if len(result) != len(handles):
        raise ValueError("handles must not contain duplicate workflow IDs")
    return result


class WaitFirst:
    @classmethod
    def wait_first(cls, handles: list[Any], *, polling_interval_sec: float = 1.0) -> Any:
        from rust_dwos.dbos import _running

        by_id = _handles(handles)

        @cls.step(name="DBOS.waitFirst")
        def wait() -> str:
            instance = _running()
            return instance._waiting(instance._engine.await_first, list(by_id), polling_interval_sec)

        return by_id[wait()]

    @classmethod
    async def wait_first_async(cls, handles: list[Any], *, polling_interval_sec: float = 1.0) -> Any:
        from rust_dwos.dbos import _running

        by_id = _handles(handles)

        @cls.step(name="DBOS.waitFirst")
        async def wait() -> str:
            return await _running()._engine.await_first(list(by_id), polling_interval_sec)

        return by_id[await wait()]
