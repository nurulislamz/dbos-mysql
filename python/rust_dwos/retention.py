from __future__ import annotations

from typing import Any

DEFAULT_DELETE_BATCH = 200


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


class Retention:
    @classmethod
    def delete_workflow(cls, workflow_id: str, *, delete_children: bool = False) -> list[str]:
        return cls.delete_workflows([workflow_id], delete_children=delete_children)

    @classmethod
    async def delete_workflow_async(cls, workflow_id: str, *, delete_children: bool = False) -> list[str]:
        return await cls.delete_workflows_async([workflow_id], delete_children=delete_children)

    @classmethod
    def delete_workflows(cls, workflow_ids: list[str], *, delete_children: bool = False) -> list[str]:
        """Returns the ids deleted. Pending rows and children of pending rows stay. A deleted ID can run again."""
        instance = _dbos()._running()
        return instance._blocking(instance._engine.delete_workflows, workflow_ids, delete_children)

    @classmethod
    async def delete_workflows_async(cls, workflow_ids: list[str], *, delete_children: bool = False) -> list[str]:
        return await _dbos()._running()._engine.delete_workflows(workflow_ids, delete_children)

    @classmethod
    def delete_finished_workflows(cls, before_epoch_ms: int, *, batch: int = DEFAULT_DELETE_BATCH) -> int:
        """Returns how many rows were deleted. Children of pending, cancelled or dead-lettered parents stay."""
        instance = _dbos()._running()
        return instance._blocking(instance._engine.delete_finished_before, before_epoch_ms, batch)

    @classmethod
    async def delete_finished_workflows_async(cls, before_epoch_ms: int, *, batch: int = DEFAULT_DELETE_BATCH) -> int:
        return await _dbos()._running()._engine.delete_finished_before(before_epoch_ms, batch)
