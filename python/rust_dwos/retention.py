from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from rust_dwos.errors import DBOSException

DEFAULT_DELETE_BATCH = 200
DEFAULT_RETENTION_INTERVAL_SECONDS = 3600.0
logger = logging.getLogger("dbos")


def retention_policy_of(config: Mapping[str, Any]) -> tuple[float | None, int | None, float] | None:
    """Hours, rows and interval from the config. None when no limit is set, so nothing is deleted in the background."""
    hours = config.get("retention_hours")
    rows = config.get("retention_rows")
    interval = float(config.get("retention_interval_seconds", DEFAULT_RETENTION_INTERVAL_SECONDS))
    if hours is None and rows is None:
        return None
    if hours is not None and not float(hours) > 0:
        raise DBOSException("retention_hours must be positive")
    if rows is not None and int(rows) < 1:
        raise DBOSException("retention_rows must be at least 1")
    if not interval > 0:
        raise DBOSException("retention_interval_seconds must be positive")
    return (None if hours is None else float(hours), None if rows is None else int(rows), interval)


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
        """Returns how many rows were deleted. Children of pending, cancelled or dead-lettered parents stay. On MySQL a row another transaction holds can cut the count short, so call again."""
        instance = _dbos()._running()
        return instance._blocking(instance._engine.delete_finished_before, before_epoch_ms, batch)

    @classmethod
    async def delete_finished_workflows_async(cls, before_epoch_ms: int, *, batch: int = DEFAULT_DELETE_BATCH) -> int:
        return await _dbos()._running()._engine.delete_finished_before(before_epoch_ms, batch)

    async def _retention_loop(self) -> None:
        """Runs the config's retention policy every interval. Does nothing when the config sets no limit."""
        if self._retention_policy is None:
            return
        hours, rows, interval = self._retention_policy
        while True:
            await asyncio.sleep(interval)
            try:
                deleted = await self._engine.apply_retention(hours, rows, DEFAULT_DELETE_BATCH)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("deleting old workflows failed, and will be tried again")
            else:
                if deleted:
                    logger.info(f"Deleted {deleted} finished workflows outside the retention policy")
