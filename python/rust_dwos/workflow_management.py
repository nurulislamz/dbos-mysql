from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rust_dwos.dbos import WorkflowHandle, WorkflowHandleAsync, WorkflowStatus


def _as_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    return value if isinstance(value, list) else [value]


def _millis(value: str | None) -> int | None:
    if value is None:
        return None
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _dbos() -> Any:
    from rust_dwos import dbos

    return dbos


def _filters(
    workflow_ids: list[str] | None,
    status: str | list[str] | None,
    start_time: str | None,
    end_time: str | None,
    completed_after: str | None,
    completed_before: str | None,
    name: str | list[str] | None,
    parent_workflow_id: str | list[str] | None,
    limit: int | None,
    offset: int | None,
    sort_desc: bool,
    workflow_id_prefix: str | list[str] | None,
    has_parent: bool | None,
) -> dict[str, Any]:
    return {
        "workflow_ids": workflow_ids,
        "status": _as_list(status),
        "name": _as_list(name),
        "parent_workflow_id": _as_list(parent_workflow_id),
        "workflow_id_prefix": _as_list(workflow_id_prefix),
        "start_time_ms": _millis(start_time),
        "end_time_ms": _millis(end_time),
        "completed_after_ms": _millis(completed_after),
        "completed_before_ms": _millis(completed_before),
        "has_parent": has_parent,
        "limit": limit,
        "offset": offset,
        "sort_desc": sort_desc,
    }


class WorkflowManagement:
    @classmethod
    def cancel_workflow(cls, workflow_id: str) -> None:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Cancelling workflow: {workflow_id}")
        instance._blocking(instance._engine.cancel_workflow, workflow_id)
        instance._wake_queues()

    @classmethod
    async def cancel_workflow_async(cls, workflow_id: str) -> None:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Cancelling workflow: {workflow_id}")
        await instance._engine.cancel_workflow(workflow_id)
        instance._wake_queues()

    @classmethod
    def resume_workflow(cls, workflow_id: str) -> WorkflowHandle[Any]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Resuming workflow: {workflow_id}")
        claim = instance._blocking(instance._engine.resume_workflow, workflow_id, list(dbos._registry))
        if claim is not None:
            return instance._resume(claim)
        instance._wake_queues()
        return dbos.WorkflowHandle(workflow_id)

    @classmethod
    async def resume_workflow_async(cls, workflow_id: str) -> WorkflowHandleAsync[Any]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Resuming workflow: {workflow_id}")
        claim = await instance._engine.resume_workflow(workflow_id, list(dbos._registry))
        if claim is not None:
            handle = instance._resume(claim)
            return dbos.WorkflowHandleAsync(handle.workflow_id, handle._local)
        instance._wake_queues()
        return dbos.WorkflowHandleAsync(workflow_id)

    @classmethod
    def list_workflows(
        cls,
        *,
        workflow_ids: list[str] | None = None,
        status: str | list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        completed_after: str | None = None,
        completed_before: str | None = None,
        name: str | list[str] | None = None,
        parent_workflow_id: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort_desc: bool = False,
        workflow_id_prefix: str | list[str] | None = None,
        load_input: bool = True,
        load_output: bool = True,
        has_parent: bool | None = None,
    ) -> list[WorkflowStatus]:
        dbos = _dbos()
        instance = dbos._running()
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        rows = instance._blocking(instance._engine.list_workflows, **filters)
        return [dbos._status_of(row, load_input=load_input, load_output=load_output) for row in rows]

    @classmethod
    async def list_workflows_async(
        cls,
        *,
        workflow_ids: list[str] | None = None,
        status: str | list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        completed_after: str | None = None,
        completed_before: str | None = None,
        name: str | list[str] | None = None,
        parent_workflow_id: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort_desc: bool = False,
        workflow_id_prefix: str | list[str] | None = None,
        load_input: bool = True,
        load_output: bool = True,
        has_parent: bool | None = None,
    ) -> list[WorkflowStatus]:
        dbos = _dbos()
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        rows = await dbos._running()._engine.list_workflows(**filters)
        return [dbos._status_of(row, load_input=load_input, load_output=load_output) for row in rows]
