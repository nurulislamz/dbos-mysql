from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from yado.attributes import _encode

if TYPE_CHECKING:
    from yado.dbos import WorkflowHandle, WorkflowHandleAsync, WorkflowStatus


def _as_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    return value if isinstance(value, list) else [value]


def _millis(value: str | None) -> int | None:
    if value is None:
        return None
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _handles(instance: Any, handle: Any, workflow_ids: list[str], claims: list[Any]) -> list[Any]:
    by_id = {claim.workflow_id: claim for claim in claims}
    handles = []
    for workflow_id in workflow_ids:
        claim = by_id.get(workflow_id)
        if claim is None:
            handles.append(handle(workflow_id))
            continue
        started = instance._resume(claim)
        handles.append(handle(started.workflow_id, started._local))
    if len(by_id) < len(workflow_ids):
        instance._wake_queues()
    return handles


def _dbos() -> Any:
    from yado import dbos

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
    def cancel_workflow(cls, workflow_id: str, *, cancel_children: bool = False) -> None:
        cls.cancel_workflows([workflow_id], cancel_children=cancel_children)

    @classmethod
    async def cancel_workflow_async(cls, workflow_id: str, *, cancel_children: bool = False) -> None:
        await cls.cancel_workflows_async([workflow_id], cancel_children=cancel_children)

    @classmethod
    def cancel_workflows(cls, workflow_ids: list[str], *, cancel_children: bool = False) -> None:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Cancelling workflow(s): {workflow_ids}")
        instance._blocking(instance._engine.cancel_workflows, workflow_ids, cancel_children)
        instance._wake_queues()

    @classmethod
    async def cancel_workflows_async(cls, workflow_ids: list[str], *, cancel_children: bool = False) -> None:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Cancelling workflow(s): {workflow_ids}")
        await instance._engine.cancel_workflows(workflow_ids, cancel_children)
        instance._wake_queues()

    @classmethod
    def resume_workflow(cls, workflow_id: str, *, queue_name: str | None = None) -> WorkflowHandle[Any]:
        return cls.resume_workflows([workflow_id], queue_name=queue_name)[0]

    @classmethod
    async def resume_workflow_async(cls, workflow_id: str, *, queue_name: str | None = None) -> WorkflowHandleAsync[Any]:
        return (await cls.resume_workflows_async([workflow_id], queue_name=queue_name))[0]

    @classmethod
    def resume_workflows(cls, workflow_ids: list[str], *, queue_name: str | None = None) -> list[WorkflowHandle[Any]]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Resuming workflow(s): {workflow_ids}")
        claims = instance._blocking(instance._engine.resume_workflows, workflow_ids, list(dbos._registry), queue_name)
        return _handles(instance, dbos.WorkflowHandle, workflow_ids, claims)

    @classmethod
    async def resume_workflows_async(cls, workflow_ids: list[str], *, queue_name: str | None = None) -> list[WorkflowHandleAsync[Any]]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Resuming workflow(s): {workflow_ids}")
        claims = await instance._engine.resume_workflows(workflow_ids, list(dbos._registry), queue_name)
        return _handles(instance, dbos.WorkflowHandleAsync, workflow_ids, claims)

    @classmethod
    def list_workflows(
        cls,
        *,
        workflow_ids: list[str] | None = None,
        attributes: dict[str, Any] | None = None,
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
        queue_name: str | list[str] | None = None,
        app_version: str | list[str] | None = None,
        forked_from: str | list[str] | None = None,
        is_fork: bool | None = None,
        was_forked_from: bool | None = None,
        user: str | list[str] | None = None,
        queues_only: bool = False,
    ) -> list[WorkflowStatus]:
        dbos = _dbos()
        instance = dbos._running()
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        filters.update(attributes=_encode(attributes), queue_name=_as_list(queue_name), app_version=_as_list(app_version), forked_from=_as_list(forked_from), is_fork=is_fork, was_forked_from=was_forked_from, user=_as_list(user), queues_only=queues_only)
        rows = instance._blocking(instance._engine.list_workflows, **filters)
        return [dbos._status_of(row, load_input=load_input, load_output=load_output) for row in rows]

    @classmethod
    async def list_workflows_async(
        cls,
        *,
        workflow_ids: list[str] | None = None,
        attributes: dict[str, Any] | None = None,
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
        queue_name: str | list[str] | None = None,
        app_version: str | list[str] | None = None,
        forked_from: str | list[str] | None = None,
        is_fork: bool | None = None,
        was_forked_from: bool | None = None,
        user: str | list[str] | None = None,
        queues_only: bool = False,
    ) -> list[WorkflowStatus]:
        dbos = _dbos()
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        filters.update(attributes=_encode(attributes), queue_name=_as_list(queue_name), app_version=_as_list(app_version), forked_from=_as_list(forked_from), is_fork=is_fork, was_forked_from=was_forked_from, user=_as_list(user), queues_only=queues_only)
        rows = await dbos._running()._engine.list_workflows(**filters)
        return [dbos._status_of(row, load_input=load_input, load_output=load_output) for row in rows]

    @classmethod
    def list_queued_workflows(cls, **filters: Any) -> list[WorkflowStatus]:
        return cls.list_workflows(**filters, queues_only=True)

    @classmethod
    async def list_queued_workflows_async(cls, **filters: Any) -> list[WorkflowStatus]:
        return await cls.list_workflows_async(**filters, queues_only=True)
