from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rust_dwos.errors import DBOSException
from rust_dwos.workflow_management import _dbos, _handles

if TYPE_CHECKING:
    from rust_dwos.dbos import WorkflowHandle, WorkflowHandleAsync


def _fork_args(workflow_id: str, start_step: int, application_version: str | None, queue_name: str | None, queue_partition_key: str | None, replacement_children: dict[str, str] | None, timeout_seconds: float | None, shard_key: str | None) -> tuple[Any, ...]:
    if shard_key is not None:
        raise DBOSException("shard_key is not supported")
    dbos = _dbos()
    timeout_ms = None if timeout_seconds is None else int(timeout_seconds * 1000)
    return (workflow_id, start_step, list(dbos._registry), dbos._next_workflow_id.get(), application_version, queue_name, queue_partition_key, replacement_children, timeout_ms)


def _rewind_args(workflow_id: str, start_step: int | None, application_version: str | None, queue_name: str | None, queue_partition_key: str | None, shard_key: str | None) -> tuple[Any, ...]:
    if shard_key is not None:
        raise DBOSException("shard_key is not supported")
    return (workflow_id, list(_dbos()._registry), start_step, application_version, queue_name, queue_partition_key)


class Forks:
    @classmethod
    def fork_workflow(cls, workflow_id: str, start_step: int, *, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, replacement_children: dict[str, str] | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> WorkflowHandle[Any]:
        """Restart a workflow with a new workflow ID from a specific step"""
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Forking workflow: {workflow_id} from step {start_step}")
        args = _fork_args(workflow_id, start_step, application_version, queue_name, queue_partition_key, replacement_children, timeout_seconds, shard_key)
        fork_id, claim = instance._blocking(instance._engine.fork_workflow, *args)
        return _handles(instance, dbos.WorkflowHandle, [fork_id], [claim] if claim else [])[0]

    @classmethod
    async def fork_workflow_async(cls, workflow_id: str, start_step: int, *, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, replacement_children: dict[str, str] | None = None, timeout_seconds: float | None = None, shard_key: str | None = None) -> WorkflowHandleAsync[Any]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Forking workflow: {workflow_id} from step {start_step}")
        args = _fork_args(workflow_id, start_step, application_version, queue_name, queue_partition_key, replacement_children, timeout_seconds, shard_key)
        fork_id, claim = await instance._engine.fork_workflow(*args)
        return _handles(instance, dbos.WorkflowHandleAsync, [fork_id], [claim] if claim else [])[0]

    @classmethod
    def rewind_workflow(cls, workflow_id: str, *, start_step: int | None = None, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, shard_key: str | None = None) -> WorkflowHandle[Any]:
        """Rewind a workflow to a step (default: the first). Only a workflow in a terminal state can be rewound."""
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Rewinding workflow: {workflow_id} to step {start_step or 1}")
        claim = instance._blocking(instance._engine.rewind_workflow, *_rewind_args(workflow_id, start_step, application_version, queue_name, queue_partition_key, shard_key))
        return _handles(instance, dbos.WorkflowHandle, [workflow_id], [claim] if claim else [])[0]

    @classmethod
    async def rewind_workflow_async(cls, workflow_id: str, *, start_step: int | None = None, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, shard_key: str | None = None) -> WorkflowHandleAsync[Any]:
        dbos = _dbos()
        instance = dbos._running()
        dbos.logger.info(f"Rewinding workflow: {workflow_id} to step {start_step or 1}")
        claim = await instance._engine.rewind_workflow(*_rewind_args(workflow_id, start_step, application_version, queue_name, queue_partition_key, shard_key))
        return _handles(instance, dbos.WorkflowHandleAsync, [workflow_id], [claim] if claim else [])[0]
