"""DBOSClient: enqueue, message and manage workflows from outside a DBOS process. API shape from dbos-transact-py's _client.py."""

from __future__ import annotations

from typing import Any, Literal

from rust_dwos import _native
from rust_dwos.attributes import _encode
from rust_dwos.dbos import _LoopThread, _call, _check_poll, _decode_result, _result_args, _status_of, _step_of
from rust_dwos.errors import DBOSException
from rust_dwos.events import SendMessage, WorkflowSerializationFormat, _bulk_args, _check_outcome, _decode, _timeout_ms
from rust_dwos.queues import _INTERNAL_QUEUE_CONSTRUCTION, DEFAULT_QUEUE_POLLING_INTERVAL_SEC, EnqueueOptions, Queue, _options_args, _stored_limit
from rust_dwos.serialization import Serializer, allow_pickle, set_custom_serializer
from rust_dwos.wait_first import _handles as _by_id
from rust_dwos.workflow_management import _as_list, _filters


class WorkflowHandleClientPolling[R]:
    def __init__(self, workflow_id: str, client: DBOSClient) -> None:
        self.workflow_id = workflow_id
        self._client = client

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self, *, polling_interval_sec: float | None = None) -> R:
        _check_poll(polling_interval_sec)
        return _decode_result(self._client._state.run(_call(self._client._engine.await_result, *_result_args(self.workflow_id, polling_interval_sec))))

    def get_status(self) -> Any:
        return _status_of(self._client._engine.get_status(self.workflow_id, blocking=True))


class WorkflowHandleClientAsyncPolling[R]:
    def __init__(self, workflow_id: str, client: DBOSClient) -> None:
        self.workflow_id = workflow_id
        self._client = client

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self, *, polling_interval_sec: float | None = None) -> R:
        _check_poll(polling_interval_sec)
        return _decode_result(await self._client._engine.await_result(*_result_args(self.workflow_id, polling_interval_sec)))

    async def get_status(self) -> Any:
        return _status_of(await self._client._engine.get_status(self.workflow_id))


def _no_shard(shard_key: str | None) -> None:
    if shard_key is not None:
        raise DBOSException("shard_key is not supported")


class DBOSClient:
    """Talks only to the system database. It never migrates the schema and runs no recovery, queue or schedule loop."""

    def __init__(self, *, system_database_url: str | None = None, serializer: Literal["json", "pickle"] | Serializer = "pickle") -> None:
        from rust_dwos import dbos

        if system_database_url is None:
            raise DBOSException("A DBOSClient requires a system_database_url.")
        custom = serializer if isinstance(serializer, Serializer) else None
        options: dict[str, Any] = {"custom_serializer": custom.name()} if custom is not None else {"serializer": serializer}
        self._state = _LoopThread("dbos-client")
        try:
            self._engine = self._state.run(_call(_native.Engine.connect, system_database_url, run_migrations=False, **options))
        except BaseException:
            self._state.stop()
            raise
        # Value decoding is set per process. A launched DBOS already set it, so the client must agree with it.
        app = dbos._instance._engine if dbos._instance is not None else None
        if app is not None and app.serialization != self._engine.serialization:
            self.destroy()
            raise DBOSException("A DBOSClient in a DBOS process must use the same serializer as DBOS")
        if app is None:
            allow_pickle(self._engine.serialization == _native.PY_PICKLE)
            set_custom_serializer(custom, self._engine.serialization if custom is not None else None)

    def check_connection(self) -> None:
        self._engine.list_queues(None, blocking=True)

    async def check_connection_async(self) -> None:
        await self._engine.list_queues(None)

    def destroy(self) -> None:
        self._engine.close()
        self._state.stop()

    # Workflows

    def _enqueue_args(self, options: EnqueueOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str, tuple[Any, ...], dict[str, Any]]:
        begin, queue, enqueue = _options_args(dict(options), args, kwargs, self._engine.serialization, None, False, None, None)
        return queue, begin, enqueue

    def enqueue(self, options: EnqueueOptions, *args: Any, **kwargs: Any) -> WorkflowHandleClientPolling[Any]:
        queue, begin, enqueue = self._enqueue_args(options, args, kwargs)
        return WorkflowHandleClientPolling(self._engine.enqueue(queue, *begin, **enqueue, blocking=True), self)

    async def enqueue_async(self, options: EnqueueOptions, *args: Any, **kwargs: Any) -> WorkflowHandleClientAsyncPolling[Any]:
        queue, begin, enqueue = self._enqueue_args(options, args, kwargs)
        return WorkflowHandleClientAsyncPolling(await self._engine.enqueue(queue, *begin, **enqueue), self)

    def retrieve_workflow(self, workflow_id: str, *, shard_key: str | None = None) -> WorkflowHandleClientPolling[Any]:
        _no_shard(shard_key)
        self._engine.get_status(workflow_id, blocking=True)  # Raises DBOSNonExistentWorkflowError.
        return WorkflowHandleClientPolling(workflow_id, self)

    async def retrieve_workflow_async(self, workflow_id: str, *, shard_key: str | None = None) -> WorkflowHandleClientAsyncPolling[Any]:
        _no_shard(shard_key)
        await self._engine.get_status(workflow_id)
        return WorkflowHandleClientAsyncPolling(workflow_id, self)

    def wait_first(self, handles: list[Any], *, polling_interval_sec: float = 1.0) -> Any:
        by_id = _by_id(handles)
        return by_id[self._state.run(_call(self._engine.await_first, list(by_id), polling_interval_sec))]

    async def wait_first_async(self, handles: list[Any], *, polling_interval_sec: float = 1.0) -> Any:
        by_id = _by_id(handles)
        return by_id[await self._engine.await_first(list(by_id), polling_interval_sec)]

    def set_workflow_delay(self, workflow_id: str, *, delay_seconds: float | None = None, delay_until_epoch_ms: int | None = None) -> None:
        self._engine.set_workflow_delay(workflow_id, delay_seconds, delay_until_epoch_ms, blocking=True)

    async def set_workflow_delay_async(self, workflow_id: str, *, delay_seconds: float | None = None, delay_until_epoch_ms: int | None = None) -> None:
        await self._engine.set_workflow_delay(workflow_id, delay_seconds, delay_until_epoch_ms)

    # Messages and events

    def _serialization(self, kind: WorkflowSerializationFormat | None) -> str:
        if kind == WorkflowSerializationFormat.PORTABLE:
            return _native.PORTABLE_JSON
        if kind not in (None, WorkflowSerializationFormat.DEFAULT, WorkflowSerializationFormat.NATIVE):
            raise DBOSException(f"Invalid serialization_type: {kind}")
        return self._engine.serialization

    def send(self, destination_id: str, message: Any, topic: str | None = None, idempotency_key: str | None = None, *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False, shard_key: str | None = None) -> None:
        self.send_bulk([SendMessage(destination_id, message, topic, idempotency_key, shard_key)], serialization_type=serialization_type, send_to_forks=send_to_forks)

    async def send_async(self, destination_id: str, message: Any, topic: str | None = None, idempotency_key: str | None = None, *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False, shard_key: str | None = None) -> None:
        await self.send_bulk_async([SendMessage(destination_id, message, topic, idempotency_key, shard_key)], serialization_type=serialization_type, send_to_forks=send_to_forks)

    def send_bulk(self, messages: list[SendMessage], *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False) -> None:
        if send_to_forks:
            raise DBOSException("send_to_forks is not supported")
        serialization = self._serialization(serialization_type)
        _check_outcome(self._engine.send_bulk(_bulk_args(messages, serialization), serialization, blocking=True), serialization)

    async def send_bulk_async(self, messages: list[SendMessage], *, serialization_type: WorkflowSerializationFormat | None = WorkflowSerializationFormat.DEFAULT, send_to_forks: bool = False) -> None:
        if send_to_forks:
            raise DBOSException("send_to_forks is not supported")
        serialization = self._serialization(serialization_type)
        _check_outcome(await self._engine.send_bulk(_bulk_args(messages, serialization), serialization), serialization)

    def get_event(self, workflow_id: str, key: str, timeout_seconds: float = 60, *, shard_key: str | None = None) -> Any:
        _no_shard(shard_key)
        return _decode(self._state.run(_call(self._engine.wait_event, workflow_id, key, _timeout_ms(timeout_seconds))))

    async def get_event_async(self, workflow_id: str, key: str, timeout_seconds: float = 60, *, shard_key: str | None = None) -> Any:
        _no_shard(shard_key)
        return _decode(await self._engine.wait_event(workflow_id, key, _timeout_ms(timeout_seconds)))

    # Queues

    def _register_args(self, name: str, global_concurrency: int | None, worker_concurrency: int | None, limiter: dict[str, Any] | None, partition_concurrency: int | None, partition_worker_concurrency: int | None, partition_limiter: dict[str, Any] | None, polling_interval_sec: float, on_conflict: str, concurrency: int | None, unsupported: dict[str, Any]) -> tuple[Any, ...]:
        Queue._validate_queue(name=name, concurrency=concurrency, global_concurrency=global_concurrency, polling_interval_sec=polling_interval_sec, unsupported=unsupported)
        if on_conflict not in ("always_update", "never_update"):
            raise DBOSException("DBOSClient.register_queue does not support on_conflict='update_if_latest_version' because clients are not associated with an application version. Use 'always_update' or 'never_update'.")
        limit = concurrency if global_concurrency is None else global_concurrency
        return (name, limit, worker_concurrency, on_conflict == "always_update", limiter["limit"] if limiter else None, limiter["period"] if limiter else None, partition_concurrency, partition_worker_concurrency, partition_limiter["limit"] if partition_limiter else None, partition_limiter["period"] if partition_limiter else None, polling_interval_sec)

    def register_queue(self, name: str, *, global_concurrency: int | None = None, partition_concurrency: int | None = None, partition_worker_concurrency: int | None = None, partition_limiter: dict[str, Any] | None = None, limiter: dict[str, Any] | None = None, worker_concurrency: int | None = None, polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC, on_conflict: str = "always_update", concurrency: int | None = None, **unsupported: Any) -> Queue:
        """Stores the queue. No worker starts here: an application that registers the queue runs it."""
        args = self._register_args(name, global_concurrency, worker_concurrency, limiter, partition_concurrency, partition_worker_concurrency, partition_limiter, polling_interval_sec, on_conflict, concurrency, unsupported)
        self._engine.register_queue(*args, blocking=True)
        return Queue(name, _stored_limit(self._engine.get_queue(name, blocking=True)), polling_interval_sec=polling_interval_sec, token=_INTERNAL_QUEUE_CONSTRUCTION)

    async def register_queue_async(self, name: str, *, global_concurrency: int | None = None, partition_concurrency: int | None = None, partition_worker_concurrency: int | None = None, partition_limiter: dict[str, Any] | None = None, limiter: dict[str, Any] | None = None, worker_concurrency: int | None = None, polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC, on_conflict: str = "always_update", concurrency: int | None = None, **unsupported: Any) -> Queue:
        args = self._register_args(name, global_concurrency, worker_concurrency, limiter, partition_concurrency, partition_worker_concurrency, partition_limiter, polling_interval_sec, on_conflict, concurrency, unsupported)
        await self._engine.register_queue(*args)
        return Queue(name, _stored_limit(await self._engine.get_queue(name)), polling_interval_sec=polling_interval_sec, token=_INTERNAL_QUEUE_CONSTRUCTION)

    def retrieve_queue(self, name: str) -> Queue | None:
        limit = self._engine.get_queue(name, blocking=True)
        return None if limit is None else Queue(name, _stored_limit(limit), token=_INTERNAL_QUEUE_CONSTRUCTION)

    async def retrieve_queue_async(self, name: str) -> Queue | None:
        limit = await self._engine.get_queue(name)
        return None if limit is None else Queue(name, _stored_limit(limit), token=_INTERNAL_QUEUE_CONSTRUCTION)

    def delete_queue(self, name: str) -> None:
        self._engine.delete_queue(name, blocking=True)

    async def delete_queue_async(self, name: str) -> None:
        await self._engine.delete_queue(name)

    def list_queues(self, *, application_name: str | list[str] | None = None) -> list[Queue]:
        return [Queue(name, _stored_limit(limit), token=_INTERNAL_QUEUE_CONSTRUCTION) for name, limit in self._engine.list_queues(_as_list(application_name), blocking=True)]

    async def list_queues_async(self, *, application_name: str | list[str] | None = None) -> list[Queue]:
        return [Queue(name, _stored_limit(limit), token=_INTERNAL_QUEUE_CONSTRUCTION) for name, limit in await self._engine.list_queues(_as_list(application_name))]

    # Workflow management

    def cancel_workflow(self, workflow_id: str, *, cancel_children: bool = False, shard_key: str | None = None) -> None:
        self.cancel_workflows([workflow_id], cancel_children=cancel_children, shard_key=shard_key)

    async def cancel_workflow_async(self, workflow_id: str, *, cancel_children: bool = False, shard_key: str | None = None) -> None:
        await self.cancel_workflows_async([workflow_id], cancel_children=cancel_children, shard_key=shard_key)

    def cancel_workflows(self, workflow_ids: list[str], *, cancel_children: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        self._engine.cancel_workflows(workflow_ids, cancel_children, blocking=True)

    async def cancel_workflows_async(self, workflow_ids: list[str], *, cancel_children: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        await self._engine.cancel_workflows(workflow_ids, cancel_children)

    # No workflow names are passed, so nothing runs here. A resumed row waits for recovery or its queue in an application.
    def resume_workflow(self, workflow_id: str, *, queue_name: str | None = None, shard_key: str | None = None) -> WorkflowHandleClientPolling[Any]:
        return self.resume_workflows([workflow_id], queue_name=queue_name, shard_key=shard_key)[0]

    async def resume_workflow_async(self, workflow_id: str, *, queue_name: str | None = None, shard_key: str | None = None) -> WorkflowHandleClientAsyncPolling[Any]:
        return (await self.resume_workflows_async([workflow_id], queue_name=queue_name, shard_key=shard_key))[0]

    def resume_workflows(self, workflow_ids: list[str], *, queue_name: str | None = None, shard_key: str | None = None) -> list[WorkflowHandleClientPolling[Any]]:
        _no_shard(shard_key)
        self._engine.resume_workflows(workflow_ids, [], queue_name, blocking=True)
        return [WorkflowHandleClientPolling(workflow_id, self) for workflow_id in workflow_ids]

    async def resume_workflows_async(self, workflow_ids: list[str], *, queue_name: str | None = None, shard_key: str | None = None) -> list[WorkflowHandleClientAsyncPolling[Any]]:
        _no_shard(shard_key)
        await self._engine.resume_workflows(workflow_ids, [], queue_name)
        return [WorkflowHandleClientAsyncPolling(workflow_id, self) for workflow_id in workflow_ids]

    def update_workflow_attributes(self, workflow_id: str, attributes: dict[str, Any] | None, *, merge: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        if merge and attributes is None:
            raise DBOSException("Cannot merge None into workflow attributes")
        _check_outcome(self._engine.update_workflow_attributes(workflow_id, _encode(attributes), merge, blocking=True), self._engine.serialization)

    async def update_workflow_attributes_async(self, workflow_id: str, attributes: dict[str, Any] | None, *, merge: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        if merge and attributes is None:
            raise DBOSException("Cannot merge None into workflow attributes")
        _check_outcome(await self._engine.update_workflow_attributes(workflow_id, _encode(attributes), merge), self._engine.serialization)

    def delete_workflow(self, workflow_id: str, *, delete_children: bool = False, shard_key: str | None = None) -> None:
        self.delete_workflows([workflow_id], delete_children=delete_children, shard_key=shard_key)

    async def delete_workflow_async(self, workflow_id: str, *, delete_children: bool = False, shard_key: str | None = None) -> None:
        await self.delete_workflows_async([workflow_id], delete_children=delete_children, shard_key=shard_key)

    def delete_workflows(self, workflow_ids: list[str], *, delete_children: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        self._engine.delete_workflows(workflow_ids, delete_children, blocking=True)

    async def delete_workflows_async(self, workflow_ids: list[str], *, delete_children: bool = False, shard_key: str | None = None) -> None:
        _no_shard(shard_key)
        await self._engine.delete_workflows(workflow_ids, delete_children)

    def list_workflows(self, *, workflow_ids: list[str] | None = None, status: str | list[str] | None = None, start_time: str | None = None, end_time: str | None = None, name: str | list[str] | None = None, app_version: str | list[str] | None = None, forked_from: str | list[str] | None = None, parent_workflow_id: str | list[str] | None = None, user: str | list[str] | None = None, queue_name: str | list[str] | None = None, limit: int | None = None, offset: int | None = None, sort_desc: bool = False, workflow_id_prefix: str | list[str] | None = None, load_input: bool = True, load_output: bool = True, has_parent: bool | None = None, completed_after: str | None = None, completed_before: str | None = None, queues_only: bool = False) -> list[Any]:
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        filters.update(queue_name=_as_list(queue_name), app_version=_as_list(app_version), forked_from=_as_list(forked_from), user=_as_list(user), queues_only=queues_only)
        return [_status_of(row, load_input=load_input, load_output=load_output) for row in self._engine.list_workflows(**filters, blocking=True)]

    async def list_workflows_async(self, *, workflow_ids: list[str] | None = None, status: str | list[str] | None = None, start_time: str | None = None, end_time: str | None = None, name: str | list[str] | None = None, app_version: str | list[str] | None = None, forked_from: str | list[str] | None = None, parent_workflow_id: str | list[str] | None = None, user: str | list[str] | None = None, queue_name: str | list[str] | None = None, limit: int | None = None, offset: int | None = None, sort_desc: bool = False, workflow_id_prefix: str | list[str] | None = None, load_input: bool = True, load_output: bool = True, has_parent: bool | None = None, completed_after: str | None = None, completed_before: str | None = None, queues_only: bool = False) -> list[Any]:
        filters = _filters(workflow_ids, status, start_time, end_time, completed_after, completed_before, name, parent_workflow_id, limit, offset, sort_desc, workflow_id_prefix, has_parent)
        filters.update(queue_name=_as_list(queue_name), app_version=_as_list(app_version), forked_from=_as_list(forked_from), user=_as_list(user), queues_only=queues_only)
        return [_status_of(row, load_input=load_input, load_output=load_output) for row in await self._engine.list_workflows(**filters)]

    def list_queued_workflows(self, **filters: Any) -> list[Any]:
        return self.list_workflows(**filters, queues_only=True)

    async def list_queued_workflows_async(self, **filters: Any) -> list[Any]:
        return await self.list_workflows_async(**filters, queues_only=True)

    def list_workflow_steps(self, workflow_id: str, *, load_output: bool = True, limit: int | None = None, offset: int | None = None, shard_key: str | None = None) -> list[dict[str, Any]]:
        _no_shard(shard_key)
        return [_step_of(step, load_output=load_output) for step in self._engine.list_steps(workflow_id, limit=limit, offset=offset, blocking=True)]

    async def list_workflow_steps_async(self, workflow_id: str, *, load_output: bool = True, limit: int | None = None, offset: int | None = None, shard_key: str | None = None) -> list[dict[str, Any]]:
        _no_shard(shard_key)
        return [_step_of(step, load_output=load_output) for step in await self._engine.list_steps(workflow_id, limit=limit, offset=offset)]
