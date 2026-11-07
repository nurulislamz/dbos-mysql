from yado.validation import make_pydantic_args_validator, pydantic_args_validator
from yado._native import StoreError
from yado.context import DBOSContextEnsure
from yado.dbos import (
    DBOS,
    DBOSConfig,
    SetWorkflowID,
    StepStatus,
    WorkflowHandle,
    WorkflowHandleAsync,
    WorkflowStatus,
    WorkflowStatusString,
)
from yado.attributes import SetWorkflowAttributes
from yado.client import DBOSClient
from yado.debouncer import Debouncer, DebouncerClient
from yado.events import SendMessage, WorkflowSerializationFormat
from yado.tracing import PropagateOtelContext
from yado.serialization import Serializer
from yado.queues import EnqueueOptions, Queue, QueueRateLimit, SetEnqueueOptions
from yado.step_reservation import StepReservation
from yado.timeouts import SetWorkflowTimeout, assert_current_dbos_context, get_workflow_start_options
from yado.errors import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSLeaseLostError,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSRecordedError,
    DBOSStepNondeterminismError,
    DBOSStepTimeoutError,
    DBOSStreamTimeoutError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowIDInUseError,
    MaxRecoveryAttemptsExceededError,
)

list_application_versions = DBOS.list_application_versions
list_application_versions_async = DBOS.list_application_versions_async
get_latest_application_version = DBOS.get_latest_application_version
get_latest_application_version_async = DBOS.get_latest_application_version_async
set_latest_application_version = DBOS.set_latest_application_version
set_latest_application_version_async = DBOS.set_latest_application_version_async
asyncio_wait = DBOS.asyncio_wait
cancel_workflow = DBOS.cancel_workflow
cancel_workflow_async = DBOS.cancel_workflow_async
cancel_workflows = DBOS.cancel_workflows
cancel_workflows_async = DBOS.cancel_workflows_async
close_stream = DBOS.close_stream
close_stream_async = DBOS.close_stream_async
complete_awakeable = DBOS.complete_awakeable
complete_awakeable_async = DBOS.complete_awakeable_async
create_awakeable = DBOS.create_awakeable
create_awakeable_async = DBOS.create_awakeable_async
delete_finished_workflows = DBOS.delete_finished_workflows
delete_finished_workflows_async = DBOS.delete_finished_workflows_async
delete_queue = DBOS.delete_queue
delete_queue_async = DBOS.delete_queue_async
delete_schedule = DBOS.delete_schedule
delete_workflow = DBOS.delete_workflow
delete_workflow_async = DBOS.delete_workflow_async
delete_workflows = DBOS.delete_workflows
delete_workflows_async = DBOS.delete_workflows_async
deprecate_patch = DBOS.deprecate_patch
deprecate_patch_async = DBOS.deprecate_patch_async
destroy = DBOS.destroy
enqueue_workflow = DBOS.enqueue_workflow
enqueue_workflow_async = DBOS.enqueue_workflow_async
enqueue_workflow_with_options = DBOS.enqueue_workflow_with_options
enqueue_workflow_with_options_async = DBOS.enqueue_workflow_with_options_async
fork_workflow = DBOS.fork_workflow
fork_workflow_async = DBOS.fork_workflow_async
get_all_events = DBOS.get_all_events
get_all_events_async = DBOS.get_all_events_async
get_event = DBOS.get_event
get_event_async = DBOS.get_event_async
get_result = DBOS.get_result
get_result_async = DBOS.get_result_async
get_workflow_status = DBOS.get_workflow_status
get_workflow_status_async = DBOS.get_workflow_status_async
heartbeat = DBOS.heartbeat
launch = DBOS.launch
list_queued_workflows = DBOS.list_queued_workflows
list_queued_workflows_async = DBOS.list_queued_workflows_async
list_queues = DBOS.list_queues
list_queues_async = DBOS.list_queues_async
list_workflow_steps = DBOS.list_workflow_steps
list_workflow_steps_async = DBOS.list_workflow_steps_async
list_workflows = DBOS.list_workflows
list_workflows_async = DBOS.list_workflows_async
listen_queues = DBOS.listen_queues
patch = DBOS.patch
patch_async = DBOS.patch_async
read_stream = DBOS.read_stream
read_stream_async = DBOS.read_stream_async
read_stream_offset = DBOS.read_stream_offset
read_stream_offset_async = DBOS.read_stream_offset_async
recv = DBOS.recv
recv_async = DBOS.recv_async
register_queue = DBOS.register_queue
register_queue_async = DBOS.register_queue_async
register_schedule = DBOS.register_schedule
reserve_step = DBOS.reserve_step
resume_workflow = DBOS.resume_workflow
resume_workflow_async = DBOS.resume_workflow_async
resume_workflows = DBOS.resume_workflows
resume_workflows_async = DBOS.resume_workflows_async
retrieve_queue = DBOS.retrieve_queue
retrieve_queue_async = DBOS.retrieve_queue_async
retrieve_workflow = DBOS.retrieve_workflow
retrieve_workflow_async = DBOS.retrieve_workflow_async
rewind_workflow = DBOS.rewind_workflow
rewind_workflow_async = DBOS.rewind_workflow_async
run_step = DBOS.run_step
run_step_async = DBOS.run_step_async
scheduled = DBOS.scheduled
send = DBOS.send
send_async = DBOS.send_async
send_bulk = DBOS.send_bulk
send_bulk_async = DBOS.send_bulk_async
set_event = DBOS.set_event
set_event_async = DBOS.set_event_async
set_workflow_delay = DBOS.set_workflow_delay
set_workflow_delay_async = DBOS.set_workflow_delay_async
sleep = DBOS.sleep
sleep_async = DBOS.sleep_async
start_workflow = DBOS.start_workflow
start_workflow_async = DBOS.start_workflow_async
step = DBOS.step
update_workflow_attributes = DBOS.update_workflow_attributes
update_workflow_attributes_async = DBOS.update_workflow_attributes_async
wait_awakeable = DBOS.wait_awakeable
wait_awakeable_async = DBOS.wait_awakeable_async
wait_first = DBOS.wait_first
wait_first_async = DBOS.wait_first_async
workflow = DBOS.workflow
write_stream = DBOS.write_stream
write_stream_async = DBOS.write_stream_async

_DATASOURCE_NAMES = ("AsyncSQLAlchemyDatasource", "DatasourceOptions", "SQLAlchemyDatasource")


def __getattr__(name: str) -> object:
    # Datasources need SQLAlchemy, an optional dependency, so they load only when asked for, and stay out of __all__.
    if name in _DATASOURCE_NAMES:
        try:
            from yado import datasources
        except ModuleNotFoundError as error:
            raise ImportError(f"{name} needs SQLAlchemy. Install yado[sqlalchemy]") from error
        return getattr(datasources, name)
    raise AttributeError(f"module 'yado' has no attribute {name!r}")


__all__ = [
    "list_application_versions",
    "list_application_versions_async",
    "get_latest_application_version",
    "get_latest_application_version_async",
    "set_latest_application_version",
    "set_latest_application_version_async",
    "DBOS",
    "DBOSAwaitedWorkflowCancelledError",
    "DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded",
    "DBOSClient",
    "DBOSConfig",
    "DBOSContextEnsure",
    "DBOSConflictingWorkflowError",
    "DBOSException",
    "DBOSLeaseLostError",
    "DBOSMaxStepRetriesExceeded",
    "DBOSNonExistentWorkflowError",
    "DBOSQueueDeduplicatedError",
    "DBOSRecordedError",
    "DBOSStepNondeterminismError",
    "DBOSStepTimeoutError",
    "DBOSStreamTimeoutError",
    "DBOSUnexpectedStepError",
    "DBOSWorkflowCancelledError",
    "DBOSWorkflowIDInUseError",
    "Debouncer",
    "DebouncerClient",
    "MaxRecoveryAttemptsExceededError",
    "Queue",
    "QueueRateLimit",
    "EnqueueOptions",
    "PropagateOtelContext",
    "SendMessage",
    "Serializer",
    "WorkflowSerializationFormat",
    "SetWorkflowAttributes",
    "SetWorkflowID",
    "SetEnqueueOptions",
    "SetWorkflowTimeout",
    "StepStatus",
    "StepReservation",
    "StoreError",
    "WorkflowHandle",
    "WorkflowHandleAsync",
    "WorkflowStatus",
    "WorkflowStatusString",
    "assert_current_dbos_context",
    "asyncio_wait",
    "cancel_workflow",
    "cancel_workflow_async",
    "cancel_workflows",
    "cancel_workflows_async",
    "close_stream",
    "close_stream_async",
    "complete_awakeable",
    "complete_awakeable_async",
    "create_awakeable",
    "create_awakeable_async",
    "delete_finished_workflows",
    "delete_finished_workflows_async",
    "delete_queue",
    "delete_queue_async",
    "delete_schedule",
    "delete_workflow",
    "delete_workflow_async",
    "delete_workflows",
    "delete_workflows_async",
    "deprecate_patch",
    "deprecate_patch_async",
    "destroy",
    "enqueue_workflow",
    "enqueue_workflow_async",
    "enqueue_workflow_with_options",
    "enqueue_workflow_with_options_async",
    "fork_workflow",
    "fork_workflow_async",
    "get_all_events",
    "get_all_events_async",
    "get_event",
    "get_event_async",
    "get_result",
    "get_result_async",
    "get_workflow_start_options",
    "get_workflow_status",
    "get_workflow_status_async",
    "heartbeat",
    "launch",
    "list_queued_workflows",
    "list_queued_workflows_async",
    "list_queues",
    "list_queues_async",
    "list_workflow_steps",
    "list_workflow_steps_async",
    "list_workflows",
    "list_workflows_async",
    "listen_queues",
    "make_pydantic_args_validator",
    "patch",
    "patch_async",
    "pydantic_args_validator",
    "read_stream",
    "read_stream_async",
    "read_stream_offset",
    "read_stream_offset_async",
    "recv",
    "recv_async",
    "register_queue",
    "register_queue_async",
    "register_schedule",
    "reserve_step",
    "resume_workflow",
    "resume_workflow_async",
    "resume_workflows",
    "resume_workflows_async",
    "retrieve_queue",
    "retrieve_queue_async",
    "retrieve_workflow",
    "retrieve_workflow_async",
    "rewind_workflow",
    "rewind_workflow_async",
    "run_step",
    "run_step_async",
    "scheduled",
    "send",
    "send_async",
    "send_bulk",
    "send_bulk_async",
    "set_event",
    "set_event_async",
    "set_workflow_delay",
    "set_workflow_delay_async",
    "sleep",
    "sleep_async",
    "start_workflow",
    "start_workflow_async",
    "step",
    "update_workflow_attributes",
    "update_workflow_attributes_async",
    "wait_awakeable",
    "wait_awakeable_async",
    "wait_first",
    "wait_first_async",
    "workflow",
    "write_stream",
    "write_stream_async",
]
