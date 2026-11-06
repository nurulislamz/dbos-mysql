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
    "get_workflow_start_options",
    "make_pydantic_args_validator",
    "pydantic_args_validator",
]
