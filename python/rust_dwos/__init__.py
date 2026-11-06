from rust_dwos.validation import make_pydantic_args_validator, pydantic_args_validator
from rust_dwos._native import StoreError
from rust_dwos.context import DBOSContextEnsure
from rust_dwos.dbos import (
    DBOS,
    DBOSConfig,
    SetWorkflowID,
    StepStatus,
    WorkflowHandle,
    WorkflowHandleAsync,
    WorkflowStatus,
    WorkflowStatusString,
)
from rust_dwos.attributes import SetWorkflowAttributes
from rust_dwos.client import DBOSClient
from rust_dwos.debouncer import Debouncer, DebouncerClient
from rust_dwos.events import SendMessage, WorkflowSerializationFormat
from rust_dwos.tracing import PropagateOtelContext
from rust_dwos.serialization import Serializer
from rust_dwos.queues import EnqueueOptions, Queue, QueueRateLimit, SetEnqueueOptions
from rust_dwos.step_reservation import StepReservation
from rust_dwos.timeouts import SetWorkflowTimeout, assert_current_dbos_context, get_workflow_start_options
from rust_dwos.errors import (
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
            from rust_dwos import datasources
        except ModuleNotFoundError as error:
            raise ImportError(f"{name} needs SQLAlchemy. Install rust-dwos[sqlalchemy]") from error
        return getattr(datasources, name)
    raise AttributeError(f"module 'rust_dwos' has no attribute {name!r}")


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
