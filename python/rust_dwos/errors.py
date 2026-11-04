from __future__ import annotations

import copyreg
from collections.abc import Sequence

from rust_dwos._native import (
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSLeaseLostError,
    DBOSNonExistentWorkflowError,
    DBOSStepNondeterminismError,
    DBOSUnexpectedStepError,
    StoreError,
)


class DBOSRecordedError(DBOSException):
    """A stored failure whose original exception class is not loaded in this process."""

    def __init__(self, type_name: str, message: str) -> None:
        self.type_name = type_name
        super().__init__(message)

    def __reduce__(self) -> tuple[type[DBOSRecordedError], tuple[str, str]]:
        return (self.__class__, (self.type_name, str(self)))


class DBOSMaxStepRetriesExceeded(DBOSException):
    def __init__(self, step_name: str, max_retries: int, errors: Sequence[BaseException] = ()) -> None:
        self.step_name = step_name
        self.max_retries = max_retries
        self.errors = list(errors)
        super().__init__(f"Step {step_name} has exceeded its maximum of {max_retries} retries")

    def __reduce__(self) -> tuple[type[DBOSMaxStepRetriesExceeded], tuple[str, int, list[BaseException]]]:
        return (self.__class__, (self.step_name, self.max_retries, self.errors))


class MaxRecoveryAttemptsExceededError(DBOSException):
    def __init__(self, workflow_id: str, max_retries: int) -> None:
        self.workflow_id = workflow_id
        self.max_retries = max_retries
        super().__init__(f"Workflow {workflow_id} has exceeded its maximum of {max_retries} recovery attempts")

    def __reduce__(self) -> tuple[type[MaxRecoveryAttemptsExceededError], tuple[str, int]]:
        return (self.__class__, (self.workflow_id, self.max_retries))


class DBOSAwaitedWorkflowCancelledError(DBOSException):
    def __init__(self, workflow_id: str) -> None:
        self.workflow_id = workflow_id
        super().__init__(f"Awaited workflow {workflow_id} was cancelled")

    def __reduce__(self) -> tuple[type[DBOSAwaitedWorkflowCancelledError], tuple[str]]:
        return (self.__class__, (self.workflow_id,))


class DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded(DBOSException):
    def __init__(self, workflow_id: str) -> None:
        self.workflow_id = workflow_id
        super().__init__(f"Awaited workflow {workflow_id} exceeded its maximum recovery attempts")

    def __reduce__(self) -> tuple[type[DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded], tuple[str]]:
        return (self.__class__, (self.workflow_id,))


# These classes live in the native module, which pickle cannot import.
_MESSAGE_EXCEPTIONS = (
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSLeaseLostError,
    DBOSNonExistentWorkflowError,
    DBOSStepNondeterminismError,
    DBOSUnexpectedStepError,
    StoreError,
)


def _rebuild_message_exception(name: str, message: str) -> BaseException:
    for kind in _MESSAGE_EXCEPTIONS:
        if kind.__name__ == name:
            return kind(message)
    return DBOSRecordedError(name, message)


def _reduce_message_exception(error: BaseException) -> tuple[object, tuple[str, str]]:
    return (_rebuild_message_exception, (type(error).__name__, str(error)))


for _kind in _MESSAGE_EXCEPTIONS:
    copyreg.pickle(_kind, _reduce_message_exception)


__all__ = [
    "DBOSAwaitedWorkflowCancelledError",
    "DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded",
    "DBOSConflictingWorkflowError",
    "DBOSException",
    "DBOSLeaseLostError",
    "DBOSMaxStepRetriesExceeded",
    "DBOSNonExistentWorkflowError",
    "DBOSRecordedError",
    "DBOSStepNondeterminismError",
    "DBOSUnexpectedStepError",
    "MaxRecoveryAttemptsExceededError",
    "StoreError",
]
