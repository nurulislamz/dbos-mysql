from __future__ import annotations

from collections.abc import Sequence

from dbos_mysql._native import (
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSLeaseLostError,
    DBOSNonExistentWorkflowError,
    DBOSStepNondeterminismError,
    DBOSUnexpectedStepError,
    StoreError,
)


class DBOSRecordedError(DBOSException):
    """A stored failure whose original exception type is not rebuilt, because stored data must not choose code to import."""

    def __init__(self, type_name: str, message: str) -> None:
        self.type_name = type_name
        super().__init__(message)


class DBOSMaxStepRetriesExceeded(DBOSException):
    def __init__(self, step_name: str, max_retries: int, errors: Sequence[BaseException] = ()) -> None:
        self.step_name = step_name
        self.max_retries = max_retries
        self.errors = list(errors)
        super().__init__(f"Step {step_name} has exceeded its maximum of {max_retries} retries")


class MaxRecoveryAttemptsExceededError(DBOSException):
    def __init__(self, workflow_id: str, max_retries: int) -> None:
        self.workflow_id = workflow_id
        self.max_retries = max_retries
        super().__init__(f"Workflow {workflow_id} has exceeded its maximum of {max_retries} recovery attempts")


__all__ = [
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
