import asyncio

DEFAULT_MAX_RECOVERY_ATTEMPTS: int
PORTABLE_JSON: str
PY_PICKLE: str

class DBOSException(Exception): ...
class StoreError(DBOSException): ...
class DBOSConflictingWorkflowError(DBOSException): ...
class DBOSUnexpectedStepError(DBOSException): ...
class DBOSStepNondeterminismError(DBOSException): ...
class DBOSNonExistentWorkflowError(DBOSException): ...
class DBOSLeaseLostError(DBOSException): ...

class Run:
    @property
    def workflow_id(self) -> str: ...
    @property
    def serialization(self) -> str: ...

class WorkflowRow:
    workflow_id: str
    name: str
    status: str
    input: str
    output: str | None
    error: str | None
    recovery_attempts: int
    max_recovery_attempts: int
    parent_workflow_id: str | None
    created_at_ms: int
    updated_at_ms: int
    completed_at_ms: int | None
    serialization: str

class StepRow:
    step_id: int
    step_name: str
    output: str | None
    error: str | None
    child_workflow_id: str | None
    serialization: str

class WorkflowResult:
    workflow_id: str
    name: str
    kind: str
    value: str | None
    max_recovery_attempts: int | None
    serialization: str

class Begun:
    workflow_id: str
    run: Run | None
    result: WorkflowResult | None

class StepReplay:
    output: str | None
    error: str | None

class Claim:
    workflow_id: str
    run: Run
    name: str
    input: str
    recovery_attempts: int

class ActiveRun:
    def lost(self) -> asyncio.Future[str]: ...
    def stop(self) -> None: ...

def silence_logs() -> None: ...

class StepPolicy:
    def __init__(self, retries_allowed: bool, interval_seconds: float, max_attempts: int, backoff_rate: float) -> None: ...
    @property
    def retries_allowed(self) -> bool: ...
    @property
    def max_attempts(self) -> int: ...
    def status(self, attempt: int) -> tuple[int | None, int | None]: ...
    def retry_delay(self, failed_attempt: int) -> float | None: ...

class Engine:
    @staticmethod
    def connect(
        url: str,
        *,
        lease_seconds: float | None = None,
        max_run_seconds: float | None = None,
        recovery_interval_seconds: float | None = None,
        max_concurrent_recoveries: int | None = None,
        run_migrations: bool | None = None,
        serializer: str | None = None,
    ) -> asyncio.Future[Engine]: ...
    @property
    def serialization(self) -> str: ...
    def close(self) -> None: ...
    def begin(
        self,
        name: str,
        input: str,
        max_recovery_attempts: int,
        workflow_id: str | None = None,
        parent: Run | None = None,
        in_step: bool = False,
    ) -> asyncio.Future[Begun]: ...
    def begin_step(self, run: Run, name: str) -> asyncio.Future[tuple[int, StepReplay | None]]: ...
    def save_step_output(self, run: Run, step_id: int, name: str, output: str) -> asyncio.Future[None]: ...
    def save_step_error(self, run: Run, step_id: int, name: str, error: str) -> asyncio.Future[None]: ...
    def finish_success(self, run: Run, output: str) -> asyncio.Future[WorkflowResult | None]: ...
    def finish_error(self, run: Run, error: str) -> asyncio.Future[WorkflowResult | None]: ...
    def release(self, run: Run) -> asyncio.Future[None]: ...
    def keep_lease(self, run: Run) -> ActiveRun: ...
    def await_result(self, workflow_id: str) -> asyncio.Future[WorkflowResult]: ...
    def get_status(self, workflow_id: str) -> asyncio.Future[WorkflowRow]: ...
    def list_workflows(self) -> asyncio.Future[list[WorkflowRow]]: ...
    def list_steps(self, workflow_id: str) -> asyncio.Future[list[StepRow]]: ...
    def recover_now(self, names: list[str]) -> asyncio.Future[list[Claim]]: ...
    def recovery_tick(self, names: list[str]) -> asyncio.Future[list[Claim]]: ...
