import asyncio

DEFAULT_MAX_RECOVERY_ATTEMPTS: int
PORTABLE_JSON: str
PY_PICKLE: str
CUSTOM_PREFIX: str

def step_id_for_call(run: Run | None, in_step: bool, reserved_step_id: int | None = None) -> int | None: ...

class DBOSException(Exception): ...
class StoreError(DBOSException): ...
class DBOSConflictingWorkflowError(DBOSException): ...
class DBOSUnexpectedStepError(DBOSException): ...
class DBOSStepNondeterminismError(DBOSException): ...
class DBOSNonExistentWorkflowError(DBOSException): ...
class DBOSLeaseLostError(DBOSException): ...
class DBOSWorkflowCancelledError(DBOSException): ...
class DBOSQueueDeduplicatedError(DBOSException): ...
class DBOSWorkflowIDInUseError(DBOSException):
    workflow_id: str
    workflow_status: str
    workflow_name: str

class Run:
    @property
    def workflow_id(self) -> str: ...
    @property
    def serialization(self) -> str: ...
    @property
    def workflow_deadline_epoch_ms(self) -> int | None: ...
    @property
    def last_step_id(self) -> int: ...
    def step_id_unless_in_step(self, in_step: bool) -> int | None: ...
    def reserve_step_id(self) -> int: ...

class WorkflowRow:
    workflow_id: str
    name: str
    status: str
    input: str
    output: str | None
    error: str | None
    recovery_attempts: int
    max_recovery_attempts: int | None
    parent_workflow_id: str | None
    created_at_ms: int
    updated_at_ms: int
    completed_at_ms: int | None
    serialization: str
    workflow_timeout_ms: int | None
    workflow_deadline_epoch_ms: int | None
    application_version: str | None
    application_name: str | None
    executor_id: str | None
    forked_from: str | None
    was_forked_from: bool
    schedule_name: str | None
    authenticated_user: str | None
    assumed_role: str | None
    authenticated_roles: str | None
    attributes: str | None
    queue_name: str | None
    priority: int | None
    deduplication_id: str | None
    queue_partition_key: str | None
    dequeued_at_ms: int | None
    delay_until_ms: int | None

class StepRow:
    started_at_epoch_ms: int | None
    completed_at_epoch_ms: int
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

class ScheduleFire:
    schedule_name: str
    queue_name: str
    workflow_id: str
    scheduled_at_ms: int
    started: bool

class ActiveRun:
    def lost(self) -> asyncio.Future[str]: ...
    def wait_lost(self, seconds: float) -> str | None: ...
    def stop(self) -> None: ...

def silence_logs() -> None: ...

class StepPolicy:
    def __init__(
        self,
        retries_allowed: bool,
        interval_seconds: float,
        max_attempts: int,
        backoff_rate: float,
        timeout_seconds: float | None = None,
        heartbeat_timeout_seconds: float | None = None,
    ) -> None: ...
    @property
    def wraps_failures(self) -> bool: ...
    @property
    def max_attempts(self) -> int: ...
    def attempts(self) -> StepAttempts: ...
    def watch(self) -> StepWatch | None: ...

class StepAttempts:
    def status(self) -> tuple[int | None, int | None]: ...
    @property
    def wraps_failures(self) -> bool: ...
    def failed(self, retryable: bool = True) -> float | None: ...

class StepWatch:
    def beat(self) -> str | None: ...
    def finish(self) -> str | None: ...
    def expired(self, *, blocking: bool = False) -> asyncio.Future[str | None] | str | None: ...

class Poll:
    kind: str
    value: str | None
    serialization: str | None
    remaining_ms: int | None
    deadline_ms: int | None

class SendResult:
    kind: str
    error: str | None

class EventRow:
    key: str
    value: str
    serialization: str

class StreamItem:
    kind: str  # "value", "closed" or "timeout"
    value: str | None
    serialization: str | None

# Every method that takes `blocking` returns the awaited value directly when it is True, with the GIL released.
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
        custom_serializer: str | None = None,
        read_only: bool | None = None,
        executor_id: str | None = None,
        application_version: str | None = None,
    ) -> asyncio.Future[Engine]: ...
    @property
    def serialization(self) -> str: ...
    def close(self) -> None: ...
    def begin(
        self,
        name: str,
        input: str,
        max_recovery_attempts: int | None,
        workflow_id: str | None = None,
        parent: Run | None = None,
        in_step: bool = False,
        timeout_ms: int | None = None,
        ignore_propagated_deadline: bool = False,
        reuse_policy: str | None = None,
        serialization: str | None = None,
        *,
        attributes: str | None = None,
        blocking: bool = False,
    ) -> asyncio.Future[Begun] | Begun: ...
    def begin_step(self, run: Run, name: str, step_id: int, *, blocking: bool = False) -> asyncio.Future[StepReplay | None] | StepReplay | None: ...
    def save_step_output(self, run: Run, step_id: int, name: str, output: str, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def save_step_error(self, run: Run, step_id: int, name: str, error: str, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def check_owner(self, run: Run, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def conclude(self, run: Run, *, output: str | None = None, error: str | None = None, lease_lost: bool = False, blocking: bool = False) -> asyncio.Future[WorkflowResult | None] | WorkflowResult | None: ...
    def keep_lease(self, run: Run) -> ActiveRun: ...
    def await_result(self, workflow_id: str, polling_interval_sec: float | None = None) -> asyncio.Future[WorkflowResult]: ...
    def await_first(self, workflow_ids: list[str], polling_interval_sec: float) -> asyncio.Future[str]: ...
    def get_status(self, workflow_id: str, *, blocking: bool = False) -> asyncio.Future[WorkflowRow] | WorkflowRow: ...
    def delete_workflows(self, workflow_ids: list[str], delete_children: bool = False, *, blocking: bool = False) -> asyncio.Future[list[str]] | list[str]: ...
    def delete_finished_before(self, cutoff_epoch_ms: int, batch: int, *, blocking: bool = False) -> asyncio.Future[int] | int: ...
    def apply_retention(self, max_age_hours: float | None, keep_rows: int | None, batch: int, *, blocking: bool = False) -> asyncio.Future[int] | int: ...
    def cancel_workflow(self, workflow_id: str, cancel_children: bool = False, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def cancel_workflows(self, workflow_ids: list[str], cancel_children: bool = False, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def resume_workflow(self, workflow_id: str, names: list[str], queue_name: str | None = None, *, blocking: bool = False) -> asyncio.Future[Claim | None] | Claim | None: ...
    def resume_workflows(self, workflow_ids: list[str], names: list[str], queue_name: str | None = None, *, blocking: bool = False) -> asyncio.Future[list[Claim]] | list[Claim]: ...
    def fork_workflow(self, workflow_id: str, start_step: int, names: list[str], new_id: str | None = None, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, replacement_children: dict[str, str] | None = None, timeout_ms: int | None = None, *, blocking: bool = False) -> asyncio.Future[tuple[str, Claim | None]] | tuple[str, Claim | None]: ...
    def rewind_workflow(self, workflow_id: str, names: list[str], start_step: int | None = None, application_version: str | None = None, queue_name: str | None = None, queue_partition_key: str | None = None, *, blocking: bool = False) -> asyncio.Future[Claim | None] | Claim | None: ...
    def send_bulk_to_forks(self, messages: list[tuple[str, str | None, str, str | None]], serialization: str, run: Run | None = None, step_id: int | None = None, *, blocking: bool = False) -> asyncio.Future[SendResult] | SendResult: ...
    def list_workflows(
        self,
        *,
        workflow_ids: list[str] | None = None,
        queue_name: list[str] | None = None,
        app_version: list[str] | None = None,
        forked_from: list[str] | None = None,
        is_fork: bool | None = None,
        was_forked_from: bool | None = None,
        user: list[str] | None = None,
        queues_only: bool = False,
        status: list[str] | None = None,
        name: list[str] | None = None,
        parent_workflow_id: list[str] | None = None,
        workflow_id_prefix: list[str] | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        completed_after_ms: int | None = None,
        completed_before_ms: int | None = None,
        has_parent: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort_desc: bool = False,
        blocking: bool = False,
    ) -> asyncio.Future[list[WorkflowRow]] | list[WorkflowRow]: ...
    def list_steps(self, workflow_id: str, *, limit: int | None = None, offset: int | None = None, blocking: bool = False) -> asyncio.Future[list[StepRow]] | list[StepRow]: ...
    def recover_now(self, names: list[str], *, blocking: bool = False) -> asyncio.Future[list[Claim]] | list[Claim]: ...
    def recovery_tick(self, names: list[str]) -> asyncio.Future[list[Claim]]: ...
    def sleep(self, run: Run, seconds: float, in_step: bool, *, blocking: bool = False) -> asyncio.Future[float] | float: ...
    def patch(self, run: Run, name: str, *, blocking: bool = False) -> asyncio.Future[bool] | bool: ...
    def deprecate_patch(self, run: Run, name: str, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def wait_sleep(self, run: Run, active: ActiveRun, seconds: float, *, blocking: bool = False) -> asyncio.Future[str | None] | str | None: ...
    def update_workflow_attributes(self, workflow_id: str, attributes: str | None, merge: bool = False, run: Run | None = None, step_id: int | None = None, *, blocking: bool = False) -> asyncio.Future[SendResult] | SendResult: ...
    def send_bulk(self, messages: list[tuple[str, str | None, str, str | None]], serialization: str, run: Run | None = None, step_id: int | None = None, *, blocking: bool = False) -> asyncio.Future[SendResult] | SendResult: ...
    def send(
        self,
        destination: str,
        message: str,
        serialization: str,
        topic: str | None = None,
        idempotency_key: str | None = None,
        run: Run | None = None,
        step_id: int | None = None,
        *,
        blocking: bool = False,
    ) -> asyncio.Future[SendResult] | SendResult: ...
    def poll_recv(
        self,
        run: Run,
        step_id: int,
        timeout_step_id: int,
        timeout_ms: int,
        topic: str | None = None,
        *,
        blocking: bool = False,
    ) -> asyncio.Future[Poll] | Poll: ...
    def wait_recv(
        self,
        run: Run,
        step_id: int,
        timeout_step_id: int,
        timeout_ms: int,
        topic: str | None = None,
    ) -> asyncio.Future[Poll]: ...
    def poll_event(
        self,
        target: str,
        key: str,
        timeout_ms: int,
        deadline_ms: int | None = None,
        run: Run | None = None,
        step_id: int | None = None,
        timeout_step_id: int | None = None,
        *,
        blocking: bool = False,
    ) -> asyncio.Future[Poll] | Poll: ...
    def wait_event(
        self,
        target: str,
        key: str,
        timeout_ms: int,
        run: Run | None = None,
        step_id: int | None = None,
        timeout_step_id: int | None = None,
    ) -> asyncio.Future[Poll]: ...
    def set_event(
        self,
        run: Run,
        key: str,
        value: str,
        serialization: str,
        step_id: int | None = None,
        *,
        blocking: bool = False,
    ) -> asyncio.Future[SendResult] | SendResult: ...
    def write_stream(self, run: Run, key: str, value: str, serialization: str, step_id: int | None = None, *, blocking: bool = False) -> asyncio.Future[SendResult] | SendResult: ...
    def close_stream(self, run: Run, key: str, step_id: int | None = None, *, blocking: bool = False) -> asyncio.Future[SendResult] | SendResult: ...
    def read_stream(self, workflow_id: str, key: str, offset: int, timeout_seconds: float | None = None, polling_interval_sec: float | None = None) -> asyncio.Future[StreamItem]: ...
    def list_events(self, workflow_id: str, *, blocking: bool = False) -> asyncio.Future[list[EventRow]] | list[EventRow]: ...
    def get_queue(self, name: str, *, blocking: bool = False) -> asyncio.Future[int | None] | int | None: ...
    def set_polling_interval(self, name: str, seconds: float, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def get_polling_interval(self, name: str, *, blocking: bool = False) -> asyncio.Future[int | None] | int | None: ...
    def register_queue(self, name: str, global_: int | None = None, worker: int | None = None, update: bool = True, limit: int | None = None, period: float | None = None, partition_concurrency: int | None = None, partition_worker_concurrency: int | None = None, partition_limit: int | None = None, partition_period: float | None = None, polling_interval: float = 1.0, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def delete_queue(self, name: str, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def list_queues(self, applications: list[str] | None = None, *, blocking: bool = False) -> asyncio.Future[list[tuple[str, int]]] | list[tuple[str, int]]: ...
    def set_global_concurrency(self, name: str, limit: int | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def set_limiter(self, name: str, limit: int | None = None, period: float | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def get_limiter(self, name: str, *, blocking: bool = False) -> asyncio.Future[tuple[int, int] | None] | tuple[int, int] | None: ...
    def get_partition_limits(self, name: str, *, blocking: bool = False) -> asyncio.Future[tuple[int | None, int | None, tuple[int, int] | None]] | tuple[int | None, int | None, tuple[int, int] | None]: ...
    def update_partition_limits(self, name: str, field: str, value: int | None = None, period: float | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def ensure_queue(self, name: str, concurrency: int | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def set_worker_concurrency(self, name: str, limit: int | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def get_worker_concurrency(self, name: str, *, blocking: bool = False) -> asyncio.Future[int | None] | int | None: ...
    def enqueue(
        self,
        queue: str,
        name: str,
        input: str,
        max_recovery_attempts: int | None,
        workflow_id: str | None = None,
        parent: Run | None = None,
        in_step: bool = False,
        timeout_ms: int | None = None,
        ignore_propagated_deadline: bool = False,
        reuse_policy: str | None = None,
        serialization: str | None = None,
        *,
        deduplication_id: str | None = None,
        duplication_policy: str | None = None,
        priority: int | None = None,
        delay_seconds: float | None = None,
        app_version: str | None = None,
        queue_partition_key: str | None = None,
        attributes: str | None = None,
        blocking: bool = False,
    ) -> asyncio.Future[str] | str: ...
    def debounce(
        self,
        queue: str,
        name: str,
        input: str,
        max_recovery_attempts: int | None,
        debounce_key: str,
        debounce_period_sec: float,
        debounce_timeout_sec: float | None = None,
        workflow_id: str | None = None,
        parent: Run | None = None,
        in_step: bool = False,
        timeout_ms: int | None = None,
        reuse_policy: str | None = None,
        serialization: str | None = None,
        *,
        app_version: str | None = None,
        application_name: str | None = None,
        attributes: str | None = None,
        blocking: bool = False,
    ) -> asyncio.Future[str] | str: ...
    def set_workflow_delay(self, workflow_id: str, delay_seconds: float | None = None, delay_until_epoch_ms: int | None = None, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def claim_queue(self, queue: str, names: list[str]) -> asyncio.Future[list[Claim]]: ...
    def wake_queue(self, queue: str) -> None: ...
    def wait_queue_claims(self, queue: str, names: list[str], interval_seconds: float) -> asyncio.Future[list[Claim]]: ...
    def ensure_schedule(
        self,
        name: str,
        workflow_name: str,
        cron: str,
        queue: str,
        inputs: str,
        serialization: str,
        max_recovery_attempts: int | None = None,
        *,
        blocking: bool = False,
    ) -> asyncio.Future[None] | None: ...
    def delete_schedule(self, name: str, *, blocking: bool = False) -> asyncio.Future[None] | None: ...
    def fire_schedules(self, *, blocking: bool = False) -> asyncio.Future[list[ScheduleFire]] | list[ScheduleFire]: ...
