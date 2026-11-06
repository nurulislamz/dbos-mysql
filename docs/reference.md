# Reference

Everything is imported from `yado`. Most calls that do I/O have an `_async` version, for example `DBOS.send_async`.

## Configuration

```python
DBOS(config={"name": "my-app", "system_database_url": url})
```

Unknown keys raise `DBOSException`.

| Key | Default | Meaning |
|---|---|---|
| `name` | Required | Your app's name |
| `system_database_url` | Required | `sqlite://`, `mysql://` or `postgres://` URL |
| `run_migrations` | `True` | Upgrade the schema at launch |
| `lease_seconds` | `30` | How long a lease lasts without renewal |
| `max_run_seconds` | `3600` | Longest a single run keeps its lease |
| `recovery_interval_seconds` | `1` | Time between recovery checks, with jitter |
| `max_concurrent_recoveries` | `50` | Most recovered workflows per check, less active runs |
| `shutdown_timeout_seconds` | `5` | How long `destroy()` waits |
| `serializer` | `"pickle"` | `"pickle"` or `"json"` |
| `log_level` | `"INFO"` when logging is set up | Level for the `dbos` logger |
| `console_log_level` | Same as `log_level` | Level for console output |
| `executor_id` | `DBOS__VMID`, else `"local"` | Label for this process. See `DBOS.executor_id` below |

## Lifecycle

| Call | Does |
|---|---|
| `DBOS(config=...)` | Configures DBOS once per process |
| `DBOS.launch()` | Connects, migrates and starts background loops. A second call does nothing |
| `DBOS.destroy(destroy_registry=False)` | Stops work and releases leases |

## Decorators

| Decorator | Options |
|---|---|
| `@DBOS.workflow()` | `name`, `max_recovery_attempts=100`, `validate_args` |
| `@DBOS.step()` | `name`, `retries_allowed=False`, `interval_seconds=1.0`, `max_attempts=3`, `backoff_rate=2.0`, `should_retry`, `timeout_seconds`, `heartbeat_timeout_seconds` |
| `@DBOS.scheduled(cron)` | `queue` (required), `name`. Goes above `@DBOS.workflow()` |

## Context

| Call | Gives |
|---|---|
| `DBOS.workflow_id` | Current workflow ID, or `None` |
| `DBOS.step_id` | Current step number, or `None` |
| `DBOS.step_status` | `StepStatus` with `step_id`, `current_attempt`, `max_attempts` |
| `DBOS.executor_id` | This process's label: config `executor_id`, else `DBOS__VMID`, else `"local"`. Each start and each lease claim writes it onto the workflow row, so `WorkflowStatus.executor_id` names the process that started the workflow or last took it over. It does not decide ownership: the lease does, so copies with the same label both run workflows |
| `DBOS.logger` | The `dbos` logger |
| `DBOS.heartbeat()` | Reports progress from inside a step |
| `with SetWorkflowID(id, workflow_id_reuse_policy=None)` | Sets the ID of the next workflow. The policy is `"reject"` or `"return-existing"` |
| `with SetWorkflowTimeout(seconds)` | Sets the timeout of workflows started inside the block. `None` opts out of an inherited deadline |

## Starting and waiting

| Call | Does |
|---|---|
| `DBOS.start_workflow(fn, *args, **kwargs)` | Starts in the background and returns a handle |
| `DBOS.retrieve_workflow(id, existing_workflow=True)` | Returns a handle to a run |
| `DBOS.get_result(id, polling_interval_sec=None)` | Waits for the result |
| `DBOS.get_workflow_status(id)` | `WorkflowStatus`, or `None` if missing |

Handles (`WorkflowHandle`, `WorkflowHandleAsync`) have `workflow_id`, `get_workflow_id()`, `get_result(polling_interval_sec=None)` and `get_status()`.

| Call | Does |
|---|---|
| `DBOS.wait_first(handles, polling_interval_sec=1.0)` | Returns the first handle that finishes. Sync only: it refuses a running event loop, so use `wait_first_async` there |
| `DBOS.run_step(opts, fn, *args, **kwargs)` | Runs `fn` as a step without a decorator. `opts` is a dict such as `{"name": ...}`, or `None`. Sync only, like `wait_first`; use `run_step_async` in async code. `preemptible=True` is for async steps only. Async steps are cancelled with their workflow even when it is `False`, while DBOS would let them finish |
| `DBOS.reserve_step()` | Draws the next step ID now. `reservation.run(opts, fn, ...)` or `run_async` runs it once, later, in the same workflow run. A reservation that never runs leaves a gap in the step IDs |
| `DBOS.asyncio_wait(fs, timeout=None, return_when=ALL_COMPLETED)` | A durable `asyncio.wait`: which tasks finished is saved, so replay agrees. Outside a workflow it is plain `asyncio.wait` |
| `with DBOSContextEnsure() as ctx` | Uses the current DBOS context, or makes one for the block and clears it on exit. One instance cannot be entered twice |

## Queues

| Call | Does |
|---|---|
| `DBOS.register_queue(name, global_concurrency=None, polling_interval_sec=0.25)` | Saves the limit and starts pulling work |
| `DBOS.retrieve_queue(name)` | Returns a `Queue`, or `None` |
| `queue.enqueue(fn, *args, **kwargs)` | Adds a workflow and returns a handle |
| `DBOS.enqueue_workflow(queue_name, fn, *args, **kwargs)` | The same, by name |

## Messages and events

| Call | Does |
|---|---|
| `DBOS.send(destination_id, message, topic=None, idempotency_key=None)` | Sends a message |
| `DBOS.recv(topic=None, timeout_seconds=60)` | Waits for a message. Workflows only |
| `DBOS.set_event(key, value)` | Publishes a value. Workflows only |
| `DBOS.get_event(workflow_id, key, timeout_seconds=60)` | Waits for a value |
| `DBOS.get_all_events(workflow_id)` | All of a workflow's values, as a `dict` |

## Time

| Call | Does |
|---|---|
| `DBOS.sleep(seconds)` | Durable sleep |
| `DBOS.register_schedule(name, workflow, cron, queue=..., args=(), kwargs=None)` | Adds or updates a cron schedule |
| `DBOS.delete_schedule(name)` | Removes a schedule |

## Management

| Call | Does |
|---|---|
| `DBOS.list_workflows(...)` | Lists workflows. See [Managing workflows](guide/management.md) |
| `DBOS.list_workflow_steps(id)` | Lists saved steps |
| `DBOS.cancel_workflow(id, cancel_children=False)`, `cancel_workflows` | Cancels |
| `DBOS.resume_workflow(id, queue_name=None)`, `resume_workflows` | Resumes |
| `DBOS.delete_workflow(id, delete_children=False)`, `delete_workflows` | Deletes finished workflows and returns the IDs deleted |
| `DBOS.delete_finished_workflows(before_epoch_ms, batch=200)` | Deletes by age and returns the count |

## Errors

| Error | Means |
|---|---|
| `DBOSException` | Base class |
| `StoreError` | A database error |
| `DBOSConflictingWorkflowError` | The ID is in use by another workflow name |
| `DBOSWorkflowIDInUseError` | The ID already exists and the policy is `reject` |
| `DBOSNonExistentWorkflowError` | No such workflow |
| `DBOSUnexpectedStepError`, `DBOSStepNondeterminismError` | The code changed, or is not deterministic, on replay |
| `DBOSMaxStepRetriesExceeded` | Retries ran out |
| `DBOSStepTimeoutError` | A step attempt timed out |
| `DBOSLeaseLostError` | This process no longer owns the workflow |
| `DBOSWorkflowCancelledError` | Raised inside a cancelled workflow |
| `DBOSAwaitedWorkflowCancelledError` | The workflow you waited on was cancelled |
| `DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded`, `MaxRecoveryAttemptsExceededError` | Too many recoveries |
| `DBOSRecordedError` | A stored failure whose class is not loaded here |
