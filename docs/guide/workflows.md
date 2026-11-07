# Workflows

A workflow is a function that always finishes. If the process dies, it runs again from the top, and its saved steps return their stored values.

```python
@yado.workflow()
def fulfil(order_id: str) -> str:
    receipt = charge(order_id)
    ship(order_id)
    return receipt
```

Sync and async functions both work.

## Workflow code must be deterministic

The body may run many times, and it must call the same steps in the same order each time. Put anything that can change in a step: API calls, database reads, the current time, random numbers.

```python
# Wrong: a different value on each run can pick a different branch.
@yado.workflow()
def bad() -> None:
    if random.random() > 0.5:
        step_a()


# Right: the choice is saved, so replay takes the same branch.
@yado.step()
def choose() -> float:
    return random.random()
```

A change seen on replay is detected and raised as an error, rather than saved over.

## Workflow IDs

Each workflow has an ID. It is random unless you set it.

```python
with SetWorkflowID("order-123"):
    result = fulfil("123")
```

One ID means one run:

| Row for this ID | What the call does |
|---|---|
| Finished | Returns the stored result or raises the stored error |
| Still running | Waits for that run |
| Same ID, another workflow name | Raises `DBOSConflictingWorkflowError` |

To refuse a repeat instead, use `SetWorkflowID("order-123", workflow_id_reuse_policy="reject")`. That raises `DBOSWorkflowIDInUseError`.

Inside a workflow, `DBOS.workflow_id` and `DBOS.step_id` give the current values.

## Starting in the background

```python
handle = yado.start_workflow(fulfil, "123")
print(handle.workflow_id)
print(handle.get_status().status)
print(handle.get_result())
```

Async code uses `await yado.start_workflow_async(...)`, and `await handle.get_result()`.

Another process can reach the same run by ID:

```python
handle = yado.retrieve_workflow("order-123")
```

`get_result(polling_interval_sec=2.0)` sets the longest wait between database reads. The default cap is 250 ms.

## Child workflows

A workflow can call or start another workflow. The child's ID is saved as a step of the parent, so replay finds the same child and does not start a second one. Without an explicit ID, the child's ID is `<parent id>-<step number>`.

```python
@yado.workflow()
def parent() -> str:
    handle = yado.start_workflow(child, "input")
    return handle.get_result()
```

## Errors

An exception that leaves the workflow is saved as its final error. Later calls with that ID raise it again.

| Stored | What a waiter sees |
|---|---|
| Your exception, class loaded | The same type |
| Your exception, class not loaded or not rebuildable | `DBOSRecordedError` |
| Cancelled | `DBOSAwaitedWorkflowCancelledError` |
| Too many recoveries | `DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded` |

Attributes are kept. `__cause__` and tracebacks are lost.

## Recovery limit

`@yado.workflow(max_recovery_attempts=10)` sets how many times a run may be recovered. The default is 100. Past the limit the row ends as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. This stops a workflow that crashes its process from doing so forever. `yado.resume_workflow` can restart it.

## Validating arguments

`@yado.workflow(validate_args=pydantic_args_validator)` checks arguments against the type hints. This is written, but the workflow execution path is not verified yet.

## Inputs and outputs

Values are stored with pickle by default, so most Python values come back as they were. See [Stored values](../production.md#stored-values) for the risks and the JSON option.
