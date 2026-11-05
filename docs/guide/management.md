# Managing workflows

These calls work from any process with DBOS launched. Each has an `_async` version.

## Look at workflows

```python
status = DBOS.get_workflow_status("order-123")  # None if missing
print(status.status, status.recovery_attempts)

failed = DBOS.list_workflows(status="ERROR", limit=50, sort_desc=True)
steps = DBOS.list_workflow_steps("order-123")
```

`list_workflows` filters by `workflow_ids`, `status`, `name`, `workflow_id_prefix`, `parent_workflow_id`, `has_parent`, `start_time`, `end_time`, `completed_after` and `completed_before`. Times are ISO 8601 text. It also takes `limit`, `offset` and `sort_desc`. `load_input=False` and `load_output=False` skip decoding of stored values.

> **Coming soon:** filters on queue, version, fork and auth fields.

## Status values

| Status | Meaning | Final |
|---|---|---|
| `ENQUEUED` | Waiting on a queue | No |
| `DELAYED` | Waiting on a queue for its start time | No |
| `PENDING` | Running, or waiting for recovery | No |
| `SUCCESS` | Returned a value | Yes |
| `ERROR` | Raised an exception | Yes |
| `CANCELLED` | Cancelled or past its deadline | Until resumed |
| `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | Recovered too many times | Until resumed |

## Cancel

```python
DBOS.cancel_workflow("order-123")
DBOS.cancel_workflow("order-123", cancel_children=True)
DBOS.cancel_workflows(["a", "b"])
```

A cancel takes the workflow's lease away. An async body is cancelled when the lease keeper notices. A sync body cannot be stopped, so it runs until its next durable call. A sleeping workflow wakes at once on the same engine, and within about 250 ms from another process.

## Resume

```python
handle = DBOS.resume_workflow("order-123")
handle = DBOS.resume_workflow("order-123", queue_name="retries")
handles = DBOS.resume_workflows(["a", "b"])
```

Resume restarts a cancelled or dead-lettered workflow by replay, so saved steps stay saved. Resuming a workflow that a live process owns leaves the owner alone. Resuming it onto a queue does nothing and raises no error.

## Delete

```python
DBOS.delete_workflow("order-123", delete_children=True)
deleted_count = DBOS.delete_finished_workflows(before_epoch_ms=cutoff, batch=200)
```

There is no automatic retention, so run `delete_finished_workflows` from a schedule of your own if your tables should not grow forever.

- Pending workflows are never deleted.
- A child that a parent's replay could still need stays. This includes children of pending, cancelled or dead-lettered parents.
- A deleted ID can run again.

Known gap: a child attached to a second parent by an explicit ID is not protected.

> **Coming soon:** fork and rewind, which restart a workflow from a chosen step.
