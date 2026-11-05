# Queues

A queue starts workflows later, in order, with a limit on how many run at once. The limit holds across every copy of your program.

```python
queue = DBOS.register_queue("emails", global_concurrency=5)

handle = queue.enqueue(send_welcome, "user-1")
print(handle.get_result())
```

`register_queue` needs a launched DBOS, so call it after `DBOS.launch()`. It saves the limit and starts pulling work from the queue in this process. Only a process that registered the queue runs its work.

Elsewhere you can look up a queue or enqueue by name:

```python
queue = DBOS.retrieve_queue("emails")
handle = DBOS.enqueue_workflow("emails", send_welcome, "user-1")
```

Async code uses `register_queue_async`, `enqueue_async` and `enqueue_workflow_async`.

## How it runs

1. `enqueue` writes the workflow row and a queue entry, and returns an ID.
2. A queue loop in each registered process asks for work. A local enqueue wakes it at once. Otherwise it reads every `polling_interval_sec` (default 0.25).
3. One database transaction revokes expired owners, counts the remaining owners, and admits the next rows in FIFO order, up to the limit.
4. The admitted workflow runs like any other, with a lease.
5. When it finishes, its slot is free.

If a process dies, its slot frees when its lease runs out. The workflow then goes through admission again.

## Status

A workflow waiting on a queue reads as `ENQUEUED` or `DELAYED`, and as `PENDING` once admitted. The row's stored status is `PENDING` the whole time.

## Avoiding duplicates

Enqueue is not idempotent by itself. To make a retry safe, set an ID, or enqueue from inside a workflow, where the call is durable.

```python
with SetWorkflowID(f"welcome-{user_id}"):
    queue.enqueue(send_welcome, user_id)
```

## Timeouts in a queue

A relative timeout starts on first admission, so waiting does not use it up. A deadline inherited from a parent keeps running while the child waits.

## Limits to know

- **The latest registration with a limit sets it.** During a rolling deploy, old and new code can overwrite each other until the old processes stop.
- **Registering with no limit keeps the stored one.** One process cannot lift the cap for all. A new queue registered this way has no limit.
- **Stop old workers before first using queues.** Old claim code does not know about queue membership and can bypass admission.
- **A large backlog is not measured.** Each slot re-scans pending rows while holding the queue row lock.
- **Names starting with `_dbos_` are reserved.**
- **Unsupported options fail.** `register_queue` raises `DBOSException` naming them.

> **Coming soon:** priorities, rate limits, partitions, per-worker limits, deduplication and delayed starts. Queues are plain FIFO today. Some columns for these exist, but nothing reads them.
