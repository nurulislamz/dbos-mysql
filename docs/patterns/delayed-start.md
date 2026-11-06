# Delayed start

> Create the workflow now, and run it after a delay. The row exists immediately and reads as `DELAYED`.

:::note[Supported]

Enqueue with `SetEnqueueOptions(delay_seconds=...)`. `DBOS.set_workflow_delay` moves the start earlier or later.

:::

## Overview

A delayed start is a one-shot schedule. The workflow ID, the arguments, and the queue entry exist at once. Nothing runs until the delay elapses. You can bring the start forward, push it back, or cancel it before it begins.

This fits a grace period, a reminder, or any job that should be durable from the moment it is requested.

## Problem

`threading.Timer` and a process sleep disappear when the process dies. A cron schedule is the wrong tool for one email that should go out an hour after signup. Creating the workflow only when the hour is up loses the record of the request if the process that was supposed to create it is down.

## Solution

Register a queue and enqueue with a delay. The queue's concurrency limit still applies when the delay ends.

```python
from yado import DBOS, SetEnqueueOptions


queue = DBOS.register_queue("later")

with SetEnqueueOptions(delay_seconds=3600):
    handle = queue.enqueue(send_email, "user-1")
```

The status reads as `DELAYED` until the workflow is eligible, then `ENQUEUED` or `PENDING` as the queue admits it. The stored status is `PENDING` throughout. See [Queues](../guide/queues.md#status).

Start early, or replace the delay:

```python
DBOS.set_workflow_delay(handle.workflow_id, delay_seconds=0)
```

Cancel before it starts with `DBOS.cancel_workflow(handle.workflow_id)`.

Give the workflow a stable ID if a retry of the request must not enqueue a second copy:

```python
from yado import SetWorkflowID

with SetWorkflowID("welcome-user-1"), SetEnqueueOptions(delay_seconds=3600):
    queue.enqueue(send_email, "user-1")
```

## When to use

Use a delayed start when:

- The work is requested now and should run once, later.
- Callers need the workflow ID immediately, so they can cancel or reschedule.
- The delay should survive a process restart.

Use a [schedule](../guide/time.md#schedules) for a cron. Use `DBOS.sleep` at the start of a workflow that is already running.

## Trade-offs

The workflow waits in a queue, so it starts only on a process that has registered that queue. A delay of an hour is fine. The row is small. The concurrency limit applies at admission, not during the delay.

## Pitfalls

- `register_queue` needs a launched DBOS, and only a process that registered the queue will run the work.
- `delay_seconds` is relative to enqueue time. A replay of an enqueue inside a workflow reuses the saved enqueue.
- Setting the delay to `0` makes the entry eligible. It still waits for a free concurrency slot.

## Related

- [Delayed callback](delayed-callback.md)
- [Updatable timer](updatable-timer.md)
- [Queues](../guide/queues.md#ordering-and-limits)
