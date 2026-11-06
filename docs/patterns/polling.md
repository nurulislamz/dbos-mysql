# Polling an external service

> Ask an outside system for status. Poll often inside one step, or sleep between attempts so the wait is saved.

:::note[Supported]

A short poll is one step that calls `DBOS.heartbeat()`. A long poll is a workflow loop: one step per attempt, then `DBOS.sleep`.

:::

## Overview

Polling is how a workflow learns about a system that will not call it back. The workflow decides the interval. The result of each attempt that matters is saved, so a crash continues from the last saved answer rather than from a timer living in one process.

Two frequencies show up in practice:

- Frequent. One step loops, and heartbeats so a hang is detected.
- Infrequent. The workflow sleeps between single attempts. The sleep is an absolute wake time.

## Problem

A carrier API returns "label pending" for minutes. Sleeping in the process is lost on a crash. Looping with no heartbeat looks alive to the engine while the HTTP call is stuck. Recording every 200 ms poll as its own step makes a long wait expensive to replay.

## Solution

### Frequent polling, one step

The step owns the loop. `heartbeat_timeout_seconds` fails the attempt if `DBOS.heartbeat()` stops.

```python
from rust_dwos import DBOS


@DBOS.step(heartbeat_timeout_seconds=30, timeout_seconds=600)
def poll_once() -> str | None:
    for _ in range(20):
        status = client.status()
        DBOS.heartbeat()
        if status is not None:
            return status
    return None
```

The next attempt does not receive the last status you passed to `heartbeat`. `heartbeat` takes no payload. Save a checkpoint by returning from the step, or by splitting the work into more steps. See [Long-running steps](long-running-step.md).

### Infrequent polling

Sleep in the workflow. Each sleep is stored, and a restart waits only for what is left.

```python
@DBOS.workflow()
def poll(shipment_id: str) -> str:
    while (status := read_status(shipment_id)) is None:
        DBOS.sleep(60)
    return status
```

`read_status` is a step. The loop condition is then stable across replay.

## When to use

Use a polling workflow when the other system has no webhook.

Use a [delayed callback](delayed-callback.md) when it can send the result to you. That avoids the poll.

Use one heartbeating step when the interval is short and you do not need each attempt recorded. Use sleep between steps when the interval is long.

## Trade-offs

A single step is fewer rows and a coarser resume point. A step per attempt resumes at the last saved status and grows the step list with the number of polls.

## Pitfalls

- `DBOS.sleep` inside a step is an ordinary sleep. Put the durable wait in the workflow.
- A sync step that ignores its timeout keeps running until it returns. Heartbeat after the timeout raises, which gives the body a place to stop.
- Polling forever needs a workflow timeout or a max number of attempts, or the run never completes.

## Related

- [Long-running steps](long-running-step.md)
- [Delayed callback](delayed-callback.md)
- [Steps](../guide/steps.md#heartbeats)
