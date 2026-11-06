# Event accumulator

> Collect messages from many senders, and close the batch after a quiet period.

:::note[Supported]

Each `DBOS.send` uses its own idempotency key. The workflow loops on `DBOS.recv` with a timeout. `None` means the window closed.

:::

## Overview

An event accumulator is a workflow that turns a stream into a batch. Producers send whenever they have an event. The workflow appends each one and starts the quiet-period timer again. When the timer fires with nothing new, it processes the batch.

Messages are stored, so a producer can send before the workflow is waiting, and a crash does not lose an unread message.

## Problem

A page of analytics events should be written as one batch, not one row per event. A fixed sleep either cuts a burst in half or waits too long after the burst already ended. A process-local buffer disappears on a crash.

## Solution

Send each event with an idempotency key. Receive in a loop. A timeout starts the next stage.

```python
from rust_dwos import DBOS


@DBOS.workflow()
def collect(page_id: str) -> list[str]:
    batch: list[str] = []
    while (item := DBOS.recv("page", timeout_seconds=30)) is not None:
        batch.append(item)
    write_batch(page_id, batch)
    return batch
```

A producer, from a webhook or another workflow:

```python
DBOS.send(page_id, event_id, topic="page", idempotency_key=event_id)
```

The timeout is the quiet period. Every message that arrives before it elapses is part of the batch and resets the wait, because each `recv` starts a new timeout only after a message has been taken.

Start the collector with [signal with start](signal-with-start.md) if the first event should create it.

## When to use

Use an accumulator when:

- Many senders contribute to one batch.
- The batch should close after the stream goes quiet, not at a fixed clock time.
- Duplicate deliveries must collapse. The idempotency key does that for `send`.

Use [polling](polling.md) when you are pulling from an outside system that does not call you.

## Trade-offs

A short timeout closes the batch quickly and can split a slow burst. A long timeout keeps the workflow open and adds latency before `write_batch`. The timeout is saved with the wait, so a restart does not extend it.

## Pitfalls

- `recv` returns `None` on timeout and also when you use that as a value. Send dicts or strings, and treat `None` only as "window closed".
- Messages are FIFO per workflow and topic. Two topics do not share one queue.
- A message that arrives after the loop has moved on stays unread. Start another collector, or keep looping for the life of the entity.

## Related

- [Signal with start](signal-with-start.md)
- [Entity workflow](entity-workflow.md)
- [Messages and events](../guide/communication.md)
