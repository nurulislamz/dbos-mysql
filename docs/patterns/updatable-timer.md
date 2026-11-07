# Updatable timer

> Wait until a deadline, and let a message move that deadline earlier, later, or clear it.

:::note[Supported]

`yado.recv` with a timeout waits until a message arrives or the deadline passes. The deadline is stored, so a restart does not reset the wait.

:::

## Overview

An updatable timer is a sleep that the outside world can still change. The workflow waits for whichever comes first: the current deadline, or a message that names a new one. On a message, it waits again with the new remaining time.

## Problem

A trial ends in fourteen days. Support can extend it. A normal `yado.sleep(14 days)` is durable, and it is also fixed. Cancelling and starting a new workflow loses the rest of the state. Sleeping in a loop of one-minute polls wakes thousands of times for no change.

## Solution

Wait on the message topic with a timeout equal to the time left. A message is the update. `None` means the deadline passed.

```python
import yado


@yado.workflow()
def trial(user_id: str) -> str:
    left = 14 * 24 * 3600
    while True:
        update = yado.recv("deadline", timeout_seconds=left)
        if update is None:
            return expire(user_id)
        left = update["seconds"]
```

The message carries the new duration, measured from the moment it arrives. `recv` stores the deadline as an absolute time, so a restart finishes the current wait instead of starting it over. Do not read the clock in the workflow. A duration that must be computed from the wall clock belongs in a step.

Send an update with [signal with start](signal-with-start.md) or `yado.send` if the workflow is already running.

## When to use

Use an updatable timer when:

- A person or another service may shorten, extend, or replace a deadline.
- The wait must survive a restart without starting over.
- You would otherwise poll.

Use `yado.sleep` when the duration is fixed. See [Sleep](../guide/time.md#durable-sleep).

## Trade-offs

Each wait is a saved step. A deadline that is updated often adds one step per update. That is usually far smaller than a poll loop.

## Pitfalls

- `timeout_seconds` is how long this wait may last, not a timestamp. Convert an absolute deadline to a duration before the call.
- `None` means the wait ended with no message. Do not send `None` as the update payload.
- A message that arrives after you have already moved on stays in the inbox until some later `recv` on that topic.

## Related

- [Entity workflow](entity-workflow.md)
- [Approval](approval.md)
- [Sleep, timeouts and schedules](../guide/time.md)
