# A time budget across retries

> Bound the total time spent retrying, no matter how many attempts fit in that time.

:::warning[Not supported yet]

`timeout_seconds` limits one attempt. It does not cap the sum of the attempts. Put the step in a child workflow started under `SetWorkflowTimeout`.

:::

## Overview

A wall-time budget is a business SLA. The step may retry as many times as it likes, and the workflow must still give up at a deadline. The deadline covers the attempts and the waits between them.

## Problem

`max_attempts=100` with a growing backoff can run for hours. `timeout_seconds=10` only stops a single hung call. Neither one expresses "this charge has two minutes, total".

## Solution

Start a child workflow with a workflow timeout. The timeout is stored when the child starts. When it passes, the child becomes `CANCELLED`.

```python
import yado
from yado import SetWorkflowTimeout


@yado.workflow()
def charge_once(order_id: str) -> str:
    return charge(order_id)


@yado.workflow()
def checkout(order_id: str) -> str:
    with SetWorkflowTimeout(120):
        handle = yado.start_workflow(charge_once, order_id)
    return handle.get_result()
```

`charge` keeps its own `retries_allowed` and `max_attempts`. The child timeout is what ends the whole effort. A waiter on a cancelled workflow gets `DBOSAwaitedWorkflowCancelledError`.

The clock starts when the child starts running, not while it sits in a queue. See [Workflow timeouts](../guide/time.md#workflow-timeouts).

## When to use

Use a time budget when the rule is "done by this time", including retries.

Use [fixed number of retries](fixed-retries.md) when the rule is "at most N calls".

## Trade-offs

The budget is a second workflow. That gives you an ID, a status, and a deadline you can see in the UI. It also means cancellation, not a typed "retries exhausted" error, is how the budget shows up.

## Pitfalls

- A parent timeout is inherited as an absolute deadline. A child that should have its own budget needs its own `SetWorkflowTimeout`. `SetWorkflowTimeout(None)` opts out.
- Cancelling the child stops the body. A [saga](saga.md) `except` branch does not run on cancel.
- Resuming a timed-out workflow does not grant a new budget by itself.

## Related

- [Fixed number of retries](fixed-retries.md)
- [Child workflows](child-workflows.md)
- [Sleep, timeouts and schedules](../guide/time.md#workflow-timeouts)
