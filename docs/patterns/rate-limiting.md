# Rate limiting

> Cap how often workflows start, so a downstream service sees a steady rate.

:::note[Supported]

`DBOS.register_queue(..., limiter={"limit": N, "period": seconds})`. The cap is on starts of workflows from that queue, across every process. It is not a cap on steps inside a running workflow.

:::

## Overview

A rate limit spaces admissions. Concurrency (`global_concurrency`) caps how many run at once. A limiter caps how many are allowed to start in each period, even if concurrency slots are free. Both can be set on the same queue.

The count is enforced when the queue admits work. A crash does not reset it in memory, because the decision is in the database.

## Problem

A partner API allows five calls per second. `global_concurrency=5` still lets all five finish and five more start inside the same second, if each call is fast. The partner then returns 429, and the retries make the next second worse.

## Solution

```python
from rust_dwos import DBOS, SetEnqueueOptions


queue = DBOS.register_queue(
    "api",
    limiter={"limit": 5, "period": 1.0},
)

with SetEnqueueOptions(priority=1):
    queue.enqueue(call_api, tenant_id)
```

`call_api` is a workflow whose body makes the partner call in a step. Five of those workflows may start each second. Further enqueued workflows wait for the next period and for a concurrency slot, if you set one.

```python
queue = DBOS.register_queue(
    "api",
    global_concurrency=5,
    limiter={"limit": 5, "period": 1.0},
)
```

## When to use

Use a limiter when the constraint is "how often", in starts per second or per minute.

Use `global_concurrency` when the constraint is "how many at once".

Use both when the partner has a rate limit and you also have a finite number of workers.

## Trade-offs

The limiter counts workflow starts. A workflow that then calls the partner ten times in a loop spends ten calls against a budget of five starts. Put one partner call in the queued workflow, or the limit does not match the bill.

## Pitfalls

- Steps are not limited. A fan-out inside `call_api` bypasses the limiter.
- The queue runs only where it was registered. A limiter with no registered process admits nothing.
- Changing the limiter requires `register_queue` again on a process that is allowed to update it. `on_conflict` controls whether a later register overwrites the stored limit.

## Related

- [Fairness](fairness.md)
- [Priority](priority.md)
- [Sliding window](sliding-window.md)
- [Queues](../guide/queues.md#ordering-and-limits)
