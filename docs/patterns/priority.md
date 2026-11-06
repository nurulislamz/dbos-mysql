# Priority

> Admit higher-priority workflows first when a queue has more work than it can start.

:::note[Supported]

`SetEnqueueOptions(priority=N)` around `enqueue`. A lower number starts first. Workflows with no priority start before workflows that have one. Priority does not interrupt a workflow that is already running.

:::

## Overview

Priority is an admission order. The queue still starts work one slot at a time, under the concurrency and rate limits. When several rows are eligible, the smaller priority number wins, and FIFO applies inside one number.

Running work keeps its slot. A priority-1 job that arrives while ten priority-9 jobs are running waits for a slot. It does not preempt them.

## Problem

A queue mixes password-reset emails with a nightly digest. FIFO runs the digest first if it was enqueued first, and the reset waits behind thousands of digest rows. Two queues split your concurrency budget and your operational picture.

## Solution

```python
from rust_dwos import DBOS, SetEnqueueOptions


mail = DBOS.register_queue("mail", global_concurrency=5)


def send_reset(user_id: str) -> None:
    with SetEnqueueOptions(priority=1):
        mail.enqueue(reset_email, user_id)


def send_digest(user_id: str) -> None:
    with SetEnqueueOptions(priority=10):
        mail.enqueue(digest_email, user_id)
```

Leave `priority` unset for work that should jump ahead of every numbered priority. Unset is admitted before `1`. Use that only for the class of work that must beat everything you have numbered.

## When to use

Use priority when one queue holds more than one class of work and a waiting urgent job should be chosen next.

Use a separate queue when the classes need different concurrency or a different rate limit.

## Trade-offs

A single queue with priorities is one limit to reason about. A flood of priority-1 work still starves priority-10. Fairness across tenants is a different control. See [Fairness](fairness.md).

## Pitfalls

- Priority is not preemption. A long low-priority workflow keeps running.
- Numbers are not a percentage. `1` and `2` do not share the queue evenly. `1` is fully preferred.
- The option is read at enqueue time. Set it in the `with` block that calls `enqueue`, not after the handle exists.

## Related

- [Fairness](fairness.md)
- [Rate limiting](rate-limiting.md)
- [Queues](../guide/queues.md#ordering-and-limits)
