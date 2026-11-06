# Fairness

> Cap each tenant so a burst from one of them cannot take every slot.

:::note[Supported]

Set `partition_concurrency` on the queue and pass `queue_partition_key` in `SetEnqueueOptions`. The key is the tenant, the user, or whatever unit should be kept from starving the others.

:::

## Overview

Fairness here is a per-key concurrency cap. The queue still has a global cap if you set `global_concurrency`. Inside that, each partition key may occupy at most `partition_concurrency` running workflows. A partition that is at its cap is skipped, and another partition can be admitted.

Rows with no partition key do not start while partition limits are active.

## Problem

One tenant enqueues 10,000 sync jobs. `global_concurrency=20` lets that tenant occupy all 20 slots. Every other tenant waits until the burst drains. A process-local round robin disappears on restart and disagrees with the other copies of the program.

## Solution

```python
from yado import DBOS, SetEnqueueOptions


queue = DBOS.register_queue(
    "api",
    global_concurrency=20,
    partition_concurrency=2,
    limiter={"limit": 5, "period": 1.0},
)


def submit(tenant: str, payload: str) -> None:
    with SetEnqueueOptions(queue_partition_key=tenant, priority=1):
        queue.enqueue(call_api, tenant, payload)
```

Twenty workflows can run. Each tenant can hold two of those. A tenant with a backlog waits while other tenants use the rest of the slots.

`partition_limiter={"limit": N, "period": seconds}` is the same idea applied to start rate instead of concurrency.

## When to use

Use a partition cap when several tenants share a queue and one of them can burst.

Use [priority](priority.md) when the distinction is the kind of work, not who submitted it.

Use [rate limiting](rate-limiting.md) without a partition when the downstream limit is global and tenants do not need a reserved share.

## Trade-offs

A small `partition_concurrency` protects neighbors and leaves slots idle when only one tenant has work and that tenant is already at its cap. Raise the per-tenant cap if a single busy tenant should be allowed to use more of an otherwise idle queue. The global cap is still there.

## Pitfalls

- Every enqueue needs a partition key while the limit is on. A missing key waits behind the partitioned rows.
- The cap is per key, as a string. `tenant` and `tenant ` are different keys. Normalize the key before enqueue.
- This limits workflow starts for that key. A workflow that then loops over many API calls can still monopolize the downstream service. Keep one unit of downstream work per queued workflow.

## Related

- [Rate limiting](rate-limiting.md)
- [Priority](priority.md)
- [Queues](../guide/queues.md#ordering-and-limits)
