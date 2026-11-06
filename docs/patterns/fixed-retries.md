# Fixed number of retries

> Cap how many times a step runs, including the first try.

:::note[Supported]

`@DBOS.step(retries_allowed=True, max_attempts=..., interval_seconds=..., backoff_rate=...)`. Retries are off unless `retries_allowed` is true.

:::

## Overview

A fixed count stops a step from calling a paid or rate-limited service forever. The engine waits `interval_seconds` before the second attempt, and multiplies that wait by `backoff_rate` for each later attempt. The last failure is saved, so replay does not spend the attempts again.

## Problem

A fraud-check API bills every call. A bug that always returns 500 will retry until someone notices, if the retry policy is "forever". The workflow stays open, the bill grows, and the failure is not yet a result the workflow can branch on.

## Solution

Turn retries on and set the count.

```python
from yado import DBOS


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=1, backoff_rate=2)
def charge(order_id: str) -> str:
    return payments.charge(
        order_id,
        idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}",
    )
```

`max_attempts` is the total number of tries. Three means one try and two retries. The waits are 1 second, then 2 seconds.

When the count runs out, the step raises `DBOSMaxStepRetriesExceeded`. That exception is saved. Its `.errors` lists each attempt's exception. The workflow can catch it.

```python
from yado import DBOSMaxStepRetriesExceeded


@DBOS.workflow()
def checkout(order_id: str) -> str:
    try:
        return charge(order_id)
    except DBOSMaxStepRetriesExceeded:
        return "charge failed"
```

## When to use

Use a fixed count when each attempt costs money, quota, or a side effect you do not want to repeat without a bound.

Leave retries off when the step should fail into the workflow on the first error so the workflow can decide.

## Trade-offs

A small count fails fast and gives up on a short outage. A large count rides out the outage and delays the failure. The backoff spaces the calls so a down service is not hit at a fixed rate.

## Pitfalls

- A crash during the retries starts the count again. Attempts that were not saved are not remembered. Keep the side effect idempotent.
- The default is no retries. `max_attempts` does nothing while `retries_allowed` is false.
- `interval_seconds` and `backoff_rate` default to `1.0` and `2.0` when you do turn retries on. Set them if those defaults are too slow or too fast.

## Related

- [Errors that must not be retried](non-retryable.md)
- [A time budget across retries](wall-time-retries.md)
- [Steps](../guide/steps.md#retries)
