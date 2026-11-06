# Fast retries, then slow ones

> Retry quickly at first. Once those attempts are spent, wait longer between tries so the workflow can outlast an outage without giving up.

:::note[Supported]

Use a step with a short interval and a small `max_attempts`. Catch `DBOSMaxStepRetriesExceeded`, then loop: `DBOS.sleep` for the slow interval and call a step that does not retry.

:::

## Overview

Two speeds cover two failures. A blip clears in a few seconds, so the first phase retries quickly and finishes. An outage that lasts hours should not exhaust a fixed count and fail the workflow, and it should not hammer the service once a second for the whole outage.

The fast phase is the step retry policy. The slow phase is a workflow loop, because only the workflow can sleep durably between unbounded attempts.

## Problem

`max_attempts=5` fails a checkout during a twenty-minute payment outage. `max_attempts=10000` with a one-second interval holds a tight loop on a service that is already down. You want the first few tries to be eager, and the rest to be patient.

## Solution

```python
from yado import DBOS, DBOSMaxStepRetriesExceeded


@DBOS.step(retries_allowed=True, max_attempts=10, interval_seconds=1, backoff_rate=1)
def fast(order_id: str) -> str:
    return payments.charge(order_id, idempotency_key=f"{DBOS.workflow_id}-fast")


@DBOS.step()
def once(order_id: str) -> str:
    return payments.charge(order_id, idempotency_key=f"{DBOS.workflow_id}-slow-{DBOS.step_id}")


@DBOS.workflow()
def checkout(order_id: str) -> str:
    try:
        return fast(order_id)
    except DBOSMaxStepRetriesExceeded:
        while True:
            DBOS.sleep(300)
            try:
                return once(order_id)
            except Exception:
                continue
```

Give the slow phase a way to stop: a max number of sleeps, or a [workflow timeout](wall-time-retries.md) on a child that contains this loop.

## When to use

Use two speeds when a short retry count is right for the common case and a long outage should keep the workflow alive at a slower pace.

Use only the fast step when the workflow should fail and let a person or a caller decide.

## Trade-offs

The slow loop stores a sleep and a step per attempt. That is the cost of being able to resume at the last wait. The idempotency key must change per slow attempt if a failed attempt did not take effect, and stay stable if you are not sure. The example uses `step_id`, which is stable for that attempt and new for the next one.

## Pitfalls

- `backoff_rate=1` keeps the fast interval flat. The default of `2` doubles it, which may already be the slow phase you wanted.
- A `while True` with no timeout never completes. Put a bound on it.
- The fast step's failure is saved. Replay does not retry those ten attempts. It goes to the slow loop.

## Related

- [Fixed number of retries](fixed-retries.md)
- [An error that sets the next wait](delayed-retry.md)
- [Alert when retries pile up](retry-alerting.md)
