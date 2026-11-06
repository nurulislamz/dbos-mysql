# An error that sets the next wait

> Wait as long as the error tells you to, then try the step again.

:::warning[Not supported yet]

`should_retry` can only say yes or no. It cannot set the next delay. Catch the error in the workflow, `DBOS.sleep` for the reported wait, and call the step again.

:::

## Overview

Some APIs fail with a `Retry-After` time. The next attempt should wait that long, not the step's configured backoff. The wait has to be durable, or a crash either retries too soon or forgets the instruction.

## Problem

A step backoff of one second, doubled each time, does not match "try again in 15 minutes" from a rate-limit response. Retrying sooner extends the limit. Retrying on the step's schedule also burns `max_attempts` on errors that were only asking for a pause.

## Solution

Turn retries off on the step. Let the workflow own the loop.

```python
from yado import DBOS


class RetryLater(Exception):
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds


@DBOS.step()
def call_api(order_id: str) -> str:
    response = client.post(order_id)
    if response.status_code == 429:
        raise RetryLater(float(response.headers["Retry-After"]))
    response.raise_for_status()
    return response.text


@DBOS.workflow()
def place(order_id: str) -> str:
    for _ in range(5):
        try:
            return call_api(order_id)
        except RetryLater as error:
            DBOS.sleep(error.seconds)
    return call_api(order_id)
```

`DBOS.sleep` stores an absolute wake time. Each failed attempt is a saved step, so replay does not call the API again for an attempt that already returned `RetryLater`.

The step has retries off, so `RetryLater` is saved as that attempt's outcome and the workflow's `except` runs. A step with `retries_allowed=True` would swallow the exception inside the step, where you cannot `DBOS.sleep`.

## When to use

Use a workflow loop when the wait comes from the error.

Use [fixed number of retries](fixed-retries.md) when the wait is the same for every failure of that step.

## Trade-offs

Each attempt is its own step, so a long series of rate limits grows the step list. The gain is that the wait is exactly what the service asked for, and it survives a restart.

## Pitfalls

- Sleep in the workflow, not in the step. A sleep inside a step is an ordinary sleep and is repeated if the step did not save.
- Bound the loop. A service that always returns `Retry-After` will keep the workflow open.
- The exception's `seconds` is saved with the step failure. Replay uses that saved outcome. It does not call the API again to ask for a new delay.

## Related

- [Errors that must not be retried](non-retryable.md)
- [Fast retries, then slow ones](fast-slow-retries.md)
- [Sleep, timeouts and schedules](../guide/time.md#durable-sleep)
