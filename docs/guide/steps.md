# Steps

A step is a function whose result is saved. Once saved, it never runs again for that workflow.

```python
@DBOS.step()
def fetch_price(sku: str) -> int:
    return requests.get(f"https://example.com/price/{sku}").json()["price"]
```

Make a step of anything that talks to the outside world or can change between runs.

## When a step runs

| Called from | What happens |
|---|---|
| A workflow | Saved and replayed as normal |
| Another step | Runs as a plain function. Its result is part of the outer step |
| Outside any workflow | Runs as a plain function. Nothing is saved |

## At least once

A crash after a side effect and before the save runs that step again. Nothing inside the engine can prevent this. Use an idempotency key at the service you call:

```python
@DBOS.step()
def charge(order_id: str) -> str:
    return payments.charge(order_id, idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}")
```

The key is the same on every attempt and every recovery.

## Retries

Retries are off by default.

```python
@DBOS.step(retries_allowed=True, max_attempts=5, interval_seconds=1.0, backoff_rate=2.0)
def flaky() -> str:
    ...
```

| Option | Default | Meaning |
|---|---|---|
| `retries_allowed` | `False` | Turn retries on |
| `max_attempts` | `3` | Total attempts |
| `interval_seconds` | `1.0` | Wait before the first retry |
| `backoff_rate` | `2.0` | Multiplier for each later wait |
| `should_retry` | `None` | Function that takes the exception and returns whether to retry. An async step may pass an async one |

When retries run out, `DBOSMaxStepRetriesExceeded` is saved and raised. Its `.errors` holds each attempt's exception. Inside a step, `DBOS.step_status` gives the current attempt and the maximum.

## Timeouts

`timeout_seconds` limits each attempt. A timed-out attempt raises `DBOSStepTimeoutError` and counts as a failed attempt, without cancelling the workflow.

```python
@DBOS.step(timeout_seconds=10, retries_allowed=True)
async def call_api() -> str:
    ...
```

An async body is cancelled. Python cannot stop a thread, so a sync body keeps running until it returns, and it can overlap the next attempt. Its result is dropped.

## Heartbeats

For long steps, set `heartbeat_timeout_seconds` and call `DBOS.heartbeat()` as work goes on. A step that goes quiet for too long times out. A sync body that calls `DBOS.heartbeat()` after its timeout gets the error there and stops.

```python
@DBOS.step(heartbeat_timeout_seconds=30)
def process(rows: list[str]) -> None:
    for row in rows:
        handle(row)
        DBOS.heartbeat()
```

This is separate from the workflow's lease, which renews by itself.

## Very long steps

A run that goes past `max_run_seconds` (default 1 hour) loses its lease. See [Limits](../architecture.md#how-crash-recovery-works). Raise this setting if one step can take longer.

> **Coming soon:** `DBOS.run_step`, which runs a plain function as a step without a decorator.
