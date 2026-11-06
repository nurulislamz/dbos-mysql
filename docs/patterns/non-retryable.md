# Errors that must not be retried

> Fail immediately on an error that will not succeed if you call it again.

:::note[Supported]

Pass `should_retry` to `@DBOS.step`. It receives the exception and returns whether this attempt should be retried. An async step may pass an async predicate.

:::

## Overview

Some failures are permanent. A validation error, an unknown account, or a 404 will fail the same way on every retry. Marking them non-retryable spends one attempt, saves the failure, and lets the workflow deal with it.

Other failures stay retryable: a timeout, a 503, a dropped connection.

## Problem

A step with `max_attempts=5` will submit the same invalid payload five times. If the service writes an error row per call, you have five error rows and a workflow that waited out the backoff for an answer you had on the first response.

## Solution

Retry only the errors you believe are transient.

```python
from rust_dwos import DBOS


class ValidationError(Exception):
    pass


@DBOS.step(
    retries_allowed=True,
    max_attempts=5,
    should_retry=lambda error: not isinstance(error, ValidationError),
)
def parse(raw: str) -> dict:
    return schema.load(raw)
```

`schema.load` raises `ValidationError` for a bad payload. That attempt fails the step at once. A `ConnectionError` is retried, up to five tries.

The predicate runs in the workflow's process. Keep it pure. It sees the exception object, not a serialized name from another language.

## When to use

Use `should_retry` when the service distinguishes permanent errors from transient ones and you can see that distinction on the exception.

Leave it unset when every failure is worth another attempt.

## Trade-offs

A predicate that is too strict turns an outage into a permanent failure. A predicate that is too loose retries bugs. Prefer retrying a small set of known transient types if you are unsure, and let the rest fail.

## Pitfalls

- `should_retry` does not by itself enable retries. Set `retries_allowed=True`.
- The exception type has to survive to the predicate. Wrapping it in a different type hides it.
- A non-retryable failure is still saved. The next recovery of this workflow does not try the step again.

## Related

- [Fixed number of retries](fixed-retries.md)
- [An error that sets the next wait](delayed-retry.md)
- [Steps](../guide/steps.md#retries)
