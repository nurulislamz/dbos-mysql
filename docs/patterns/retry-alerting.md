# Alert when retries pile up

> Emit a metric from the step once the attempt count crosses a threshold, so someone sees a stuck call before the step finally fails.

:::note[Supported]

`DBOS.step_status.current_attempt` is the attempt number while the step is running. yado does not ship a metrics pipeline. Use the client your program already has.

:::

## Overview

Retry alerting is an ordinary metric, emitted from inside the step, keyed off the attempt count the engine already tracks. The page, the alert, and the on-call rotation stay in your existing monitoring. The workflow does not have to fail for the alert to fire.

## Problem

A step with eight attempts and a one-minute backoff fails half an hour after the dependency started returning errors. The workflow status is the first sign, and it arrives after the retries are over. The people who could fail over or rate-limit never saw the intermediate attempts.

## Solution

```python
from yado import DBOS


@DBOS.step(retries_allowed=True, max_attempts=8, interval_seconds=60)
def call() -> str:
    status = DBOS.step_status
    if status is not None and status.current_attempt >= 4:
        metrics.increment("call.retrying")
    return client.call()
```

`current_attempt` starts at 1. The metric fires on attempt 4 and on each later attempt. Put the threshold where an operator still has time to act before `max_attempts`.

The increment runs every time that attempt runs. A crash before the step saves will retry the attempt and can increment again. Treat the metric as at-least-once, the same way you treat the step.

## When to use

Use an in-step metric when you already alert on counters and you want attempt count to be one of them.

Poll workflow status from outside if you would rather not touch the step. `get_status()` shows a run that is still `PENDING`, which is a coarser signal than the attempt number.

## Trade-offs

The metric is only as durable as your metrics system. Losing one increment loses the alert for that attempt, not the workflow. Emitting on every attempt past the threshold, rather than only on the threshold, gives a later attempt a chance to deliver the signal.

## Pitfalls

- `DBOS.step_status` is `None` outside a step. The check above keeps a direct call in a test from crashing.
- Do not raise from the metrics call if you can help it. A metrics outage would become a failed attempt.
- This does not page by itself. Wire the counter to whatever you already use.

## Related

- [Fixed number of retries](fixed-retries.md)
- [Fast retries, then slow ones](fast-slow-retries.md)
- [Steps](../guide/steps.md#retries)
