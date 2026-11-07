# Early return

> Return the fast part of the work to the caller, and keep running the rest after the caller has its answer.

:::note[Supported]

Start the workflow in the background. Publish the early result with `yado.set_event`. The caller reads it with `yado.get_event` and returns. The workflow keeps going.

:::

## Overview

Early return splits one workflow into a phase the caller waits for and a phase it does not. Both phases are the same run. The early result is an event, so a caller that times out can read it later, and a crash after the event is saved does not repeat the first phase.

## Problem

Building a report validates the request and assigns an ID in milliseconds, then spends a minute rendering. The HTTP handler that waits for the render holds a worker and a connection for the whole minute. Starting a background thread loses the job on a crash and gives the caller no durable ID.

## Solution

Start the workflow, wait only for the event that says the first phase is done, and return that to the caller.

```python
import yado


@yado.workflow()
def build(report_id: str) -> str:
    summary = validate(report_id)
    yado.set_event("init", summary)
    render(report_id)
    yado.set_event("done", True)
    return summary


def request_report(report_id: str) -> str:
    handle = yado.start_workflow(build, report_id)
    ready = yado.get_event(handle.workflow_id, "init", timeout_seconds=10)
    if ready is None:
        raise TimeoutError(report_id)
    return ready
```

`build` is still running when `request_report` returns. The caller can poll `done`, or `handle.get_result()` later, to see the finished report.

## When to use

Use early return when:

- The caller needs a result before the workflow is finished.
- The rest of the work must still be durable.
- You can name the event that marks the end of the first phase.

Use `handle.get_result()` when the caller needs the final result.

## Trade-offs

The caller and the workflow agree on an event name. That is the contract. There is no typed return from the first phase separate from the workflow's real return value. The workflow's return value is still whatever `build` returns at the end.

## Pitfalls

- `get_event` returns `None` on timeout. Publish a value, and treat a missing event as "not ready".
- Set the event after the steps that make it true. Setting it first and then failing validation tells the caller the report exists when it does not.
- The caller that only waits for `init` will not see a later failure unless it also reads status or the `done` event.

## Related

- [Early return with local steps](early-return-local.md)
- [Eager start](eager-start.md)
- [Messages and events](../guide/communication.md#events)
