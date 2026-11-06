# Early return with local steps

> Do the first phase as in-process steps, publish that result, and let the caller return while later work continues.

:::note[Supported]

This is [early return](early-return.md) together with [local steps](local-steps.md). The first phase never leaves the process that started the workflow. There is no extra option to turn that on.

:::

## Overview

The caller's first response should come from work that is already in this process: validate, write the early event, return. Heavier work stays in the same workflow and runs after the caller has gone. Both phases are durable, because both are steps.

Temporal splits this so the first phase can be local activities and skip the server. Here the first phase is local because every step is.

## Problem

An API should return "accepted" and an ID quickly, then send email and update a warehouse. Waiting for the email holds the request. Doing the email in a background thread loses it on a crash. Routing the validation through a queue adds a hop before you can even say "accepted".

## Solution

Start the workflow in the caller. The first steps run in that process. `set_event` marks the phase the HTTP handler is allowed to return.

```python
from rust_dwos import DBOS


@DBOS.step()
def validate(order_id: str) -> dict:
    return {"order_id": order_id, "ok": True}


@DBOS.workflow()
def accept(order_id: str) -> str:
    summary = validate(order_id)
    DBOS.set_event("init", summary)
    send_email(order_id)
    notify_warehouse(order_id)
    return order_id


def post_order(order_id: str) -> dict:
    handle = DBOS.start_workflow(accept, order_id)
    ready = DBOS.get_event(handle.workflow_id, "init", timeout_seconds=5)
    if ready is None:
        raise TimeoutError(order_id)
    return ready
```

`validate` runs in the process that called `start_workflow`. `send_email` does too, unless you enqueue the rest. The handler does not wait for the email, because it only waits for the `init` event.

## When to use

Use this when the response the client needs is available after a few steps, and the remaining steps must still be durable.

Use a queue for the whole workflow when the caller should not run the body at all. The caller then waits on the event while another process runs the steps, which is early return without the "local" part.

## Trade-offs

The process that served the HTTP request is also the process running the workflow, until that run finishes or hits `max_run_seconds`. A slow `send_email` occupies that process. If the first phase is the only work you want on the web process, enqueue the later work as a child on a queue the workers consume.

## Pitfalls

- `get_event` timing out does not mean the workflow failed. The event may appear a moment later. The workflow ID is the handle the client can poll.
- Set `init` only after the steps that make the summary true.
- A step in the first phase that talks to the network still blocks the caller. Keep that phase to the work you are willing to put on the request path.

## Related

- [Early return](early-return.md)
- [Local steps](local-steps.md)
- [Eager start](eager-start.md)
