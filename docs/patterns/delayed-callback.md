# Delayed callback

> Sleep, call an outside system, and finish when that system sends the result back. The workflow ID is the callback reference.

:::note[Supported]

`DBOS.sleep` stores the wake time. `DBOS.recv` waits for the callback. Pass `DBOS.workflow_id` to the other system so it knows where to `send`.

:::

## Overview

A delayed callback covers both directions. Outbound, the workflow waits durably and then calls. Inbound, the other system's webhook is a message, not a thread blocked in your process. The workflow completes when the message arrives, whenever that is.

## Problem

A payment provider captures an hour from now and posts the result to a URL. Holding an HTTP request open for the capture is not possible. A row in a table that a cron scans is a second scheduler you have to make crash-safe. If the webhook arrives before your handler is waiting, a process-local future drops it.

## Solution

Sleep, submit with the workflow ID as the callback token, then receive.

```python
from rust_dwos import DBOS


@DBOS.step()
def submit(order_id: str, callback: str) -> None:
    payments.capture_later(order_id, callback_token=callback)


@DBOS.workflow()
def charge_later(order_id: str) -> str:
    DBOS.sleep(3600)
    submit(order_id, callback=DBOS.workflow_id)
    result = DBOS.recv("provider", timeout_seconds=86400)
    if result is None:
        return "timed out"
    return result
```

The provider's webhook handler sends into the workflow. It does not need to be a workflow itself.

```python
def provider_webhook(token: str, body: dict, delivery_id: str) -> None:
    DBOS.send(token, body, topic="provider", idempotency_key=delivery_id)
```

The message is stored if it arrives before `recv`. A retry of the webhook with the same `idempotency_key` does not add a second message.

## When to use

Use a delayed callback when:

- The other system can call you back and you can give it an ID.
- You may need to wait before the outbound call.
- The inbound call must be safe if it is delivered twice or early.

Use [polling](polling.md) when the other system cannot call you.

## Trade-offs

The workflow row stays open until `recv` returns or the timeout fires. That is one lease and one waiting run, which is cheaper than a poll loop and easier to inspect than a side table.

## Pitfalls

- `recv` always takes a timeout here. Decide what "no callback" means, as `charge_later` does.
- The webhook must send to the workflow ID, on the topic the workflow receives.
- `DBOS.workflow_id` inside a step is the workflow's ID. Inside the step it is stable. Do not invent a second ID for the callback.

## Related

- [Delayed start](delayed-start.md)
- [Approval](approval.md)
- [Messages and events](../guide/communication.md)
