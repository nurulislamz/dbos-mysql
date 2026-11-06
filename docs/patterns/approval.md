# Approval

> Stop the workflow until a person approves, rejects, or the wait times out.

:::note[Supported]

`DBOS.recv` waits for the decision. Your UI or webhook calls `DBOS.send` with the approver and the answer. A timeout returns `None`.

:::

## Overview

An approval is a durable wait for a person. The workflow records the request, publishes enough state for a UI to render, and blocks in `recv`. The person's action is a message. The workflow then continues with the decision, or escalates if nobody answered in time.

The wait is an absolute deadline. Restarting the process does not start the day over.

## Problem

A refund above a limit needs a manager. An email with a link, plus a row you hope is still there tomorrow, loses the decision if two servers handle the click or the process restarts before you write it down. The workflow that asked for the approval has to be the thing that records the answer.

## Solution

Receive on a topic. Treat timeout as its own branch.

```python
from rust_dwos import DBOS


@DBOS.workflow()
def refund(order_id: str, amount: int) -> str:
    DBOS.set_event("pending", {"order_id": order_id, "amount": amount})
    decision = DBOS.recv("approval", timeout_seconds=86400)
    if decision is None:
        escalate(order_id)
        return "escalated"
    record(decision["approver"], decision["ok"])
    if not decision["ok"]:
        return "rejected"
    issue_refund(order_id, amount)
    return "refunded"
```

The approval endpoint sends the decision. Use an idempotency key so a double click is one message.

```python
DBOS.send(
    workflow_id,
    {"approver": user, "ok": True},
    topic="approval",
    idempotency_key=f"{workflow_id}:{user}",
)
```

`DBOS.get_event(workflow_id, "pending")` is what the UI reads to show the request.

## When to use

Use an approval when a person must decide before the workflow continues, and the decision has to survive restarts.

Use an [updatable timer](updatable-timer.md) when the person is changing a deadline rather than accepting or rejecting.

## Trade-offs

The workflow occupies a run for as long as you are willing to wait. That is intentional: the run is the record. A timeout is how you stop waiting. Escalation can be another approval, a child workflow, or a step that files a ticket.

## Pitfalls

- A decision that arrives after the timeout stays unread. If you escalate into another `recv`, read it there, or it sits in the inbox.
- Do not put the business side effect in the webhook handler. The workflow should be the one that refunds, so a replay does not refund twice and a crash does not skip it.
- `set_event` replaces the previous value of that name. Use a new name if you also need the decision recorded for readers. The message itself is consumed by `recv`.

## Related

- [Delayed callback](delayed-callback.md)
- [Updatable timer](updatable-timer.md)
- [Resume after a correction](resumable.md)
