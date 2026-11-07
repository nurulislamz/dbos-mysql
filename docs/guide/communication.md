# Messages and events

Workflows can talk to each other and to outside code. Messages go to one workflow. Events are values a workflow publishes for anyone to read. Each call has an `_async` version.

## Messages

Use a message to send a workflow a signal, such as "the payment came through".

```python
@yado.workflow()
def checkout(order_id: str) -> str:
    start_payment(order_id)
    status = yado.recv("payment", timeout_seconds=600)
    return "paid" if status == "ok" else "timed out"


# Anywhere, such as a webhook handler:
yado.send("checkout-123", "ok", topic="payment", idempotency_key=webhook_id)
```

| Call | Rules |
|---|---|
| `yado.send` | Works inside or outside a workflow. Raises `DBOSNonExistentWorkflowError` if the destination does not exist |
| `yado.recv` | Workflows only, and not inside a step. On timeout it returns `None` |

- Messages are FIFO within a destination and topic, and each one is read once.
- Messages are stored, so they wait for a workflow that is not at its `recv` yet, or is down.
- Inside a workflow, `send` and `recv` are saved as steps, so replay does not send twice or read a different message.
- Outside a workflow, `send` is not protected by replay. Pass an `idempotency_key` if the caller can retry.

## Events

Use an event to publish progress or a result that others may read while the workflow runs.

```python
@yado.workflow()
def report(report_id: str) -> None:
    yado.set_event("progress", "started")
    build(report_id)
    yado.set_event("progress", "done")


value = yado.get_event("report-1", "progress", timeout_seconds=30)
everything = yado.get_all_events("report-1")
```

| Call | Rules |
|---|---|
| `yado.set_event` | Workflows only. A later call with the same key replaces the value |
| `yado.get_event` | Works anywhere. On timeout it returns `None` |
| `yado.get_all_events` | Works anywhere. Returns a `dict` |

Older values are kept as revisions until the workflow is deleted. A `get_event` inside a workflow replays the value it first saw, even if the event has changed since.

## How fast it is

A write on the same engine wakes the waiter at once, in about 10 to 17 ms. A write from another process is seen by re-reading, within about 250 ms. See [How waiting works](../architecture.md#how-waiting-works).

> **Coming soon:** streams, for a live series of values from a workflow.
