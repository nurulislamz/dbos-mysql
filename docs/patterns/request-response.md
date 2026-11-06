# Request and response

> Validate a request, change the workflow's state, and return a typed result to the caller in one call.

:::warning[Not supported yet]

Temporal calls this an Update. There is no call that does the validate, the write, and the reply together. The workaround below is three calls, and they can be observed in between.

:::

## Overview

A request-response update is a synchronous message. The caller sends a value, the workflow accepts or rejects it, the workflow state changes, and the caller receives the result before it continues.

Messages and events cover the pieces. They do not cover the single round trip.

## Problem

A caller wants to know, before it returns, that the account workflow accepted a debit and what the new balance is. A message alone does not return a value. An event alone does not carry the request.

## Solution

There is no Update API. Write the three steps yourself.

1. `DBOS.send` the request, with an idempotency key.
2. The workflow `recv`s it, decides, and `DBOS.set_event`s the answer under a key the caller knows.
3. The caller `DBOS.get_event`s that key.

```python
from yado import DBOS


def debit(account_id: str, request_id: str, amount: int) -> int:
    DBOS.send(
        account_id,
        {"id": request_id, "amount": amount},
        topic="debit",
        idempotency_key=request_id,
    )
    answer = DBOS.get_event(account_id, request_id, timeout_seconds=10)
    if answer is None:
        raise TimeoutError(request_id)
    return answer["balance"]
```

The workflow side:

```python
@DBOS.workflow()
def account() -> None:
    balance = 0
    while True:
        request = DBOS.recv("debit", timeout_seconds=3600)
        if request is None:
            continue
        balance -= request["amount"]
        DBOS.set_event(request["id"], {"balance": balance})
```

## When to use

Use this shape when the caller must see the workflow's decision before returning, and you can tolerate the gap between the three calls.

Use [signal with start](signal-with-start.md) when the caller does not need a result.

Use [early return](early-return.md) when the workflow is the one publishing a result the caller polls.

## Trade-offs

The caller can time out after the workflow has already applied the debit. The event remains stored, so a retry of `get_event` with the same key still sees the answer. A retry of `send` needs the same `idempotency_key`.

## Pitfalls

- Nothing rejects a request that the workflow has not read yet. Validation happens when the workflow reaches `recv`, which may be later than the caller's timeout.
- `get_event` returns `None` on timeout. `None` is also a value you can publish, so publish a dict and treat a missing event as "not yet".
- Event keys are per workflow. Include the request ID in the key so two debits do not overwrite each other.

## Related

- [Signal with start](signal-with-start.md)
- [Entity workflow](entity-workflow.md)
- [Messages and events](../guide/communication.md)
