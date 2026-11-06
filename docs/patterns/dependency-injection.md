# Pass clients into steps

> Build clients once, when the process starts. Let steps close over them. Keep them out of workflow code.

:::note[Supported]

A step is a normal function. It can use a client from the enclosing scope. Called outside a workflow, it runs as a plain function, so a test can call it without a database. Workflow code must stay deterministic, so the workflow does not construct clients and does not read them for control flow.

:::

## Overview

Activity dependency injection, in Temporal, exists so a workflow definition stays free of network clients and an activity can be tested on its own. The same split holds here. The workflow calls steps. The step talks to the network through a client the process created at startup.

Nothing has to be registered in a worker container. The closure is the injection.

## Problem

Constructing an HTTP client inside the workflow runs that constructor again on every replay, and a client that is not deterministic can change a branch. Constructing it inside the step works, and it pays setup cost on every attempt and makes the step harder to test, because the client is hidden.

## Solution

Create the client next to `DBOS.launch`, and close over it.

```python
from yado import DBOS

payments = PaymentsClient("https://payments.internal")


@DBOS.step()
def charge(order_id: str) -> str:
    return payments.charge(
        order_id,
        idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}",
    )


@DBOS.workflow()
def checkout(order_id: str) -> str:
    return charge(order_id)
```

A test calls `charge` directly. No workflow is running, so the step is a plain function and `payments` can be a fake assigned in the test module or passed another way you already use.

Prefer passing the client into a factory if you want the binding to be obvious:

```python
def make_charge(client: PaymentsClient):
    @DBOS.step()
    def charge(order_id: str) -> str:
        return client.charge(order_id, idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}")

    return charge
```

The workflow receives `charge` and calls it. It does not see `client`.

## When to use

Use a closed-over client for every network or database handle a step needs.

Construct the client inside the step only when it is cheap and the step owns its lifetime, such as a short-lived SDK object that cannot be shared.

Keep the workflow limited to arguments, step results, and the durable APIs (`sleep`, `recv`, `start_workflow`, and the rest).

## Trade-offs

A process-wide client is shared by concurrent steps. It has to be safe for that, which a connection pool usually is. A client with mutable per-call state is a bug under concurrency whether or not a workflow is involved.

## Pitfalls

- `DBOS.workflow_id` and `DBOS.step_id` are empty outside a workflow. A test that calls `charge` directly needs a fake key, or the client has to tolerate a missing one. Branch on `DBOS.workflow_id` if the same function is used both ways.
- Do not read the client inside the workflow to decide a branch. Read it in a step and branch on the saved result.
- Replacing the global client after workflows have started is a race. Build it before `DBOS.launch` and leave it.

## Related

- [Local steps](local-steps.md)
- [A queue per host](worker-queues.md)
- [Steps](../guide/steps.md#when-a-step-runs)
