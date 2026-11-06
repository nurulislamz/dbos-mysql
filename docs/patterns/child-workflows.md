# Child workflows

> Break a large workflow into smaller workflows. Each child has its own workflow ID, saved steps, and lifecycle.

:::note[Supported]

Call the child directly, or start it with `DBOS.start_workflow`. The child's ID is saved as a step of the parent, so replay attaches to the same child.

:::

## Overview

A child workflow is a workflow that another workflow starts. It has its own ID, its own steps, and its own result. The parent can wait for that result, or start the child and keep going.

A step runs code and saves one result. A child workflow orchestrates. Use a child when that piece of work should be tracked, cancelled, timed out, or reused on its own.

What you get:

- An ID you can look up in the [workflow UI](../production.md#see-your-workflows).
- A separate step list, so the parent does not store every inner call.
- A blocking call, or a start that returns immediately.
- A child that keeps running after the parent ends, until you cancel it.

## Problem

One workflow that performs every stage of an order stores every step in one list. Recovery replays all of them. An operator cannot open "just the refund", and a second parent cannot reuse that refund without copying it.

## Solution

Call the other workflow from the parent. The engine saves the child's ID on the parent. On replay it finds that child and does not start a second one. If you do not set an ID, the child ID is `<parent id>-<step number>`.

### Wait for the child

A direct call starts the child and blocks until it returns.

```python
from yado import DBOS


@DBOS.workflow()
def refund(order_id: str) -> str:
    return issue_refund(order_id)


@DBOS.workflow()
def cancel_order(order_id: str) -> str:
    receipt = refund(order_id)
    notify(order_id)
    return receipt
```

### Start the child and keep going

`start_workflow` returns as soon as the child row exists. `get_result` waits.

```python
@DBOS.workflow()
def cancel_order(order_id: str) -> str:
    handle = DBOS.start_workflow(refund, order_id)
    notify(order_id)
    return handle.get_result()
```

Async code uses `await DBOS.start_workflow_async(...)` and `await handle.get_result()`.

## When the parent ends

The child keeps running after the parent finishes. Cancelling the parent does not cancel the child unless you ask for that:

```python
DBOS.cancel_workflow(parent_id, cancel_children=True)
```

## When to use

Use a child workflow when:

- You want a separate workflow ID.
- The work may outlive the parent.
- More than one parent should run the same workflow.
- The inner work needs its own timeout. A child inherits the parent's deadline. Wrap the start in `SetWorkflowTimeout` to set another one, or `SetWorkflowTimeout(None)` to opt out.

Use a step when:

- The work is one call to an outside service.
- You do not need to look that call up as its own workflow.

## Trade-offs

A child is another workflow row and another lease. A step is cheaper for a single outside call. A parent that starts many children stores one step per start.

## Pitfalls

- A child without an explicit ID is tied to the parent's step number. Inserting a step ahead of it changes the ID for new runs. A run that already saved the ID keeps it.
- `cancel_children` defaults to false. A cancel of the parent leaves children running.
- A parent timeout is an absolute deadline, and children inherit it.

## Related

- [Parallel execution](parallel-execution.md)
- [Pick first](pick-first.md)
- [Fan-out](fan-out.md)
- [Workflows](../guide/workflows.md#child-workflows)
