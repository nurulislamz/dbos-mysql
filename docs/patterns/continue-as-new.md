# Continue-as-new

> Finish the current run and start the same workflow again under the same ID, with an empty step list and the state you pass forward.

:::warning[Not supported yet]

There is no call that clears the step list and keeps the workflow ID. A long loop keeps every step, and recovery replays all of them.

:::

## Overview

Continue-as-new is how a long-lived workflow sheds history. The current run completes. A new run starts with the same workflow ID, a new step list, and whatever state the previous run handed it. Callers that hold the ID keep using it. In-flight messages for that ID still arrive.

## Problem

An [entity workflow](entity-workflow.md) that handles one command a minute stores hundreds of thousands of steps a year. After a crash, replay walks every one of them before it can take the next command. The entity is correct and too slow to recover.

## Solution

There is no continue-as-new call. The workaround is to start the successor under a new ID as the last thing the current run does, and to tell callers the new ID.

```python
from yado import DBOS, SetWorkflowID


@DBOS.workflow()
def account(account_id: str, balance: int, generation: int) -> int:
    seen = 0
    while seen < 1000:
        command = DBOS.recv("command", timeout_seconds=3600)
        if command is None:
            continue
        balance += command["amount"]
        DBOS.set_event("balance", balance)
        seen += 1
    nxt = f"{account_id}-{generation + 1}"
    with SetWorkflowID(nxt):
        DBOS.start_workflow(account, account_id, balance, generation + 1)
    DBOS.set_event("next_id", nxt)
    return balance
```

Callers that still have the old ID read `next_id` and follow it. Messages sent to the old ID after `recv` has stopped are not delivered to the new run. Drain, or send to the ID you just published.

## When to use

You want this when one workflow ID must live longer than you are willing to replay.

Until the call exists, prefer a workflow that finishes, or accept the replay cost and keep the entity loop.

## Trade-offs

A new ID is a new workflow. Status, the [UI](../production.md#see-your-workflows), and anyone holding the old ID see a completed run plus a different run. That is the gap continue-as-new is meant to close.

## Pitfalls

- Do not start the next run and then keep receiving on the old one. Commands split across the two.
- Pass the state you still need as arguments. The new run cannot read the old run's local variables.
- `set_event` on the old run is not visible on the new run. Publish `balance` again at the start of the successor if readers look at the new ID.

## Related

- [Entity workflow](entity-workflow.md)
- [Batch iterator](batch-iterator.md)
- [Workflows](../guide/workflows.md)
