# Entity workflow

> One workflow per business entity. The entity ID is the workflow ID. Commands arrive as messages, and the current state is an event anyone can read.

:::note[Supported]

Use `SetWorkflowID` with the entity ID, then `yado.recv` and `yado.set_event`. A very long life stores every command, and recovery replays them all.

:::

## Overview

An entity workflow is the source of truth for one account, device, subscription, or order. It starts once, then loops. Each command is a message. Each state you want the outside world to see is an event.

Because the workflow ID is the entity ID, a second start attaches to the running one instead of creating another.

## Problem

An account balance updated from many services races. Two handlers read the old balance and both write. Locking in the application process does not survive a crash, and it does not order commands that arrived while the process was down.

## Solution

Own the entity with one workflow. Apply commands one at a time.

```python
import yado
from yado import SetWorkflowID


@yado.workflow()
def account(account_id: str) -> None:
    balance = 0
    while True:
        command = yado.recv("command", timeout_seconds=3600)
        if command is None:
            continue
        balance += command["amount"]
        yado.set_event("balance", balance)


def open_account(account_id: str) -> None:
    with SetWorkflowID(account_id):
        yado.start_workflow(account, account_id)
```

Send a command from anywhere:

```python
yado.send(account_id, {"amount": 10}, topic="command", idempotency_key=command_id)
```

Read the latest balance without waiting for the workflow to finish:

```python
yado.get_event(account_id, "balance", timeout_seconds=5)
```

`recv` returns `None` when the hour passes with no command. The loop waits again. The workflow stays the same run, so messages that arrive during the gap are still queued.

## When to use

Use an entity workflow when:

- One real-world object should have one ordered history.
- Callers send commands and read state, and they are not the ones running the rules.
- The rules must survive a crash in the middle of a command.

Use a normal workflow that runs and finishes when the work is a single job, not a life cycle.

## Trade-offs

The entity is easy to reason about because commands are applied one by one. The step list grows with every command and every wait. Recovery replays the whole list. A busy account gets slower to recover over time. [Continue-as-new](continue-as-new.md) is the usual escape, and it is not available yet.

## Pitfalls

- `set_event` stores the latest value for that name. It is not a log. The message log is the source of the commands.
- A finished workflow ID does not start again. Leave the loop running for as long as the entity exists.
- `timeout_seconds` on `recv` is required in practice: there is no wait-forever. Pick a timeout and loop.

## Related

- [Continue-as-new](continue-as-new.md)
- [Updatable timer](updatable-timer.md)
- [Signal with start](signal-with-start.md)
- [Messages and events](../guide/communication.md)
