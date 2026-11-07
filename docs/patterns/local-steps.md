# Local steps

> Run a short step in the same process as the workflow, with no trip to a separate worker.

:::note[Supported]

This is how every step already runs. `@yado.step()` executes in the workflow's process. There is no local-versus-remote distinction to opt into.

:::

## Overview

A local step is a step that does not wait to be dispatched to another worker. The workflow calls it, the function runs, and the result is saved. The next line of the workflow runs after that save.

Use it for a short, idempotent read or a small computation you still want recorded, so replay does not do it again and does not see a different value.

## Problem

In a server-based system, a tiny lookup pays a full activity round trip: schedule, pick a worker, run, record. People then hide the lookup in the workflow body, where a different result on replay takes a different branch. The two bad options are latency or non-determinism.

## Solution

Make the lookup a step. It runs where the workflow runs.

```python
import yado


@yado.step()
def load_flags(user_id: str) -> dict:
    return flags.get(user_id)


@yado.workflow()
def greet(user_id: str) -> str:
    if load_flags(user_id).get("beta"):
        return render_beta(user_id)
    return render(user_id)
```

The flag map is saved. Replay of `greet` takes the same branch even if the flag service has changed.

A step called from another step is an ordinary function call. Its result is part of the outer step. A step called with no workflow around it is also an ordinary call, which is what makes tests easy. See [Pass clients into steps](dependency-injection.md).

## When to use

Use a step for any value the workflow will branch on, including a value you could have computed locally, if that value can change between runs.

Call a plain function from the workflow only when it is deterministic: pure, and using only the workflow's arguments and the results of earlier steps.

## Trade-offs

You pay a database write per step. That is much smaller than a network hop to another worker, and it is not free. Fold several tiny reads into one step if you always want them together and you do not need to retry them separately.

## Pitfalls

- In-process does not mean exactly-once. A crash after a side effect and before the save runs the step again.
- A step that calls another workflow starts a child. That is a separate run, not an inlined call.
- There is nothing to configure. Looking for a "local activity" option will not find one, because the remote option is not there either.

## Related

- [Early return with local steps](early-return-local.md)
- [Eager start](eager-start.md)
- [Steps](../guide/steps.md)
