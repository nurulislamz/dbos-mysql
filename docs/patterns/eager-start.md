# Eager start

> Run the workflow body in the process that started it, as soon as the row is saved, instead of handing the first task to a matcher.

:::note[Supported]

`yado.start_workflow` writes the row and runs the body in the caller. That is the normal path. Work you put on a queue waits until a process that registered the queue admits it.

:::

## Overview

Eager start removes the gap between "the workflow exists" and "the first line is running". The caller has the function in memory. After the insert commits, it runs. Another copy of the program can still recover the run if this process dies, because the row and the lease are what matter, not which process began it.

Queued work is the other path. Enqueue returns an ID immediately and does not run the body. A worker admits it later. Use that when you want the delay, the concurrency cap, or a different set of processes to do the work.

## Problem

A system that always parks a new workflow on a queue adds a poll interval to every start, including the ones the caller is about to wait for. The caller already loaded the code. The extra hop only helps when you needed a queue.

## Solution

Call `start_workflow` and, if you need the result, wait on the handle.

```python
import yado


def on_request(order_id: str) -> str:
    handle = yado.start_workflow(fulfil, order_id)
    return handle.get_result()
```

The body of `fulfil` starts in this process. `get_result` returns when it finishes. If this process dies mid-run, a recovery on any process that has registered `fulfil` continues it.

To opt into a hop, enqueue:

```python
queue = yado.register_queue("fulfilment", global_concurrency=10)
handle = queue.enqueue(fulfil, order_id)
```

The row exists. The body runs when the queue admits it.

## When to use

Use `start_workflow` when the caller can run the body and you want it to start now.

Use a queue when you need a [delay](delayed-start.md), a [concurrency cap](sliding-window.md), a [rate limit](rate-limiting.md), or you want the web processes to stay out of the work.

## Trade-offs

Eager start puts the workflow's load on the caller. That is the right default for a worker that exists to run workflows. It is the wrong default for a request handler that should return in a few milliseconds and leave a long job to a pool. Early return plus a queue is that split. See [Early return with local steps](early-return-local.md).

## Pitfalls

- A direct call, `fulfil(order_id)`, also runs eagerly, and it blocks the caller until the workflow finishes. `start_workflow` is what lets the caller do something else, or return a handle, while the body runs.
- The body runs only if this process registered the workflow name. Recovery has the same rule.
- Eager does not skip the database. The row is written before the body runs, so a crash still has something to recover.

## Related

- [Local steps](local-steps.md)
- [Early return with local steps](early-return-local.md)
- [Delayed start](delayed-start.md)
- [Workflows](../guide/workflows.md#starting-in-the-background)
