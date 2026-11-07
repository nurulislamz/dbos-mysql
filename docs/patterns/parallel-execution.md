# Parallel execution

> Run independent steps or child workflows at the same time, then wait until every one has finished.

:::note[Supported]

An async workflow can `await asyncio.gather(...)` on steps or on child handles. Replay returns the saved results.

:::

## Overview

Parallel execution overlaps work that does not depend on other work. The workflow waits until the whole set has finished, then continues with every result.

Each step still saves its own result. A crash while the set is in flight replays the finished steps and runs the ones that were not saved.

## Problem

Charging, reserving stock, and writing the invoice can happen together. Running them one after another adds their latencies. Starting them with no record of which ones finished makes a crash repeat the ones that already succeeded.

## Solution

Gather the calls. Each call is a step or a child workflow, so its result is saved under a stable step.

```python
import asyncio

import yado


@yado.step()
async def reserve(sku: str) -> str:
    return warehouse.reserve(sku)


@yado.workflow()
async def prepare(sku: str) -> list[str]:
    stock, price = await asyncio.gather(reserve(sku), quote(sku))
    return [stock, price]
```

To fan out child workflows and collect their results:

```python
@yado.workflow()
def prepare_all(skus: list[str]) -> list[str]:
    handles = [yado.start_workflow(reserve_one, sku) for sku in skus]
    return [handle.get_result() for handle in handles]
```

Cap how many run at once with a [queue](../guide/queues.md) and `global_concurrency`. That limit holds across every copy of the program. See [Sliding window](sliding-window.md).

## When to use

Use parallel execution when:

- The calls do not need each other's results.
- You need every result before the workflow continues.
- A failure in one of them should surface to the workflow.

Use [pick first](pick-first.md) when the first success is enough.

## Trade-offs

More concurrency finishes sooner and puts more load on the downstream service. A queue limit protects that service. Unbounded `gather` does not.

## Pitfalls

- Gather steps, or child workflows. A bare HTTP call inside the workflow is not saved, and a replay can issue it again.
- The order of the gathered calls is the order of the saved steps. Keep that order stable across deploys.
- One failure fails the gather. The other calls may already have saved a result. Undo those with a [saga](saga.md) if they have side effects.

## Related

- [Child workflows](child-workflows.md)
- [Pick first](pick-first.md)
- [Fan-out](fan-out.md)
