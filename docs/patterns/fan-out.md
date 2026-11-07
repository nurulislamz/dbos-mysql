# Fan-out

> Split a record set into chunks and start one child workflow per chunk. The parent waits for every child.

:::note[Supported]

`yado.start_workflow` once per chunk, then `get_result` on each handle. Give each child a deterministic ID so a replay does not start it twice.

:::

## Overview

Fan-out is the unbounded form of parallel children. The parent decides the chunks, starts them all, and collects the results. Each child has its own steps, so one chunk's failure can be retried without redoing the chunks that already finished.

The parent's step list grows by one step per child start. Keep the chunk count in a range you are willing to replay.

## Problem

A nightly job processes a million rows. One workflow that handles each row as a step stores a million steps and replays all of them after a crash near the end. One process that pulls rows into memory loses the cursor when it dies.

## Solution

Pass each child an offset and a length. The child loads its own rows. The parent only stores two integers per child.

```python
import yado
from yado import DBOS, SetWorkflowID


@yado.workflow()
def page(offset: int, length: int) -> int:
    rows = load_rows(offset, length)
    for row in rows:
        handle_row(row)
    return len(rows)


@yado.workflow()
def fan(total: int, length: int = 100) -> int:
    handles = []
    for offset in range(0, total, length):
        child_id = f"{DBOS.workflow_id}-{offset}"
        with SetWorkflowID(child_id):
            handles.append(yado.start_workflow(page, offset, length))
    return sum(handle.get_result() for handle in handles)
```

`load_rows` and `handle_row` are steps. The child ID is derived from the parent ID and the offset, so recovery starts the same children.

Set `cancel_children=True` when you cancel `fan` if the pages should stop too.

## When to use

Use fan-out when:

- You know the total, or an upper bound, before the job starts.
- The downstream service can absorb every chunk at once.
- Each chunk should be visible as its own workflow.

Use a [sliding window](sliding-window.md) when starting every child at once is too much load.

Use a [batch iterator](batch-iterator.md) when you cannot name the total up front.

## Trade-offs

Maximum concurrency finishes sooner and can overwhelm the database, the carrier API, or the worker processes. There is no cap in the example above. A queue in front of `page` adds one. See [Rate limiting](rate-limiting.md).

## Pitfalls

- Starting thousands of children from one parent stores thousands of steps on the parent. That is the list recovery replays.
- A child without a stable ID is tied to the parent's step number. The explicit ID above survives an edit that inserts a step before the loop, for new runs. Runs that already saved the old IDs keep those IDs.
- `get_result` raises if the child fails. One failed page fails the parent after the earlier pages have already completed. Catch per handle if the parent should record partial success.

## Related

- [Sliding window](sliding-window.md)
- [Map-reduce](mapreduce.md)
- [Parallel execution](parallel-execution.md)
