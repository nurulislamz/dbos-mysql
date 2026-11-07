# Map-reduce

> Split a set into a tree of child workflows. The leaves do the work. Each parent adds up the results from its children.

:::note[Supported]

A workflow that starts itself on a smaller slice, then returns the sum of `get_result()`, is the tree. The leaves are the same workflow once the slice is small enough to process directly.

:::

## Overview

Map-reduce is fan-out with a shape. A wide set started from one parent stores one step per child on that parent. A tree keeps each node's child count small. The leaves run the steps that touch the records. The internal nodes only start children and add numbers.

Results flow back through `get_result`. You do not need a message from the child to the parent for the total.

## Problem

One parent starting a child per record, for a million records, writes a million steps onto the parent. Recovery of the parent replays that list. The work itself is embarrassingly parallel: each record is independent, and the only combined result is a count or a sum.

## Solution

Split until the slice is at the leaf size, then process it.

```python
import yado
from yado import DBOS, SetWorkflowID


LEAF = 100


@yado.workflow()
def reduce(offset: int, length: int, depth: int) -> int:
    if length <= LEAF or depth >= 16:
        return process_slice(offset, length)
    mid = length // 2
    handles = []
    for child_offset, child_length in ((offset, mid), (offset + mid, length - mid)):
        child_id = f"{DBOS.workflow_id}-{child_offset}"
        with SetWorkflowID(child_id):
            handles.append(yado.start_workflow(reduce, child_offset, child_length, depth + 1))
    return sum(handle.get_result() for handle in handles)
```

`process_slice` is a workflow or a step that handles `LEAF` records and returns a count. `depth` stops a bad `length` from recursing without a bound.

The child ID includes the offset, so replay attaches to the same child.

## When to use

Use a tree when:

- You want one aggregated result.
- A single parent cannot hold a child per record.
- The records are independent until the fold.

Use [fan-out](fan-out.md) when the number of chunks already fits on one parent.

Use a [sliding window](sliding-window.md) when the limit that matters is downstream load, not the shape of the tree. A tree of unbounded leaves can still stampede. Enqueue the leaves if you need a cap.

## Trade-offs

A smaller leaf makes a deeper tree and more workflow rows. A larger leaf puts more steps on each leaf and less overhead on the tree. A leaf of a few hundred records is a reasonable start. The right size is whatever one workflow recovers comfortably.

## Pitfalls

- Internal nodes should stay deterministic. Compute the split from the arguments, not from a fresh read of the table, unless that read is a step shared by every replay.
- `get_result` waits for the subtree. A failed leaf fails its parent and then the root. Catch at the leaf if a bad record should count as zero and let the rest finish.
- Cancelling the root with `cancel_children=True` cancels every descendant that is still running. A node that has already finished is left as it is. The default, `cancel_children=False`, cancels only the root.

## Related

- [Fan-out](fan-out.md)
- [Child workflows](child-workflows.md)
- [Batch iterator](batch-iterator.md)
