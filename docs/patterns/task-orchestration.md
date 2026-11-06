# Task orchestration

> Split a workflow into pieces, run those pieces together, or race them and keep the first result.

These patterns coordinate more than one unit of work inside a workflow.

## Patterns in this section

- [Child workflows](child-workflows.md). Break a workflow into smaller workflows. Each child has its own ID, steps, and lifecycle.
- [Parallel execution](parallel-execution.md). Run independent steps or children at the same time, and wait until they all finish.
- [Pick first](pick-first.md). Start several alternatives, take the first result, and cancel the others.

## Choosing a pattern

The piece of work is large, reused, or worth tracking on its own: start a [child workflow](child-workflows.md).

The pieces are independent and you need every result: use [parallel execution](parallel-execution.md), and put a queue in front if the downstream service cannot take all of them at once.

Several alternatives compete and the first finished result is enough: use [pick first](pick-first.md) and cancel the losers.
