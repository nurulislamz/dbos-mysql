# Latency

> Shorten the path from the call to the first saved result. In rust-dwos that path is already inside your process.

Temporal's latency patterns exist because a workflow task and an activity task are separate trips through the server. A step here runs in the workflow's process. Starting a workflow writes the row and runs the body in the caller, unless the workflow was enqueued.

## Patterns in this section

- [Local steps](local-steps.md). Every `@DBOS.step()` already runs in-process. There is no second kind of step.
- [Early return with local steps](early-return-local.md). The caller's first answer comes from those in-process steps.
- [Eager start](eager-start.md). `DBOS.start_workflow` runs the body in the caller. Queued work waits to be admitted instead.

## Choosing a pattern

You want a short, idempotent call on the way to the first response: it is already a [local step](local-steps.md).

The HTTP handler should return before the workflow finishes: [early return](early-return-local.md), which is [early return](early-return.md) with those local steps.

You want the body to start without a queue hop: [eager start](eager-start.md), which is the normal `start_workflow`.
