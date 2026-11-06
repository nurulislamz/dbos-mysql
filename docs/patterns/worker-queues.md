# A queue per host

> Pin work to one process by giving that process a queue only it registers.

:::note[Supported]

A queue runs only on a process that has called `DBOS.register_queue` for that name. A queue per host is a queue whose name is the host, registered only there.

:::

:::warning[No deadline for a dead host]

If that process is gone, the queued work waits until a process registers the queue again. There is no timeout that fails the wait while the host is dead.

:::

## Overview

Worker-specific routing is how you run a step next to a resource only one machine has: a GPU, a mounted disk, a device, a single-threaded license. The workflow that decides to do the work can run anywhere. The queued child runs on the machine that registered the queue.

The pin is the registration, not a sticky session. Stop registering and the work stops being admitted. Start registering on a replacement machine and the backlog moves there, as long as the queue name matches.

## Problem

A render job has to run on the machine that holds the source files. A shared queue admits it on whichever worker is free, including one that cannot see the files. A hostname check inside the step then fails, retries, and may land on the wrong machine again.

## Solution

Register a queue named for the host, only on that host.

```python
import socket

from yado import DBOS, SetWorkflowID


@DBOS.workflow()
def render_here(job_id: str) -> str:
    return render_local_files(job_id)


def register_host_queue() -> None:
    DBOS.register_queue(f"host-{socket.gethostname()}", global_concurrency=1)


def submit(job_id: str, host: str):
    with SetWorkflowID(job_id):
        return DBOS.enqueue_workflow(f"host-{host}", render_here, job_id)
```

`register_host_queue` runs at startup on the render machine. `submit` can run anywhere. `enqueue_workflow` writes the queue entry. It does not make the caller pull from the queue. The host has to have registered that name, or the entry waits until one does.

`global_concurrency=1` is optional. Use it when the machine should do one of these at a time.

## When to use

Use a per-host queue when the work is useless, or unsafe, on any other machine.

Use a shared queue when any registered process can do the work. That is the normal case, and a dead process then loses only its lease. Another process recovers the workflow.

## Trade-offs

Pinning is simple and strict. You gain affinity and you lose automatic failover. A host that is down is a queue that is not moving. Watch the depth of those queues if you operate them.

## Pitfalls

- Nobody else will drain the queue. A deploy that forgets `register_queue` on the new host strands the backlog until you register it.
- There is no "wait at most ten minutes for this host" on the queue entry. If you need that, start a timer workflow that cancels the queued ID, or enqueue on a shared queue and let any host take it.
- The workflow name still has to be registered on the host. A queue registration does not by itself register `render_here`.

## Related

- [Pass clients into steps](dependency-injection.md)
- [Delayed start](delayed-start.md)
- [Queues](../guide/queues.md)
