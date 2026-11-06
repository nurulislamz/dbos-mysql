# Long-running steps

> A step that runs for a long time reports progress by heartbeating. If it goes quiet, the attempt times out.

:::note[Supported]

Set `heartbeat_timeout_seconds` and call `DBOS.heartbeat()` as the work advances.

:::

:::warning[Heartbeat details are not supported yet]

`DBOS.heartbeat()` takes no payload and returns nothing. The next attempt cannot read what the previous attempt had finished. Split the work into steps so a retry starts at the first unsaved chunk.

:::

## Overview

A long-running step is still one saved result, but the engine needs a sign that the attempt is alive. The workflow lease renews on its own. A step that is blocked on a socket does not. Heartbeats are that sign.

Cancellation and a per-attempt timeout also apply. `timeout_seconds` bounds one attempt. An async body is cancelled. A sync body keeps running until it next calls into the engine.

## Problem

A step that encodes a video can run for an hour. With no heartbeat, a dead process looks the same as a busy one until the workflow lease expires, and a hung call can hold the attempt until a coarse timeout. With no saved checkpoints, the retry encodes the video from the start.

## Solution

Heartbeat on each chunk. Return only when the step's result is complete.

```python
from rust_dwos import DBOS


@DBOS.step(heartbeat_timeout_seconds=30, timeout_seconds=3600)
def encode(video_id: str) -> str:
    for chunk in chunks(video_id):
        encode_chunk(chunk)
        DBOS.heartbeat()
    return "done"
```

To resume in the middle, make each chunk its own step. The workflow remembers which chunks have a saved result.

```python
@DBOS.workflow()
def encode_all(video_id: str) -> str:
    done = []
    for chunk in list_chunks(video_id):
        done.append(encode_chunk(chunk))
    return ",".join(done)
```

`list_chunks` must be a step too, so replay sees the same list.

## When to use

Use a heartbeating step when:

- The work is one logical result.
- You can afford to repeat it after a crash.
- You need a hang to fail faster than the workflow lease.

Use a step per chunk when repeating the finished chunks is too expensive.

## Trade-offs

Heartbeats add database writes on the interval you choose. Too rare and a hang sits until the timeout. Too often and the step spends its time recording liveness.

## Pitfalls

- `DBOS.heartbeat()` does not store a cursor. Do not rely on it to skip finished work.
- A sync body is not preempted. Call `heartbeat` or return so a timeout can land.
- `heartbeat_timeout_seconds` is not a substitute for an idempotent side effect. A crash after the side effect and before the step is saved runs the step again.

## Related

- [Polling an external service](polling.md)
- [Steps](../guide/steps.md#heartbeats)
- [Resume after a correction](resumable.md)
