# Temporal patterns

Temporal keeps a [catalog of design patterns](https://docs.temporal.io/design-patterns). This page marks each one against yado as it is on `main` today.

| Mark | Means |
|---|---|
| Supported | Calls on `main` do the job. The table names them and the page that describes them |
| Can support | The engine could do it in its own database, with no new server. One piece is missing |
| Does not fit | It needs something yado will not have, such as a separate cluster or a hosted control plane |

Of the 35 patterns, 28 are supported and 7 can be supported. None needs a separate server, so none is marked "does not fit".

The marks come from the code and tests at commit `6218da0`. Queue ordering and limits are described in the [Queues](guide/queues.md#ordering-and-limits) guide.

## Words that differ

| Temporal | yado |
|---|---|
| Activity | Step |
| Signal | `DBOS.send` to a workflow, read there with `DBOS.recv` |
| Query | `DBOS.set_event` in the workflow, read with `DBOS.get_event` |
| Task Queue | Queue, from `DBOS.register_queue` |
| Event history | Saved steps, one row each |
| Worker | Any copy of your program |
| Workflow ID | Workflow ID. One run per ID. A finished ID returns its stored result |

## Task orchestration

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Child Workflows](https://docs.temporal.io/design-patterns/child-workflows) | Supported | Call a workflow, or `DBOS.start_workflow` it, from inside a workflow, then `handle.get_result()`. The child's ID is saved as a parent step. There is no parent close policy: a child keeps running after its parent ends, unless you cancel with `cancel_children=True` | [Workflows](guide/workflows.md#child-workflows), [Managing workflows](guide/management.md#cancel) |
| [Parallel Execution](https://docs.temporal.io/design-patterns/parallel-execution) | Supported | In an async workflow, `asyncio.gather` over steps or child handles. Replay returns the saved results. `DBOS.asyncio_wait` saves which tasks finished. To cap parallel work, enqueue on a queue with `global_concurrency` | [Reference](reference.md#starting-and-waiting), [Queues](guide/queues.md) |
| [Pick First](https://docs.temporal.io/design-patterns/pick-first) | Supported | Start the racers as workflows, take `DBOS.wait_first(handles)`, then `DBOS.cancel_workflow` the rest. For steps, use `DBOS.asyncio_wait(..., return_when=asyncio.FIRST_COMPLETED)` and cancel the pending tasks. A cancelled sync body runs on until its next durable call | [Reference](reference.md#starting-and-waiting), [Managing workflows](guide/management.md#cancel) |

## Workflow messaging

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Signal with Start](https://docs.temporal.io/design-patterns/signal-with-start) | Supported | Two calls: `DBOS.start_workflow` under `SetWorkflowID(id)`, then `DBOS.send(id, ..., idempotency_key=...)`. A start on a running ID attaches to it. A message waits in the database until `recv` reads it. The calls are not atomic: the workflow can finish before the send, and a message sent to a finished workflow is not read. A finished ID does not start again | [Workflows](guide/workflows.md#workflow-ids), [Messages and events](guide/communication.md#messages) |
| [Request-Response via Updates](https://docs.temporal.io/design-patterns/request-response-via-updates) | Can support | Missing: a call that validates a request, changes the workflow, and returns its answer to the caller at once. Today the caller sends a request with a reply key, the workflow answers with `set_event(reply_key, ...)`, and the caller waits on `get_event` | [Messages and events](guide/communication.md) |
| [Event Accumulator](https://docs.temporal.io/design-patterns/event-accumulator) | Supported | Producers start the accumulator and send each event with its ID as the `idempotency_key`, so a resent event is dropped. Coordinate the window closing with producers: a send to a finished workflow is not read, and the same ID cannot start another run. The workflow loops on `DBOS.recv(topic, timeout_seconds=window)`. Each message starts the wait again, and `None` means the window closed | [Messages and events](guide/communication.md#messages) |

## Entity and lifecycle

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Entity Workflow](https://docs.temporal.io/design-patterns/entity-workflow) | Supported | One workflow per entity, with the entity ID as the workflow ID. Commands arrive by `send` and `recv`. The workflow publishes its state with `set_event`, and anyone reads it with `get_event`, as a Query would. Each command adds saved steps, and recovery replays all of them, so a very long-lived entity gets slower to recover | [Workflows](guide/workflows.md#workflow-ids), [Messages and events](guide/communication.md) |
| [Continue-As-New](https://docs.temporal.io/design-patterns/continue-as-new) | Can support | Missing: a call that ends the current run and starts the same workflow ID again, with new input and no saved steps. A branch for this exists but is not on `main`. Today a workflow can start its successor under a new ID as its last step | [Workflows](guide/workflows.md#child-workflows) |
| [Updatable Timer](https://docs.temporal.io/design-patterns/updatable-timer) | Supported | Loop on `DBOS.recv("deadline", timeout_seconds=left)`, where `left` comes from the current deadline and the time read in a step. A message with a new deadline ends the wait, and the loop waits again. The wait's deadline is stored, so a restart does not reset it. To push back a start instead, `Debouncer` delays the start again on each call; it has no page yet | [Messages and events](guide/communication.md#messages), [Sleep, timeouts and schedules](guide/time.md) |

## External interaction

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Polling External Services](https://docs.temporal.io/design-patterns/polling) | Supported | Frequent: one step that loops, sleeps and calls `DBOS.heartbeat()`, with `heartbeat_timeout_seconds` set. Infrequent: a step with `retries_allowed=True, interval_seconds=60, backoff_rate=1.0` that raises until the job is ready. For polls that last hours, loop in the workflow on a check step and `DBOS.sleep(60)`. That sleep is saved and passes the one-hour `max_run_seconds` limit. Retry waits inside a step are not saved | [Steps](guide/steps.md#retries), [Sleep, timeouts and schedules](guide/time.md#durable-sleep) |
| [Long-Running Activity](https://docs.temporal.io/design-patterns/long-running-activity) | Can support | Missing: heartbeat details that the next attempt can read, so a step can resume from its last checkpoint. `heartbeat_timeout_seconds` and `DBOS.heartbeat()` already find a stuck step, and an async step is cancelled with its workflow. Today, make each chunk its own step, so a retry starts at the first unsaved chunk | [Steps](guide/steps.md#heartbeats) |
| [Delayed Start](https://docs.temporal.io/design-patterns/delayed-start) | Supported | `with SetEnqueueOptions(delay_seconds=3600):`, then `queue.enqueue(...)`. The row exists at once and reads as `DELAYED`. `DBOS.set_workflow_delay(id, delay_seconds=0)` starts it early, and `DBOS.cancel_workflow` stops it before it runs | [Managing workflows](guide/management.md#status-values). See also [Queues](guide/queues.md#ordering-and-limits) |
| [Delayed Callback](https://docs.temporal.io/design-patterns/delayed-callback) | Supported | Inbound: the webhook handler does Signal with Start. Outbound: `DBOS.sleep(delay)`, then a step that makes the call, with retries. Async completion: a step submits the job with the workflow ID as its callback reference. The workflow waits on `DBOS.recv(topic, timeout_seconds=...)`, and the callback handler calls `DBOS.send(workflow_id, result, topic, idempotency_key=...)`. There are no task tokens; the workflow ID and topic do that job | [Messages and events](guide/communication.md#messages), [Sleep, timeouts and schedules](guide/time.md#durable-sleep) |
| [Approval](https://docs.temporal.io/design-patterns/approval) | Supported | `DBOS.recv("approval", timeout_seconds=...)`, with a dict that carries the decision, approver and reason. `None` means the timeout passed: escalate with a step, then wait again. Publish the state with `set_event` for status checks. An approval that arrives after the timeout stays unread | [Messages and events](guide/communication.md) |

## Distributed transactions

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Saga](https://docs.temporal.io/design-patterns/saga-pattern) | Supported | Steps and a plain `try`/`except` in the workflow. Add each compensation to a list before its step runs. On error, run the list in reverse, each as a step. Replay takes the same path, because each step's result or error is saved. A cancel or a workflow timeout stops the body, so compensations do not run then | [Workflows](guide/workflows.md#errors), [Steps](guide/steps.md) |
| [Early Return](https://docs.temporal.io/design-patterns/early-return) | Supported | Start the workflow, and have it call `DBOS.set_event("init", result)` after its fast first part. The caller waits on `DBOS.get_event(id, "init", timeout_seconds=...)` and returns, while the workflow goes on. Like Update-with-Start, these are two calls, not one transaction | [Messages and events](guide/communication.md#events) |

## Error handling and retries

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Fixed Count of Retries](https://docs.temporal.io/design-patterns/fixed-count-retries) | Supported | `@DBOS.step(retries_allowed=True, max_attempts=3)`. The count includes the first attempt. When it runs out, `DBOSMaxStepRetriesExceeded` is saved, with each attempt's error in `.errors`. Retries are off by default, the opposite of Temporal. The count is held in memory, so a crash during retries starts it again | [Steps](guide/steps.md#retries) |
| [Fixed Wall-Time Retries](https://docs.temporal.io/design-patterns/fixed-wall-time-retries) | Can support | Missing: a time budget across all of a step's attempts, like `ScheduleToCloseTimeout`. `timeout_seconds` limits one attempt only. Today, put the step in a child workflow started under `SetWorkflowTimeout(seconds)`. The parent's `get_result` raises `DBOSAwaitedWorkflowCancelledError` when the budget runs out | [Steps](guide/steps.md#timeouts), [Sleep, timeouts and schedules](guide/time.md#workflow-timeouts) |
| [Non-Retryable Errors](https://docs.temporal.io/design-patterns/non-retryable-errors) | Supported | `should_retry=lambda error: not isinstance(error, ValidationError)` on the step. A refused error is raised at once, as its own type | [Steps](guide/steps.md#retries) |
| [Delayed Retry](https://docs.temporal.io/design-patterns/delayed-retry) | Can support | Missing: a way for one error to set the wait before the next attempt; `should_retry` can only say yes or no. Today, leave step retries off, catch the error in the workflow, call `DBOS.sleep(error.retry_after)`, and call the step again | [Steps](guide/steps.md#retries), [Sleep, timeouts and schedules](guide/time.md#durable-sleep) |
| [Fast/Slow Retries](https://docs.temporal.io/design-patterns/fast-slow-retries) | Supported | Fast phase: a step with `retries_allowed=True, interval_seconds=1, max_attempts=10`. Catch `DBOSMaxStepRetriesExceeded` in the workflow. Slow phase: loop on `DBOS.sleep(300)` and one more attempt, as a step without retries. The slow loop's waits are saved, so it survives restarts and the `max_run_seconds` limit | [Steps](guide/steps.md#retries), [Sleep, timeouts and schedules](guide/time.md#durable-sleep) |
| [Retry Alerting via Metrics](https://docs.temporal.io/design-patterns/retry-metrics) | Supported | Inside the step, read `DBOS.step_status.current_attempt` and bump your own metric when it passes a threshold. yado has no metrics pipeline of its own, so use the client your program already has | [Steps](guide/steps.md#retries), [Reference](reference.md#context) |
| [Resumable Activity](https://docs.temporal.io/design-patterns/resumable-activity) | Supported | Catch `DBOSMaxStepRetriesExceeded` in the workflow, call `set_event("status", "AWAITING_CORRECTION")`, and wait on `DBOS.recv("correction", timeout_seconds=...)`. Call the step again with the corrected input, then wait on `recv("approve")`. `recv` always has a timeout, so loop for an open-ended wait | [Steps](guide/steps.md#retries), [Messages and events](guide/communication.md) |

## Batch processing

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Fan-Out with Child Workflows](https://docs.temporal.io/design-patterns/fanout-child-workflows) | Supported | Start one child per chunk with `DBOS.start_workflow(child, offset, length)`, then `get_result` on each handle. To cap how many run at once, enqueue them on a queue instead. Each start is one saved step of the parent | [Workflows](guide/workflows.md#child-workflows), [Queues](guide/queues.md) |
| [Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator) | Can support | Missing: Continue-As-New, which is not on `main`. Today one workflow can loop over pages, one step per page, but its saved steps grow with the data. Or each page's workflow can start the next page under a new ID | [Workflows](guide/workflows.md#child-workflows) |
| [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window) | Supported | `DBOS.register_queue("records", global_concurrency=10)`, then one enqueue per record. The queue starts the next workflow as each one finishes, and the limit holds across every copy. A parent workflow saves one step per enqueue, so for a very large set, enqueue from outside a workflow with a `SetWorkflowID` per record | [Queues](guide/queues.md) |
| [MapReduce Tree](https://docs.temporal.io/design-patterns/mapreduce-tree) | Supported | A workflow starts child workflows of itself on parts of its slice, down to a leaf size, and adds up their `get_result` values. Results come back as results, not as messages. Keep side effects in leaf steps | [Workflows](guide/workflows.md#child-workflows) |

## QoS and throughput

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting) | Supported | `DBOS.register_queue("api", limiter={"limit": 5, "period": 1.0})` caps starts per period across every copy. Put the throttled call in a small workflow and enqueue it on that queue. It limits workflow starts, not single steps | [Queues](guide/queues.md#ordering-and-limits) |
| [Priority](https://docs.temporal.io/design-patterns/priority-task-queues) | Supported | `with SetEnqueueOptions(priority=1):`, then `enqueue`. Admission takes the lowest number first, and FIFO within a number. Work with no priority set goes first of all. Like Temporal, this orders starts and does not stop running work | [Queues](guide/queues.md#ordering-and-limits) |
| [Fairness](https://docs.temporal.io/design-patterns/fairness) | Supported | `register_queue(..., partition_concurrency=2)` or `partition_limiter=...`, and `SetEnqueueOptions(queue_partition_key=tenant)`. Admission skips a tenant at its cap and admits other tenants' work. This uses caps, not weights, so a capped tenant cannot use idle slots. On such a queue, work without a key is not admitted | [Queues](guide/queues.md#ordering-and-limits) |

## Performance and latency

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Local Activities](https://docs.temporal.io/design-patterns/local-activities) | Supported | Every `@DBOS.step()` already runs in the workflow's own process. There is no server between them, and a step costs one database write. Unlike a Local Activity, each completed step is saved on its own, so a crash does not rerun a saved step; an unsaved step may run again | [Architecture](architecture.md#life-of-a-workflow), [Steps](guide/steps.md) |
| [Early Return + Local Activities](https://docs.temporal.io/design-patterns/early-return-local-activities) | Supported | Use `DBOS.set_event` and `DBOS.get_event` for Early Return as above. Its fast `@DBOS.step()` calls already run in the process | [Messages and events](guide/communication.md#events), [Architecture](architecture.md#life-of-a-workflow) |
| [Eager Workflow Start](https://docs.temporal.io/design-patterns/eager-workflow-start) | Supported | `DBOS.start_workflow` writes the row and runs the workflow in the calling process. There is no dispatch hop to skip. Queued work goes through admission instead | [Workflows](guide/workflows.md#starting-in-the-background), [Architecture](architecture.md#life-of-a-workflow) |

## Worker configuration

| Pattern | Mark | How in yado | Page |
|---|---|---|---|
| [Worker-Specific Task Queues](https://docs.temporal.io/design-patterns/worker-specific-taskqueue) | Can support | Missing: a limit on how long queued work may wait to start, so work for a dead host waits until that host returns. Routing already works: only a process that registered a queue runs its work, so a queue per host pins work to that host. All steps of one run also run in one process, until a crash moves the run | [Queues](guide/queues.md) |
| [Activity Dependency Injection](https://docs.temporal.io/design-patterns/activity-dependency-injection) | Supported | Build clients once at startup, and let step functions use them. In tests, swap the objects; a step called outside a workflow runs as a plain function. Keep them out of workflow code, which must be deterministic | [Steps](guide/steps.md#when-a-step-runs), [Workflows](guide/workflows.md#workflow-code-must-be-deterministic) |
