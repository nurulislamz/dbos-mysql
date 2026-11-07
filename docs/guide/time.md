# Sleep, timeouts and schedules

All of these use the database clock and survive restarts.

## Durable sleep

```python
@yado.workflow()
def reminder(user_id: str) -> None:
    send_first(user_id)
    yado.sleep(7 * 24 * 3600)
    send_second(user_id)
```

The first call saves an absolute wake time as a step. If the process restarts, the workflow replays and sleeps only for what is left. Async code uses `await yado.sleep_async(...)`.

A cancel wakes a sleeper in about 4 ms on the same engine, or about 250 ms from another process. Outside a workflow, or inside a step, this is a plain sleep.

A sleep longer than `max_run_seconds` (default 1 hour) passes through that limit: the run gives up its lease and the workflow continues by replay, with the same wake time.

## Workflow timeouts

```python
with SetWorkflowTimeout(60):
    handle = yado.start_workflow(report, "q3")
```

The clock starts when the workflow starts running. When the deadline passes, the workflow becomes `CANCELLED` and a waiter gets `DBOSAwaitedWorkflowCancelledError`.

- The deadline is stored, so a restart does not reset it.
- Resuming does not silently grant a new budget.
- Children inherit the parent's absolute deadline. Give a child its own timeout, or use `SetWorkflowTimeout(None)` to opt out.

For one attempt of one step, use [`timeout_seconds`](steps.md#timeouts).

## Schedules

A schedule enqueues a workflow on a five-field UTC cron.

```python
@yado.scheduled("0 8 * * *", queue="reports")
@yado.workflow()
def morning_report() -> None:
    ...


DBOS(config=config)
yado.launch()
yado.register_queue("reports")  # Needs a launched DBOS.
```

`@yado.scheduled` goes above `@yado.workflow()`, and that workflow takes no arguments. To pass arguments, or make schedules per user, use `register_schedule`:

```python
yado.register_schedule(
    "digest-user-1",
    digest,
    "0 8 * * *",
    queue="reports",
    args=("user-1",),
)
yado.delete_schedule("digest-user-1")
```

The name identifies the schedule. Registering the same name again updates it. Deleting a missing name is not an error, and runs already started keep going.

A process needs a registered queue of that name to run the work. If the queue does not exist when a tick is due, a warning is logged and the tick stays due.

Each run gets the ID `sched-<name>-<tick>`.

### What happens at each tick

| Situation | Behaviour |
|---|---|
| New schedule | Does not run a tick that has already passed |
| Cron text changed | Counts as a new schedule |
| Several ticks missed while stopped | Only the latest is enqueued |
| Previous run still pending | The tick is recorded and skipped |
| Many copies running | Each tick fires once. Firing locks the schedule row and records the tick |
| Both day of month and day of week set | Rejected |
