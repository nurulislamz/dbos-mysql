# rust-dwos

Durable workflows on Postgres, MySQL 8.4 and SQLite. A Rust core decides what runs. A thin Python layer only calls your function and returns the result. Finished steps are saved, so a crash resumes without repeating them.

> **Unofficial and early.** This project is not affiliated with DBOS, Inc. The core feature set passes its tests and crash tests on MySQL and SQLite. Postgres is not ready yet. Nothing has run in production, so give payment or email steps an idempotency key at the service.

There is no extra server. A running copy of your program is the coordinator. When every copy is stopped, the rows wait until you start one again.

## Direction

| Piece | Role |
|---|---|
| Rust core | Decides the step, the saved result, and the lease. Writes and reads the rows |
| Python | Runs your function and returns the result. Holds no step, result, or lease decisions |
| Postgres, MySQL 8.4 or SQLite | Stores the workflow and each finished step |
| TypeScript SDK | Not part of this version |

Crash detection uses leases that expire. A dead process loses the workflow, and another running copy can take it.

## Done when

- Killing the process mid-workflow leaves that workflow unfinished.
- Starting the program again finishes it without repeating finished steps.
- A shutdown is not stored as a final error.
- Python contains no step, result, or lease decisions.

To learn and use it, read the [docs](docs/index.md). To browse them as a site, run `uvx --from mkdocs-material mkdocs serve`. The plan is [`docs/PLAN.md`](docs/PLAN.md). Status is in [`PROGRESS.md`](PROGRESS.md), including which databases work so far.

## Feature checklist

Compared with [`dbos-transact-py`](https://github.com/dbos-inc/dbos-transact-py) at commit `236142b`, where the SDK's API comes from. The DBOS marks come from this project's earlier checklist and have not been rechecked against that commit.

| Mark | Meaning |
|---|---|
| ✅ | Built and passing its own tests |
| 🚧 | Written, but not fully verified yet |
| 📅 | Planned or deferred |
| ❌ | Not planned, or not supported |
| ❓ | Not checked yet |

### Databases

| Feature | DBOS | rust-dwos |
|---|---|---|
| Postgres | ✅ | 🚧 Full tests and crash tests pass. An older restart incident is unexplained, though it did not come back in 5 restart runs |
| SQLite | ✅ | ✅ One machine only |
| MySQL 8.4 | ❌ | ✅ |

### Core workflows

| Feature | DBOS | rust-dwos |
|---|---|---|
| Workflow and step state storage | ✅ | ✅ Rust store |
| Stale owners blocked from saving results | ✅ | ✅ Rust store |
| `@DBOS.workflow()`, `@DBOS.step()` | ✅ | ✅ |
| `validate_args`, `pydantic_args_validator` | ✅ | 🚧 Implemented; workflow execution not verified |
| `DBOSContextEnsure` | ✅ | ✅ One instance cannot be entered twice |
| `DBOS.asyncio_wait` | ✅ | ✅ |
| Async and sync functions | ✅ | ✅ |
| `SetWorkflowID`, one run per workflow ID | ✅ | ✅ `reject` and `return-existing`. Other reuse policies are not in this API |
| Replaying saved steps and stored errors | ✅ | ✅ |
| Changed code detected during replay | ✅ | ✅ |
| Step retries | ✅ | ✅ |
| Per-attempt step timeout (`timeout_seconds`) | ✅ Async steps only | ✅ A timed-out sync body keeps running on its thread |
| Step heartbeat (`DBOS.heartbeat()`) | ❌ | ✅ Separate from lease renewal |
| Child workflows | ✅ | ✅ |
| `start_workflow`, `retrieve_workflow`, handles | ✅ | ✅ `get_result(polling_interval_sec=)` sets the longest wait between polls. Omitting it keeps the 250 ms cap |
| `list_workflows`, `list_workflow_steps` | ✅ | ✅ Queue, version, fork and user filters; step output and pagination options. No `shard_key` |
| Workflow attributes | ✅ | ✅ `SetWorkflowAttributes` and `update_workflow_attributes` work for direct and queued starts, status reads, and replacing, clearing and merging. Children do not inherit initial attributes |
| Crash recovery | ✅ | ✅ Crash tests pass on all 3 on the newest code |
| Recovery attempt limit | ✅ | ✅ |

### Coordination and control

| Feature | DBOS | rust-dwos |
|---|---|---|
| Durable sleep | ✅ | ✅ |
| `send`, `recv`, `send_bulk` | ✅ | ✅ Bulk sends are atomic. `send_to_forks` delivers to forks of the destination |
| `set_event`, `get_event` | ✅ | ✅ |
| Workflow timeouts | ✅ | ✅ |
| Queues | ✅ | ✅ FIFO with deduplication, priority, delayed start, a global and per-worker concurrency limit, rate limits and partitions. `list_queues`, `delete_queue`, `register_queue(on_conflict=)`, `SetEnqueueOptions`, `enqueue_workflow_with_options`, `DBOS.listen_queues` and the runtime setters all work. The latest registration with a limit sets it, and registering with none keeps the stored limit, so during a rolling deploy processes on different code can overwrite each other until the old ones stop. A row still waiting reads as `ENQUEUED` or `DELAYED`; the stored status stays `PENDING` |
| Queue rate limits, priorities, partitions | ✅ | ✅ Rate limits per queue and per partition, priorities, and `queue_partition_key` |
| Cancel and resume | ✅ | ✅ `cancel_children`, `queue_name`, and bulk cancel and resume. No `shard_key` |
| Fork and rewind | ✅ | ✅ `fork_workflow`, `rewind_workflow` and `send_to_forks`. A fork does not inherit the original timeout, and events are matched to the kept steps by time. `is_fork` and `was_forked_from` filters are not built |
| Deleting old workflows | ✅ | ✅ `delete_workflow`, `delete_workflows` and `delete_finished_workflows`. Pending workflows and children of pending workflows stay. A deleted ID can run again |
| `run_step`, `wait_first` | ✅ | ✅ `preemptible` is accepted on async `run_step` only; a sync step with `preemptible=True` is refused like DBOS |
| Schedules | ✅ | ✅ Five-field UTC cron. A new schedule does not run a tick that has already passed, and changing the cron counts as a new schedule. Later, only the latest missed tick is enqueued, and that tick is skipped while a previous run of the same schedule is still pending. `register_schedule` takes `args` and `kwargs`, and `delete_schedule` removes one. `@DBOS.scheduled` takes no arguments and goes above `@DBOS.workflow()` |
| Streams | ✅ | ✅ `write_stream`, `read_stream`, `read_stream_offset` and `close_stream`, woken by the same hub as `recv`. No `shard_key` |
| Debouncer | ✅ | 📅 |
| External client (`DBOSClient`) | ✅ | ✅ Enqueue, send, events, results and status, list, cancel, resume, delete, attributes and queue management from outside a DBOS process. No fork, streams, schedules, versions, `*_in_transaction` or `shard_key` |
| App database transactions (datasources) | ✅ | ✅ `SQLAlchemyDatasource`, `AsyncSQLAlchemyDatasource` and `run_tx_step`. SQLAlchemy is optional. The app-database role grants are untested on Postgres |
| Patching and app versions | ✅ | 📅 |
| Pydantic AI integration | ✅ | 📅 |
| Kafka | ✅ | ❌ |
| Class-based workflows | ✅ | ❌ |
| External object storage | ✅ | ❌ |
| Sharding | ✅ Enterprise only | ❌ |
| `auto_upgrade` | ✅ Enterprise only | ❌ |
| SQL functions `dbos.enqueue_workflow`, `dbos.send_message` | ✅ | ❌ They need DBOS's exact `dbos.` schema |

### Performance

No benchmark exists in this repository, and nothing has been measured side by side with DBOS. The times below are single samples on SQLite on a busy machine. DBOS's own published figures are about 1,000 workflows per second per Postgres vCPU, and 20K on 16 vCPU. Those are vendor figures, not checked here.

| Check | DBOS | rust-dwos |
|---|---|---|
| Workflows and steps per second | ✅ Vendor figure: about 1,000 per Postgres vCPU, 20K on 16 vCPU | ❓ Not measured |
| Extra time per step | ❓ | ❓ Not measured |
| Memory and connections per process | ❓ | ❓ Not measured |
| Behaviour with a large backlog | ❓ | 🚧 A claim of 10 queue slots takes under 1 s with 100k waiting and 100k finished rows, on a busy machine. Recovery takes at most 50 workflows per interval |
| Wake on a message from the same engine | ❓ | ✅ About 10-17 ms |
| Wake on a message from another process | ❓ | ✅ Within about 250 ms |
| Queue enqueue to workflow start, same engine | ❓ | ✅ About 11 ms |
| Cancel reaches a sleeping workflow | ❓ | ✅ About 4 ms on the same engine. About 250 ms from another process |
| Database reads while waiting | ❓ | 🚧 4 reads per second per sleeper, and per waiter with no local wake |

### Security

| Check | DBOS | rust-dwos |
|---|---|---|
| Authentication and roles | ✅ | ❌ |
| Safe stored format | ❓ | 🚧 Pickle by default, so database write access can run code. JSON is an option |
| Encrypted database connections | ❓ | 🚧 Tested on MySQL and Postgres when the URL requires TLS. Without that, a connection can use plain text |
| Memory safety | ❓ | ✅ `dwos-core` forbids `unsafe` code |
| Dependency audit | ❓ | 🚧 `cargo audit` 2026-11-05: one advisory, `rsa`, whose affected code is not reached. Accepted, because the client holds no private key. `pip-audit` of the installed Python packages found nothing, but they have no lock file |
| Independent security review | ❓ | 📅 Not done |
| Viewer access | ❓ | 🚧 Local only, with a host check and a content security policy. It has no login |

### Reliability

| Check | DBOS | rust-dwos |
|---|---|---|
| `kill -9`, pause and terminate tests | ❓ | ✅ Pass on all 3. Under heavy parallel load, SQLite's 1 s test lease can lapse with no fault |
| Database restart tests | ❓ | ✅ 3 seeds clean on MySQL and 5 on Postgres. An older Postgres run failed and is unexplained |
| Long-running stress tests | ❓ | 🚧 The stress tests pass. Nothing has run for hours or days |
| DBOS's own tests | ✅ | 🚧 A subset is adapted, see [`MANIFEST.md`](tests/upstream/MANIFEST.md) |
| Production use | ✅ | ❌ Not used in production yet |

### Operations

| Check | DBOS | rust-dwos |
|---|---|---|
| OpenTelemetry tracing | ✅ | ✅ `otlp_*` config, `DBOS.span`, `PropagateOtelContext`, spans for workflows and steps. Optional: a clean no-op when opentelemetry is not installed |
| `log_level`, `console_log_level` | ✅ | ✅ Applied at launch |
| Custom `Serializer` | ✅ | ✅ A custom format tag is stored per value. A process without that serializer raises on those rows, as pickle rows already do |
| OpenAI Agents, Google ADK, LlamaIndex integrations | ✅ | 📅 |
| Web view of workflows | ✅ Through the DBOS Console | ✅ Read-only, built in |
| Conductor (DBOS's hosted service) and what depends on it | ✅ | ❌ Not built. It is DBOS's own service: connection, alerting, Prometheus through Conductor, retention, autoscaling, audit logs, RBAC, API keys, `dbosctl`, MCP |
| Schema upgrades that keep existing rows | ✅ | ✅ Numbered migrations |
| Reading rows written by another SDK | ✅ | ❌ Not supported. The tables are not DBOS's `dbos.` schema |

### Design differences

| Topic | DBOS | rust-dwos |
|---|---|---|
| Core language | Python | Rust, with a Python SDK |
| Crash detection | Executor IDs and ownership tokens | Leases that expire |
| Custom exceptions after replay | Restored as their own type | Restored when the class is loaded and can be rebuilt. Otherwise `DBOSRecordedError` |

## Refused or turned off

What this project rejects or does not run, so nothing fails silently.

| What | What happens |
|---|---|
| Unknown `DBOSConfig` keys | `DBOSException` naming the keys |
| Queue options that are not supported | `DBOSException` naming the options |
| Using DBOS after `os.fork` | `DBOSException`. Call `DBOS.launch()` in the child |
| MessagePack | Not built. The user deferred it |
| Python chaos tests (`tests/chaos`) | Not in the normal test run. See `PROGRESS.md` for how to run them |
| Ignored tests | `cargo test` reports the ignored Rust tests. See `PROGRESS.md` |
| Dropped upstream tests | Listed with reasons in [`MANIFEST.md`](tests/upstream/MANIFEST.md) |

## Guarantees

| Topic | Guarantee |
|---|---|
| Workflow result | Each workflow ID reaches one final state, which never changes |
| Workflow code | Runs at least once, and may replay |
| Steps | At least once. A crash after a side effect but before the save runs that step again |
| Saved steps | Never run again for that workflow |
| Stale owners | A process that lost its lease cannot save a step or a result |

A step that charges a card or sends email still needs an idempotency key at that service. `DBOS.workflow_id` and `DBOS.step_id` give a stable key.

## What can be stored

Values are stored with pickle by default, so most Python values come back as they were, with no annotations.

- **Trust:** loading a pickle can run code. Anyone who can write to this database could run code on every process that reads it, so keep write access to admins and your own program.
- **Renames:** a pickle stores the path to its class. Renaming or moving a class can break unfinished workflows when they recover.
- **Exceptions:** attributes are kept, but `__cause__` and tracebacks are lost. An exception that cannot be rebuilt comes back as `DBOSRecordedError`.
- **Viewer:** the web viewer shows pickled values as base64 and never loads them.

### Using JSON instead

```python
DBOS(config={"name": "my-app", "system_database_url": url, "serializer": "json"})
```

With JSON, a database row cannot run code, and the viewer shows readable values. A row's tag cannot turn pickle on: a JSON process refuses pickled values and leaves pickled workflows unclaimed. Annotate your functions to keep rich types, for example `-> Order` or `tuple[int, str]`.

| Value, with JSON | Result |
|---|---|
| `NaN`, `Infinity`, non-UTF-8 `bytes`, arbitrary class instances | `TypeError`, nothing is stored |
| `tuple` or `set` with no annotation | Comes back as `list` |
| `datetime`, `UUID`, `Decimal`, `bytes` with no annotation | Comes back as `str` |
| Dataclass or Pydantic model with no annotation | Comes back as `dict` |
| Non-string dict keys with no annotation | Come back as strings |
| Exception attributes that are not JSON, and `__cause__` | Dropped |
| Exception whose class is not loaded in the process | Comes back as `DBOSRecordedError` |

Each workflow keeps the format it started with, so you can switch without breaking old rows.

## Try it

There is no release yet. You need Rust, [uv](https://docs.astral.sh/uv/), and Python 3.14. The install name is `rust-dwos`, the import is `rust_dwos`, and the class is `DBOS`.

```sh
uv venv --python 3.14 .venv
source .venv/bin/activate
uv pip install maturin
maturin develop
```

```python
import asyncio

from rust_dwos import DBOS, SetWorkflowID


@DBOS.step()
async def add_one(value: int) -> int:
    return value + 1


@DBOS.workflow()
async def double_after_add(value: int) -> int:
    return await add_one(value) * 2


async def main() -> None:
    with SetWorkflowID("job-1"):
        print(await double_after_add(1))


DBOS(config={"name": "my-app", "system_database_url": "sqlite://app.sqlite"})
DBOS.launch()
asyncio.run(main())
DBOS.destroy()
```

For MySQL, use `mysql://user:password@host:3306/database`. For Postgres, use `postgres://user:password@host:5432/database`.

## See your workflows

A small read-only page shows workflows, their status, and each saved step. Your program never needs it.

```sh
python -m rust_dwos.ui --db sqlite://app.sqlite --port 8000
```

Then open <http://127.0.0.1:8000>. Stored values are shown as stored, without decoding. It answers only on your own machine and has no login, so do not expose the port without adding one. A database URL given with `--db` is visible to other users of the machine in the process list.

**SQLite warning:** do not open the workflow database with Python's `sqlite3` in a process that also runs this library. The library has its own copy of SQLite, and two copies in one process can corrupt the file.

## Tests

The same tests run against MySQL and SQLite. `tests/upstream` holds tests adapted from DBOS. [`MANIFEST.md`](tests/upstream/MANIFEST.md) records each one. Those tests are a record of what was copied. They are not the product plan.

## Licence

Apache 2.0. Tests in `tests/upstream` are adapted from DBOS under the MIT licence. See [`NOTICE`](NOTICE) and [`tests/upstream/LICENSE-DBOS`](tests/upstream/LICENSE-DBOS).
