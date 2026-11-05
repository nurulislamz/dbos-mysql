# rust-dwos

Durable workflows on Postgres, MySQL 8.4 and SQLite. A Rust core decides what runs. A thin Python layer only calls your function and returns the result. Finished steps are saved, so a crash resumes without repeating them.

> **Unofficial and not production ready.** This project is not affiliated with DBOS, Inc. Do not run payment or email steps on it yet.

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

The plan is [`docs/PLAN.md`](docs/PLAN.md). Status is in [`PROGRESS.md`](PROGRESS.md), including which databases work so far.

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
| Postgres | ✅ | 🚧 Newest features not tested here. A restart incident is unexplained |
| SQLite | ✅ | ✅ One machine only |
| MySQL 8.4 | ❌ | ✅ |

### Core workflows

| Feature | DBOS | rust-dwos |
|---|---|---|
| Workflow and step state storage | ✅ | ✅ Rust store |
| Stale owners blocked from saving results | ✅ | ✅ Rust store |
| `@DBOS.workflow()`, `@DBOS.step()` | ✅ | ✅ |
| Async and sync functions | ✅ | ✅ |
| `SetWorkflowID`, one run per workflow ID | ✅ | ✅ |
| Replaying saved steps and stored errors | ✅ | ✅ |
| Changed code detected during replay | ✅ | ✅ |
| Step retries | ✅ | ✅ |
| Per-attempt step timeout (`timeout_seconds`) | ❓ | ✅ A timed-out sync body keeps running on its thread |
| Step heartbeat (`DBOS.heartbeat()`) | ❓ | ✅ Separate from lease renewal |
| Child workflows | ✅ | ✅ |
| `start_workflow`, `retrieve_workflow`, handles | ✅ | ✅ |
| `list_workflows`, `list_workflow_steps` | ✅ | ✅ |
| Crash recovery | ✅ | 🚧 Crash tests last ran before the newest features |
| Recovery attempt limit | ✅ | ✅ |

### Coordination and control

| Feature | DBOS | rust-dwos |
|---|---|---|
| Durable sleep | ✅ | ✅ |
| `send`, `recv` | ✅ | ✅ |
| `set_event`, `get_event` | ✅ | ✅ |
| Workflow timeouts | ✅ | ✅ |
| Queues | ✅ | ✅ FIFO, with one global concurrency limit |
| Queue rate limits, priorities, partitions | ❓ | 📅 Deferred |
| Cancel and resume | ✅ | ✅ |
| Fork and rewind | ✅ | 📅 |
| Deleting old workflows | ✅ | 📅 |
| `run_step`, `wait_first` | ✅ | 📅 |
| Schedules | ✅ | 📅 Not started |
| Streams | ✅ | 📅 |
| Debouncer | ✅ | 📅 |
| External client (`DBOSClient`) | ✅ | 📅 |
| App database transactions (datasources) | ✅ | 📅 |
| Patching and app versions | ✅ | 📅 |
| Pydantic AI integration | ✅ | 📅 |
| Kafka | ✅ | ❌ |
| Class-based workflows | ✅ | ❌ |
| External object storage | ✅ | ❌ |
| Sharding | ✅ | ❌ |

### Performance

No benchmark exists in this repository, and nothing has been measured side by side with DBOS. The times below are single samples on SQLite on a busy machine.

| Check | DBOS | rust-dwos |
|---|---|---|
| Workflows and steps per second | ❓ | ❓ Not measured |
| Extra time per step | ❓ | ❓ Not measured |
| Memory and connections per process | ❓ | ❓ Not measured |
| Behaviour with a large backlog | ❓ | ❓ Not measured. Recovery takes at most 50 workflows per interval |
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
| Encrypted database connections | ❓ | 🚧 TLS is compiled in. Not tested |
| Memory safety | ❓ | ✅ `dwos-core` forbids `unsafe` code |
| Dependency audit | ❓ | 📅 Not run |
| Independent security review | ❓ | 📅 Not done |
| Viewer access | ❓ | 🚧 Local only, with a host check and a content security policy. It has no login |

### Reliability

| Check | DBOS | rust-dwos |
|---|---|---|
| `kill -9`, pause and terminate tests | ❓ | 🚧 Passed on SQLite and MySQL before the newest features |
| Database restart tests | ❓ | 🚧 Passed on MySQL. One Postgres rerun failed |
| Long-running stress tests | ❓ | 🚧 One SQLite stress test hung in 1 of 3 runs. Cause unknown |
| DBOS's own tests | ✅ | 🚧 A subset is adapted, see [`MANIFEST.md`](tests/upstream/MANIFEST.md) |
| Production use | ❓ | ❌ Not production ready |

### Operations

| Check | DBOS | rust-dwos |
|---|---|---|
| OpenTelemetry tracing | ✅ | ❌ |
| Web view of workflows | ❓ | ✅ Read-only |
| Schema upgrades that keep existing rows | ❓ | ✅ Numbered migrations |
| Reading rows written by another SDK | ❓ | ❓ Not tested |

### Design differences

| Topic | DBOS | rust-dwos |
|---|---|---|
| Core language | Python | Rust, with a Python SDK |
| Crash detection | Executor IDs and ownership tokens | Leases that expire |
| Custom exceptions after replay | Restored as their own type | Restored when the class is loaded and can be rebuilt. Otherwise `DBOSRecordedError` |

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
