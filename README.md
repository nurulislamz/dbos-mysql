# dbos-mysql

Durable workflows for Python on MySQL 8.4 and SQLite, with a Rust core and an API that follows [DBOS](https://github.com/dbos-inc/dbos-transact-py). Workflow state is stored in the database, so a workflow survives a crash and resumes without repeating finished steps.

> **Unofficial and not production ready.** This project is not affiliated with DBOS, Inc. It is new code, plus tests adapted from DBOS under the MIT licence. It is under active development, and crash recovery has not yet passed load and fault testing. Do not run payment or email steps on it yet.

## Feature checklist

Compared with [`dbos-transact-py`](https://github.com/dbos-inc/dbos-transact-py) at commit `236142b`, from its public API.

| Mark | Meaning |
|---|---|
| ✅ | Built and passing its own tests |
| 🚧 | Written, but not fully verified yet |
| 📅 | Planned after 0.1 |
| ❌ | Not planned |

### Databases

| Feature | DBOS | dbos-mysql |
|---|---|---|
| Postgres | ✅ | ❌ |
| SQLite | ✅ | ✅ One machine only |
| MySQL 8.4 | ❌ | ✅ |

### Core workflows

| Feature | DBOS | dbos-mysql |
|---|---|---|
| Workflow and step state storage | ✅ | ✅ Rust store |
| Stale owners blocked from saving results | ✅ | ✅ Rust store |
| `@DBOS.workflow()`, `@DBOS.step()` | ✅ | ✅ |
| Async and sync functions | ✅ | ✅ |
| `SetWorkflowID`, one run per workflow ID | ✅ | ✅ |
| Replaying saved steps and stored errors | ✅ | ✅ |
| Changed code detected during replay | ✅ | ✅ |
| Step retries | ✅ | ✅ |
| Child workflows | ✅ | ✅ |
| `start_workflow`, `retrieve_workflow`, handles | ✅ | ✅ |
| `list_workflows`, `list_workflow_steps` | ✅ | ✅ No filters |
| Crash recovery | ✅ | 🚧 Passes crash tests, but load and fault testing is not done |
| Recovery attempt limit | ✅ | ✅ |

### Coordination and control

| Feature | DBOS | dbos-mysql |
|---|---|---|
| Durable sleep | ✅ | 📅 |
| `send`, `recv` | ✅ | 📅 |
| `set_event`, `get_event` | ✅ | 📅 |
| Workflow timeouts | ✅ | 📅 |
| Queues | ✅ | 📅 |
| Cancel and resume | ✅ | 📅 |
| Fork and rewind | ✅ | 📅 |
| Deleting old workflows | ✅ | 📅 Needed for 0.1 |
| `run_step`, `wait_first` | ✅ | 📅 |
| Schedules | ✅ | 📅 |
| Streams | ✅ | 📅 |
| Debouncer | ✅ | 📅 |
| External client (`DBOSClient`) | ✅ | 📅 |
| App database transactions (datasources) | ✅ | 📅 |
| Patching and app versions | ✅ | 📅 |
| Pydantic AI integration | ✅ | 📅 |
| Kafka | ✅ | ❌ |
| OpenTelemetry tracing | ✅ | ❌ |
| Authentication and roles | ✅ | ❌ |
| Class-based workflows | ✅ | ❌ |
| External object storage | ✅ | ❌ |
| Sharding | ✅ | ❌ |

### Design differences

| Topic | DBOS | dbos-mysql |
|---|---|---|
| Core language | Python | Rust, with a Python SDK |
| Crash detection | Executor IDs and ownership tokens | Leases that expire |
| Default stored format | Pickle | JSON through Pydantic, so stored data cannot run code |
| Custom exceptions after replay | Restored as their own type | `DBOSRecordedError`. Built-in types are restored |

## Guarantees

| Topic | Guarantee |
|---|---|
| Workflow result | Each workflow ID reaches one final state, which never changes |
| Workflow code | Runs at least once, and may replay |
| Steps | **At least once.** A crash after a side effect but before the save runs that step again |
| Saved steps | Never run again for that workflow |
| Stale owners | A process that lost its lease cannot save a step or a result |

A step sending email or taking payment still needs an idempotency key at the outside service. `DBOS.workflow_id` and `DBOS.step_id` give a stable key.

## Try it

There is no release yet, so build it from source. You need Rust, [`uv`](https://docs.astral.sh/uv/) and Python 3.14.

```sh
uv venv --python 3.14 .venv
source .venv/bin/activate
uv pip install maturin pydantic
maturin develop
```

```python
import asyncio

from dbos_mysql import DBOS, SetWorkflowID


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

For MySQL, use `mysql://user:password@host:3306/database`.

**SQLite warning:** never open the workflow database with Python's `sqlite3` module in a process that also runs this library. The library has its own copy of SQLite, and two copies in one process can corrupt the file. Use a separate process to inspect it.

## Testing

The same tests run against MySQL and SQLite. `tests/upstream` holds tests adapted from DBOS, and [`MANIFEST.md`](tests/upstream/MANIFEST.md) records what happened to each one.

## Project plan

See [`docs/PLAN.md`](docs/PLAN.md).

## Licence

Apache 2.0. Tests in `tests/upstream` are adapted from DBOS under the MIT licence; see [`NOTICE`](NOTICE) and [`tests/upstream/LICENSE-DBOS`](tests/upstream/LICENSE-DBOS).
