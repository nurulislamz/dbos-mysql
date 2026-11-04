# rust-dwos

Durable workflows on Postgres, MySQL 8.4 and SQLite. A Rust core decides what runs. A thin Python layer only calls your function and returns the result. Finished steps are stored as JSON, so a crash resumes without repeating them.

> **Unofficial and not production ready.** This project is not affiliated with DBOS, Inc. Do not run payment or email steps on it yet.

There is no extra server. A running copy of your program is the coordinator. When every copy is stopped, the rows wait until you start one again.

## Direction

| Piece | Role |
|---|---|
| Rust core | Decides the step, the saved result, and the lease. Writes and reads the rows |
| Python | Runs your function and returns the result. Holds no step, result, or lease decisions |
| Postgres, MySQL 8.4 or SQLite | Stores the workflow and each finished step as JSON |
| TypeScript SDK | Not part of this version |
| Queues and durable sleep | Not part of this version |

Crash detection uses leases that expire. A dead process loses the workflow, and another running copy can take it.

## Done when

- Killing the process mid-workflow leaves that workflow unfinished.
- Starting the program again finishes it without repeating finished steps.
- A shutdown is not stored as a final error.
- Python contains no step, result, or lease decisions.

The plan is [`docs/PLAN.md`](docs/PLAN.md). Status is in [`PROGRESS.md`](PROGRESS.md), including which databases work so far.

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

Values are stored as JSON, not pickle, so a database row can never run code. Annotate your functions to keep rich types, for example `-> Order` or `tuple[int, str]`.

| Value | Result |
|---|---|
| `NaN`, `Infinity`, non-UTF-8 `bytes`, arbitrary class instances | `TypeError`, nothing is stored |
| `tuple` or `set` with no annotation | Comes back as `list` |
| `datetime`, `UUID`, `Decimal`, `bytes` with no annotation | Comes back as `str` |
| Dataclass or Pydantic model with no annotation | Comes back as `dict` |
| Non-string dict keys with no annotation | Come back as strings |
| Exception attributes that are not JSON, and `__cause__` | Dropped |
| Exception whose class is not loaded in the process | Comes back as `DBOSRecordedError` |

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

**SQLite warning:** do not open the workflow database with Python's `sqlite3` in a process that also runs this library. The library has its own copy of SQLite, and two copies in one process can corrupt the file.

## Tests

The same tests run against MySQL and SQLite. `tests/upstream` holds tests adapted from DBOS. [`MANIFEST.md`](tests/upstream/MANIFEST.md) records each one. Those tests are a record of what was copied. They are not the product plan.

## Licence

Apache 2.0. Tests in `tests/upstream` are adapted from DBOS under the MIT licence. See [`NOTICE`](NOTICE) and [`tests/upstream/LICENSE-DBOS`](tests/upstream/LICENSE-DBOS).
