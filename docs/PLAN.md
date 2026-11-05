# rust-dwos: plan

Durable workflows on Postgres, MySQL 8.4 and SQLite. Unofficial, and not affiliated with DBOS, Inc.

- A Rust core stores the workflow and its steps, and decides what still has to run.
- A Python SDK calls that core. The API behaves like DBOS.
- The program itself is enough. There is no extra server.

Status and task order are in [`PROGRESS.md`](../PROGRESS.md).

## Done when

- Killing the process mid-workflow leaves that workflow unfinished.
- Starting the program again finishes it without repeating finished steps.
- A shutdown is not stored as a final error.
- Python contains no step, result, or lease decisions.
- All of this passes on all three databases.

Core work now includes durable sleep, workflow deadlines, events/messages, workflow management and durable FIFO queues. The user deferred SDK integration and restricted current verification to Rust.

## Architecture

```text
your process
┌──────────────────────────────────────────────┐
│  your workflow code                          │
│  Python: runs the function, returns result   │
│  Rust core: which step, already saved?,      │
│             whose lease, write or read row   │
└────────────────────┬─────────────────────────┘
                     ▼
        Postgres, MySQL 8.4 or SQLite
```

Python calls Rust. Rust never calls Python. Python asks what to run, runs it, and hands back the result.

## What goes where

**Python holds only what needs the Python interpreter. If an SDK in another language would have to rewrite it, it belongs in Rust.**

Two checks apply the rule:

1. Would a TypeScript SDK need the same logic? If yes, it goes in Rust.
2. Can the rule be tested in Rust without Python? If it can only be tested from Python, it is in the wrong place.

| Python keeps | Why it needs Python |
|---|---|
| `DBOS` class, decorators, handles | They are the Python API |
| Calling the function and catching its exceptions | Only Python can run it |
| Current workflow and step (`contextvars`) | They follow Python's async tasks |
| Converting values and errors to stored text | It works on Python objects and type hints |
| `asyncio` tasks, threads and the actual sleep call | They belong to Python's event loop |
| Mapping workflow names to functions | Those are Python functions |

Rust holds SQL, step IDs, replay, leases, recovery, retry limits and delays, recovery limits, stored statuses and formats, and what happens when a workflow ends.

`tests/test_boundary.py` fails if `python/rust_dwos/` imports a database driver, contains SQL, or calls something `_native.pyi` does not declare. It cannot catch every misplaced decision, so reviews check the rule too.

## Where a new feature goes

Each feature gets its own files, so features can be built in parallel without conflicts:

| Part | File |
|---|---|
| SQL | `crates/dwos-core/src/store/<feature>.rs`, as an `impl Store` block |
| Decisions | `crates/dwos-core/src/engine/<feature>.rs`, as an `impl Engine` block |
| Tables | A new numbered `schema/<database>/NNN_<change>.sql` for each database, listed in `crates/dwos-core/src/store/migrations.rs`. Never edit a file that has shipped, including `001_init.sql`. Migrations must keep every row; see [`dwos-core/README.md`](../crates/dwos-core/README.md#schema-upgrades) |
| Python API | `python/rust_dwos/<feature>.py`, as a class that `DBOS` inherits from |
| Bindings | `crates/dwos-py/src/<feature>.rs`, as its own `#[pymethods] impl PyEngine` block |

## Reuse

We do not fork `dbos-transact-rust`. It is Postgres only, its storage trait has 70 methods, and its engine runs Rust workflows. We reuse DBOS's table names, statuses, replay rules and tests. Code copied from it needs a `NOTICE` entry.

## One store for all databases

`store.rs` holds the logic and SQL once, through the `sqlx` `Any` driver. Checked on all three in task 0.

| Rule | Why |
|---|---|
| Write SQL with `?`. The store rewrites it to `$n` for Postgres | `Any` does not translate placeholders |
| After an insert-if-absent, compare `lease_token` with our token | A duplicate insert reports 1 row on MySQL and 0 on the others |
| Bind only `&str`, `Option<&str>`, `i32`, `i64` | `Any` has no unsigned types |
| Postgres text columns are `TEXT`, never `CHAR` | `Any` cannot read Postgres `CHAR` |
| Read the time from the database clock | Machines can disagree on the time |

What may differ per database: the schema and migration files, "now in milliseconds", the insert-if-absent clause, the row lock clause, the transaction start, connection settings, and which errors count as retryable.

## Durable queues

The core now offers durable FIFO enqueue and global lease-owner concurrency. Queue metadata and membership use new companion tables; workflow identity, status, results and ownership remain in the existing workflow row. A queue transaction revokes expired owners, counts all remaining tokens and admits compatible work. Ordinary recovery and direct resume claims exclude queue members. Relative timeout budgets begin on first admission; inherited deadlines apply while waiting.

Stop old workers before upgrading the schema or using queues: older claim SQL does not enforce queue admission, and old workers do not read the schema version. The core API and remaining SDK obligations are described in [`dwos-core/README.md`](../crates/dwos-core/README.md).

## Behaviour we keep

| Topic | Behaviour |
|---|---|
| Workflow ID | One run per ID. The same ID returns the stored result or error. Another workflow name on that ID is a conflict |
| Steps | Saved steps never run again. An unsaved step may run again after a crash |
| Step IDs | Start at 1, in call order. Rust assigns them |
| Crash detection | A lease with an expiry, not an executor ID. A surviving copy can take over without a server |
| Stored values | Pickle by default, because only admins and the program can write to the database. `"serializer": "json"` stores `portable_json` instead, and a JSON process never loads a pickle, whatever a row's tag says. A workflow keeps the format it started with |
| Stale owner | A process that lost its lease cannot save a step or a result |
| Recovery limit | Too many recoveries end as `MAX_RECOVERY_ATTEMPTS_EXCEEDED` |

## Not in this version

Streams, fork, a TypeScript SDK, and a separate coordinator. Schedules are five-field UTC crons: a new schedule does not run a tick that has already passed (changing the cron counts as a new schedule), and a later tick is skipped while a previous run of that schedule is still pending. Queue rate limits, priorities, partitions and runtime reconfiguration are outside the current basic FIFO/global-concurrency scope. MessagePack is deferred by the user.

## Tests

`tests/upstream` holds tests copied from DBOS, and its manifest says what changed. A test that hides a missing feature does not count as that feature. `tests/chaos` injects real faults.

## Web viewer

`python -m rust_dwos.ui` is an optional, read-only page for looking at workflows and steps. Programs never need it. It listens on `127.0.0.1` with no login, so do not expose it without adding one.
