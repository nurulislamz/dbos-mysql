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

Queues and durable sleep come after this bar.

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

What may differ per database: the schema file, "now in milliseconds", the insert-if-absent clause, the row lock clause, the transaction start, connection settings, and which errors count as retryable.

## Behaviour we keep

| Topic | Behaviour |
|---|---|
| Workflow ID | One run per ID. The same ID returns the stored result or error. Another workflow name on that ID is a conflict |
| Steps | Saved steps never run again. An unsaved step may run again after a crash |
| Step IDs | Start at 1, in call order. Rust assigns them |
| Crash detection | A lease with an expiry, not an executor ID. A surviving copy can take over without a server |
| Stored values | JSON text (`portable_json`). Not pickle |
| Stale owner | A process that lost its lease cannot save a step or a result |
| Recovery limit | Too many recoveries end as `MAX_RECOVERY_ATTEMPTS_EXCEEDED` |

## Not in this version

Queues, durable sleep, send and receive, events, schedules, streams, cancel and resume, fork, a TypeScript SDK, and a separate coordinator.

## Tests

`tests/upstream` holds tests copied from DBOS, and its manifest says what changed. A test that hides a missing feature does not count as that feature. `tests/chaos` injects real faults.

## Web viewer

`python -m rust_dwos.ui` is an optional, read-only page for looking at workflows and steps. Programs never need it. It listens on `127.0.0.1` with no login, so do not expose it without adding one.
