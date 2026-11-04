# dbos-mysql: plan

Durable workflows for Python on MySQL 8.4 and SQLite, with a Rust core and a Python SDK that follows the DBOS API. A workflow's state is stored in the database, so it survives a crash and resumes without repeating finished steps.

This is an unofficial project and is not affiliated with DBOS, Inc. It is new code, plus tests adapted from DBOS under the MIT licence.

Status: planning, no code yet. Plan date: 2026-11-04.

## 1. What we are building first

The first usable version does five things:

1. Stores workflow and step state in MySQL or SQLite, chosen by the database URL.
2. Runs async workflows made of steps.
3. Returns saved results when a workflow ID or step is seen again.
4. Stores failures, and replays them the same way.
5. Recovers workflows from a crashed process. MySQL covers the same or another machine, and SQLite covers one machine.

**Usable means:** you can `pip install` a wheel, follow the README, `kill -9` the example mid-workflow, start it again, and see it finish without repeating finished steps.

### Left out until the first version works

Each has a matching DBOS test file, so adding one later starts with copying its tests.

| Feature | Upstream Python tests |
|---|---|
| Queues | `test_queue.py` |
| Durable sleep, send/recv, events | Parts of `test_dbos.py` and `test_async.py` |
| Workflow timeouts | Parts of `test_dbos.py` and `test_async.py` |
| Cancel, resume, list, fork | `test_workflow_management.py`, `test_rewind.py` |
| Schedules | `test_scheduler.py` |
| Streaming | `test_streaming.py` |
| Debouncer | `test_debouncer.py` |
| External client | `test_client.py` |
| App databases and transactions | `test_datasource.py` |
| Telemetry, conductor, Kafka, admin server | Not planned |

Pydantic AI support and integration with any app also come after this plan.

## 2. Which tests we copy

**Copy from [`dbos-transact-py`](https://github.com/dbos-inc/dbos-transact-py), and use [`dbos-transact-ts`](https://github.com/dbos-inc/dbos-transact-ts) for extra scenarios.**

| Repo | Test format | How we use it |
|---|---|---|
| `dbos-transact-py` | pytest, same language and near-identical API | Copy files and adapt them |
| `dbos-transact-ts` | Jest and TypeScript | Cannot run against a Python SDK. Rewrite chosen scenarios in pytest, mainly `timinghole.test.ts` and `oaoo.test.ts` |

Both are MIT licensed, so copying is allowed if we keep their copyright and licence text. Those go in `tests/upstream/LICENSE-DBOS`, with a mention in `NOTICE`.

### What copying needs

Copied tests look like this:

```python
@DBOS.workflow()
async def test_workflow(var1: str, var2: str) -> str:
    res = await test_step(var2)
    return var1 + res

with SetWorkflowID(wfuuid):
    result = await test_workflow("alice", "bob")
```

So the SDK has to follow the DBOS API and behaviour for everything in scope. Otherwise every test becomes a rewrite.

The tests still will not run unchanged:

- Imports change from `dbos` to `dbos_mysql`.
- `conftest.py` is new, because the upstream one is built around Postgres and SQLite.
- Tests that read internal tables or call `dbos._sys_db` get rewritten.
- Recovery tests get adapted, because we use leases where DBOS tests use executor IDs.
- Tests that rely on pickle behaviour are adapted or dropped.

`tests/upstream/MANIFEST.md` lists every copied test with its source file, upstream commit, status (`passing`, `adapted`, `rewritten` or `dropped`) and the reason for any change. A test is never dropped without a reason.

### Test tiers

Each tier belongs to a milestone. Names are upstream test names. Some may need dropping once they are read in full, and the manifest records that.

**Tier 1: state and replay (M3)**

| Source | Tests |
|---|---|
| `test_async.py` | `test_async_workflow`, `test_async_step`, `test_async_completed_replay_no_hook_rerun` |
| `test_dbos.py`, converted to async | `test_simple_workflow`, `test_exception_workflow`, `test_workflow_returns_none`, `test_custom_names`, `test_step_without_dbos`, `test_nested_steps`, `test_duplicate_registration` |
| `test_failures.py` | `test_nondeterministic_workflow`, `test_error_serialization`, `test_workflow_error_serialization`, `test_nonserializable_return` |
| `oaoo.test.ts`, rewritten | `step-oaoo`, `workflow-oaoo`, `reject-name-mismatch-throws-in-use` |

**Tier 2: handles, children, step retries (M4)**

| Source | Tests |
|---|---|
| `test_async.py` | `test_start_workflow_async`, `test_retrieve_workflow_async`, `test_child_workflow_async`, `test_unawaited_workflow`, `test_unawaited_workflow_exception`, `test_workflow_with_task_cancellation`, `test_asyncio_wait`, `test_asyncio_wait_all_completed`, `test_asyncio_wait_first_exception`, `test_asyncio_wait_timeout`, `test_main_loop` |
| `test_dbos.py`, converted to async | `test_child_workflow`, `test_child_workflow_assigned_id`, `test_retrieve_workflow`, `test_simple_workflow_attempts_counter` |
| `test_failures.py` | `test_step_retries`, `test_step_retries_no_final_sleep_async`, `test_step_status` |

**Tier 3: recovery and stale owners (M5)**

| Source | Tests |
|---|---|
| `test_async.py` | `test_workflow_recovery_async` |
| `test_dbos.py`, converted | `test_recovery_workflow`, `test_recovery_workflow_step` |
| `test_failures.py` | `test_recovery_attempts`, `test_dead_letter_queue`, `test_recovery_during_retries` |
| `timinghole.test.ts`, rewritten | `stale-owner-cannot-write-the-outcome`, `step-conc`, `step-recorded-twice-by-the-owner-is-nondeterminism` |
| `recovery.test.ts`, rewritten | `local-recovery`, `dead-letter-queue`, `child-workflow-recovery` |
| `chaos-tests/test_workflows.py` | `test_workflow`, kept as a real chaos test |

## 3. Behaviour

We match DBOS unless this table says otherwise.

| Topic | Behaviour | Same as DBOS? |
|---|---|---|
| Workflow ID | Reusing an ID returns the stored result or error. Reusing it with another workflow's name raises a conflict | Yes |
| Calling a workflow | Runs in the caller's task, and is saved | Yes |
| `start_workflow_async` | Runs as a background task in the same process | Yes |
| Workflow exception | Final `ERROR`, stored and raised again on replay. Only crashes are recovered | Yes |
| Step exception | Stored after its retries, and raised again on replay. A workflow can catch it and continue | Yes |
| Step IDs | Start at 1, taken when the step is called, so `asyncio.gather` keeps a stable order | Yes |
| Changed code | A saved step name that differs from the running code raises an error | Yes |
| Nested steps | A step called within a step runs as a plain function | Yes |
| Too many recoveries | Ends as `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | Yes |
| Crash detection | **Leases with expiry, not executor IDs** | No |
| Stored format | **JSON through Pydantic, not pickle** | No |
| Recovered errors | **Built-in exception types come back as themselves, others as `DBOSRecordedError`** | No |
| Sync functions | **Async first, sync added in M6** | Not at first |

### Guarantees

| Topic | Guarantee |
|---|---|
| Workflow result | Each workflow ID reaches one final state, which never changes |
| Workflow code | Runs at least once, and may replay |
| Steps | **At least once.** A crash after a side effect but before the save runs that step again |
| Saved steps | Never run again for that workflow |
| Stale owners | A process that lost its lease cannot save a step or a result |

A step sending email or taking payment still needs an idempotency key at the outside service. `DBOS.step_id` and `DBOS.workflow_id` give a stable key. No engine can remove this limit, including DBOS.

### Workflow code rules

Steps are matched by call order, so anything that can change between runs goes in a step: time, random values, database reads and HTTP calls.

## 4. Architecture

The library is a compiled Python extension inside your process. There is no separate service.

```text
┌───────────────────── your Python process ─────────────────────┐
│  python/dbos_mysql                                            │
│  decorators · registry · JSON · calling your functions        │
│                         │ IDs and JSON strings only           │
│  crates/dbos-mysql-py   PyO3 bindings, no logic               │
│                         │                                     │
│  crates/dbos-mysql-core Tokio threads                         │
│  engine decisions · SQL · leases · recovery · DB error retries│
└─────────────────────────┼─────────────────────────────────────┘
                          ▼
               MySQL 8.4 or SQLite
```

### Who owns what

| Concern | Owner |
|---|---|
| Run, attach or return stored; child IDs and child pointers | Rust, `engine.rs` |
| Step IDs and replay decisions | Rust, `engine.rs` |
| Step retry timing and attempt accounting | Rust, `engine.rs` |
| Waiting for results; a lost lease at finish | Rust, `engine.rs` |
| Recovery schedule, limit and reclaim | Rust, `engine.rs` |
| SQL and leases | Rust, `store.rs`, `mysql.rs`, `sqlite.rs`, `lease.rs` |
| Decorators and the function registry | Python |
| Calling your functions, including `asyncio` tasks and threads | Python |
| Cancelling a body when Rust reports a lost lease | Python |
| Context variables such as `DBOS.workflow_id` | Python |
| Converting values and exceptions to and from JSON | Python |

### Repository layout

```text
Cargo.toml
Cargo.lock
pyproject.toml
compose.yml
NOTICE
schema/mysql/001_init.sql
schema/sqlite/001_init.sql
crates/dbos-mysql-core/src/
    lib.rs  engine.rs  error.rs  store.rs  mysql.rs  sqlite.rs  lease.rs  retry.rs  workflow.rs
crates/dbos-mysql-core/tests/
crates/dbos-mysql-py/src/lib.rs
python/dbos_mysql/
    __init__.py  _native.pyi  py.typed
    dbos.py  serialization.py  errors.py
tests/
    conftest.py
    upstream/
        LICENSE-DBOS  MANIFEST.md
        test_*.py
    chaos/
examples/resume_after_crash.py
```

### Stack

| Part | Choice | Checked version |
|---|---|---|
| Database | `sqlx` with runtime queries, so builds need no database | 0.9.0 |
| Runtime | `tokio` | 1.54.1 |
| Bindings | `pyo3`, `pyo3-async-runtimes` | 0.30.1, 0.30.0 |
| Build | `maturin` | 1.16.0 |
| Python | CPython 3.14 | 3.14.8 |
| Tests | `pytest`, `pytest-asyncio`, Rust integration tests | Latest at M0 |

## 5. Public API

```python
from dbos_mysql import DBOS, SetWorkflowID

DBOS(config={"name": "my-app", "system_database_url": "mysql://user:password@host:3306/app"})


@DBOS.step(retries_allowed=True, max_attempts=3)
async def fetch_text(job_id: str) -> str: ...


@DBOS.step()
async def summarise(text: str) -> Summary: ...


@DBOS.workflow(max_recovery_attempts=10)
async def summarise_job(job_id: str) -> Summary:
    text = await fetch_text(job_id)
    return await summarise(text)


DBOS.launch()

with SetWorkflowID("job-1"):
    summary = await summarise_job("job-1")

handle = await DBOS.start_workflow_async(summarise_job, "job-2")
status = await handle.get_status()
summary = await handle.get_result()

handle = await DBOS.retrieve_workflow_async(handle.workflow_id)

DBOS.destroy()
```

First version scope:

- `DBOS(config=...)`, `DBOS.launch()`, `DBOS.destroy()`
- `@DBOS.workflow()`, `@DBOS.step()`
- `SetWorkflowID`
- `DBOS.start_workflow_async`, `DBOS.retrieve_workflow_async`
- `DBOS.workflow_id`, `DBOS.step_id`, `DBOS.step_status`, `DBOS.logger`
- Handles with `workflow_id`, `get_result()`, `get_status()`
- Upstream's exception and status names

Option names and defaults, including `max_recovery_attempts`, are taken from upstream when each test is copied.

## 6. Storing state

```sql
CREATE TABLE dbos_workflows (
    workflow_id           VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    name                  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    status                VARCHAR(40)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    input                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    output                LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error                 LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    claim_after           DATETIME(6)  NULL,
    lease_token           CHAR(36)     CHARACTER SET ascii COLLATE ascii_bin NULL,
    recovery_attempts     INT UNSIGNED NOT NULL DEFAULT 1,
    max_recovery_attempts INT UNSIGNED NOT NULL,
    created_at            DATETIME(6)  NOT NULL,
    updated_at            DATETIME(6)  NOT NULL,
    PRIMARY KEY (workflow_id),
    KEY ix_dbos_workflows_claim (claim_after)
) ENGINE=InnoDB;

CREATE TABLE dbos_steps (
    workflow_id       VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    step_id           INT UNSIGNED NOT NULL,
    step_name         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    output            LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    error             LONGTEXT     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    child_workflow_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
    created_at        DATETIME(6)  NOT NULL,
    PRIMARY KEY (workflow_id, step_id),
    CONSTRAINT fk_dbos_steps_workflow
        FOREIGN KEY (workflow_id) REFERENCES dbos_workflows (workflow_id) ON DELETE CASCADE
) ENGINE=InnoDB;
```

Why it looks like this:

- **`claim_after`** means "another process may take this after this time". For a `PENDING` row it is the lease expiry, and for a finished row it is `NULL`. Finding crashed work then needs one indexed range. Queues can reuse it later.
- **`utf8mb4_0900_bin` collation** stops `Job-1` and `job-1` from colliding on servers with a case-insensitive default. `utf8mb4_bin` is not enough, because it pads, so `job` and `job ` would collide.
- **`LONGTEXT`** is needed because `TEXT` stops at 64 KB. The real limit is `max_allowed_packet`, and going over it must give a clear error.
- **`UTC_TIMESTAMP(6)`** is the only clock, so differences between machines cannot break leases.
- **Connection settings:** `READ COMMITTED` and `time_zone = '+00:00'`.

The SQL below is the MySQL version. Section 6a gives the SQLite differences.

The library does not create tables on its own at first. `DBOS.launch()` checks that the schema exists and raises a clear error if not. `schema/<backend>/001_init.sql` is the source, and `python -m dbos_mysql migrate` runs it.

### SQL protocol

**Start or attach.** One transaction:

```sql
INSERT INTO dbos_workflows (workflow_id, name, status, input, claim_after, lease_token,
                            max_recovery_attempts, created_at, updated_at)
VALUES (?, ?, 'PENDING', ?, UTC_TIMESTAMP(6) + INTERVAL ? SECOND, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE workflow_id = workflow_id;

SELECT name, status, output, error, lease_token FROM dbos_workflows WHERE workflow_id = ?;
```

| Result of the read | Action |
|---|---|
| Our token | We own it, so run it |
| Different name | Raise the conflict error |
| `SUCCESS` or `ERROR` | Return or raise the stored result |
| `PENDING`, another owner | Poll until it finishes |
| `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | Raise upstream's error |

**Reclaim crashed work.** A short transaction, every few seconds, with jitter:

```sql
SELECT workflow_id, name, input, recovery_attempts, max_recovery_attempts
FROM dbos_workflows
WHERE claim_after <= UTC_TIMESTAMP(6)
  AND name IN (?, ?, ?)
ORDER BY claim_after
LIMIT ?
FOR UPDATE SKIP LOCKED;

UPDATE dbos_workflows
SET lease_token = ?, recovery_attempts = recovery_attempts + 1,
    claim_after = UTC_TIMESTAMP(6) + INTERVAL ? SECOND, updated_at = UTC_TIMESTAMP(6)
WHERE workflow_id = ?;
```

- `name IN` means a process only takes work it can run, so a rolling deploy cannot burn attempts.
- Attempts are counted here, so a workflow that keeps crashing its process still stops. As upstream does, a row becomes `MAX_RECOVERY_ATTEMPTS_EXCEEDED` when attempts exceed `max_recovery_attempts + 1`, and does not run. The default limit is 100.

**Renew the lease.** Every `lease_seconds / 3`:

```sql
UPDATE dbos_workflows
SET claim_after = UTC_TIMESTAMP(6) + INTERVAL ? SECOND, updated_at = UTC_TIMESTAMP(6)
WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING';
```

**Save a step.** One transaction, with the row lock fencing stale owners:

```sql
SELECT 1 FROM dbos_workflows WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING' FOR UPDATE;

INSERT INTO dbos_steps (workflow_id, step_id, step_name, output, error, child_workflow_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE workflow_id = workflow_id;

SELECT step_name, output, error FROM dbos_steps WHERE workflow_id = ? AND step_id = ?;
```

A different token means roll back and report the lost lease. If the step already exists, as upstream treats it, an identical record counts as a retry and a different one raises a nondeterminism error. The Python SDK still decodes what it encoded, so a first run and a replay return the same value.

**Finish.** Same guard for success and error:

```sql
UPDATE dbos_workflows
SET status = ?, output = ?, error = ?, lease_token = NULL, claim_after = NULL, updated_at = UTC_TIMESTAMP(6)
WHERE workflow_id = ? AND lease_token = ? AND status = 'PENDING';
```

Zero rows means the lease was lost, and the result is discarded.

**Graceful release** on `DBOS.destroy()`: clear the token and set `claim_after` to now, so another process takes over immediately. The attempt is handed back, so deploys do not use up recoveries.

### Lost leases and stuck work

| Event | Action |
|---|---|
| Renewal matches zero rows | Cancel the local run |
| Renewal hits database errors | Keep trying, and cancel once the lease length has passed since the last success |
| `max_run_seconds` passes | Stop renewing and cancel. Default is one hour, and it can be set per workflow |
| Renewal task panics | Cancel all runs, and make `launch` state fail loudly |
| An inline caller loses its lease | It polls for the new owner's result and returns that |

`max_run_seconds` is needed because Rust keeps renewing while Python hangs. A blocked event loop cannot run cancellation, but the lease still lapses, and the token check rejects anything it writes later.

### Database errors

| Error | Handling |
|---|---|
| Deadlock (1213), lock wait timeout (1205) | Retry the transaction with jitter, up to 5 times |
| Lost connection | Reconnect and retry, up to a time limit, then raise |
| Oversized value | Permanent, clear error, never retried |
| Anything else | Permanent, with context |

Renewals use their own small connection pool, so busy work cannot starve leases.

## 6a. SQLite

Both backends implement the same `Store` operations and pass the same tests. They do not share SQL text, because the dialects differ in the places that matter most for correctness.

| Concern | MySQL | SQLite |
|---|---|---|
| Upsert | `ON DUPLICATE KEY UPDATE` | `ON CONFLICT DO NOTHING` |
| Claiming | `FOR UPDATE SKIP LOCKED` | `BEGIN IMMEDIATE`, which serialises writers |
| Lease fence | `SELECT ... FOR UPDATE` | `BEGIN IMMEDIATE` |
| Clock | `UTC_TIMESTAMP(6)` | `unixepoch('subsec')`, stored as integer milliseconds |
| Text columns | `LONGTEXT` with binary collation | `TEXT`, whose default collation is binary |
| Busy errors | Retry 1213 and 1205 | `busy_timeout` of 30 s. No extra retry yet |

SQLite connections always set `journal_mode=WAL`, `synchronous=FULL` and `foreign_keys=ON`. `synchronous=FULL` matters here: with lower settings, WAL mode can lose a committed checkpoint in a power cut, and this library exists to keep those.

**Limits:**

- One machine only. Several processes can share a file on a local disk, but never on a network filesystem such as NFS, where its locks are unreliable.
- One writer at a time, so write-heavy loads belong on MySQL.
- SQLite is compiled into the wheel, so there is nothing else to install.

**Testing:** every Rust, upstream and chaos test runs against both backends, except the database-restart fault, which only applies to MySQL.

## 7. Rust and Python boundary

| Rule | Why |
|---|---|
| Only strings, numbers, `None` and small result objects cross | Tokio threads hold no Python objects |
| Every async method returns a Python awaitable | The event loop never blocks |
| Tokio and pools start at `DBOS.launch()`, never at import | A runtime does not survive `fork` |
| Using `DBOS` in a forked child raises a clear error | Otherwise it hangs |
| Rust errors become specific Python exceptions | Callers can catch them normally |
| Rust panics become a normal exception with the panic text | PyO3's default is a `BaseException` that handlers miss |
| Cancelling an await drops the Rust future and rolls back | No leaked connections or locks |
| Rust logs go to Python `logging` | One place to read them |
| The database password is never logged | It is a secret |

## 8. Security and data

- **No pickle.** Unpickling database contents can run arbitrary code. This is the main reason our stored format differs from upstream's default.
- **Plain-text storage.** Inputs and outputs are stored unencrypted, so do not pass secrets as workflow inputs or step outputs. Pass an ID and fetch the secret inside the step.
- **Retention.** The tables grow forever until a cleanup exists. `DBOS.delete_workflow`-style cleanup arrives in M7.
- **SQL.** All values use bound parameters, and table names are fixed in the code.
- **Privileges.** Runtime needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the two tables. Only the migration needs DDL.
- **Supply chain.** Commit lockfiles, pin CI actions to commit SHAs, and run `cargo deny` and `pip-audit`.

## 9. Milestones

Each one must pass before the next starts.

### Build order: SQLite first, then MySQL

The storage work in M2 happens in two phases:

1. **Phase 1, SQLite (done):** port DBOS's minimal workflow and step logic into Rust, on SQLite. `crates/dbos-mysql-core/src/sqlite.rs` holds it.
2. **Phase 2, MySQL (done):** `mysql.rs` has the same operations, and the same test suite runs against both.

Phase 1 ports only what the first version needs: start or attach, read status, load and save steps, finish, renew, release, and reclaim. Bindings and the Python SDK come after Phase 2.

### M0: prove the toolchain

Do this first, because it can change the plan.

- Cargo workspace, `pyproject.toml`, and an async function called from Python.
- `sqlx` runs `SELECT 1` against MySQL 8.4 and SQLite, called from Python.
- `compose.yml` with MySQL on port `${DBOS_TEST_DB_PORT:-13307}`, working on Docker and Podman. Ports 3306 and 13306 are already used on the dev machine.

**Done when:**

- [x] A wheel installs in `python:3.14.8-slim` with no Rust on the image, and works against MySQL and SQLite.
- [x] Cancelling a Python task during a Rust await rolls back and leaks nothing.
- [x] Rust logs reach Python's `logging`.

**If this fails:** the core moves to Python. The schema, SQL, API and tests all stay.

**Findings to carry forward:**

- Rust awaitables are `asyncio.Future` objects, not coroutines, so `asyncio.create_task` rejects them. The SDK must wrap them in `async def`.
- A failing `after_connect` makes `sqlx` retry until the pool times out, and the caller only sees "pool timed out", after 10 s. M2 must surface the real cause.
- The manylinux wheel builds with `maturin[zig]`.

### M1: copy the tests

Write the tests before the code.

- Copy Tier 1, Tier 2 and Tier 3 files into `tests/upstream/`, with their licence text and upstream commit.
- Write `tests/conftest.py` for MySQL: one database per pytest worker, rows cleared per test, `DBOS` reset per test.
- Create `MANIFEST.md`.

**Done when:** Tier 1 collects under pytest and fails for the expected reason, because `dbos_mysql` has nothing in it yet.

### M2: store state

- Both schema files, `store.rs`, `mysql.rs` and `sqlite.rs`, covering sections 6 and 6a.
- Retries of database errors.

**Done when these Rust tests pass on real MySQL and on SQLite:**

- [ ] Same workflow ID twice gives one row. A different name is reported as a conflict.
- [ ] `Job-1` and `job-1` are separate workflows.
- [ ] N processes race for one expired row, and exactly one wins.
- [ ] A process does not reclaim names it did not register.
- [ ] A row over its attempt limit becomes `MAX_RECOVERY_ATTEMPTS_EXCEEDED` and is not returned.
- [ ] A stale token cannot save a step or finish a workflow.
- [ ] Renewal after takeover matches zero rows. This also checks how `sqlx` reports matched versus changed rows.
- [ ] A 10 MB value round-trips, and one above `max_allowed_packet` gives the clear error.
- [ ] Non-ASCII text and emoji round-trip exactly.
- [ ] An induced deadlock is retried.

### M3: durable workflows and steps

The first version where state is actually used from Python. No recovery yet.

- `dbos-mysql-py` bindings, `_native.pyi`, exception mapping.
- `@DBOS.workflow()`, `@DBOS.step()`, `SetWorkflowID`, step replay, stored errors, JSON serialization.
- Leases renewed for running workflows.

**Done when:** Tier 1 passes.

### M4: handles, children, step retries

- `start_workflow_async`, `retrieve_workflow_async`, handles, child workflows, step retries, step status.

**Done when:** Tier 2 passes.

### M5: crash recovery

- `recovery.py` with the reclaim loop, graceful release, `max_run_seconds` and panic handling.
- A chaos harness using real processes, with these faults:

| Fault | How |
|---|---|
| Process crash | `SIGKILL` at random times |
| Stale owner | `SIGSTOP` past the lease length, then `SIGCONT` |
| Crash in the risk window | A failpoint between a step's side effect and its save |
| Database restart | Restart the MySQL container during work |
| Contention | 4 processes and 1,000 workflows |

Each step writes to a separate `ledger` table, using the database clock. After every run:

1. Every workflow is final. None is stuck.
2. Each step has at most one row per ID.
3. No ledger row for a step starts after that step's save was committed.
4. Each workflow's output matches its saved steps.
5. A repeated step only appears where a fault explains it.
6. No transaction is left open.

**Done when:**

- [ ] Tier 3 passes.
- [ ] The chaos run is clean **20 times in a row**, with its seed printed so failures can be reproduced.
- [ ] Forking and cancellation tests from section 7 pass.

### M6: sync functions

Most of `test_dbos.py` uses sync functions, and many Python apps do too.

- Sync workflows and steps run on threads, with `start_workflow` and `retrieve_workflow`.
- Rust calls block their own thread only, with the GIL released.

**Done when:** the original sync versions of Tier 1, 2 and 3 tests pass unconverted.

### M7: make it usable

- README with the guarantees from section 3, word for word.
- `examples/resume_after_crash.py`.
- `python -m dbos_mysql migrate`.
- Cleanup of old finished workflows, in batches.
- CI: formatting, `clippy -D warnings`, `cargo deny`, `ruff`, type checks, all tests against MySQL, a short chaos run on each push, and the full chaos gate nightly.
- Wheels for `linux/amd64` through GitHub Releases, with hashes. Other platforms get added on request.

**Done when:**

- [ ] On a clean machine, the wheel installs from a release and the README example runs.
- [ ] `kill -9` during the example, then a restart, finishes without repeating finished steps.
- [ ] A one-hour soak run is clean.

**This is version 0.1.0.**

### After 0.1.0

Add features one at a time, copying the upstream tests first. The likely order is:

1. Durable sleep
2. `send`/`recv` and events
3. Workflow timeouts
4. Queues
5. Cancel, resume and list
6. Pydantic AI adapter

## 10. Risks

| Risk | Impact | Response |
|---|---|---|
| Lease or crash bug repeats work | **High** | Chaos gate. No payment or email steps until M5 passes |
| Stale owner's Python still running after its lease is lost | High | Quick cancellation, plus idempotency keys at outside services |
| Rust and Python boundary bugs | High | M0 first, plus boundary tests |
| Matching DBOS's behaviour costs more than expected | Medium | Adapt or drop tests case by case, with the reason in the manifest |
| Upstream tests assume Postgres timing or internals | Medium | The manifest shows how much of the suite really carried over |
| JSON changes results compared with pickle | Medium | Round-trip on first run, so problems show up immediately |
| Rust adds build and CI cost with little speed gain | Medium | Accepted by decision. Record real numbers at M5 |
| Changed code breaks replay | Medium | Clear error. Finish old runs before breaking deploys, until versioning exists |
| The name suggests a DBOS link | Low | "Unofficial" at the top of the README. Reconsider before publishing to PyPI |

## 11. Decisions

| Decision | Choice | Reason |
|---|---|---|
| Backends | MySQL and SQLite, equal status | Owner's decision |
| Build order | SQLite in Rust first, then MySQL | Owner's decision |
| Language | Rust core, Python SDK | Owner's decision |
| Shape | In-process extension | No extra service to run |
| API | Follow DBOS Python | Makes upstream tests portable |
| Test source | Python repo first, TS for extra scenarios | Python tests match the SDK's language |
| Crash detection | Leases | No need to track which process owns what |
| Serialization | JSON through Pydantic | Stored data cannot run code |
| Package | `dbos-mysql`, imported as `dbos_mysql` | Matches the repo. Cannot clash with a real `dbos` install |

## 12. Open questions

None of these block M0 to M5.

1. **Exception fidelity:** is `DBOSRecordedError` good enough for custom exceptions, or do we need a way to register them?
2. **Platforms:** which should 0.1.0 support beyond `linux/amd64`?
3. **Parallel steps under load:** do step IDs stay stable in every case upstream's tests cover? `test_asyncio_wait*` in Tier 2 will show this.

## 13. What has been checked

Everything else in this plan is design, and nothing has been run.

| Fact | How |
|---|---|
| Both upstream repos are MIT licensed | Read each `LICENSE` |
| Test names listed in section 2 exist | Listed from `dbos-transact-py` and `dbos-transact-ts`, at commits `236142b` and `306b732`. Only samples were read in full |
| DBOS Python 3.4.0 has Postgres and SQLite backends, and no MySQL one | Read its source |
| Crate and `maturin` versions in section 4 exist | crates.io and PyPI |
| PyO3's build script lists Python 3.14 | Read `build.rs` on `main` |
| The dev machine has Rust 1.99.1, `uv`, Python 3.14.8, and Podman with Compose | Ran the commands |

**Proven at M0:** the crates compile together on 3.14, and cancellation and Rust log forwarding work, on both backends.

**Proven in Phases 1 and 2:** the store's rules, by the same Rust tests on both backends, including a child process aborting mid-transaction and a real InnoDB deadlock. Deliberate breaks of the code were caught by a test. `sqlx` sets `CLIENT_FOUND_ROWS`, so `rows_affected` counts matched rows on MySQL.
