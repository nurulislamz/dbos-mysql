# Add it to your app

rust-dwos runs inside your program. You do not deploy anything new. You need the library, a database, and these steps.

## 1. Pick a database

| Database | Use it when |
|---|---|
| MySQL 8.4 | You run several copies of your program. This has the most crash testing |
| Postgres | You run several copies and can accept the open restart incident in `PROGRESS.md` |
| SQLite | One machine only |

The tables can share a database with your app, but a separate database is simpler to back up and limit. The table names are in [Architecture](architecture.md#tables). Your app's own data stays outside rust-dwos, because app database transactions are [not built yet](coming-soon.md).

## 2. Set it up at startup

The order matters.

```python
import os

from rust_dwos import DBOS

# 1. Decorators run at import, so import your workflows first.
import myapp.workflows

# 2. Configure once per process.
DBOS(config={"name": "my-app", "system_database_url": os.environ["DWOS_URL"]})

# 3. Connect, upgrade the schema, and start the background loops.
DBOS.launch()

# 4. Queues need a launched DBOS.
DBOS.register_queue("emails", global_concurrency=5)
```

`DBOS.launch()` starts recovery, so register every workflow before calling it. A process only claims workflows whose names it has registered.

## 3. Mark your functions

Put anything that touches the outside world in a step. Keep the workflow body to plain decisions, because it runs again on recovery.

```python
@DBOS.step(retries_allowed=True, max_attempts=5)
def send_email(to: str, body: str) -> None:
    mailer.send(to, body, idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}")


@DBOS.workflow()
def welcome(user_id: str) -> None:
    user = load_user(user_id)  # This should be a step too.
    send_email(user["email"], "Welcome")
```

## 4. Start workflows from your handlers

Give each piece of work an ID that comes from your domain. A retried HTTP request then attaches to the same run rather than starting a second one.

```python
with SetWorkflowID(f"welcome-{user_id}"):
    handle = DBOS.start_workflow(welcome, user_id)
```

## A web server example

FastAPI needs no special support. This pattern, and `@app.get(...)` directly above `@DBOS.workflow()`, ran on SQLite. FastAPI is not part of this repository's tests.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from rust_dwos import DBOS, SetWorkflowID


@DBOS.workflow()
async def welcome(user_id: str) -> str:
    return await send_email(user_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DBOS(config={"name": "my-app", "system_database_url": URL})
    DBOS.launch()
    yield
    DBOS.destroy()


app = FastAPI(lifespan=lifespan)


@app.post("/users/{user_id}/welcome")
async def post_welcome(user_id: str) -> dict[str, str]:
    with SetWorkflowID(f"welcome-{user_id}"):
        handle = await DBOS.start_workflow_async(welcome, user_id)
    return {"workflow_id": handle.workflow_id}
```

When `DBOS.launch()` is called inside a running event loop, background async workflows run on that loop while it is alive.

## Rules to follow

| Rule | Why |
|---|---|
| Call steps in the same order on every run | Steps are matched by number and name. A change raises `DBOSUnexpectedStepError` or a similar error on replay |
| Do not change a workflow while old runs are unfinished | Version patching is [not built yet](coming-soon.md). Let old runs finish or cancel them |
| Keep class paths stable | The default pickle format stores the path. A rename can break recovery of unfinished runs |
| Call `DBOS.launch()` after `os.fork`, not before | DBOS raises `DBOSException` if used after a fork |
| Call `DBOS.destroy()` on shutdown | Running workflows are released at once. Otherwise they wait for their lease to run out |
| Keep database write access to admins and your program | With pickle, a database writer can run code in your process. See [Stored values](production.md#stored-values) |

## Running several copies

Run as many copies as you like against one Postgres or MySQL database. They do not need to know about each other. The database row says who owns each workflow. If a copy dies, any other copy with that workflow registered takes over when the lease runs out. If every copy is stopped, rows wait until one starts.

Read [Upgrades](production.md#upgrades) before you deploy a new version.
