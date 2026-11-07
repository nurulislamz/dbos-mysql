# Quickstart

This page installs yado from source, then kills a workflow halfway and watches it finish.

## Install

There is no release yet. You need Rust, [uv](https://docs.astral.sh/uv/) and Python 3.14.

```sh
uv venv --python 3.14 .venv
source .venv/bin/activate
uv pip install maturin
maturin develop
```

The install and import name is `yado`, and the class is `DBOS`.

## Write a workflow

Save this as `demo.py`.

```python
import os
import sys

import yado
from yado import DBOS, SetWorkflowID


@yado.step()
def step_one() -> int:
    print("step one ran", flush=True)
    return 1


@yado.step()
def step_two() -> int:
    print("step two ran")
    return 2


@yado.workflow()
def demo() -> int:
    total = step_one()
    if "crash" in sys.argv:
        os._exit(1)  # Dies at once, like kill -9. No cleanup runs.
    return total + step_two()


# A short lease makes recovery quick for this demo. The default is 30 seconds.
DBOS(config={"name": "demo", "system_database_url": "sqlite://demo.sqlite", "lease_seconds": 5})
yado.launch()
with SetWorkflowID("demo-1"):
    print("result:", demo())
yado.destroy()
```

## Crash it

```sh
python demo.py crash
```

This prints `step one ran`, then the process dies. The workflow is still `PENDING` in the database, and step 1 is saved.

## Start it again

```sh
python demo.py
```

The call to `demo()` finds that `demo-1` is already running, so it waits for the result. The old lease runs out within 5 seconds. The recovery loop then claims the workflow and runs it again.

```text
step two ran
result: 3
```

`step one ran` does not print again, because its result came from the database.

Run the command a third time. It prints only `result: 3`, because one workflow ID has one result.

## Look at it

```sh
python -m yado.ui --db sqlite://demo.sqlite --port 8000
```

Open <http://127.0.0.1:8000> to see the workflow and its saved steps.

## Use another database

| Database | URL |
|---|---|
| SQLite | `sqlite://app.sqlite` |
| MySQL 8.4 | `mysql://user:password@host:3306/database` |
| Postgres | `postgres://user:password@host:5432/database` |

The tables are created at `yado.launch()`.

**Next:** [Add it to your app](integrate.md).
