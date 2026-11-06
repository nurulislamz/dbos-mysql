# Introduction

rust-dwos runs durable workflows on Postgres, MySQL 8.4 or SQLite. If your program crashes, it starts again where it stopped. Steps that already finished do not run again.

It is a library. You add decorators to your Python functions and point them at a database. There is no extra server to run.

> **Unofficial and not production ready.** This project is not affiliated with DBOS, Inc. Do not run payment or email steps on it yet. The current state of each feature is in `PROGRESS.md`.

## What it looks like

```python
from rust_dwos import DBOS


@DBOS.step()
def charge(order_id: str) -> str:
    return payments.charge(order_id, idempotency_key=f"{DBOS.workflow_id}-{DBOS.step_id}")


@DBOS.step()
def ship(order_id: str) -> str:
    return warehouse.ship(order_id)


@DBOS.workflow()
def fulfil(order_id: str) -> str:
    receipt = charge(order_id)
    return f"{receipt}: {ship(order_id)}"
```

If the process dies after `charge` is saved, another run of `fulfil` reads the saved receipt and goes on to `ship`. The card is not charged twice by that saved step.

## What you get

| Feature | Page |
|---|---|
| Workflows and steps that survive a crash | [Workflows](guide/workflows.md), [Steps](guide/steps.md) |
| One run per workflow ID | [Workflows](guide/workflows.md) |
| Step retries and timeouts | [Steps](guide/steps.md) |
| FIFO queues with a global limit | [Queues](guide/queues.md) |
| Messages and events between workflows | [Messages and events](guide/communication.md) |
| Durable sleep, workflow timeouts, cron schedules | [Sleep, timeouts and schedules](guide/time.md) |
| Cancel, resume, list and delete | [Managing workflows](guide/management.md) |
| A read-only web page of your workflows | [Production](production.md#see-your-workflows) |

Anything not built yet is on the [Coming soon](coming-soon.md) page.

## What it promises

| Topic | Promise |
|---|---|
| Workflow result | Each workflow ID reaches one final state, which never changes |
| Workflow code | Runs at least once, and may replay |
| Steps | At least once. A crash after a side effect but before the save runs that step again |
| Saved steps | Never run again for that workflow |
| Stale owners | A process that lost its lease cannot save a step or a result |

A step that charges a card or sends email still needs an idempotency key at that service. `DBOS.workflow_id` and `DBOS.step_id` give a stable key.

## How it relates to DBOS

The Python API copies [DBOS Transact](https://github.com/dbos-inc/dbos-transact-py), so `DBOS`, `@DBOS.workflow()` and `@DBOS.step()` look the same. The engine underneath is different: it is written in Rust, adds MySQL, and finds crashes with leases. See [how it differs from DBOS](architecture.md#how-it-differs-from-dbos).

## Where to go next

- New here: the [Quickstart](quickstart.md) makes a workflow survive a crash.
- Have an app: [Add it to your app](integrate.md).
- Want to know how it works: [Architecture](architecture.md).
- Coming from Temporal: [Temporal patterns](temporal-patterns.md) shows which of its design patterns work here.
