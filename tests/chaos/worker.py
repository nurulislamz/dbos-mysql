import asyncio
import json
import os
import random
import signal
import sqlite3
import sys
import time
from typing import Any

import psycopg
import pymysql

from rust_dwos import DBOS, DBOSConfig, DBOSException, SetWorkflowID

WORKER = int(os.environ["CHAOS_WORKER"])
WORKERS = int(os.environ["CHAOS_WORKERS"])
TOTAL = int(os.environ["CHAOS_WORKFLOWS"])
URL = os.environ["CHAOS_URL"]
LEDGER = os.environ["CHAOS_LEDGER"]
CRASH_RATE = float(os.environ.get("CHAOS_CRASH_RATE", "0"))
STEPS = 3
random.seed(os.getpid())


def _ledger() -> pymysql.Connection | psycopg.Connection:
    host, port, user, password, database = LEDGER.split(":")
    if URL.startswith("postgres"):
        return psycopg.connect(host=host, port=int(port), user=user, password=password, dbname=database, autocommit=True, connect_timeout=5)
    return pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, autocommit=True, connect_timeout=5)


def _write_answer(workflow_id: str, answer: dict[str, Any]) -> None:
    text = json.dumps(answer)
    while True:
        try:
            if not URL.startswith("sqlite"):
                with _ledger() as connection, connection.cursor() as cursor:
                    cursor.execute("INSERT INTO answers (workflow_id, answer) VALUES (%s, %s)", (workflow_id, text))
            else:
                with sqlite3.connect(LEDGER, timeout=30) as connection:
                    connection.execute("INSERT INTO answers (workflow_id, answer) VALUES (?, ?)", (workflow_id, text))
            return
        except (pymysql.Error, psycopg.Error, sqlite3.Error):
            time.sleep(0.2)


def _write_ledger(workflow_id: str, step_id: int) -> None:
    pid = os.getpid()
    while True:
        try:
            if not URL.startswith("sqlite"):
                now = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT" if URL.startswith("postgres") else "CAST(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) * 1000 AS SIGNED)"
                with _ledger() as connection, connection.cursor() as cursor:
                    cursor.execute(f"INSERT INTO ledger (workflow_id, step_id, pid, started_at_ms) VALUES (%s, %s, %s, {now})", (workflow_id, step_id, pid))  # noqa: S608
            else:
                with sqlite3.connect(LEDGER, timeout=30) as connection:
                    connection.execute(
                        "INSERT INTO ledger (workflow_id, step_id, pid, started_at_ms) VALUES (?, ?, ?, ?)",
                        (workflow_id, step_id, pid, int(time.time() * 1000)),
                    )
            return
        except (pymysql.Error, psycopg.Error, sqlite3.Error):
            time.sleep(0.2)


def _after_side_effect() -> None:
    if random.random() < CRASH_RATE:  # noqa: S311
        os._exit(42)


@DBOS.step(name="chaos_step")
async def chaos_step(step_id: int) -> str:
    workflow_id = DBOS.workflow_id
    assert workflow_id is not None
    await asyncio.to_thread(_write_ledger, workflow_id, step_id)
    await asyncio.sleep(random.uniform(0, 0.03))  # noqa: S311
    _after_side_effect()
    return f"{workflow_id}:{step_id}:{os.getpid()}:{random.random()}"


@DBOS.workflow(name="chaos_workflow")
async def chaos_workflow(steps: int) -> dict[str, Any]:
    saved = [await chaos_step(step_id) for step_id in range(1, steps + 1)]
    return {"steps": saved, "run": random.random()}  # noqa: S311


@DBOS.step(name="chaos_sync_step")
def chaos_sync_step(step_id: int) -> str:
    workflow_id = DBOS.workflow_id
    assert workflow_id is not None
    _write_ledger(workflow_id, step_id)
    time.sleep(random.uniform(0, 0.03))  # noqa: S311
    _after_side_effect()
    return f"{workflow_id}:{step_id}:{os.getpid()}:{random.random()}"


@DBOS.workflow(name="chaos_sync_workflow")
def chaos_sync_workflow(steps: int) -> dict[str, Any]:
    saved = [chaos_sync_step(step_id) for step_id in range(1, steps + 1)]
    return {"steps": saved, "run": random.random()}  # noqa: S311


async def _run_one(index: int, slots: asyncio.Semaphore) -> None:
    async with slots:
        workflow_id = f"wf-{index}"
        while True:
            try:
                with SetWorkflowID(workflow_id):
                    if index % 2 == 0:
                        answer = await chaos_workflow(STEPS)
                    else:
                        answer = await asyncio.to_thread(_run_sync, workflow_id)
                await asyncio.to_thread(_write_answer, workflow_id, answer)
                return
            except DBOSException as error:
                print(f"worker {WORKER}: {workflow_id}: {error}", file=sys.stderr, flush=True)
                await asyncio.sleep(0.5)


def _run_sync(workflow_id: str) -> dict[str, Any]:
    with SetWorkflowID(workflow_id):
        return chaos_sync_workflow(STEPS)


async def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    config: DBOSConfig = {
        "name": "chaos",
        "system_database_url": URL,
        "lease_seconds": float(os.environ.get("CHAOS_LEASE", "1.0")),
        "recovery_interval_seconds": 0.2,
        "serializer": os.environ["CHAOS_SERIALIZER"],
    }
    DBOS(config=config)
    while True:
        try:
            DBOS.launch()
            break
        except Exception as error:  # noqa: BLE001
            print(f"worker {WORKER}: launch failed: {error}", file=sys.stderr, flush=True)
            await asyncio.sleep(0.5)
    slots = asyncio.Semaphore(8)
    mine = [index for index in range(TOTAL) if index % WORKERS == WORKER]
    await asyncio.gather(*(_run_one(index, slots) for index in mine))
    print("WORKER_DONE", flush=True)
    await asyncio.sleep(float(os.environ.get("CHAOS_LINGER", "0")))
    DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
