"""Measure SDK speed: python tests/bench_sdk.py [--db sqlite,postgres] [--workflows 300] [--repeat 3] [--count].

Uses only the public API, so it runs on any commit. Build with `maturin develop --release` first.
The file name does not match test_*.py, so pytest does not collect it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg

from rust_dwos import DBOS

PG = {"host": "127.0.0.1", "port": int(os.environ.get("DBOS_TEST_PG_PORT", "13309")), "user": "postgres", "password": "dbos-test-root", "autocommit": True}
STEPS_PER_WORKFLOW = 50


@contextmanager
def database(kind: str) -> Iterator[str]:
    if kind == "sqlite":
        with tempfile.TemporaryDirectory() as folder:
            yield f"sqlite://{Path(folder) / 'bench.sqlite'}"
        return
    name = f"dbos_bench_{uuid.uuid4().hex}"
    with psycopg.connect(dbname="postgres", **PG) as connection:
        connection.execute(f"CREATE DATABASE {name}")
    try:
        yield f"postgres://{PG['user']}:{PG['password']}@{PG['host']}:{PG['port']}/{name}"
    finally:
        with psycopg.connect(dbname="postgres", **PG) as connection:
            connection.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def workflows() -> dict[str, tuple[Callable[[], object], int]]:
    """Returns each case's function and how many steps one call runs."""

    @DBOS.step()
    def noop_step() -> None:
        return None

    @DBOS.step()
    async def noop_step_async() -> None:
        return None

    @DBOS.workflow()
    def plain() -> None:
        return None

    @DBOS.workflow()
    def one_step() -> None:
        noop_step()

    @DBOS.workflow()
    def many_steps() -> None:
        for _ in range(STEPS_PER_WORKFLOW):
            noop_step()

    @DBOS.workflow()
    async def plain_async() -> None:
        return None

    @DBOS.workflow()
    async def one_step_async() -> None:
        await noop_step_async()

    @DBOS.workflow()
    async def many_steps_async() -> None:
        for _ in range(STEPS_PER_WORKFLOW):
            await noop_step_async()

    return {
        "sync plain": (plain, 0),
        "sync one step": (one_step, 1),
        "sync 50 steps": (many_steps, STEPS_PER_WORKFLOW),
        "async plain": (plain_async, 0),
        "async one step": (one_step_async, 1),
        "async 50 steps": (many_steps_async, STEPS_PER_WORKFLOW),
    }


def timed(function: Callable[[], object], calls: int) -> float:
    if asyncio.iscoroutinefunction(function):

        async def run() -> float:
            started = time.perf_counter()
            for _ in range(calls):
                await function()
            return time.perf_counter() - started

        return asyncio.run(run())
    started = time.perf_counter()
    for _ in range(calls):
        function()
    return time.perf_counter() - started


def measure(kind: str, calls: int, repeat: int) -> list[dict[str, object]]:
    rows = []
    with database(kind) as url:
        DBOS.destroy(destroy_registry=True)
        DBOS(config={"name": "bench", "system_database_url": url, "recovery_interval_seconds": 3600.0})
        DBOS.launch()
        try:
            cases = workflows()
            for case, (function, steps) in cases.items():
                count = max(calls // 10, 10) if steps > 1 else calls
                timed(function, min(count, 20))  # warm the pool and caches
                seconds = statistics.median(timed(function, count) for _ in range(repeat))
                workflows_per_second = count / seconds
                rows.append({"db": kind, "case": case, "workflows_per_s": round(workflows_per_second, 1), "steps_per_s": round(workflows_per_second * steps, 1), "ms_per_workflow": round(seconds / count * 1000, 3)})
        finally:
            DBOS.destroy(destroy_registry=True)
    return rows


def count_transactions(calls: int) -> None:
    """Counts committed Postgres transactions per call, so an extra round trip shows up."""
    with database("postgres") as url:
        name = url.rsplit("/", 1)[1]
        DBOS.destroy(destroy_registry=True)
        DBOS(config={"name": "bench", "system_database_url": url, "recovery_interval_seconds": 3600.0})
        DBOS.launch()

        def commits() -> int:
            time.sleep(2.5)  # backends report their counters at most once a second
            with psycopg.connect(dbname="postgres", **PG) as connection:
                return connection.execute("SELECT xact_commit FROM pg_stat_database WHERE datname = %s", (name,)).fetchone()[0]

        try:
            for case, (function, steps) in workflows().items():
                if steps > 1:
                    continue
                timed(function, 5)
                before = commits()
                timed(function, calls)
                print(f"{case}: {(commits() - before - 1) / calls:.2f} transactions per workflow")
        finally:
            DBOS.destroy(destroy_registry=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="sqlite,postgres")
    parser.add_argument("--workflows", type=int, default=300)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--count", action="store_true", help="count Postgres transactions per workflow instead of timing")
    parser.add_argument("--json", type=Path, help="also write the rows to this file")
    arguments = parser.parse_args()
    if arguments.count:
        count_transactions(arguments.workflows)
        return
    rows = [row for kind in arguments.db.split(",") for row in measure(kind, arguments.workflows, arguments.repeat)]
    print(f"{'db':9}{'case':17}{'workflows/s':>12}{'steps/s':>10}{'ms/workflow':>13}")
    for row in rows:
        print(f"{row['db']:9}{row['case']:17}{row['workflows_per_s']:>12}{row['steps_per_s']:>10}{row['ms_per_workflow']:>13}")
    if arguments.json:
        arguments.json.write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    main()
