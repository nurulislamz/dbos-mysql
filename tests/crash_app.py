import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from yado import DBOS, SetWorkflowID


def register(ledger: Path, crash: bool) -> dict[str, Callable[..., Any]]:
    def record(name: str) -> str:
        with ledger.open("a") as file:
            file.write(name + "\n")
        return name

    @DBOS.step(name="async_step")
    async def async_step(name: str) -> str:
        return record(name)

    @DBOS.workflow(name="async_crash")
    async def async_crash() -> str:
        first = await async_step("one")
        if crash:
            os._exit(9)
        return first + await async_step("two")

    @DBOS.step(name="sync_step")
    def sync_step(name: str) -> str:
        return record(name)

    @DBOS.workflow(name="sync_crash")
    def sync_crash() -> str:
        first = sync_step("one")
        if crash:
            os._exit(9)
        return first + sync_step("two")

    return {"async": async_crash, "sync": sync_crash}


def main() -> None:
    url, workflow_id, kind, ledger, *serializer = sys.argv[1:]
    workflows = register(Path(ledger), crash=True)
    options = {"serializer": serializer[0]} if serializer else {}
    DBOS(config={"name": "crash", "system_database_url": url, "lease_seconds": 1.0, "recovery_interval_seconds": 3600.0, **options})
    DBOS.launch()
    with SetWorkflowID(workflow_id):
        if kind == "async":
            import asyncio

            asyncio.run(workflows[kind]())
        else:
            workflows[kind]()


if __name__ == "__main__":
    main()
