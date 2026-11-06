"""`yado`: list, inspect, cancel and resume workflows in a system database, and migrate it.

It connects straight to the database and never starts recovery, so it is safe next to live processes.
Output is JSON on stdout. Stored inputs, outputs and errors are printed as stored, not decoded.
Not here yet, because the core has no call for them: `workflow fork`, `workflow queue list`, `reset`
and `rename-application`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Sequence
from typing import Any

from yado import _native

URL_ENV = "DBOS_SYSTEM_DATABASE_URL"
WORKFLOW_FIELDS = ("workflow_id", "name", "status", "input", "output", "error", "recovery_attempts", "max_recovery_attempts", "parent_workflow_id", "created_at_ms", "updated_at_ms", "completed_at_ms", "serialization", "workflow_timeout_ms", "workflow_deadline_epoch_ms")
STEP_FIELDS = ("step_id", "step_name", "output", "error", "child_workflow_id", "serialization")
READ_ONLY = ("list", "get", "steps")


async def _connect(url: str, **options: Any) -> _native.Engine:
    return await _native.Engine.connect(url, **options)


def _fields(rows: Any, names: tuple[str, ...]) -> list[dict[str, Any]]:
    return [{name: getattr(row, name) for name in names} for row in rows]


def _print(value: Any) -> None:
    json.dump(value, sys.stdout, indent=2)
    sys.stdout.write("\n")


def _list(engine: _native.Engine, args: argparse.Namespace) -> None:
    rows = engine.list_workflows(status=args.status or None, name=args.name or None, limit=args.limit, offset=args.offset, sort_desc=args.desc, blocking=True)
    _print(_fields(rows, WORKFLOW_FIELDS))


def _get(engine: _native.Engine, args: argparse.Namespace) -> None:
    _print(_fields([engine.get_status(args.workflow_id, blocking=True)], WORKFLOW_FIELDS)[0])


def _steps(engine: _native.Engine, args: argparse.Namespace) -> None:
    _print(_fields(engine.list_steps(args.workflow_id, blocking=True), STEP_FIELDS))


def _cancel(engine: _native.Engine, args: argparse.Namespace) -> None:
    engine.cancel_workflows(args.workflow_ids, False, blocking=True)
    _print({"cancelled": args.workflow_ids})


def _resume(engine: _native.Engine, args: argparse.Namespace) -> None:
    # No names: the row becomes PENDING, and a running process that knows the workflow picks it up.
    engine.resume_workflows(args.workflow_ids, [], None, blocking=True)
    _print({"resumed": args.workflow_ids})


ACTIONS = {"list": _list, "get": _get, "steps": _steps, "cancel": _cancel, "resume": _resume}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="yado", description=__doc__.splitlines()[0])
    parser.add_argument("--system-database-url", default=os.environ.get(URL_ENV), help=f"default: ${URL_ENV}")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="create or upgrade the system database")
    workflow = commands.add_parser("workflow", help="list, inspect and change workflows").add_subparsers(dest="action", required=True)
    listing = workflow.add_parser("list", help="list workflows")
    listing.add_argument("--status", action="append", default=[], help="repeatable")
    listing.add_argument("--name", action="append", default=[], help="repeatable")
    listing.add_argument("--limit", type=int)
    listing.add_argument("--offset", type=int)
    listing.add_argument("--desc", action="store_true", help="newest first")
    for action in ("get", "steps"):
        workflow.add_parser(action, help=f"{action} one workflow").add_argument("workflow_id")
    for action in ("cancel", "resume"):
        workflow.add_parser(action, help=f"{action} workflows").add_argument("workflow_ids", nargs="+")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.system_database_url:
        print(f"yado: pass --system-database-url or set {URL_ENV}", file=sys.stderr)
        return 2
    url = args.system_database_url
    try:
        if args.command == "migrate":
            asyncio.run(_connect(url, run_migrations=True)).close()
            _print({"migrated": True})
            return 0
        engine = asyncio.run(_connect(url, run_migrations=False, read_only=args.action in READ_ONLY))
        try:
            ACTIONS[args.action](engine, args)
        finally:
            engine.close()
    except Exception as error:  # noqa: BLE001
        print(f"yado: {error}", file=sys.stderr)
        return 1
    return 0
