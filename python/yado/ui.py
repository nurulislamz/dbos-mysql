"""Read-only web viewer: python -m yado.ui --db <url> [--port 8000]."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from yado import _native
from yado.dbos import _call, _LoopThread
from yado.errors import DBOSNonExistentWorkflowError

BIND_ADDRESS = "127.0.0.1"
DB_TIMEOUT_SECONDS = 35
_PAGE = Path(__file__).with_name("ui.html").read_bytes()
_NO_TABLES = "This database has no yado tables yet. Start your program once to create them."
_DB_FAILED = "Could not read the database. See the viewer's terminal for the reason."
_MISSING_TABLE_TEXT = ("no such table", "doesn't exist", "does not exist")
_SUMMARY_FIELDS = (
    "workflow_id",
    "name",
    "status",
    "recovery_attempts",
    "max_recovery_attempts",
    "parent_workflow_id",
    "created_at_ms",
    "updated_at_ms",
    "completed_at_ms",
)
_DETAIL_FIELDS = (*_SUMMARY_FIELDS, "input", "output", "error")
_STEP_FIELDS = ("step_id", "step_name", "output", "error", "child_workflow_id")


def _inline_hashes(tag: str) -> str:
    blocks = re.findall(rf"<{tag}>(.*?)</{tag}>".encode(), _PAGE, re.DOTALL)
    return " ".join(f"'sha256-{base64.b64encode(hashlib.sha256(block).digest()).decode()}'" for block in blocks)


# Hashes allow only the page's own inline CSS and JS. Nothing is loaded from elsewhere.
_CSP = (
    f"default-src 'none'; script-src {_inline_hashes('script')}; style-src {_inline_hashes('style')}; "
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)


def _redact(text: str, url: str) -> str:
    """Remove the database URL and its password, which an error message might repeat."""
    secrets = [url]
    password = urlsplit(url).password
    if password:
        secrets += [password, unquote(password)]
    for secret in secrets:
        text = text.replace(secret, "***")
    return text


def _fields(row: Any, names: tuple[str, ...]) -> dict[str, Any]:
    return {name: getattr(row, name) for name in names}


class Viewer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, url: str, port: int = 0) -> None:
        # Connecting to SQLite would create a missing file, and a viewer must not write.
        if url.startswith("sqlite://") and not Path(url.removeprefix("sqlite://").split("?")[0]).is_file():
            raise FileNotFoundError("That SQLite database file does not exist.")
        self._url = url
        self._loop = _LoopThread("yado-ui")
        try:
            self._engine = self._loop.run(_call(_native.Engine.connect, url, run_migrations=False, read_only=True))
            super().__init__((BIND_ADDRESS, port), _Handler)
        except BaseException:
            self._loop.stop()
            raise
        bound = self.server_address[1]
        hosts = {f"localhost:{bound}", f"127.0.0.1:{bound}"}
        self.allowed_hosts = hosts | {"localhost", "127.0.0.1"} if bound == 80 else hosts

    def read(self, method: str, *args: Any) -> Any:
        future = self._loop.submit(_call(getattr(self._engine, method), *args))
        return future.result(timeout=DB_TIMEOUT_SECONDS)

    def explain(self, error: Exception) -> str:
        """Return fixed text for the page, because database errors can hold private details."""
        text = str(error)
        if any(phrase in text.lower() for phrase in _MISSING_TABLE_TEXT):
            return _NO_TABLES
        print(f"yado viewer: {type(error).__name__}: {_redact(text, self._url)}", file=sys.stderr)
        return _DB_FAILED

    def close(self) -> None:
        self.server_close()
        self._loop.stop()


class _Handler(BaseHTTPRequestHandler):
    server: Viewer
    server_version = "yado-viewer"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def end_headers(self) -> None:
        self.send_header("Content-Security-Policy", _CSP)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, value: Any) -> None:
        self._send(status, json.dumps(value).encode(), "application/json")

    def do_GET(self) -> None:
        # Refusing other Host values stops a web page reaching this port through DNS rebinding.
        if self.headers.get("Host") not in self.server.allowed_hosts:
            self._send_json(403, {"error": "This viewer only answers on localhost."})
            return
        path = urlsplit(self.path).path
        if path == "/":
            self._send(200, _PAGE, "text/html; charset=utf-8")
            return
        try:
            self._api(path)
        except DBOSNonExistentWorkflowError:
            self._send_json(404, {"error": "No workflow has that ID."})
        except Exception as error:  # noqa: BLE001
            self._send_json(503, {"error": self.server.explain(error)})

    def _api(self, path: str) -> None:
        # Split before decoding, so an encoded slash stays inside the workflow ID.
        parts = [unquote(part) for part in path.split("/")[1:]]
        match parts:
            case ["api", "workflows"]:
                # shortcut: list_workflows has no paging and reads every row, add paging before showing very large tables.
                rows = self.server.read("list_workflows")
                self._send_json(200, {"workflows": [_fields(row, _SUMMARY_FIELDS) for row in reversed(rows)]})
            case ["api", "workflows", workflow_id]:
                self._send_json(200, _fields(self.server.read("get_status", workflow_id), _DETAIL_FIELDS))
            case ["api", "workflows", workflow_id, "steps"]:
                rows = self.server.read("list_steps", workflow_id)
                self._send_json(200, {"steps": [_fields(row, _STEP_FIELDS) for row in rows]})
            case _:
                self._send_json(404, {"error": "Not found."})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m yado.ui", description="Read-only viewer for workflows and steps.")
    parser.add_argument("--db", required=True, metavar="URL", help="database URL, for example sqlite://app.sqlite")
    parser.add_argument("--port", type=int, default=8000)
    arguments = parser.parse_args(argv)
    try:
        viewer = Viewer(arguments.db, arguments.port)
    except Exception as error:  # noqa: BLE001
        print(f"Could not start the viewer: {_redact(str(error), arguments.db)}", file=sys.stderr)
        return 1
    print(f"Viewer on http://{BIND_ADDRESS}:{viewer.server_address[1]} (read-only). Press Ctrl-C to stop.", flush=True)
    try:
        viewer.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        viewer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
