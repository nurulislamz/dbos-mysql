import asyncio
import http.client
import json
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import pytest

from yado import DBOS, SetWorkflowID, StoreError
from yado.ui import _CSP, _DB_FAILED, _PAGE, Viewer
from tests.conftest import Database

SECRET = "hunter2-secret"


def _request(viewer: Viewer, path: str, method: str = "GET", host: str | None = None) -> tuple[int, dict[str, str], bytes]:
    port = viewer.server_address[1]
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    connection.request(method, path, headers={"Host": host or f"127.0.0.1:{port}"})
    response = connection.getresponse()
    body = response.read()
    connection.close()
    return response.status, dict(response.getheaders()), body


def _json(viewer: Viewer, path: str) -> tuple[int, Any]:
    status, _, body = _request(viewer, path)
    return status, json.loads(body)


@pytest.fixture
def open_viewer(database: Database) -> Iterator[Any]:
    viewers: list[Viewer] = []

    def start() -> Viewer:
        viewer = Viewer(database.url)
        threading.Thread(target=viewer.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        viewers.append(viewer)
        return viewer

    yield start
    for viewer in viewers:
        viewer.shutdown()
        viewer.close()


def _run_workflow(workflow_id: str) -> None:
    @DBOS.step()
    async def add_one(value: int) -> int:
        return value + 1

    @DBOS.workflow()
    async def double_after_add(value: int) -> int:
        return await add_one(value) * 2

    async def main() -> int:
        with SetWorkflowID(workflow_id):
            return await double_after_add(1)

    assert asyncio.run(main()) == 4


def test_lists_and_shows_values_exactly_as_stored(dbos: DBOS, database: Database, open_viewer: Any) -> None:
    workflow_id = f"wf-{uuid.uuid4().hex}"
    _run_workflow(workflow_id)
    stored_input, stored_output = database.execute("SELECT inputs, output FROM workflow_status WHERE workflow_uuid = %s", workflow_id)[0]
    stored_step = database.execute("SELECT output FROM operation_outputs WHERE workflow_uuid = %s", workflow_id)[0][0]
    viewer = open_viewer()

    status, listed = _json(viewer, "/api/workflows")
    assert status == 200
    row = next(item for item in listed["workflows"] if item["workflow_id"] == workflow_id)
    assert row["status"] == "SUCCESS"
    assert "input" not in row

    status, detail = _json(viewer, f"/api/workflows/{workflow_id}")
    assert status == 200
    assert (detail["input"], detail["output"], detail["error"]) == (stored_input, stored_output, None)

    status, steps = _json(viewer, f"/api/workflows/{workflow_id}/steps")
    assert status == 200
    assert [(step["step_id"], step["output"]) for step in steps["steps"]] == [(1, stored_step)]


def test_newest_workflow_is_listed_first(dbos: DBOS, open_viewer: Any) -> None:
    _run_workflow("older")
    time.sleep(0.01)
    _run_workflow("newer")
    _, listed = _json(open_viewer(), "/api/workflows")
    assert [row["workflow_id"] for row in listed["workflows"]] == ["newer", "older"]


def test_workflow_id_with_special_characters_round_trips(dbos: DBOS, open_viewer: Any) -> None:
    workflow_id = "a/b c?d#e%2Ff<é>"
    _run_workflow(workflow_id)
    viewer = open_viewer()
    encoded = quote(workflow_id, safe="")
    status, detail = _json(viewer, f"/api/workflows/{encoded}")
    assert (status, detail["workflow_id"]) == (200, workflow_id)
    status, steps = _json(viewer, f"/api/workflows/{encoded}/steps")
    assert (status, len(steps["steps"])) == (200, 1)


def test_unknown_workflow_and_route_are_not_found(dbos: DBOS, open_viewer: Any) -> None:
    viewer = open_viewer()
    assert _json(viewer, "/api/workflows/nope")[0] == 404
    assert _json(viewer, "/api/nothing")[0] == 404
    assert _json(viewer, "/../../etc/passwd")[0] == 404


def test_viewer_leaves_an_expired_workflow_untouched(dbos: DBOS, database: Database, open_viewer: Any) -> None:
    workflow_id = f"wf-{uuid.uuid4().hex}"
    _run_workflow(workflow_id)
    DBOS.destroy(destroy_registry=True)
    database.expire_lease(workflow_id)
    query = "SELECT status, lease_token, claim_after, recovery_attempts, updated_at FROM workflow_status WHERE workflow_uuid = %s"
    before = database.execute(query, workflow_id)
    assert before[0][:2] == ("PENDING", "crashed")

    viewer = open_viewer()
    for path in ("/", "/api/workflows", f"/api/workflows/{workflow_id}", f"/api/workflows/{workflow_id}/steps"):
        assert _request(viewer, path)[0] == 200
    time.sleep(1.5)
    assert _request(viewer, "/api/workflows")[0] == 200

    assert database.execute(query, workflow_id) == before


def test_missing_tables_get_a_clear_message_and_are_not_created(database: Database, open_viewer: Any) -> None:
    database.execute("SELECT 1")  # creates the empty SQLite file
    viewer = open_viewer()
    status, body = _json(viewer, "/api/workflows")
    assert status == 503
    assert "no yado tables" in body["error"]
    with pytest.raises(Exception, match="workflow_status"):
        database.execute("SELECT 1 FROM workflow_status")


def test_only_localhost_hosts_are_answered(dbos: DBOS, open_viewer: Any) -> None:
    viewer = open_viewer()
    port = viewer.server_address[1]
    assert viewer.server_address[0] == "127.0.0.1"
    for host in (f"127.0.0.1:{port}", f"localhost:{port}"):
        assert _request(viewer, "/", host=host)[0] == 200
    for host in ("evil.example", f"evil.example:{port}", f"localhost:{port + 1}", "localhost.evil.example"):
        status, _, body = _request(viewer, "/api/workflows", host=host)
        assert status == 403, host
        assert b"workflows" not in body


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
def test_only_get_is_allowed(method: str, dbos: DBOS, open_viewer: Any) -> None:
    status, headers, _ = _request(open_viewer(), "/api/workflows", method=method)
    assert status == 501
    assert headers["X-Content-Type-Options"] == "nosniff"


def test_every_response_carries_the_security_headers(dbos: DBOS, open_viewer: Any) -> None:
    viewer = open_viewer()
    for path, expected in (("/", 200), ("/api/workflows", 200), ("/api/nope", 404)):
        status, headers, _ = _request(viewer, path)
        assert status == expected
        assert headers["Content-Security-Policy"] == _CSP
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert headers["Cache-Control"] == "no-store"
    assert _request(viewer, "/", host="evil.example")[1]["Content-Security-Policy"] == _CSP
    assert "default-src 'none'" in _CSP
    assert "connect-src 'self'" in _CSP
    assert "unsafe" not in _CSP
    assert _CSP.count("sha256-") == 2


def test_page_loads_nothing_from_elsewhere_and_never_builds_html_from_data() -> None:
    page = _PAGE.decode()
    assert "innerHTML" not in page
    assert "outerHTML" not in page
    assert "insertAdjacentHTML" not in page
    assert "document.write" not in page
    assert "http://" not in page
    assert "https://" not in page
    assert "<link" not in page


def test_responses_never_contain_the_database_url(dbos: DBOS, database: Database, open_viewer: Any) -> None:
    _run_workflow("wf")
    viewer = open_viewer()
    for path in ("/", "/api/workflows", "/api/workflows/wf", "/api/workflows/wf/steps", "/api/nope"):
        assert database.url.encode() not in _request(viewer, path)[2]


def test_database_errors_are_redacted(database: Database, open_viewer: Any, capsys: pytest.CaptureFixture[str]) -> None:
    database.execute("SELECT 1")
    viewer = open_viewer()
    leaked = StoreError(f"could not reach {database.url} with {SECRET}")
    viewer._url = f"mysql://root:{SECRET}@host/db"
    assert viewer.explain(leaked) == _DB_FAILED
    assert SECRET not in capsys.readouterr().err


def _run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-m", "yado.ui", *arguments], capture_output=True, text=True, timeout=60, check=False)


def test_failed_start_does_not_print_the_password() -> None:
    result = _run_cli("--db", f"mysql://root:{SECRET}@127.0.0.1:1/nothing", "--port", "0")
    assert result.returncode == 1
    assert "Could not start the viewer" in result.stderr
    assert SECRET not in result.stdout + result.stderr


def test_missing_sqlite_file_is_refused_and_not_created(tmp_path: Any) -> None:
    path = tmp_path / "absent.sqlite"
    result = _run_cli("--db", f"sqlite://{path}", "--port", "0")
    assert result.returncode == 1
    assert "does not exist" in result.stderr
    assert not path.exists()


def test_no_option_can_change_the_bind_address() -> None:
    result = _run_cli("--help")
    assert "--host" not in result.stdout
    assert "--bind" not in result.stdout
