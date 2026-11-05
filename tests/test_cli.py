import json

import pytest

from rust_dwos import DBOS, DBOSConfig
from rust_dwos.cli import main


def test_the_cli_lists_and_gets_workflows_from_the_system_database(
    dbos: DBOS, config: DBOSConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    @DBOS.workflow()
    def wf() -> str:
        return "done"

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == "done"
    url = config["system_database_url"]
    assert main(["--system-database-url", url, "workflow", "list"]) == 0
    assert [row["workflow_id"] for row in json.loads(capsys.readouterr().out)] == [handle.workflow_id]
    assert main(["--system-database-url", url, "workflow", "get", handle.workflow_id]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "SUCCESS"
    assert main(["--system-database-url", url, "workflow", "get", "missing"]) == 1
    assert "dwos:" in capsys.readouterr().err
