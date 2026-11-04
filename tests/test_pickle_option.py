import base64
import os
import pickle
import subprocess
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from rust_dwos import DBOS, DBOSConfig, DBOSException, DBOSMaxStepRetriesExceeded, DBOSRecordedError, SetWorkflowID
from tests.conftest import Database
from tests.crash_app import register

RICH = {
    "tuple": (1, 2),
    "set": {3},
    "when": datetime(2026, 1, 2, tzinfo=UTC),
    "id": uuid.UUID(int=7),
    "price": Decimal("1.50"),
    "bytes": b"\xff",
    "keys": {1: "one"},
}


@dataclass
class Order:
    item: str


class Declined(Exception):
    def __init__(self, code: int, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(code, reason)


class _Detonator:
    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return (os.mkdir, (str(self.marker),))


def _blob(marker: Path) -> str:
    return base64.b64encode(pickle.dumps(_Detonator(marker))).decode("ascii")


def _launch(config: DBOSConfig, serializer: str) -> None:
    DBOS.destroy()
    DBOS(config={**config, "serializer": serializer})
    DBOS.launch()


@pytest.fixture
def clean(config: DBOSConfig) -> Iterator[DBOSConfig]:
    DBOS.destroy(destroy_registry=True)
    yield config
    DBOS.destroy(destroy_registry=True)


@pytest.fixture
def pickled(clean: DBOSConfig) -> DBOSConfig:
    DBOS(config=clean)  # pickle is the default
    DBOS.launch()
    return clean


def test_rich_values_keep_their_types_with_no_annotations(pickled: DBOSConfig, database: Database) -> None:
    runs = 0

    @DBOS.step()
    def step(value):  # noqa: ANN001, ANN202
        nonlocal runs
        runs += 1
        return value

    @DBOS.workflow()
    def workflow(value, order):  # noqa: ANN001, ANN202
        return {"step": step(value), "order": order}

    workflow_id = str(uuid.uuid4())
    expected = {"step": RICH, "order": Order("pen")}
    with SetWorkflowID(workflow_id):
        assert workflow(RICH, Order("pen")) == expected
    assert runs == 1
    with SetWorkflowID(workflow_id):
        assert workflow(RICH, Order("pen")) == expected
    assert runs == 1

    tags = database.execute("SELECT serialization FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert tags[0][0] == "py_pickle"
    tags = database.execute("SELECT serialization FROM operation_outputs WHERE workflow_uuid = %s", workflow_id)
    assert tags[0][0] == "py_pickle"
    assert DBOS.list_workflow_steps(workflow_id)[0]["output"] == RICH
    assert DBOS.retrieve_workflow(workflow_id).get_status().output == expected


def test_pickle_is_the_default(pickled: DBOSConfig, database: Database) -> None:
    @DBOS.workflow()
    def workflow() -> int:
        return 1

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    rows = database.execute("SELECT serialization FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
    assert rows == [("py_pickle",)]


def test_a_pickled_exception_replays_as_the_same_type(pickled: DBOSConfig) -> None:
    @DBOS.workflow()
    def workflow() -> None:
        raise Declined(7, "no funds")

    workflow_id = str(uuid.uuid4())
    for _ in range(2):
        with SetWorkflowID(workflow_id), pytest.raises(Declined) as caught:
            workflow()
        assert (caught.value.code, caught.value.reason) == (7, "no funds")


def test_a_failed_step_replays_with_its_retry_errors(pickled: DBOSConfig) -> None:
    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=2)
    def step() -> None:
        raise Declined(1, "again")

    @DBOS.workflow()
    def workflow() -> None:
        step()

    workflow_id = str(uuid.uuid4())
    for _ in range(2):
        with SetWorkflowID(workflow_id), pytest.raises(DBOSMaxStepRetriesExceeded) as caught:
            workflow()
        assert [type(error) for error in caught.value.errors] == [Declined, Declined]


def test_an_exception_that_cannot_be_rebuilt_is_recorded_rather_than_lost(pickled: DBOSConfig) -> None:
    class Local(Exception):
        pass

    @DBOS.workflow()
    def workflow() -> None:
        raise Local("kept")

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id), pytest.raises(Local):
        workflow()
    with SetWorkflowID(workflow_id), pytest.raises(DBOSRecordedError, match="kept"):
        workflow()


def test_a_value_pickle_cannot_store_fails_clearly(pickled: DBOSConfig) -> None:
    @DBOS.workflow()
    def workflow() -> Any:
        return lambda: None

    with pytest.raises(TypeError, match="cannot be stored with pickle"):
        workflow()


def test_json_is_used_when_the_config_asks(clean: DBOSConfig, database: Database) -> None:
    _launch(clean, "json")

    @DBOS.workflow()
    def workflow() -> int:
        return 1

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    rows = database.execute("SELECT serialization, output FROM workflow_status WHERE workflow_uuid = %s", handle.workflow_id)
    assert rows == [("portable_json", "1")]


@pytest.mark.parametrize("column", ["workflow_output", "workflow_error", "step_output", "step_error"])
def test_a_json_process_refuses_a_row_tagged_pickle_and_never_runs_it(clean: DBOSConfig, database: Database, tmp_path: Path, column: str) -> None:
    _launch(clean, "pickle")  # the flag must not survive into the next launch
    _launch(clean, "json")
    marker = tmp_path / "pickle-ran"
    blob = _blob(marker)

    @DBOS.step()
    def step() -> int:
        return 1

    @DBOS.workflow()
    def workflow() -> int:
        return step()

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    workflow_id = handle.workflow_id

    if column.startswith("workflow"):
        database.execute("UPDATE workflow_status SET serialization = 'py_pickle' WHERE workflow_uuid = %s", workflow_id)
        if column == "workflow_output":
            database.execute("UPDATE workflow_status SET output = %s WHERE workflow_uuid = %s", blob, workflow_id)
        else:
            database.execute("UPDATE workflow_status SET status = 'ERROR', output = NULL, error = %s WHERE workflow_uuid = %s", blob, workflow_id)
        with pytest.raises(DBOSException, match="uses pickle"):
            DBOS.retrieve_workflow(workflow_id).get_result()
    else:
        database.execute("UPDATE operation_outputs SET serialization = 'py_pickle' WHERE workflow_uuid = %s", workflow_id)
        if column == "step_output":
            database.execute("UPDATE operation_outputs SET output = %s WHERE workflow_uuid = %s", blob, workflow_id)
        else:
            database.execute("UPDATE operation_outputs SET output = NULL, error = %s WHERE workflow_uuid = %s", blob, workflow_id)
        with pytest.raises(DBOSException, match="uses pickle"):
            DBOS.list_workflow_steps(workflow_id)
    assert not marker.exists()


def test_with_pickle_on_a_row_tagged_json_is_still_read_as_json(pickled: DBOSConfig, database: Database, tmp_path: Path) -> None:
    marker = tmp_path / "pickle-ran"

    @DBOS.workflow()
    def workflow() -> int:
        return 1

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    database.execute("UPDATE workflow_status SET serialization = 'portable_json', output = %s WHERE workflow_uuid = %s", _blob(marker), handle.workflow_id)
    with pytest.raises(DBOSException, match="not valid JSON"):
        DBOS.retrieve_workflow(handle.workflow_id).get_result()
    assert not marker.exists()


def test_an_unknown_format_is_refused(pickled: DBOSConfig, database: Database) -> None:
    @DBOS.workflow()
    def workflow() -> int:
        return 1

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    database.execute("UPDATE workflow_status SET serialization = 'mystery' WHERE workflow_uuid = %s", handle.workflow_id)
    with pytest.raises(DBOSException, match="unknown format"):
        DBOS.retrieve_workflow(handle.workflow_id).get_result()


def test_a_pickled_workflow_waits_for_a_process_that_can_read_it(pickled: DBOSConfig, database: Database) -> None:
    @DBOS.workflow()
    def workflow(value: Any) -> Any:
        return value

    handle = DBOS.start_workflow(workflow, RICH)
    assert handle.get_result() == RICH
    workflow_id = handle.workflow_id
    database.expire_lease(workflow_id)

    _launch(pickled, "json")
    assert DBOS._recover_pending_workflows() == []
    attempts = "SELECT recovery_attempts FROM workflow_status WHERE workflow_uuid = %s"
    before = database.execute(attempts, workflow_id)
    assert DBOS._recover_pending_workflows() == []
    assert database.execute(attempts, workflow_id) == before

    _launch(pickled, "pickle")
    recovered = DBOS._recover_pending_workflows()
    assert [item.workflow_id for item in recovered] == [workflow_id]
    assert recovered[0].get_result() == RICH


def test_a_json_workflow_stays_json_when_recovered_with_pickle_on(clean: DBOSConfig, database: Database) -> None:
    _launch(clean, "json")

    @DBOS.step()
    def step() -> int:
        return 2

    @DBOS.workflow()
    def workflow(value: int) -> int:
        return value + step()

    handle = DBOS.start_workflow(workflow, 1)
    assert handle.get_result() == 3
    workflow_id = handle.workflow_id
    database.execute("DELETE FROM operation_outputs WHERE workflow_uuid = %s", workflow_id)
    database.expire_lease(workflow_id)

    _launch(clean, "pickle")
    recovered = DBOS._recover_pending_workflows()
    assert recovered[0].get_result() == 3
    rows = database.execute("SELECT serialization, output FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert rows == [("portable_json", "3")]
    steps = database.execute("SELECT serialization, output FROM operation_outputs WHERE workflow_uuid = %s", workflow_id)
    assert steps == [("portable_json", "2")]


def test_an_unknown_serializer_is_rejected(clean: DBOSConfig) -> None:
    with pytest.raises(DBOSException, match="serializer must be json or pickle"):
        _launch(clean, "yaml")


@pytest.mark.parametrize("kind", ["async", "sync"])
def test_a_killed_process_recovers_with_pickle_without_repeating_steps(clean: DBOSConfig, database: Database, tmp_path: Path, kind: str) -> None:
    ledger = tmp_path / "ledger.txt"
    workflow_id = str(uuid.uuid4())
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).parent.parent)}
    child = subprocess.run(
        [sys.executable, "-m", "tests.crash_app", database.url, workflow_id, kind, str(ledger), "pickle"],
        env=environment,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert child.returncode == 9, child.stderr.decode()
    assert ledger.read_text().split() == ["one"]

    register(ledger, crash=False)
    DBOS(config={**clean, "serializer": "pickle", "lease_seconds": 1.0, "recovery_interval_seconds": 0.1})
    DBOS.launch()

    handle = DBOS.retrieve_workflow(workflow_id)
    assert handle.get_result() == "onetwo"
    assert ledger.read_text().split() == ["one", "two"]
    assert handle.get_status().recovery_attempts == 2
    rows = database.execute("SELECT serialization FROM workflow_status WHERE workflow_uuid = %s", workflow_id)
    assert rows == [("py_pickle",)]
