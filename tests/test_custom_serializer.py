import base64
import json
from typing import Any

import pytest

from yado import DBOS, DBOSConfig, SetWorkflowID, Serializer, StoreError, WorkflowSerializationFormat
from tests.conftest import Database


class Base64JsonSerializer(Serializer):
    """Writes base64 JSON, so a stored value shows that this encoder ran."""

    def serialize(self, data: Any) -> str:
        return base64.b64encode(json.dumps(data).encode()).decode("ascii")

    def deserialize(self, serialized_data: str) -> Any:
        return json.loads(base64.b64decode(serialized_data))

    def name(self) -> str:
        return "b64_json"


@pytest.fixture
def config(config: DBOSConfig) -> DBOSConfig:
    return {**config, "serializer": Base64JsonSerializer()}


def test_a_custom_serializer_writes_and_reads_inputs_outputs_steps_and_errors(dbos: DBOS, database: Database) -> None:
    @DBOS.step()
    def double(value: int) -> int:
        return value * 2

    @DBOS.workflow()
    def flow(value: int) -> dict[str, int]:
        return {"doubled": double(value)}

    @DBOS.workflow()
    def failing(reason: str) -> None:
        raise ValueError(reason)

    with SetWorkflowID("custom-ok"):
        assert flow(21) == {"doubled": 42}
    with SetWorkflowID("custom-error"), pytest.raises(ValueError, match="nope"):
        failing("nope")

    row = database.execute("SELECT serialization, output FROM workflow_status WHERE workflow_uuid = %s", "custom-ok")
    assert row[0][0] == "custom:b64_json"
    assert json.loads(base64.b64decode(row[0][1])) == {"doubled": 42}
    step = database.execute("SELECT serialization, output FROM operation_outputs WHERE workflow_uuid = %s", "custom-ok")
    assert step[0][0] == "custom:b64_json"
    assert json.loads(base64.b64decode(step[0][1])) == 42
    assert database.execute("SELECT serialization FROM workflow_status WHERE workflow_uuid = %s", "custom-error") == [("custom:b64_json",)]
    assert DBOS.get_result("custom-ok") == {"doubled": 42}


def test_a_workflow_can_choose_portable_json_under_a_custom_serializer(dbos: DBOS, database: Database) -> None:
    @DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
    def portable(value: int) -> int:
        return value + 1

    with SetWorkflowID("portable-one"):
        assert portable(1) == 2

    assert database.execute("SELECT serialization, output FROM workflow_status WHERE workflow_uuid = %s", "portable-one") == [("portable_json", "2")]


def test_a_serializer_name_must_be_safe_and_fit_the_column(config: DBOSConfig) -> None:
    class LongName(Base64JsonSerializer):
        def name(self) -> str:
            return "n" * 34

    DBOS.destroy(destroy_registry=True)
    DBOS(config={**config, "serializer": LongName()})
    try:
        with pytest.raises(StoreError, match="must be 1 to 33"):
            DBOS.launch()
    finally:
        DBOS.destroy(destroy_registry=True)
