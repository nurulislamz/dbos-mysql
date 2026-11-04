import base64
import json
import math
import os
import pickle
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rust_dwos import DBOS, DBOSException, DBOSMaxStepRetriesExceeded, DBOSRecordedError
from rust_dwos.serialization import Codec, decode_exception, encode_exception
from tests.conftest import Database


class _Detonator:
    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return (os.mkdir, (str(self.marker),))


class Declined(Exception):
    built = 0

    def __init__(self, code: int) -> None:
        type(self).built += 1
        self.code = code
        super().__init__(f"declined {code}")


@dataclass
class Order:
    item: str
    placed: datetime


def _blob(marker: Path) -> str:
    return base64.b64encode(pickle.dumps(_Detonator(marker))).decode("ascii")


def test_a_stored_pickle_blob_is_never_executed(dbos: DBOS, database: Database, tmp_path: Path) -> None:
    marker = tmp_path / "pickle-ran"
    blob = _blob(marker)
    pickle.loads(base64.b64decode(blob))  # noqa: S301
    assert marker.exists(), "the blob must really run when unpickled, or this test proves nothing"
    marker.rmdir()

    @DBOS.step()
    def step() -> int:
        return 1

    @DBOS.workflow()
    def workflow() -> int:
        return step()

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 1
    workflow_id = handle.workflow_id

    database.execute("UPDATE operation_outputs SET output = %s WHERE workflow_uuid = %s", blob, workflow_id)
    with pytest.raises(DBOSException, match="not valid JSON"):
        DBOS.list_workflow_steps(workflow_id)

    database.execute("UPDATE workflow_status SET output = %s WHERE workflow_uuid = %s", blob, workflow_id)
    with pytest.raises(DBOSException, match="not valid JSON"):
        DBOS.retrieve_workflow(workflow_id).get_result()

    database.execute("UPDATE workflow_status SET status = 'ERROR', output = NULL, error = %s WHERE workflow_uuid = %s", blob, workflow_id)
    with pytest.raises(DBOSRecordedError, match="could not be loaded"):
        DBOS.retrieve_workflow(workflow_id).get_result()

    with pytest.raises(DBOSException, match="not valid JSON"):
        Codec(workflow).decode_input(blob)
    assert not marker.exists()


@pytest.mark.parametrize("text", ["not json", "", "{", "gASVAAAA"])
def test_malformed_values_give_a_clear_error(text: str) -> None:
    codec = Codec(lambda: None)
    with pytest.raises(DBOSException, match="not valid JSON"):
        codec.decode_output(text)
    with pytest.raises(DBOSException, match="not valid JSON"):
        codec.decode_input(text)


def test_a_workflow_input_of_the_wrong_shape_gives_a_clear_error() -> None:
    with pytest.raises(DBOSException, match="wrong shape"):
        Codec(lambda: None).decode_input("[1]")


@pytest.mark.parametrize("text", ["not json", "[]", "1", '{"error": "boom"}', '{"name": 1}'])
def test_malformed_errors_become_a_recorded_error(text: str) -> None:
    assert isinstance(decode_exception(text), DBOSRecordedError)


def test_inputs_use_the_dbos_envelope_and_keep_gaps_in_defaults() -> None:
    def function(a: int, b: int = 2, *, c: int = 3) -> None: ...

    def gap(a: int, b: int = 2, c: int = 3) -> None: ...

    assert json.loads(Codec(function).encode_input((1,), {"c": 9})) == {"positionalArgs": [1], "namedArgs": {"c": 9}}
    encoded = Codec(gap).encode_input((1,), {"c": 9})
    assert json.loads(encoded) == {"positionalArgs": [1], "namedArgs": {"c": 9}}
    assert Codec(gap).decode_input(encoded) == ((1,), {"c": 9})


def test_inputs_written_by_another_sdk_can_leave_fields_empty() -> None:
    def function(a: int = 1) -> None: ...

    assert Codec(function).decode_input('{"positionalArgs": null, "namedArgs": null}') == ((), {})
    assert Codec(function).decode_input('{"positionalArgs": [5]}') == ((5,), {})


def test_var_args_round_trip_with_their_types() -> None:
    moment = datetime(2026, 1, 2, tzinfo=UTC)

    def function(*items: datetime, **named: datetime) -> None: ...

    codec = Codec(function)
    assert codec.decode_input(codec.encode_input((moment,), {"at": moment})) == ((moment,), {"at": moment})


def test_annotations_restore_rich_types() -> None:
    order = Order("book", datetime(2026, 1, 2, tzinfo=UTC))

    def function(order: Order, pair: tuple[int, str]) -> Order: ...

    codec = Codec(function)
    assert codec.decode_input(codec.encode_input((order, (1, "a")), {})) == ((order, (1, "a")), {})
    assert codec.decode_output(codec.encode_output(order)) == order


def test_without_annotations_values_come_back_as_plain_json() -> None:
    codec = Codec(lambda: None)
    assert codec.decode_output(codec.encode_output((1, 2))) == [1, 2]
    assert codec.decode_output(codec.encode_output({1: "a"})) == {"1": "a"}


@dataclass
class Reading:
    value: float


@pytest.mark.parametrize("value", [object(), math.nan, math.inf, {"deep": [math.nan]}, {math.inf}, Reading(math.nan), b"\xff"])
def test_values_json_cannot_hold_are_refused(value: Any) -> None:
    with pytest.raises(TypeError, match="cannot be stored"):
        Codec(lambda: None).encode_output(value)


def test_errors_use_the_dbos_envelope() -> None:
    payload = json.loads(encode_exception(ValueError("bad")))
    assert payload["name"] == "ValueError"
    assert payload["message"] == "bad"
    assert payload["code"] is None
    assert set(payload["data"]) == {"module", "args", "attributes"}


def test_an_error_from_another_sdk_is_recorded() -> None:
    loaded = decode_exception('{"name": "IllegalStateException", "message": "boom", "code": null, "data": null}')
    assert isinstance(loaded, DBOSRecordedError)
    assert loaded.type_name == "IllegalStateException"
    assert str(loaded) == "boom"


def test_custom_errors_keep_type_and_attributes_without_running_their_constructor() -> None:
    encoded = encode_exception(Declined(7))
    built = Declined.built
    loaded = decode_exception(encoded)
    assert Declined.built == built
    assert isinstance(loaded, Declined)
    assert loaded.code == 7
    assert str(loaded) == "declined 7"


def test_builtin_errors_keep_their_arguments() -> None:
    loaded = decode_exception(encode_exception(FileNotFoundError(2, "gone")))
    assert isinstance(loaded, FileNotFoundError)
    assert loaded.errno == 2
    assert str(loaded) == "[Errno 2] gone"


def test_values_an_error_cannot_store_do_not_block_saving_it() -> None:
    error = ValueError(object())
    error.unstorable = object()  # type: ignore[attr-defined]
    error.kept = 1  # type: ignore[attr-defined]
    loaded = decode_exception(encode_exception(error))
    assert isinstance(loaded, ValueError)
    assert loaded.kept == 1  # type: ignore[attr-defined]
    assert not hasattr(loaded, "unstorable")


def test_nested_step_errors_round_trip() -> None:
    loaded = decode_exception(encode_exception(DBOSMaxStepRetriesExceeded("step", 2, [Declined(1), KeyError("k")])))
    assert isinstance(loaded, DBOSMaxStepRetriesExceeded)
    assert isinstance(loaded.errors[0], Declined)
    assert isinstance(loaded.errors[1], KeyError)


def test_stored_text_cannot_import_a_module_or_raise_a_non_exception() -> None:
    assert "antigravity" not in sys.modules
    imported = '{"name": "geohash", "message": "m", "data": {"module": "antigravity", "args": ["m"]}}'
    assert isinstance(decode_exception(imported), DBOSRecordedError)
    assert "antigravity" not in sys.modules

    for name, module in (("SystemExit", "builtins"), ("KeyboardInterrupt", "builtins"), ("system", "os"), ("Path", "pathlib")):
        text = json.dumps({"name": name, "message": "m", "data": {"module": module, "args": ["m"]}})
        assert isinstance(decode_exception(text), DBOSRecordedError), name
