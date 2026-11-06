# Adapted from dbos-transact-py tests/test_duplication_policy.py at commit 236142b. See MANIFEST.md.
import uuid

import pytest

from yado import DBOS, DBOSException, DBOSWorkflowIDInUseError, SetWorkflowID


def test_invalid_reuse_policy() -> None:
    with pytest.raises(DBOSException, match="Invalid workflow_id_reuse_policy"):
        SetWorkflowID("some-id", workflow_id_reuse_policy="bogus")  # type: ignore[arg-type]


def test_reject_reuse_policy(dbos: DBOS) -> None:
    @DBOS.workflow()
    def echo(value: str) -> str:
        return value

    @DBOS.workflow()
    def other(value: str) -> str:
        return value

    workflow_id = f"reuse-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
        assert DBOS.start_workflow(echo, "first").get_result() == "first"
    with SetWorkflowID(workflow_id, workflow_id_reuse_policy="return-existing"):
        assert echo("second") == "first"
    with pytest.raises(DBOSWorkflowIDInUseError) as caught:
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            echo("third")
    assert caught.value.workflow_id == workflow_id
    assert caught.value.workflow_status == "SUCCESS"
    assert caught.value.workflow_name == echo.__qualname__
    assert dbos.retrieve_workflow(workflow_id).get_result() == "first"

    with pytest.raises(DBOSWorkflowIDInUseError) as mismatch:
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            other("nope")
    assert mismatch.value.workflow_name == echo.__qualname__
