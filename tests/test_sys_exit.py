import sys
import uuid
from typing import Any

import pytest

from yado import DBOS, SetWorkflowID
from tests.conftest import Database, retry_until_success


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_sys_exit_in_a_background_workflow_is_released_and_other_work_keeps_going(
    dbos: DBOS, database: Database, kind: str
) -> None:
    """Find out what sys.exit() does in a background workflow, then keep this as the bar.

    It must not kill the workflows thread or store a final error. The workflow is
    released for recovery, and later work still runs.
    """

    @DBOS.workflow()
    def dying_sync() -> None:
        sys.exit(1)

    @DBOS.workflow()
    async def dying_async() -> None:
        sys.exit(1)

    @DBOS.workflow()
    def living_sync() -> str:
        return "ok"

    @DBOS.workflow()
    async def living_async() -> str:
        return "ok"

    dying = dying_async if kind == "async" else dying_sync
    living = living_async if kind == "async" else living_sync

    handle = DBOS.start_workflow(dying)

    def released() -> tuple[Any, ...]:
        row = database.execute(
            "SELECT status, error, output, lease_token FROM workflow_status WHERE workflow_uuid = %s",
            handle.workflow_id,
        )[0]
        assert row[0] == "PENDING", row
        assert row[1] is None and row[2] is None
        assert row[3] is None, "the workflow must be unclaimed so recovery can take it"
        return row

    retry_until_success(released, interval=0.05, max_attempts=40)

    if kind == "async":
        assert DBOS.start_workflow(living).get_result() == "ok"
    else:
        assert living() == "ok"

    database.expire_lease(handle.workflow_id)
    recovered = DBOS._recover_pending_workflows()
    assert [item.workflow_id for item in recovered] == [handle.workflow_id]
    retry_until_success(released, interval=0.05, max_attempts=40)

    still = str(uuid.uuid4())
    with SetWorkflowID(still):
        assert DBOS.start_workflow(living).get_result() == "ok"
    assert DBOS.get_workflow_status(still).status == "SUCCESS"
