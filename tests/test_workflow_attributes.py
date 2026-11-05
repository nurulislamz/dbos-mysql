import pytest

from rust_dwos import DBOS, DBOSException, SetWorkflowAttributes, SetWorkflowID


def test_workflow_attributes(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> None:
        pass

    @DBOS.workflow()
    def parent() -> None:
        child()

    with SetWorkflowAttributes({"customer": "acme", "nested": {"x": 1}}), SetWorkflowID("attr-parent"):
        parent()
    assert DBOS.get_workflow_status("attr-parent").attributes == {"customer": "acme", "nested": {"x": 1}}
    assert DBOS.get_workflow_status("attr-parent-1").attributes is None
    DBOS.update_workflow_attributes("attr-parent", {"nested": {"y": 2}, "null": None}, merge=True)
    assert DBOS.list_workflows(workflow_ids=["attr-parent"])[0].attributes == {"customer": "acme", "nested": {"y": 2}, "null": None}
    DBOS.update_workflow_attributes("attr-parent", {})
    assert DBOS.get_workflow_status("attr-parent").attributes == {}
    DBOS.update_workflow_attributes("attr-parent", None)
    assert DBOS.get_workflow_status("attr-parent").attributes is None
    with pytest.raises(DBOSException):
        DBOS.update_workflow_attributes("attr-parent", None, merge=True)
    with pytest.raises(DBOSException):
        SetWorkflowAttributes({"bad": object()})


@pytest.mark.asyncio
async def test_workflow_attributes_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow() -> None:
        await DBOS.update_workflow_attributes_async(DBOS.workflow_id, {"inside": True}, merge=True)

    with SetWorkflowAttributes({"initial": 1}), SetWorkflowID("async-attr"):
        await workflow()
    status = await DBOS.get_workflow_status_async("async-attr")
    assert status.attributes == {"initial": 1, "inside": True}
    await DBOS.update_workflow_attributes_async("async-attr", {"replacement": 2})
    assert (await DBOS.get_workflow_status_async("async-attr")).attributes == {"replacement": 2}
