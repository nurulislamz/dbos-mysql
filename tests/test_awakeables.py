from rust_dwos import DBOS, DBOSClient


def test_client_completes_awakeable(dbos: DBOS, database) -> None:
    @DBOS.workflow()
    def workflow() -> dict:
        id = DBOS.create_awakeable()
        DBOS.set_event("awakeable", id)
        return DBOS.wait_awakeable(id, timeout_seconds=30)

    handle = DBOS.start_workflow(workflow)
    id = DBOS.get_event(handle.get_workflow_id(), "awakeable", timeout_seconds=30)
    client = DBOSClient(system_database_url=database.url)
    try:
        assert client.complete_awakeable(id, {"ok": True})
        assert not client.complete_awakeable(id, {"ok": False})
        assert handle.get_result() == {"ok": True}
    finally:
        client.destroy()
