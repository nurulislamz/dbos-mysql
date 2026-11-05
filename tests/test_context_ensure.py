import pytest

from rust_dwos import DBOS, DBOSConfig, DBOSContextEnsure, assert_current_dbos_context


def test_ensure_context(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBOS__VMID", "worker-1")
    executor_id = DBOS.executor_id
    with DBOSContextEnsure() as context:
        assert assert_current_dbos_context() is context
        assert not context.is_workflow()
        with DBOSContextEnsure() as nested:
            assert nested is context

        @DBOS.workflow()
        def workflow() -> str:
            original = assert_current_dbos_context()
            with DBOSContextEnsure() as current:
                assert current is original
                assert current.is_workflow()
                assert assert_current_dbos_context() is current
                assert current.executor_id == executor_id
                return current.workflow_id

        assert workflow()
    with pytest.raises(AssertionError):
        assert_current_dbos_context()


def test_executor_id_config(config: DBOSConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBOS__VMID", "environment")
    DBOS.destroy(destroy_registry=True)
    config["executor_id"] = "configured"
    try:
        DBOS(config=config)
        assert DBOS.executor_id == "configured"
    finally:
        DBOS.destroy(destroy_registry=True)
