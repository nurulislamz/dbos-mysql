from yado import DBOS, DBOSConfig, SetWorkflowID


def test_status_records_the_executor(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["executor_id"] = "worker-a"
    DBOS(config=config)
    DBOS.launch()
    try:

        @DBOS.workflow()
        def workflow() -> str:
            return DBOS.executor_id

        with SetWorkflowID("executor-run"):
            assert workflow() == "worker-a"
        status = DBOS.get_workflow_status("executor-run")
        assert status is not None
        assert status.executor_id == "worker-a"
    finally:
        DBOS.destroy(destroy_registry=True)
