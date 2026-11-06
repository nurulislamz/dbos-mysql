import pytest

from yado import DBOS, DBOSConfig, SetWorkflowID


def test_patch_and_application_version(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["application_version"] = "v1"
    DBOS(config=config)
    DBOS.launch()
    try:
        @DBOS.workflow()
        def workflow() -> bool:
            return DBOS.patch("change")

        with SetWorkflowID("patched"):
            assert workflow() is True
        status = DBOS.get_workflow_status("patched")
        assert status is not None and status.application_version == "v1"
    finally:
        DBOS.destroy(destroy_registry=True)


@pytest.mark.asyncio
async def test_async_patch_and_sync_guard(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()
    try:
        @DBOS.workflow()
        async def workflow() -> bool:
            with pytest.raises(RuntimeError, match="patch_async"):
                DBOS.patch("change")
            assert await DBOS.patch_async("change")
            return await DBOS.deprecate_patch_async("retired")

        assert await workflow()
    finally:
        DBOS.destroy(destroy_registry=True)
