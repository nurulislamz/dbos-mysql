import pytest

import yado
from yado import DBOS, DBOSConfig, DBOSException


def test_application_versions(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["application_version"] = "v1"
    DBOS(config=config)
    DBOS.launch()
    try:
        assert DBOS.application_version == "v1"
        assert [v["version_name"] for v in yado.list_application_versions()] == ["v1"]
        assert yado.get_latest_application_version()["version_name"] == "v1"
        yado.set_latest_application_version("v1")
        with pytest.raises(DBOSException, match="not registered"):
            yado.set_latest_application_version("missing")
    finally:
        DBOS.destroy(destroy_registry=True)


@pytest.mark.asyncio
async def test_application_versions_async(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["application_version"] = "v1"
    DBOS(config=config)
    DBOS.launch()
    try:
        assert (await yado.get_latest_application_version_async())["version_name"] == "v1"
        await yado.set_latest_application_version_async("v1")
        assert len(await yado.list_application_versions_async()) == 1
        with pytest.raises(RuntimeError, match="list_application_versions_async"):
            yado.list_application_versions()
    finally:
        DBOS.destroy(destroy_registry=True)
