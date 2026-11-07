from __future__ import annotations

from typing import Any, TypedDict

from yado.errors import DBOSException
from yado.sdk_calls import _no_running_loop

class VersionInfo(TypedDict):
    version_id: str
    version_name: str
    version_timestamp: int
    created_at: int
    application_name: str | None


def _dbos() -> Any:
    from yado import dbos

    return dbos


def _info(row: Any) -> VersionInfo:
    return VersionInfo(
        version_id=row.version_id,
        version_name=row.version_name,
        version_timestamp=row.version_timestamp,
        created_at=row.created_at,
        application_name=None,
    )


class Versions:
    @classmethod
    def list_application_versions(cls) -> list[VersionInfo]:
        """All versions, newest first."""
        _no_running_loop("list_application_versions")
        instance = _dbos()._running()
        return [_info(row) for row in instance._blocking(instance._engine.list_versions)]

    @classmethod
    async def list_application_versions_async(cls) -> list[VersionInfo]:
        return [_info(row) for row in await _dbos()._running()._engine.list_versions()]

    @classmethod
    def get_latest_application_version(cls) -> VersionInfo:
        versions = cls.list_application_versions()
        if not versions:
            raise DBOSException("No application versions found")
        return versions[0]

    @classmethod
    async def get_latest_application_version_async(cls) -> VersionInfo:
        versions = await cls.list_application_versions_async()
        if not versions:
            raise DBOSException("No application versions found")
        return versions[0]

    @classmethod
    def set_latest_application_version(cls, version_name: str) -> None:
        """Make a registered version the latest. It then also runs version-less workflows."""
        _no_running_loop("set_latest_application_version")
        instance = _dbos()._running()
        instance._blocking(instance._engine.set_latest_version, version_name)

    @classmethod
    async def set_latest_application_version_async(cls, version_name: str) -> None:
        await _dbos()._running()._engine.set_latest_version(version_name)

