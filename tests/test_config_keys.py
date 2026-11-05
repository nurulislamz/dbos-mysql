import pytest

from rust_dwos import DBOS, DBOSException


def test_unknown_config_key_is_refused() -> None:
    DBOS.destroy(destroy_registry=True)
    with pytest.raises(DBOSException, match="Unknown DBOSConfig keys: nope"):
        DBOS(config={"name": "app", "system_database_url": "sqlite://x.sqlite", "nope": 1})  # type: ignore[typeddict-unknown-key]
    DBOS.destroy(destroy_registry=True)
