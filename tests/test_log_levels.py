import logging

import pytest

from rust_dwos import DBOS, DBOSConfig

_CONSOLE = "__dbos_console_log_handler__"


def test_launching_with_log_levels_applies_them(config: DBOSConfig, caplog: pytest.LogCaptureFixture) -> None:
    dbos_logger = logging.getLogger("dbos")
    original = dbos_logger.level
    DBOS.destroy(destroy_registry=True)
    try:
        DBOS(config={**config, "log_level": "WARNING", "console_log_level": "ERROR"})
        DBOS.launch()

        handler = next(h for h in dbos_logger.handlers if h.name == _CONSOLE)
        assert dbos_logger.level == logging.WARNING
        assert handler.level == logging.ERROR

        @DBOS.workflow()
        def flow() -> int:
            DBOS.logger.info("dropped")
            DBOS.logger.warning("kept")
            return 1

        assert flow() == 1
        messages = [record.getMessage() for record in caplog.records if record.name == "dbos"]
        assert "kept" in messages
        assert "dropped" not in messages
    finally:
        DBOS.destroy(destroy_registry=True)
        dbos_logger.setLevel(original)
