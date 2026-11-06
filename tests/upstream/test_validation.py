from typing import Any

import pytest

from yado import make_pydantic_args_validator


def test_pydantic_validation_variadic_params() -> None:
    def typed(n: int, *rest: int, flag: str, **options: int) -> None:
        pass

    validate = make_pydantic_args_validator(typed)
    assert validate(("1", "5", "6"), {"flag": "y", "a": "7"}) == (
        (1, 5, 6), {"flag": "y", "a": 7}
    )
    with pytest.raises(ValueError, match="Argument 'rest'"):
        validate((1, "x"), {"flag": "y"})
    with pytest.raises(ValueError, match="Missing required argument: 'flag'"):
        validate((1,), {})


def test_pydantic_validator_concurrent_first_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading
    import time
    import pydantic

    real_adapter = pydantic.TypeAdapter

    def slow_adapter(hint: Any) -> Any:
        time.sleep(0.05)
        return real_adapter(hint)

    monkeypatch.setattr(pydantic, "TypeAdapter", slow_adapter)

    def variadic(n: int, *rest: str) -> None:
        pass

    validate = make_pydantic_args_validator(variadic)
    errors: list[Exception] = []

    def call() -> None:
        try:
            validate((1, "a"), {})
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=call) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert validate((1, "a", "b"), {}) == ((1, "a", "b"), {})


def test_workflow_pydantic_validation(dbos: Any) -> None:
    from yado import DBOS, pydantic_args_validator

    @DBOS.workflow(validate_args=pydantic_args_validator)
    def typed(n: int, *rest: int, **options: int) -> str:
        return f"{n}-{list(rest)}-{options}"

    assert typed("1", "5", a="7") == "1-[5]-{'a': 7}"
    with pytest.raises(ValueError, match="Argument 'rest'"):
        typed(1, "bad")


def test_log_config(config: Any) -> None:
    import logging
    from yado import DBOS

    DBOS.destroy(destroy_registry=True)
    config.update(log_level="DEBUG", console_log_level="ERROR")
    try:
        dbos = DBOS(config=config)
        DBOS.launch()
        assert dbos.logger.level == logging.DEBUG
        assert any(h.name == "__dbos_console_log_handler__" and h.level == logging.ERROR for h in dbos.logger.handlers)
    finally:
        DBOS.destroy(destroy_registry=True)
        logger = logging.getLogger("dbos")
        logger.setLevel(logging.NOTSET)
        for handler in logger.handlers[:]:
            if handler.name == "__dbos_console_log_handler__":
                logger.removeHandler(handler)


def test_log_config_before_launch() -> None:
    import logging
    from yado import DBOS

    logger = logging.getLogger("dbos")
    old_level = logger.level
    try:
        DBOS(config={"name": "test", "system_database_url": "sqlite://unused", "log_level": "DEBUG", "console_log_level": "ERROR"})
        assert logger.level == logging.DEBUG
        assert any(h.name == "__dbos_console_log_handler__" and h.level == logging.ERROR for h in logger.handlers)
    finally:
        DBOS.destroy(destroy_registry=True)
        logger.setLevel(old_level)
        for handler in logger.handlers[:]:
            if handler.name == "__dbos_console_log_handler__":
                logger.removeHandler(handler)
