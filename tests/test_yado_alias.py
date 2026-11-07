import pytest

import yado
from yado import DBOS


def test_module_methods_are_the_class_methods() -> None:
    # A classmethod lookup builds a new bound-method object, so compare the function and the class.
    for name in ("workflow", "step", "sleep"):
        alias = getattr(yado, name)
        method = getattr(DBOS, name)
        assert alias.__func__ is method.__func__
        assert alias.__self__ is method.__self__


@pytest.mark.parametrize("database", ["sqlite"], indirect=True)
def test_yado_decorators_run_on_sqlite(dbos: DBOS) -> None:
    @yado.step()
    def add_one(value: int) -> int:
        return value + 1

    @yado.workflow()
    def total(value: int) -> int:
        return add_one(value)

    assert total(1) == 2
    assert dbos is not None
