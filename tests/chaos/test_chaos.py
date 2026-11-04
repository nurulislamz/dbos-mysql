import pytest

from tests.chaos.harness import ALL_FAULTS, ChaosConfig, run_chaos

pytestmark = [pytest.mark.chaos, pytest.mark.timeout(400)]


def _assert_clean(config: ChaosConfig) -> None:
    result = run_chaos(config)
    print(result)
    assert not result.violations, f"seed {result.seed}: {result.violations}"


@pytest.mark.parametrize("backend", ["sqlite", "mysql", "postgres"])
def test_no_step_repeats_without_faults(backend: str) -> None:
    _assert_clean(ChaosConfig(backend=backend, seed=1, faults=(), workflows=40))


@pytest.mark.parametrize("backend", ["sqlite", "mysql", "postgres"])
def test_workflows_survive_every_fault(backend: str) -> None:
    _assert_clean(ChaosConfig(backend=backend, seed=2, faults=ALL_FAULTS))
