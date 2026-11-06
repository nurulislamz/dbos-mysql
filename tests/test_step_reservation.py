import pytest

from yado import DBOS, DBOSException


def test_reservation_order_and_single_use(dbos: DBOS) -> None:
    @DBOS.step()
    def inner() -> int | None:
        return DBOS.step_id

    @DBOS.workflow()
    def workflow() -> tuple[int, int, int | None]:
        first = DBOS.reserve_step()
        second = DBOS.reserve_step()
        value = second.run(None, inner)
        assert first.run(None, inner) == first.step_id
        with pytest.raises(DBOSException, match="only be run once"):
            first.run(None, inner)
        return first.step_id, second.step_id, value

    assert workflow() == (1, 2, 2)
    with pytest.raises(DBOSException, match="workflow"):
        DBOS.reserve_step()


async def test_reservation_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow() -> int:
        reservation = DBOS.reserve_step()

        async def step() -> int:
            return DBOS.step_id or 0

        return await reservation.run_async(None, step)

    assert await workflow() == 1
