import pytest

from yado import DBOS, DBOSException, DBOSStreamTimeoutError, SetWorkflowID


def test_write_read_and_close_a_stream(dbos: DBOS) -> None:
    @DBOS.step()
    def from_step() -> None:
        DBOS.write_stream("k", {"step": 2})

    @DBOS.workflow()
    def producer() -> None:
        DBOS.write_stream("k", 1)
        from_step()
        DBOS.close_stream("k")
        DBOS.write_stream("open", "x")

    with SetWorkflowID("producer"):
        producer()
    assert list(DBOS.read_stream("producer", "k")) == [1, {"step": 2}]
    assert list(DBOS.read_stream("producer", "k", offset=1)) == [{"step": 2}]
    # The workflow ended, so the stream it never closed ends too.
    assert list(DBOS.read_stream("producer", "open")) == ["x"]
    assert DBOS.read_stream_offset("producer", "k", 1) == {"step": 2}
    with pytest.raises(DBOSStreamTimeoutError):
        DBOS.read_stream_offset("producer", "k", 2)
    with pytest.raises(DBOSException):
        DBOS.write_stream("k", 1)


async def test_stream_async_reader_waits_for_the_writer(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def producer() -> None:
        await DBOS.recv_async("go")
        await DBOS.write_stream_async("k", "a")
        await DBOS.close_stream_async("k")

    with SetWorkflowID("async-producer"):
        handle = await DBOS.start_workflow_async(producer)
    with pytest.raises(DBOSStreamTimeoutError):
        await DBOS.read_stream_offset_async("async-producer", "k", 0, timeout_seconds=0.05)
    await DBOS.send_async("async-producer", None, "go")
    assert [value async for value in DBOS.read_stream_async("async-producer", "k", timeout_seconds=60)] == ["a"]
    await handle.get_result()
