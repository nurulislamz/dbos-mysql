"""One call per native binding. A call only has to connect and succeed."""

from collections.abc import AsyncIterator

import pytest

from rust_dwos import _native

# No concurrency limit is stored as the largest 32-bit value.
UNLIMITED = 2**32 - 1


@pytest.fixture
async def engine(database) -> AsyncIterator[_native.Engine]:
    connected = await _native.Engine.connect(database.url)
    yield connected
    connected.close()


async def _run(engine: _native.Engine, workflow_id: str) -> _native.Run:
    begun = await engine.begin("wf", "{}", None, workflow_id)
    assert begun.run is not None
    return begun.run


async def test_connect_and_close(database) -> None:
    connected = await _native.Engine.connect(database.url)
    assert isinstance(connected.serialization, str)
    connected.close()


async def test_begin(engine: _native.Engine) -> None:
    run = await _run(engine, "begin")
    assert run.workflow_id == "begin"
    assert isinstance(run.serialization, str)
    assert run.workflow_deadline_epoch_ms is None
    assert run.step_id_unless_in_step(False) == 1


async def test_begin_step(engine: _native.Engine) -> None:
    run = await _run(engine, "step")
    step_id, replay = await engine.begin_step(run, "s", False)
    assert step_id == 1
    assert replay is None


async def test_save_step_output(engine: _native.Engine) -> None:
    run = await _run(engine, "output")
    step_id, _replay = await engine.begin_step(run, "s", False)
    await engine.save_step_output(run, step_id, "s", "1")


async def test_save_step_error(engine: _native.Engine) -> None:
    run = await _run(engine, "error")
    step_id, _replay = await engine.begin_step(run, "s", False)
    await engine.save_step_error(run, step_id, "s", "boom")


async def test_conclude(engine: _native.Engine) -> None:
    run = await _run(engine, "conclude")
    await engine.conclude(run, output="1")


async def test_keep_lease(engine: _native.Engine) -> None:
    run = await _run(engine, "lease")
    active = engine.keep_lease(run)
    assert active.wait_lost(0) is None
    active.lost().cancel()
    active.stop()


async def test_await_result(engine: _native.Engine) -> None:
    run = await _run(engine, "result")
    await engine.conclude(run, output="1")
    result = await engine.await_result("result")
    assert result.workflow_id == "result"


async def test_get_status(engine: _native.Engine) -> None:
    await _run(engine, "status")
    row = await engine.get_status("status")
    assert isinstance(row.status, str)


async def test_cancel_workflow(engine: _native.Engine) -> None:
    await _run(engine, "cancel")
    await engine.cancel_workflow("cancel")


async def test_resume_workflow(engine: _native.Engine) -> None:
    await _run(engine, "resume")
    await engine.cancel_workflow("resume")
    claim = await engine.resume_workflow("resume", ["wf"])
    assert claim is None or claim.workflow_id == "resume"


async def test_list_workflows(engine: _native.Engine) -> None:
    await _run(engine, "list")
    rows = await engine.list_workflows()
    assert any(row.workflow_id == "list" for row in rows)


async def test_list_steps(engine: _native.Engine) -> None:
    run = await _run(engine, "steps")
    step_id, _replay = await engine.begin_step(run, "s", False)
    await engine.save_step_output(run, step_id, "s", "1")
    steps = await engine.list_steps("steps")
    assert len(steps) == 1


async def test_recover_now(engine: _native.Engine) -> None:
    claims = await engine.recover_now(["wf"])
    assert claims == []


async def test_recovery_tick(database) -> None:
    connected = await _native.Engine.connect(database.url, recovery_interval_seconds=0.01)
    try:
        claims = await connected.recovery_tick(["wf"])
        assert claims == []
    finally:
        connected.close()


async def test_sleep(engine: _native.Engine) -> None:
    run = await _run(engine, "sleep")
    remaining = await engine.sleep(run, 0.001, False)
    assert remaining >= 0


async def test_wait_sleep(engine: _native.Engine) -> None:
    run = await _run(engine, "wait-sleep")
    active = engine.keep_lease(run)
    assert await engine.wait_sleep(run, active, 0.001) is None
    active.stop()


async def test_send(engine: _native.Engine) -> None:
    await _run(engine, "inbox")
    sent = await engine.send("inbox", "1", _native.PORTABLE_JSON)
    assert sent.kind == "sent"


async def test_poll_recv(engine: _native.Engine) -> None:
    run = await _run(engine, "recv")
    poll = await engine.poll_recv(run, 1, 2, 0)
    assert poll.kind in {"ready", "waiting"}


async def test_wait_recv(engine: _native.Engine) -> None:
    run = await _run(engine, "wait-recv")
    poll = await engine.wait_recv(run, 1, 2, 0)
    assert poll.kind == "ready"


async def test_set_event(engine: _native.Engine) -> None:
    run = await _run(engine, "event")
    written = await engine.set_event(run, "k", "1", _native.PORTABLE_JSON)
    assert written.kind == "sent"


async def test_poll_event(engine: _native.Engine) -> None:
    run = await _run(engine, "poll-event")
    await engine.set_event(run, "k", "1", _native.PORTABLE_JSON)
    poll = await engine.poll_event("poll-event", "k", 0)
    assert poll.kind in {"ready", "waiting"}


async def test_wait_event(engine: _native.Engine) -> None:
    run = await _run(engine, "wait-event")
    await engine.set_event(run, "k", "1", _native.PORTABLE_JSON)
    poll = await engine.wait_event("wait-event", "k", 0)
    assert poll.kind == "ready"


async def test_list_events(engine: _native.Engine) -> None:
    run = await _run(engine, "events")
    await engine.set_event(run, "k", "1", _native.PORTABLE_JSON)
    rows = await engine.list_events("events")
    assert [row.key for row in rows] == ["k"]


async def test_queue_without_a_concurrency_limit(engine: _native.Engine, database) -> None:
    await engine.ensure_queue("q", None)
    stored = database.execute("SELECT concurrency FROM workflow_queues WHERE name = %s", "q")
    assert stored == [(UNLIMITED,)]


async def test_enqueue(engine: _native.Engine) -> None:
    await engine.ensure_queue("q", 1)
    workflow_id = await engine.enqueue("q", "wf", "{}", None, "queued")
    assert workflow_id == "queued"


async def test_get_queue(engine: _native.Engine) -> None:
    await engine.ensure_queue("q", 1)
    assert await engine.get_queue("q") == 1


async def test_claim_queue(engine: _native.Engine) -> None:
    await engine.ensure_queue("q", 1)
    await engine.enqueue("q", "wf", "{}", None, "claimed")
    claims = await engine.claim_queue("q", ["wf"])
    assert [claim.workflow_id for claim in claims] == ["claimed"]


async def test_wait_queue_claims(engine: _native.Engine) -> None:
    await engine.ensure_queue("q", 1)
    await engine.enqueue("q", "wf", "{}", None, "waited")
    claims = await engine.wait_queue_claims("q", ["wf"], 0.01)
    assert [claim.workflow_id for claim in claims] == ["waited"]


async def test_wake_queue(engine: _native.Engine) -> None:
    engine.wake_queue("q")


async def test_ensure_queue_with_no_limit_keeps_the_stored_limit(engine: _native.Engine) -> None:
    await engine.ensure_queue("capped", 2)
    await engine.ensure_queue("capped")
    assert await engine.get_queue("capped") == 2


async def test_delete_workflows(engine: _native.Engine) -> None:
    await _run(engine, "pending")
    assert await engine.delete_workflows(["pending"]) == []


async def test_delete_finished_before(engine: _native.Engine) -> None:
    assert await engine.delete_finished_before(0, 10) == 0


async def test_schedule_arms_without_firing(engine: _native.Engine) -> None:
    await engine.ensure_queue("notifications", 1)
    await engine.ensure_schedule(
        "daily-job-digest",
        "DispatchDailyDigestWorkflow",
        "0 8 * * *",
        "notifications",
        "{}",
        _native.PORTABLE_JSON,
        None,
    )
    assert await engine.fire_schedules() == []
    await engine.delete_schedule("daily-job-digest")


def test_step_policy() -> None:
    policy = _native.StepPolicy(False, 0, 1, 1)
    assert policy.wraps_failures is False
    assert policy.max_attempts == 1
    attempts = policy.attempts()
    assert attempts.status() == (None, None)
    assert attempts.failed() is None


async def test_step_watch() -> None:
    assert _native.StepPolicy(False, 0, 1, 1).watch() is None
    watch = _native.StepPolicy(False, 0, 1, 1, 60, 60).watch()
    assert watch.beat() is None
    assert watch.finish() is None
    assert await watch.expired() is None
    assert watch.expired(blocking=True) is None


def test_silence_logs() -> None:
    _native.silence_logs()
