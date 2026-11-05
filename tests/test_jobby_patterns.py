"""Patterns Jobby's Temporal workflows rely on, through the SDK, on MySQL."""

import asyncio
import dataclasses
import subprocess
import sys
import textwrap
import time

import pytest

from rust_dwos import DBOS, DBOSException, SetWorkflowID
from rust_dwos.errors import DBOSStepTimeoutError

pytestmark = pytest.mark.parametrize("database", ["mysql"], indirect=True)

QUEUES = ("jobagami-job-finder", "jobagami-resume-studio", "jobagami-interview-prep", "jobagami-notifications", "jobagami-system-ops")


class ValidationError(Exception):
    pass


def retry_unless_validation(error: BaseException) -> bool:
    return not isinstance(error, ValidationError)


@dataclasses.dataclass
class CvPipelineInput:
    cv_id: str
    run_suggestions: bool = False


@dataclasses.dataclass
class CvPipelineResult:
    cv_id: str
    suggestions: dict[str, str] | None = None


def wait_until(predicate, seconds: float = 10.0) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline and not predicate():
        time.sleep(0.02)
    assert predicate()


def test_fingerprint_backfill_runs_from_its_schedule(dbos: DBOS, database) -> None:
    beats: list[int] = []

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3, backoff_rate=2.0, should_retry=retry_unless_validation, timeout_seconds=1800, heartbeat_timeout_seconds=120)
    async def backfill_job_fingerprints(payload: dict) -> dict[str, int]:
        jobs = ["a", "b", "c"]
        for index, _ in enumerate(jobs, start=1):
            await asyncio.sleep(0)
            DBOS.heartbeat()
            beats.append(index)
        return {"processed": len(jobs)}

    @DBOS.workflow(name="FingerprintBackfillWorkflow")
    async def fingerprint_backfill(payload: dict | None = None) -> dict[str, int]:
        return await backfill_job_fingerprints(payload or {})

    DBOS.register_queue("jobagami-system-ops")
    DBOS.register_schedule("fingerprint-backfill", fingerprint_backfill, "* * * * *", queue="jobagami-system-ops")
    database.execute("UPDATE workflow_schedules SET last_fired_epoch_ms = 0 WHERE schedule_name = 'fingerprint-backfill'")
    wait_until(lambda: [row.status for row in DBOS.list_workflows() if row.name == "FingerprintBackfillWorkflow"] == ["SUCCESS"])
    assert beats == [1, 2, 3]
    row = next(row for row in DBOS.list_workflows() if row.name == "FingerprintBackfillWorkflow")
    assert DBOS.retrieve_workflow(row.workflow_id).get_result() == {"processed": 3}


def test_a_retry_policy_gets_three_attempts_in_total(dbos: DBOS) -> None:
    calls: list[int] = []

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3, backoff_rate=2.0)
    async def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("provider down")
        return "ok"

    @DBOS.workflow()
    async def wf() -> str:
        return await flaky()

    assert DBOS.start_workflow(wf).get_result() == "ok"
    assert len(calls) == 3


def test_a_validation_error_is_not_retried_and_keeps_its_cause(dbos: DBOS) -> None:
    calls: list[int] = []

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3, should_retry=retry_unless_validation)
    async def validate() -> None:
        calls.append(1)
        raise ValidationError("missing id")

    @DBOS.workflow()
    async def wf() -> str:
        try:
            await validate()
        except ValidationError as error:
            return type(error).__name__
        return "no error"

    assert DBOS.start_workflow(wf).get_result() == "ValidationError"
    assert len(calls) == 1


def test_evaluation_records_a_timed_out_generation_as_failed(dbos: DBOS) -> None:
    marked: list[str] = []

    @DBOS.step(timeout_seconds=0.05)
    async def run_evaluation_llm(payload: dict) -> dict:
        await asyncio.sleep(30)
        return {"status": "done"}

    @DBOS.step(retries_allowed=True, interval_seconds=0.01, max_attempts=3, timeout_seconds=120)
    async def mark_evaluation_failed(payload: dict) -> dict:
        marked.append(payload["reason"])
        return {"status": "failed"}

    @DBOS.workflow(name="EvaluationRunWorkflow")
    async def evaluation(payload: dict) -> dict:
        try:
            return await run_evaluation_llm(payload)
        except DBOSStepTimeoutError as error:
            return await mark_evaluation_failed({**payload, "reason": str(error)})

    assert DBOS.start_workflow(evaluation, {"report_id": "r1"}).get_result() == {"status": "failed"}
    assert len(marked) == 1


def test_cancelling_an_evaluation_does_not_record_a_generation_failure(dbos: DBOS) -> None:
    marked: list[str] = []
    started: list[int] = []

    @DBOS.step(timeout_seconds=60)
    async def run_evaluation_llm() -> dict:
        started.append(1)
        await asyncio.sleep(30)
        return {"status": "done"}

    @DBOS.step()
    async def mark_evaluation_failed() -> dict:
        marked.append("failed")
        return {"status": "failed"}

    @DBOS.workflow()
    async def evaluation() -> dict:
        try:
            return await run_evaluation_llm()
        except Exception:  # noqa: BLE001
            return await mark_evaluation_failed()

    handle = DBOS.start_workflow(evaluation)
    wait_until(lambda: started)
    DBOS.cancel_workflow(handle.workflow_id)
    wait_until(lambda: handle.get_status().status == "CANCELLED")
    time.sleep(0.2)
    assert marked == []


def test_a_silent_step_times_out_on_its_heartbeat(dbos: DBOS) -> None:
    @DBOS.step(heartbeat_timeout_seconds=0.05, timeout_seconds=60)
    async def scrape() -> str:
        await asyncio.sleep(30)
        return "never"

    @DBOS.workflow()
    async def wf() -> str:
        try:
            return await scrape()
        except DBOSStepTimeoutError:
            return "stalled"

    assert DBOS.start_workflow(wf).get_result() == "stalled"


def test_a_recovered_pipeline_does_not_repeat_saved_steps(dbos: DBOS, database) -> None:
    extracted: list[str] = []
    suggested: list[str] = []
    release = asyncio.Event()

    @DBOS.step()
    async def extract_and_map_cv(cv_id: str) -> str:
        extracted.append(cv_id)
        return cv_id

    @DBOS.step()
    async def suggest_cv_improvements(cv_id: str) -> dict[str, str]:
        suggested.append(cv_id)
        return {"hint": "shorter"}

    @DBOS.workflow(name="CvPipelineWorkflow")
    async def cv_pipeline(payload: CvPipelineInput) -> CvPipelineResult:
        cv_id = await extract_and_map_cv(payload.cv_id)
        suggestions = await suggest_cv_improvements(cv_id) if payload.run_suggestions else None
        return CvPipelineResult(cv_id, suggestions)

    del release
    handle = DBOS.start_workflow(cv_pipeline, CvPipelineInput("cv-1", run_suggestions=True))
    assert handle.get_result() == CvPipelineResult("cv-1", {"hint": "shorter"})
    assert (extracted, suggested) == (["cv-1"], ["cv-1"])

    database.expire_lease(handle.workflow_id)
    assert [recovered.workflow_id for recovered in DBOS._recover_pending_workflows()] == [handle.workflow_id]  # noqa: SLF001
    wait_until(lambda: handle.get_status().status == "SUCCESS")
    assert handle.get_result() == CvPipelineResult("cv-1", {"hint": "shorter"})
    assert (extracted, suggested) == (["cv-1"], ["cv-1"])


def test_five_queues_run_in_one_process_and_keep_their_limits(dbos: DBOS) -> None:
    running: dict[str, int] = dict.fromkeys(QUEUES, 0)
    peak: dict[str, int] = dict.fromkeys(QUEUES, 0)

    @DBOS.workflow()
    async def work(queue: str) -> str:
        running[queue] += 1
        peak[queue] = max(peak[queue], running[queue])
        await asyncio.sleep(0.05)
        running[queue] -= 1
        return queue

    queues = {name: DBOS.register_queue(name, global_concurrency=2) for name in QUEUES}
    handles = [(name, queue.enqueue(work, name)) for name, queue in queues.items() for _ in range(5)]
    for name, handle in handles:
        assert handle.get_result() == name
    assert all(1 <= seen <= 2 for seen in peak.values()), peak


def test_a_duplicate_workflow_id_is_rejected(dbos: DBOS) -> None:
    runs: list[int] = []

    @DBOS.workflow()
    async def cv_pipeline() -> str:
        runs.append(1)
        return "done"

    queue = DBOS.register_queue("jobagami-resume-studio")
    with SetWorkflowID("cv-pipeline:1", workflow_id_reuse_policy="reject"):
        first = queue.enqueue(cv_pipeline)
    assert first.get_result() == "done"
    with SetWorkflowID("cv-pipeline:1", workflow_id_reuse_policy="reject"), pytest.raises(DBOSException):
        queue.enqueue(cv_pipeline)
    assert runs == [1]


async def test_an_api_handler_can_stop_waiting_and_the_work_goes_on(dbos: DBOS) -> None:
    done: list[int] = []

    @DBOS.workflow()
    async def discovery(payload: dict) -> dict:
        await asyncio.sleep(0.3)
        done.append(1)
        return {"companies": [payload["industry"]]}

    queue = await DBOS.register_queue_async("jobagami-job-finder")
    handle = await queue.enqueue_async(discovery, {"industry": "ai"})
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(handle.get_result(), timeout=0.02)
    assert await handle.get_result() == {"companies": ["ai"]}
    assert done == [1]


def test_an_api_process_enqueues_and_the_worker_process_runs(dbos: DBOS, config) -> None:
    ran: list[str] = []

    @DBOS.workflow(name="SendEmailWorkflow")
    async def send_email(send_id: str) -> str:
        ran.append(send_id)
        return send_id

    DBOS.register_queue("jobagami-notifications")
    api = textwrap.dedent(
        f"""
        from rust_dwos import DBOS

        @DBOS.workflow(name="SendEmailWorkflow")
        async def send_email(send_id: str) -> str:
            raise AssertionError("the API process must not run this")

        DBOS(config={{"name": "api", "system_database_url": {config["system_database_url"]!r}, "recovery_interval_seconds": 3600.0}})
        DBOS.launch()
        print(DBOS.enqueue_workflow("jobagami-notifications", send_email, "send-1").workflow_id)
        DBOS.destroy()
        """
    )
    result = subprocess.run([sys.executable, "-c", api], capture_output=True, text=True, timeout=60, check=False)  # noqa: S603
    assert result.returncode == 0, result.stderr
    workflow_id = result.stdout.strip().splitlines()[-1]
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "send-1"
    assert ran == ["send-1"]
