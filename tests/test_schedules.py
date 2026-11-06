import time

import pytest

from yado import DBOS, DBOSException


def test_a_due_schedule_runs_its_workflow(dbos: DBOS, database) -> None:
    ran: list[str] = []

    @DBOS.workflow()
    def digest() -> str:
        ran.append("ran")
        return "sent"

    DBOS.register_queue("notifications")
    DBOS.register_schedule("daily-job-digest", digest, "* * * * *", queue="notifications")
    database.execute(
        "UPDATE workflow_schedules SET last_fired_epoch_ms = %s WHERE schedule_name = %s",
        0,
        "daily-job-digest",
    )
    deadline = time.time() + 10
    while time.time() < deadline and not ran:
        time.sleep(0.05)
    assert ran == ["ran"]
    finished = [row for row in DBOS.list_workflows() if row.name.endswith("digest")]
    assert len(finished) == 1
    # The body appends before its result row is saved.
    assert DBOS.retrieve_workflow(finished[0].workflow_id).get_result() == "sent"
    assert DBOS.get_workflow_status(finished[0].workflow_id).status == "SUCCESS"


def test_schedule_arguments_must_fit_the_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def digest(user_id: str) -> str:
        return user_id

    with pytest.raises(DBOSException, match="user_id"):
        DBOS.register_schedule("daily-job-digest", digest, "0 8 * * *", queue="notifications")


def test_a_schedule_passes_arguments_and_can_be_deleted(dbos: DBOS, database) -> None:
    ran: list[tuple[str, str]] = []

    @DBOS.workflow()
    def discovery(user_id: str, definition_id: str) -> None:
        ran.append((user_id, definition_id))

    DBOS.register_queue("system-ops")
    DBOS.register_schedule(
        "career-discovery-1",
        discovery,
        "* * * * *",
        queue="system-ops",
        args=("user-1",),
        kwargs={"definition_id": "1"},
    )
    database.execute(
        "UPDATE workflow_schedules SET last_fired_epoch_ms = %s WHERE schedule_name = %s",
        0,
        "career-discovery-1",
    )
    deadline = time.time() + 10
    while time.time() < deadline and not ran:
        time.sleep(0.05)
    assert ran == [("user-1", "1")]

    DBOS.delete_schedule("career-discovery-1")
    DBOS.delete_schedule("career-discovery-1")
    assert database.execute("SELECT schedule_name FROM workflow_schedules") == []


def test_an_invalid_cron_is_rejected(dbos: DBOS) -> None:
    @DBOS.workflow()
    def digest() -> str:
        return "sent"

    DBOS.register_queue("notifications")
    with pytest.raises(DBOSException):
        DBOS.register_schedule("daily-job-digest", digest, "not a cron", queue="notifications")


def test_scheduled_wraps_a_workflow(dbos: DBOS, database) -> None:
    @DBOS.scheduled("0 8 * * *", queue="notifications", name="deadline-reminders")
    @DBOS.workflow()
    def reminders() -> str:
        return "sent"

    rows = database.execute(
        "SELECT workflow_name, cron, queue_name FROM workflow_schedules WHERE schedule_name = %s",
        "deadline-reminders",
    )
    assert len(rows) == 1
    assert str(rows[0][0]).endswith("reminders")
    assert rows[0][1:] == ("0 8 * * *", "notifications")
    assert reminders() == "sent"
