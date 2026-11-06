import importlib
import sqlite3
import sys


def test_ledger_records_first_attempt_and_retries(monkeypatch, tmp_path):
    for name, value in {
        "CHAOS_WORKER": "0", "CHAOS_WORKERS": "1", "CHAOS_WORKFLOWS": "1",
        "CHAOS_URL": "sqlite://test", "CHAOS_LEDGER": str(tmp_path / "ledger.sqlite"),
    }.items():
        monkeypatch.setenv(name, value)
    sys.modules.pop("tests.chaos.worker", None)
    worker = importlib.import_module("tests.chaos.worker")
    connect = sqlite3.connect
    with connect(worker.LEDGER) as connection:
        connection.execute("CREATE TABLE ledger (workflow_id TEXT, step_id INTEGER, pid INTEGER, started_at_ms INTEGER, first_attempt_at_ms INTEGER, retries INTEGER)")
    calls = 0

    def fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("unavailable")
        return connect(*args, **kwargs)

    monkeypatch.setattr(worker.sqlite3, "connect", fail_once)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    worker._write_ledger("wf-1", 1)
    with connect(worker.LEDGER) as connection:
        started, first_attempt, retries = connection.execute("SELECT started_at_ms, first_attempt_at_ms, retries FROM ledger").fetchone()
    assert first_attempt <= started
    assert retries == 1
