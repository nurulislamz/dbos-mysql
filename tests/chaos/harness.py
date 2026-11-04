import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pymysql

REPO = Path(__file__).resolve().parents[2]
MYSQL_PORT = int(os.environ.get("DBOS_CHAOS_DB_PORT", "13308"))
POSTGRES_PORT = int(os.environ.get("DBOS_CHAOS_PG_PORT", "13310"))
MYSQL_PASSWORD = "dbos-test-root"
COMPOSE_PROJECT = "rust-dwos-chaos"
ALL_FAULTS = ("kill", "pause", "term", "crash_window", "db_restart")
STEPS = 3


@dataclass
class ChaosConfig:
    backend: str
    seed: int
    faults: tuple[str, ...] = ALL_FAULTS
    workers: int = 4
    workflows: int = 1000
    fault_seconds: float = 15.0
    finish_deadline_seconds: float = 100.0
    lease_seconds: float = 1.0


@dataclass
class ChaosResult:
    seed: int
    backend: str
    faults_fired: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0
    repeated_steps: int = 0
    recovered_workflows: int = 0
    violations: list[str] = field(default_factory=list)


class _Target:
    def __init__(self, config: ChaosConfig, directory: Path) -> None:
        self.backend = config.backend
        self._directory = directory
        self._name = f"dbos_c_{uuid.uuid4().hex}"
        if self.backend == "sqlite":
            self.url = f"sqlite://{directory / 'workflows.sqlite'}"
            self.ledger = str(directory / "ledger.sqlite")
            with sqlite3.connect(self.ledger) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("CREATE TABLE ledger (workflow_id TEXT, step_id INTEGER, pid INTEGER, started_at_ms INTEGER)")
                connection.execute("CREATE TABLE answers (workflow_id TEXT, answer TEXT)")
            return
        if self.backend == "postgres":
            self.url = f"postgres://postgres:{MYSQL_PASSWORD}@127.0.0.1:{POSTGRES_PORT}/{self._name}"
            self.ledger = f"127.0.0.1:{POSTGRES_PORT}:postgres:{MYSQL_PASSWORD}:{self._name}"
            with self._postgres() as connection:
                connection.execute(f"CREATE DATABASE {self._name}")
            with self._postgres(self._name) as connection:
                connection.execute("CREATE TABLE ledger (workflow_id TEXT, step_id INT, pid INT, started_at_ms BIGINT)")
                connection.execute("CREATE INDEX ON ledger (workflow_id, step_id)")
                connection.execute("CREATE TABLE answers (workflow_id TEXT, answer TEXT)")
            return
        self.url = f"mysql://root:{MYSQL_PASSWORD}@127.0.0.1:{MYSQL_PORT}/{self._name}"
        self.ledger = f"127.0.0.1:{MYSQL_PORT}:root:{MYSQL_PASSWORD}:{self._name}"
        with self._admin() as connection, connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {self._name}")
            cursor.execute(f"USE {self._name}")
            cursor.execute("CREATE TABLE ledger (workflow_id VARCHAR(255), step_id INT, pid INT, started_at_ms BIGINT, KEY (workflow_id, step_id))")
            cursor.execute("CREATE TABLE answers (workflow_id VARCHAR(255), answer LONGTEXT)")

    def _postgres(self, database: str = "postgres") -> psycopg.Connection:
        return psycopg.connect(host="127.0.0.1", port=POSTGRES_PORT, user="postgres", password=MYSQL_PASSWORD, dbname=database, autocommit=True, connect_timeout=5)

    def _admin(self) -> pymysql.Connection:
        return pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password=MYSQL_PASSWORD, autocommit=True, connect_timeout=5)

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        if self.backend == "sqlite":
            path = self.ledger if "ledger" in sql or "answers" in sql else str(self._directory / "workflows.sqlite")
            code = "import json, sqlite3, sys; print(json.dumps(sqlite3.connect(sys.argv[1], timeout=30).execute(sys.argv[2]).fetchall()))"
            output = subprocess.run([sys.executable, "-c", code, path, sql], capture_output=True, text=True, check=True).stdout
            return [tuple(row) for row in json.loads(output)]
        if self.backend == "postgres":
            with self._postgres(self._name) as connection:
                return list(connection.execute(sql).fetchall())
        with self._admin() as connection, connection.cursor() as cursor:
            cursor.execute(f"USE {self._name}")
            cursor.execute(sql)
            return list(cursor.fetchall())

    def open_transactions(self) -> int:
        if self.backend == "sqlite":
            return 0
        if self.backend == "postgres":
            with self._postgres() as connection:
                row = connection.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = %s AND xact_start IS NOT NULL", (self._name,)).fetchone()
                return row[0]
        with self._admin() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.innodb_trx t "
                "JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id WHERE p.db = %s",
                (self._name,),
            )
            return cursor.fetchone()[0]

    def drop(self) -> None:
        if self.backend == "mysql":
            with self._admin() as connection, connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS {self._name}")
        elif self.backend == "postgres":
            with self._postgres() as connection:
                connection.execute(f"DROP DATABASE IF EXISTS {self._name} WITH (FORCE)")


def _restart_database(service: str) -> None:
    environment = {**os.environ, "DBOS_TEST_DB_PORT": str(MYSQL_PORT), "DBOS_TEST_PG_PORT": str(POSTGRES_PORT)}
    subprocess.run(["docker", "compose", "-p", COMPOSE_PROJECT, "-f", str(REPO / "compose.yml"), "restart", service], check=True, capture_output=True, env=environment, timeout=120)


class _Workers:
    def __init__(self, config: ChaosConfig, target: _Target, log_path: Path, rng: random.Random) -> None:
        self._config = config
        self._target = target
        self._log = log_path.open("ab")
        self._rng = rng
        self._crash_rate = 0.01 if "crash_window" in config.faults else 0.0
        self.processes: dict[int, subprocess.Popen[bytes]] = {}
        self.ever_paused: set[int] = set()
        self._terminated: set[int] = set()

    def start(self, index: int) -> None:
        environment = {
            **os.environ,
            "PYTHONPATH": str(REPO),
            "CHAOS_WORKER": str(index),
            "CHAOS_WORKERS": str(self._config.workers),
            "CHAOS_WORKFLOWS": str(self._config.workflows),
            "CHAOS_URL": self._target.url,
            "CHAOS_LEDGER": self._target.ledger,
            "CHAOS_CRASH_RATE": str(self._crash_rate),
            "CHAOS_LEASE": str(self._config.lease_seconds),
        }
        self.processes[index] = subprocess.Popen(
            [sys.executable, "-m", "tests.chaos.worker"], cwd=REPO, env=environment, stdout=subprocess.DEVNULL, stderr=self._log
        )

    def stop_all(self) -> None:
        for process in self.processes.values():
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)
                process.kill()
                process.wait(timeout=20)
        if not self._log.closed:
            self._log.close()


def _inject(fault: str, workers: _Workers, config: ChaosConfig, rng: random.Random) -> bool:
    if fault == "db_restart":
        if config.backend == "sqlite":
            return False
        _restart_database(config.backend)
        return True
    alive = [index for index, process in workers.processes.items() if process.poll() is None]
    if not alive:
        return False
    index = rng.choice(alive)
    process = workers.processes[index]
    if fault == "kill":
        process.send_signal(signal.SIGCONT)
        process.kill()
    elif fault == "term":
        workers._terminated.add(index)
        process.send_signal(signal.SIGTERM)
    elif fault == "pause":
        workers.ever_paused.add(process.pid)
        process.send_signal(signal.SIGSTOP)
        time.sleep(config.lease_seconds * 2.5)
        process.send_signal(signal.SIGCONT)
    return True


def _check(target: _Target, config: ChaosConfig, workers: _Workers, result: ChaosResult) -> None:
    rows = target.query("SELECT workflow_uuid, status, output, recovery_attempts, max_recovery_attempts FROM workflow_status")
    steps = target.query("SELECT workflow_uuid, function_id, output, completed_at_epoch_ms FROM operation_outputs")
    ledger = target.query("SELECT workflow_id, step_id, pid, started_at_ms FROM ledger")
    answers: dict[str, list[str]] = {}
    for workflow_id, answer in target.query("SELECT workflow_id, answer FROM answers"):
        answers.setdefault(workflow_id, []).append(answer)
    violations = result.violations

    if len(rows) != config.workflows:
        violations.append(f"expected {config.workflows} workflows, found {len(rows)}")
    saved: dict[tuple[str, int], tuple[str, int]] = {(row[0], row[1]): (row[2], row[3]) for row in steps}
    executions: dict[tuple[str, int], list[tuple[int, int]]] = {}
    for workflow_id, step_id, pid, started in ledger:
        executions.setdefault((workflow_id, step_id), []).append((pid, started))

    fault_free = not any(result.faults_fired.values()) and "crash_window" not in config.faults
    for workflow_id, status, output, attempts, max_attempts in rows:
        result.recovered_workflows += int(attempts > 1)
        if status == "PENDING":
            violations.append(f"{workflow_id} is stuck in PENDING")
        elif status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED":
            if attempts <= max_attempts:
                violations.append(f"{workflow_id} was dead-lettered at {attempts} of {max_attempts} attempts")
            continue
        elif status != "SUCCESS":
            violations.append(f"{workflow_id} ended as {status}")
            continue
        expected = [json.loads(saved[(workflow_id, step_id)][0]) for step_id in range(1, STEPS + 1) if (workflow_id, step_id) in saved]
        stored = json.loads(output)
        if status == "SUCCESS" and stored["steps"] != expected:
            violations.append(f"{workflow_id} returned {stored}, but its saved steps are {expected}")
        for answer in answers.get(workflow_id, []):
            if status == "SUCCESS" and json.loads(answer) != stored:
                violations.append(f"{workflow_id}: a caller received {answer}, but {stored} is stored")
        for step_id in range(1, STEPS + 1):
            row = saved.get((workflow_id, step_id))
            if row is None:
                violations.append(f"{workflow_id} has no saved step {step_id}")
                continue
            if not json.loads(row[0]).startswith(f"{workflow_id}:{step_id}:"):
                violations.append(f"{workflow_id} step {step_id} saved {row[0]}")
            runs = executions.get((workflow_id, step_id), [])
            if not runs:
                violations.append(f"{workflow_id} step {step_id} was saved but never ran")
            if len(runs) > 1:
                result.repeated_steps += 1
                if fault_free:
                    violations.append(f"{workflow_id} step {step_id} ran {len(runs)} times with no fault")
            # A paused process may wake holding a step it had already decided to run; that is the documented overlap.
            late = [pid for pid, started in runs if started > row[1] + 50 and pid not in workers.ever_paused]
            if late:
                violations.append(f"{workflow_id} step {step_id} started after its save, by {late}")

    open_transactions = target.open_transactions()
    if open_transactions:
        violations.append(f"{open_transactions} transactions left open")


def run_chaos(config: ChaosConfig) -> ChaosResult:
    rng = random.Random(config.seed)  # noqa: S311
    result = ChaosResult(config.seed, config.backend, dict.fromkeys(config.faults, 0))
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="dbos-chaos-") as directory:
        target = _Target(config, Path(directory))
        workers = _Workers(config, target, Path(directory) / "workers.log", rng)
        injectable = [fault for fault in config.faults if fault != "crash_window" and (fault != "db_restart" or config.backend != "sqlite")]
        passed = False
        try:
            for index in range(config.workers):
                workers.start(index)
            restarted_database = False
            fault_deadline = started + config.fault_seconds
            finish_deadline = started + config.finish_deadline_seconds
            while time.monotonic() < finish_deadline:
                for index, process in list(workers.processes.items()):
                    code = process.poll()
                    if code is None:
                        continue
                    if code == 0 and index not in workers._terminated:
                        del workers.processes[index]
                        continue
                    workers._terminated.discard(index)
                    workers.start(index)
                if not workers.processes:
                    break
                if time.monotonic() < fault_deadline and injectable:
                    fault = rng.choice(injectable)
                    if fault == "db_restart" and restarted_database:
                        fault = "kill"
                    if _inject(fault, workers, config, rng):
                        result.faults_fired[fault] = result.faults_fired.get(fault, 0) + 1
                        restarted_database = restarted_database or fault == "db_restart"
                    time.sleep(rng.uniform(0.2, 1.0))
                else:
                    time.sleep(0.1)
            if workers.processes:
                result.violations.append(f"workers did not finish within {config.finish_deadline_seconds} seconds")
            workers.stop_all()
            _check(target, config, workers, result)
            passed = not result.violations
        finally:
            result.seconds = time.monotonic() - started
            workers.stop_all()
            if not passed:
                log = (Path(directory) / "workers.log").read_text(errors="replace")
                result.violations.append("worker log tail: " + log[-1500:])
            target.drop()
    return result
